#![allow(unused_variables)]

use std::env;
use std::sync::Arc;
use std::time::SystemTime;
use axum::{routing::get, Router, Json};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum_route_error::RouteError;
use axum_valid::Valid;
use deadpool_postgres::{GenericClient};
use deadpool_postgres::Runtime::Tokio1;
use deadpool_postgres::tokio_postgres::{NoTls, Row};
use tokio::net::TcpListener;
use validator::Validate;

pub struct AppState {
    pub pg_pool: deadpool_postgres::Pool
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SaldoLimite {
    pub limite: i32,
    pub saldo: i32
}

impl From<&Row> for SaldoLimite {
    fn from(row: &Row) -> Self {
        Self {
            limite: row.get(1),
            saldo: row.get(0)
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Extrato {
    pub saldo: Saldo,
    pub ultimas_transacoes: Vec<Transacao>
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Saldo {
    pub total: i32,
    pub data_extrato: String,
    pub limite: i32
}

impl From<&Row> for Saldo {
    fn from(row: &Row) -> Self {
        Self {
            total: row.get(0),
            data_extrato: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs().to_string(),
            limite: row.get(1)
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Validate)]
pub struct TransacaoPayload {
    pub valor: i32,
    pub tipo: String,
    #[validate(length(min = 1, max = 10))]
    pub descricao: String
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Transacao {
    pub valor: i32,
    pub tipo: String,
    pub descricao: String,
    pub realizada_em: i64
}

impl From<&Row> for Transacao {
    fn from(row: &Row) -> Self {
        Self {
            valor: row.get(0),
            tipo: row.get(1),
            descricao: row.get(2),
            realizada_em: row.get::<usize,SystemTime>(3)
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap().as_secs() as i64
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error>{

    dotenv::from_path(".env.local").ok().unwrap_or_else(|| {dotenv::dotenv().ok();});
    let cfg = Config::from_env()?;

    let pg_pool = cfg.pg.create_pool(Some(Tokio1), NoTls)?;
    let app_state = Arc::new( AppState {
        pg_pool
    });

    let app = Router::new()
        .route("/clientes/:id/transacoes", post(post_transacoes))
        .route("/clientes/:id/extrato", get(get_extrato))
        .with_state(app_state);

    let http_port = env::var("HTTP_PORT").unwrap_or_else(|_| "80".to_string());
    let listener = TcpListener::bind(format!("0.0.0.0:{http_port}")).await?;

    axum::serve(listener, app).await?;

    Ok(())
}

pub async fn post_transacoes(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
    Valid(Json(transacao)): Valid<Json<TransacaoPayload>>
) -> Result<Json<SaldoLimite>, RouteError>   {

    if id > 5 { return Err(RouteError::new_not_found()); }

    let valor = match transacao.tipo.as_str() {
        "c" => transacao.valor,
        "d" => -transacao.valor,
        _ => return Err(RouteError::new_from_status(StatusCode::UNPROCESSABLE_ENTITY))
    };

    let trans = state.pg_pool.get().await.expect("Failed to get connection from pool");
    //let trans = conn.transaction().await?;

    let sql_prep_update = trans.prepare_cached(
        r#"UPDATE cliente
                  SET saldo = saldo + $1
                  WHERE id = $2 AND saldo + $3 >= - limite
                  RETURNING saldo, limite"#
    ).await.expect("Failed to prepare sql_prep_update");

    let row = trans.query(&sql_prep_update,
        &[&valor, &id, &valor])
        .await.expect("Failed to execute sql_prep_update");

    if row.is_empty() { return Err(RouteError::new_from_status(StatusCode::UNPROCESSABLE_ENTITY)); }

    //trans.commit().await.expect("Failed to commit transaction");

    let sql_prep_trans = trans.prepare_cached(
        r#"INSERT INTO transacao (cliente_id, valor, tipo, descricao) VALUES ($1, $2, $3, $4);"#)
        .await.expect("Failed to prepare sql_prep_trans");

    trans.query(&sql_prep_trans,
        &[&id, &transacao.valor, &transacao.tipo, &transacao.descricao])
        .await.expect("Failed to execute sql_prep_trans");

    Ok(Json(SaldoLimite::from(&row[0])))
}

pub async fn get_extrato(State(state): State<Arc<AppState>>, Path(id): Path<i32>) -> Result<Json<Extrato>, RouteError> {

    if id > 5 { return Err(RouteError::new_not_found()); }

    let conn = state.pg_pool.get().await.expect("Failed to get connection from pool");

    let sql_prep_cliente = conn
        .prepare_cached(r#"SELECT C.saldo, C.limite FROM cliente C WHERE C.id = $1;"#)
        .await.expect("Failed to prepare statement");

    let row = conn
        .query(&sql_prep_cliente, &[&id])
        .await.expect("Failed to execute sql_prep_cliente");

    if row.is_empty() { return Err(RouteError::new_not_found()); }

    let saldo = Saldo::from(&row[0]);

    let sql_pre_trans = conn.prepare_cached(
        r#"SELECT T.valor, T.tipo, T.descricao, T.realizada_em
        FROM transacao T WHERE T.cliente_id = $1
        ORDER BY T.realizada_em DESC LIMIT 10;"#
    ).await.expect("Failed to prepare statement");

    let rows = conn
        .query(&sql_pre_trans, &[&id])
        .await.expect("Failed to execute sql_pre_trans");

    let ultimas_transacoes = rows
        .iter()
        .map(|row| Transacao::from(row))
        .collect();

    Ok(Json(Extrato { saldo, ultimas_transacoes }))
}

#[derive(Debug, serde::Deserialize)]
pub struct Config {
    pub pg: deadpool_postgres::Config
}

impl Config {
    pub fn from_env() -> Result<Self, config::ConfigError> {
        config::Config::builder()
            .add_source(config::Environment::default().separator("__"))
            .build()?
            .try_deserialize()
    }
}
