use std::{env, sync::Arc, time::SystemTime};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Router, Json,
};
use axum_route_error::RouteError;
use axum_valid::Valid;
use deadpool_postgres::{tokio_postgres::NoTls, GenericClient, Runtime::Tokio1, tokio_postgres::Row};
use tokio::net::TcpListener;
use tokio::try_join;
use validator::Validate;

pub struct AppState {
    pub pg_pool: deadpool_postgres::Pool
}


#[tokio::main]
async fn main() -> Result<(), anyhow::Error>{
    dotenv::from_path(".env.local").ok().unwrap_or_else(|| {dotenv::dotenv().ok();});

    let cfg = Config::from_env()?;
    let app_state = Arc::new( AppState { pg_pool: cfg.pg.create_pool(Some(Tokio1), NoTls)? });

    let app = Router::new()
        .route("/clientes/:id/transacoes", post(post_transacoes))
        .route("/clientes/:id/extrato", get(get_extrato))
        .with_state(app_state);

    let http_port = env::var("HTTP_PORT").unwrap_or_else(|_| "80".to_string());
    let listener = TcpListener::bind(format!("0.0.0.0:{http_port}")).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

pub async fn post_transacoes(State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
    Valid(Json(transacao)): Valid<Json<TransacaoPayload>>
) -> Result<Json<SaldoLimite>, RouteError>   {

    if id > 5 { return Err(RouteError::new_not_found()) }

    let valor = match transacao.tipo {
        'c' => transacao.valor,
        'd' => -transacao.valor,
        _ => return Err(RouteError::new_from_status(StatusCode::UNPROCESSABLE_ENTITY))
    };

    let conn = state.pg_pool.get().await?;
    let result_transacionar = conn.query(
        r#"CALL T($1, $2, $3, $4);"#, &[&id, &valor, &transacao.tipo.to_string(), &transacao.descricao])
        .await?;

    match result_transacionar[0].get::<_, Option<i32>>(0) {
        Some(_) => Ok(Json(SaldoLimite::from(&result_transacionar[0]))),
        None => Err(RouteError::new_from_status(StatusCode::UNPROCESSABLE_ENTITY))
    }

}

pub async fn get_extrato(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>
) -> Result<Json<Extrato>, RouteError> {

    if id > 5 || id < 0 { return Err(RouteError::new_not_found()); }

    let conn = state.pg_pool.get().await?;

    let (cliente, transacoes) = try_join!(
        async {
            conn.query(&conn.prepare_cached(
                r#"SELECT C.saldo, C.limite FROM cliente C WHERE C.id = $1;"#).await?, &[&id])
            .await
        },
        async {
            conn.query(&conn.prepare_cached(
                    r#"SELECT T.valor, T.tipo, T.descricao, T.realizada_em
                    FROM transacao T WHERE T.cliente_id = $1
                    ORDER BY T.realizada_em DESC LIMIT 10;"#).await?, &[&id])
            .await
        }
    )?;

    Ok(Json(Extrato {
        saldo: Saldo::from(&cliente[0]),
        ultimas_transacoes: transacoes.iter().map(|row| Transacao::from(row)).collect() })
    )
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

#[derive(serde::Serialize, serde::Deserialize, Validate)]
pub struct TransacaoPayload {
    pub valor: i32,
    pub tipo: char,
    #[validate(length(min = 1, max = 10))]
    pub descricao: String
}

#[derive(serde::Serialize, serde::Deserialize)]
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

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Extrato {
    pub saldo: Saldo,
    pub ultimas_transacoes: Vec<Transacao>
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Saldo {
    pub total: i32,
    pub data_extrato: u64,
    pub limite: i32
}

impl From<&Row> for Saldo {
    fn from(row: &Row) -> Self {
        Self {
            total: row.get(0),
            data_extrato: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            limite: row.get(1)
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
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
