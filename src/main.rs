use std::{env, sync::Arc};
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
    pub pg_pool: deadpool_postgres::Pool,
    pub limites: Vec<i32>
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error>{
    dotenv::from_path(".env.local").ok().unwrap_or_else(|| {dotenv::dotenv().ok();});

    let cfg = Config::from_env()?;
    let pg_pool = cfg.pg.create_pool(Some(Tokio1), NoTls)?;

    let conn = pg_pool.get().await?;

    let limites = conn.query(r#"SELECT limite FROM cliente ORDER BY ID ASC"#, &[])
        .await?.iter().map(|row| row.get(0)).collect();

    let app_state = Arc::new(
        AppState{
            pg_pool,
            limites
        }
    );

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
    Path(id): Path<i16>,
    Valid(Json(transacao)): Valid<Json<TransacaoPayload>>
) -> Result<Json<SaldoLimite>, RouteError>   {

    if id > 5 { return Err(RouteError::new_not_found()); }

    let valor = match transacao.tipo {
        'd' => -transacao.valor,
        'c' => transacao.valor,
        _ => return Err(RouteError::new_from_status(StatusCode::UNPROCESSABLE_ENTITY))
    };

    let limite = state.limites[(id-1) as usize];
    let conn = state.pg_pool.get().await?;
    let result_transacionar = conn.query(
        r#"CALL T($1, $2, $3, $4, $5, $6);"#,
        &[&id, &valor, &transacao.tipo.to_string(), &transacao.descricao, &transacao.valor, &limite])
        .await?;

    match result_transacionar[0].get::<_, Option<i32>>(0) {
        Some(_) => Ok(Json(
            SaldoLimite {
                saldo: result_transacionar[0].get(0),
                limite
            }
        )),
        None => Err(RouteError::new_from_status(StatusCode::UNPROCESSABLE_ENTITY))
    }

}

pub async fn get_extrato(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i16>
) -> Result<Json<Extrato>, RouteError> {

    if id > 5 { return Err(RouteError::new_not_found()); }

    let (cliente, transacoes) = try_join!(
        async {
            let conn = state.pg_pool.get().await.unwrap();
            conn.query(r#"SELECT saldo FROM cliente WHERE id = $1;"#, &[&id])
            .await
        },
        async {
            let conn = state.pg_pool.get().await.unwrap();
            conn.query(
                    r#"SELECT valor, tipo, descricao, realizada_em
                    FROM transacao
                    WHERE cliente_id = $1
                    ORDER BY realizada_em DESC LIMIT 10;"#, &[&id])
            .await
        }
    )?;

    Ok(Json(Extrato {
        saldo: Saldo{
            total: cliente[0].get(0),
            data_extrato: now_monotonic(),
            limite: state.limites[(id-1) as usize]
        },
        ultimas_transacoes: transacoes.iter().map(|row| Transacao::from(row)).collect() })
    )
}

#[derive(serde::Deserialize)]
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

#[derive(serde::Serialize)]
pub struct SaldoLimite {
    pub limite: i32,
    pub saldo: i32
}

#[derive(serde::Serialize)]
pub struct Extrato {
    pub saldo: Saldo,
    pub ultimas_transacoes: Vec<Transacao>
}

#[derive(serde::Serialize)]
pub struct Saldo {
    pub total: i32,
    pub data_extrato: u64,
    pub limite: i32
}

#[derive(serde::Serialize)]
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
            realizada_em: row.get::<usize,i64>(3)
        }
    }
}

pub fn now_monotonic() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut time) };
    time.tv_sec as u64
}
