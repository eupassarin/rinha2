use std::{env, sync::Arc};
use axum::{extract::{Path, State}, http::StatusCode, routing::{get, post}, Router, body, response::IntoResponse};
use deadpool_postgres::{tokio_postgres::NoTls, GenericClient, Runtime::Tokio1};
use serde_json::to_string;
use tokio::{net::TcpListener, try_join};

pub struct AppState {
    pub pg_pool: deadpool_postgres::Pool,
    pub limites: Vec<i32>
}

#[tokio::main]
async fn main() {
    dotenv::from_path(".env.local").ok().unwrap_or_else(|| {dotenv::dotenv().ok();});

    let cfg = Config::from_env().unwrap();
    let pg_pool = cfg.pg.create_pool(Some(Tokio1), NoTls).unwrap();

    let conn = pg_pool.get().await.unwrap();

    let limites = conn.query(r#"SELECT limite FROM cliente ORDER BY ID ASC"#, &[])
        .await.unwrap().iter().map(|row| row.get(0)).collect();

    let app_state = Arc::new(AppState{ pg_pool, limites});

    let app = Router::new()
        .route("/clientes/:id/transacoes", post(post_transacoes))
        .route("/clientes/:id/extrato", get(get_extrato))
        .with_state(app_state);

    let http_port = env::var("HTTP_PORT").unwrap_or_else(|_| "80".to_string());
    let listener = TcpListener::bind(format!("0.0.0.0:{http_port}")).await.unwrap();
    axum::serve(listener, app).await.unwrap();

}

pub async fn post_transacoes(State(state): State<Arc<AppState>>, Path(id): Path<i16>, payload: body::Bytes)
    -> impl IntoResponse
{

    if id > 5 { return (StatusCode::NOT_FOUND, String::new()) }

    let t = match serde_json::from_slice::<TransacaoPayload>(&payload) {
        Ok(p) if (p.descricao.len() >= 1 && p.descricao.len() <= 10) => p,
        _ => return (StatusCode::UNPROCESSABLE_ENTITY, String::new())
    };

    let conn = state.pg_pool.get().await.unwrap();
    let limite = state.limites[(id-1) as usize];

    let res = match t.tipo {
        'd' => conn.query("SELECT D($1, $2, $3, $4)", &[&id, &t.valor, &t.descricao, &limite]).await.unwrap(),
        'c' => conn.query("SELECT C($1, $2, $3)", &[&id, &t.valor, &t.descricao]).await.unwrap(),
        _ => return (StatusCode::UNPROCESSABLE_ENTITY, String::new())
    };

    match res[0].get::<_, Option<i32>>(0) {
        Some(saldo) => (StatusCode::OK, to_string(&SaldoLimite { saldo, limite }).unwrap()),
        None => (StatusCode::UNPROCESSABLE_ENTITY, String::new())
    }
}

pub async fn get_extrato(State(state): State<Arc<AppState>>, Path(id): Path<i16>) -> impl IntoResponse  {

    if id > 5 { return (StatusCode::NOT_FOUND, String::new()) }

    let (cliente, transacoes) = try_join!(
        async {
            let conn = state.pg_pool.get().await.unwrap();
            conn.query(&conn.prepare_cached(r#"SELECT saldo FROM cliente WHERE id = $1"#).await.unwrap(), &[&id])
            .await
        },
        async {
            let conn = state.pg_pool.get().await.unwrap();
            conn.query(&conn.prepare_cached(
                    r#"SELECT valor, tipo, descricao, realizada_em
                    FROM transacao
                    WHERE cliente_id = $1
                    ORDER BY realizada_em DESC LIMIT 10"#).await.unwrap(), &[&id])
            .await
        }
    ).unwrap();

    (StatusCode::OK, serde_json::to_string(
        &Extrato {
                saldo: Saldo{
                    total: cliente[0].get(0),
                    data_extrato: now_monotonic(),
                    limite: state.limites[(id-1) as usize]
            },
            ultimas_transacoes: transacoes.iter().map(|row| Transacao {
                valor: row.get(0),
                tipo: row.get(1),
                descricao: row.get(2),
                realizada_em: row.get::<usize,i64>(3)
            }).collect() }
        ).unwrap()
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

#[derive(serde::Deserialize)]
pub struct TransacaoPayload {
    pub valor: i32,
    pub tipo: char,
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

pub fn now_monotonic() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut time) };
    time.tv_sec as u64
}
