use std::{env, sync::Arc};
use std::io::Error;
use axum::{extract::{Path, State}, http::StatusCode, routing::{get, post}, Router, body, response::IntoResponse};
use deadpool_postgres::{tokio_postgres::NoTls, GenericClient, Runtime::Tokio1, Pool};
use serde_json::{from_slice, to_string};
use tokio::{net::TcpListener, task, try_join};

pub struct AppState {
    pub pg_pool: Pool,
    pub limites: Vec<i32>,
}

#[tokio::main]
async fn main() {
    dotenv::from_path(".env.local").ok().unwrap_or_else(|| {dotenv::dotenv().ok();});

    let cfg = Config::from_env().unwrap();
    let pg_pool = cfg.pg.create_pool(Some(Tokio1), NoTls).unwrap();

    let conn = pg_pool.get().await.unwrap();
    let limites = conn.query(SELECT_LIMITE, &[])
        .await.unwrap().iter().map(|row| row.get(0)).collect();

    warmup(&pg_pool).await;

    let app_state = Arc::new(AppState{ pg_pool, limites });

    let app = Router::new()
        .route("/clientes/:id/transacoes", post(post_transacoes))
        .route("/clientes/:id/extrato", get(get_extrato))
        .with_state(app_state);

    let http_port = env::var("HTTP_PORT").unwrap_or_else(|_| "80".to_string());
    let listener = TcpListener::bind(format!("0.0.0.0:{http_port}")).await.unwrap();
    axum::serve(listener, app).await.unwrap();

}

async fn warmup(pg_pool: &Pool) {
    for _ in 0..500 {
        let conn = pg_pool.get().await.unwrap();
        task::spawn(async move {
            let _ = conn.query(SELECT_LIMITE, &[]).await.unwrap();
        });
    }
    println!("{:?}/{:?}", pg_pool.status().size, pg_pool.status().available);
}

pub async fn post_transacoes(State(state): State<Arc<AppState>>, Path(id): Path<i16>, transacao: body::Bytes)
                             -> impl IntoResponse
{
    if id > 5 { return (StatusCode::NOT_FOUND, String::new()) }

    let t = match from_slice::<TransacaoPayload>(&transacao) {
        Ok(p) if p.descricao.len() >= 1 && p.descricao.len() <= 10 && (p.tipo == 'd' || p.tipo == 'c') => p,
        _ => return (StatusCode::UNPROCESSABLE_ENTITY, String::new())
    };

    let conn = state.pg_pool.get().await.unwrap();
    match conn.query_one(UPDATE_SALDO, &[&(if t.tipo == 'd' { -t.valor } else { t.valor }), &id]).await {
        Ok(result) => {
            task::spawn(async move {
                conn.query(INSERT_TRANSACTION,
                           &[&id,
                               &t.valor,
                               &t.tipo.to_string(),
                               &t.descricao])
                    .await.unwrap();
            });
            (
                StatusCode::OK,
                to_string(&SaldoLimite {
                    saldo: Ok::<i32, Error>(result.get(0)).unwrap(),
                    limite: state.limites[(id - 1) as usize]
                }).unwrap()
            )
        },
        Err(_) => (StatusCode::UNPROCESSABLE_ENTITY, String::new())
    }
}

pub async fn get_extrato(State(state): State<Arc<AppState>>, Path(id): Path<i16>) -> impl IntoResponse  {

    if id > 5 { return (StatusCode::NOT_FOUND, String::new()) }

    let conn = state.pg_pool.get().await.unwrap();

    let (cliente, transacoes) = try_join!(
        async {
            conn.query_one(&conn.prepare_cached(SELECT_SALDO).await.unwrap(), &[&id]).await
        },
        async {
            conn.query(&conn.prepare_cached(SELECT_TRANSACAO).await.unwrap(), &[&id]).await
        }
    ).unwrap();

    (StatusCode::OK, to_string(
        &Extrato {
                saldo: Saldo{
                    total: cliente.get(0),
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

const UPDATE_SALDO: &str = "UPDATE C SET S = S + $1 WHERE I = $2 RETURNING S";
const INSERT_TRANSACTION: &str = "INSERT INTO T(I, V, P, D) VALUES($1, $2, $3, $4)";
const SELECT_TRANSACAO: &str = "SELECT V, P, D, R FROM T WHERE I = $1 ORDER BY R DESC LIMIT 10";
const SELECT_SALDO: &str = "SELECT S FROM C WHERE I = $1";
const SELECT_LIMITE: &str = "SELECT L FROM C ORDER BY I ASC";

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
pub struct TransacaoPayload { pub valor: i32, pub tipo: char, pub descricao: String }
#[derive(serde::Serialize)]
pub struct SaldoLimite { pub limite: i32, pub saldo: i32 }
#[derive(serde::Serialize)]
pub struct Extrato { pub saldo: Saldo, pub ultimas_transacoes: Vec<Transacao> }
#[derive(serde::Serialize)]
pub struct Saldo { pub total: i32, pub data_extrato: u64, pub limite: i32 }
#[derive(serde::Serialize)]
pub struct Transacao { pub valor: i32, pub tipo: String, pub descricao: String, pub realizada_em: i64 }
pub fn now_monotonic() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut time) };
    time.tv_sec as u64
}
