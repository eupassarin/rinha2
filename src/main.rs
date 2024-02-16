use std::{env, sync::Arc};
use std::convert::Infallible;
use std::io::Error;
use hyper::body::Incoming;
use std::path::PathBuf;
use axum::{extract::{Path, State}, http::StatusCode, routing::{get, post}, Router, body, response::IntoResponse};
use axum::extract::{connect_info};
use axum::http::Request;
use deadpool_postgres::{tokio_postgres::NoTls, GenericClient, Runtime::Tokio1, Pool};
use serde_json::{from_slice, to_string};
use tokio::{task, try_join};
use tokio::net::{unix::UCred, UnixListener, UnixStream};
use tower_service::Service;


use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server,
};

#[tokio::main]
async fn main() {
    dotenv::from_path(".env.local").ok().unwrap_or_else(|| {dotenv::dotenv().ok();});

    let cfg = Config::from_env().unwrap();
    let app_state = Arc::new(cfg.pg.create_pool(Some(Tokio1), NoTls).unwrap());

    let http_port = env::var("HTTP_PORT").unwrap_or_else(|_| "80".to_string());
    let path = PathBuf::from(format!("/temp/{http_port}"));
    let _ = tokio::fs::remove_file(&path).await;

    let uds = UnixListener::bind(path.clone()).expect("erro bind");

    let app = Router::new()
        .route("/clientes/:id/transacoes", post(post_transacoes))
        .route("/clientes/:id/extrato", get(get_extrato))
        .with_state(app_state);
    let mut make_service = app.into_make_service_with_connect_info::<UdsConnectInfo>();

    loop {
        let (socket, _remote_addr) = uds.accept().await.expect("erro accept socket");
        let tower_service = unwrap_infallible(make_service.call(&socket).await);
        tokio::spawn(async move {
            let socket = TokioIo::new(socket);
            let hyper_service =
                hyper::service::service_fn(move |request: Request<Incoming>| {
                    tower_service.clone().call(request)
                });
        });
    }
}

pub async fn post_transacoes(State(pg_pool): State<Arc<Pool>>, Path(id): Path<i16>, transacao: body::Bytes)
    -> impl IntoResponse
{
    if id > 5 { return (StatusCode::NOT_FOUND, String::new()) }

    let t = match from_slice::<TransacaoPayload>(&transacao) {
        Ok(p) if p.descricao.len() >= 1 && p.descricao.len() <= 10 && (p.tipo == 'd' || p.tipo == 'c') => p,
        _ => return (StatusCode::UNPROCESSABLE_ENTITY, String::new())
    };

    let conn = pg_pool.get().await.unwrap();
    match conn.query_one(UPDATE_SALDO, &[&(if t.tipo == 'd' { -t.valor } else { t.valor }), &id]).await {
        Ok(result) => {
            task::spawn(async move {
                conn.query(INSERT_TRANSACAO, &[&id, &t.valor, &t.tipo.to_string(), &t.descricao]).await.unwrap();
            });
            (
                StatusCode::OK,
                to_string(&SaldoLimite {
                    saldo: Ok::<i32, Error>(result.get(0)).unwrap(),
                    limite: LIMITES[(id - 1) as usize]
                }).unwrap()
            )
        },
        Err(_) => (StatusCode::UNPROCESSABLE_ENTITY, String::new())
    }
}

pub async fn get_extrato(State(pg_pool): State<Arc<Pool>>, Path(id): Path<i16>) -> impl IntoResponse  {

    if id > 5 { return (StatusCode::NOT_FOUND, String::new()) }

    let conn = pg_pool.get().await.unwrap();
    let (cliente, transacoes) = try_join!(
        async {conn.query_one(&conn.prepare_cached(SELECT_SALDO).await.unwrap(), &[&id]).await},
        async {conn.query(&conn.prepare_cached(SELECT_TRANSACAO).await.unwrap(), &[&id]).await}
    ).unwrap();

    (StatusCode::OK, to_string(
        &Extrato {
                saldo: Saldo{
                    total: cliente.get(0),
                    data_extrato: now_monotonic(),
                    limite: LIMITES[(id-1) as usize]
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
const INSERT_TRANSACAO: &str = "INSERT INTO T(I, V, P, D) VALUES($1, $2, $3, $4)";
const SELECT_TRANSACAO: &str = "SELECT V, P, D, R FROM T WHERE I = $1 ORDER BY R DESC LIMIT 10";
const SELECT_SALDO: &str = "SELECT S FROM C WHERE I = $1";
static LIMITES: &'static [i32] = &[1000_00, 800_00,10000_00,100000_00,5000_00];

#[derive(serde::Deserialize)]
pub struct Config { pub pg: deadpool_postgres::Config }

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

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct UdsConnectInfo {
    peer_addr: Arc<tokio::net::unix::SocketAddr>,
    peer_cred: UCred,
}

impl connect_info::Connected<&UnixStream> for UdsConnectInfo {
    fn connect_info(target: &UnixStream) -> Self {
        let peer_addr = target.peer_addr().unwrap();
        let peer_cred = target.peer_cred().unwrap();

        Self {
            peer_addr: Arc::new(peer_addr),
            peer_cred,
        }
    }
}

fn unwrap_infallible<T>(result: Result<T, Infallible>) -> T {
    match result {
        Ok(value) => value,
        Err(err) => match err {},
    }
}
