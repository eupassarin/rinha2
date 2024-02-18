use std::{env, sync::Arc};
use std::fs::{OpenOptions, write};
use std::time::Duration;
use axum::{body, extract::{Path, State}, http::StatusCode, response::IntoResponse, Router};
use axum::routing::{get, post};
use deadpool_postgres::{GenericClient, Pool, Runtime::Tokio1, tokio_postgres::NoTls};
use memmap::{MmapMut, MmapOptions};
use serde_json::{from_slice, to_string};
use spin::mutex::Mutex;
use spin::MutexGuard;
use tokio::net::TcpListener;
use tokio::task;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    dotenv::from_path(".env.local").ok().unwrap_or_else(|| {dotenv::dotenv().ok();});
    let mmap_path = env::var("MMAP_PATH")?;

    let cfg = Config::from_env()?;
    let pg_pool = cfg.pg.create_pool(Some(Tokio1), NoTls)?;

    write(mmap_path.clone(),  bincode::serialize(&[0;6])?)?;
    let mmap = unsafe {
        MmapOptions::new().map_mut(&OpenOptions::new().read(true).write(true).create(true).open(mmap_path)?)?
    };
    let spinlock = Mutex::new(mmap);

    let app_state = Arc::new(AppState { pg_pool, spinlock });
    let app = Router::new()
        .route("/clientes/:id/transacoes", post(post_transacoes))
        .route("/clientes/:id/extrato", get(get_extrato))
        .with_state(app_state);

    let http_port = env::var("HTTP_PORT").unwrap_or_else(|_| "80".to_string());
    let listener = TcpListener::bind(format!("0.0.0.0:{http_port}")).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

pub async fn post_transacoes(State(app_state): State<Arc<AppState>>, Path(id): Path<i16>, transacao: body::Bytes)
    -> impl IntoResponse
{
    if id > 5 { return (StatusCode::NOT_FOUND, String::new()); }

    let t = match from_slice::<TransacaoPayload>(&transacao) {
        Ok(p) if p.descricao.len() >= 1 && p.descricao.len() <= 10 && (p.tipo == 'd' || p.tipo == 'c') => p,
        _ => return (StatusCode::UNPROCESSABLE_ENTITY, String::new())
    };

    let id_ref = (id - 1) as usize;
    let valor = if t.tipo == 'd' { -t.valor } else { t.valor };

    let saldo = app_state.recuperar_saldo(id_ref).await;
    if saldo + valor < -LIMITES[id_ref] { return (StatusCode::UNPROCESSABLE_ENTITY, String::new()) }
    app_state.atualizar_saldo(id_ref, valor).await;

    let conn = app_state.pg_pool.get().await.unwrap();
    conn.query(INSERT_TRANSACAO, &[&id, &t.valor, &t.tipo.to_string(), &t.descricao]).await.unwrap();

    task::spawn(
        async move {
            conn.query_one(UPDATE_SALDO_SP, &[&(if t.tipo == 'd' { -t.valor } else { t.valor }), &(id as i32)]).await
        }
    );
    (StatusCode::OK, to_string(&SaldoLimite { saldo: saldo + valor, limite: LIMITES[(id - 1) as usize] }).unwrap())
}
pub async fn get_extrato(State(app_state): State<Arc<AppState>>, Path(id): Path<i16>) -> impl IntoResponse  {

    if id > 5 { return (StatusCode::NOT_FOUND, String::new()) }

    let id_ref = (id - 1) as usize;
    let saldo = app_state.recuperar_saldo(id_ref).await;

    let conn = app_state.pg_pool.get().await.unwrap();
    let transacoes = conn.query(&conn.prepare_cached(SELECT_TRANSACAO).await.unwrap(), &[&id]).await.unwrap();

    (StatusCode::OK, to_string(
        &Extrato {
            saldo: Saldo{
                total: saldo,
                data_extrato: now_monotonic(),
                limite: LIMITES[id_ref]
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

const UPDATE_SALDO_SP: &str = "CALL U($1, $2)";
const INSERT_TRANSACAO: &str = "INSERT INTO T(I, V, P, D) VALUES($1, $2, $3, $4)";
const SELECT_TRANSACAO: &str = "SELECT V, P, D, R FROM T WHERE I = $1 ORDER BY R DESC LIMIT 10";
static LIMITES: &'static [i32] = &[1000_00, 800_00, 10000_00, 100000_00, 5000_00];

pub struct AppState { pub pg_pool: Pool, pub spinlock: Mutex<MmapMut> }

const  SLP: usize = 4 * 4;
const ELP: usize = 5 * 4;

const TIME_SLEEP: Duration = Duration::from_nanos(100);

impl AppState {
    pub async fn recuperar_saldo(&self, id_ref: usize) -> i32 {
        bincode::deserialize::<[i32; 6]>(&self.spinlock.lock()).unwrap()[id_ref]
    }

    fn lock(mmap: &mut MutexGuard<MmapMut>) {
        mmap[SLP..ELP].copy_from_slice(&bincode::serialize(&[1]).unwrap());
    }
    fn unlock(&self) {
        self.spinlock.lock()[SLP..ELP].copy_from_slice(&bincode::serialize(&[0]).unwrap());
    }

    pub async fn atualizar_saldo(&self, id_ref: usize, valor: i32) {
        loop {
            let mut mmap = self.spinlock.lock();
            let mmap_decoded = bincode::deserialize::<[i32; 6]>(&mmap).unwrap();
            if mmap_decoded[5] == 0 {
                Self::lock(&mut mmap);
                drop(mmap);
                self.spinlock.lock()[id_ref * 4..(id_ref+1) * 4].copy_from_slice(&bincode::serialize(&(mmap_decoded[id_ref] + valor)).unwrap());
                self.unlock();
                return;
            }
            drop(mmap);
            sleep(TIME_SLEEP).await;
        }
    }
}

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
