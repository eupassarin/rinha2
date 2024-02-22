use std::{env, sync::Arc};
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::sync::{Mutex, MutexGuard};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{body, extract::{Path, State}, http::StatusCode, response::IntoResponse, Router, routing::{get, post}};
use deadpool_postgres::{Pool, Runtime::Tokio1, tokio_postgres::NoTls};
use memmap::MmapMut;
use serde_json::{from_slice, to_string};
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{info, Level};

const TIME_SLEEP: Duration = Duration::from_nanos(1);

static LIMITES: &'static [i32] = &[1000_00, 800_00,10000_00,100000_00,5000_00];

const TAMANHO_SEQ: usize = 2;
pub trait Row {
    fn from_bytes(slice: &[u8]) -> Self;
    fn to_bytes(&self) -> Vec<u8>;
}

const TAMANHO_CLIENTE_ROW: usize = 7;
#[derive(Debug)]
pub struct ClienteRow { pub id: u16, pub saldo: i32, pub lock: bool }
impl ClienteRow { pub fn new( saldo: i32) -> Self {
        Self { id: 0,  saldo, lock: false }
    } }
impl Row for ClienteRow {
    fn from_bytes(slice: &[u8]) -> Self {
        Self {
            id: u16::from_ne_bytes(slice[0..2].try_into().unwrap()),
            saldo: i32::from_ne_bytes(slice[2..6].try_into().unwrap()),
            lock: slice[6] == 1,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(7);
        bytes.extend_from_slice(&self.id.to_ne_bytes());
        bytes.extend_from_slice(&self.saldo.to_ne_bytes());
        bytes.push(self.lock as u8);
        bytes
    }
}

const TAMANHO_TRANSACAO_ROW: usize = 28;
#[derive(Debug)]
pub struct TransacaoRow {
    pub id: u16,
    pub cliente_id: i16,
    pub valor: i32,
    pub tipo: char,
    pub descricao: String,
    pub realizada_em: u64,
    pub lock: bool
}
impl TransacaoRow {
    pub fn new(cliente_id: i16 , valor: i32, tipo: char, descricao: &str, realizada_em: u64) -> Self {
        Self { id: 0, cliente_id, valor, tipo, descricao: descricao.to_string(), realizada_em, lock: false }
    }
}
impl Row for TransacaoRow {
    fn from_bytes(slice: &[u8]) -> Self {
        Self {
            id: u16::from_ne_bytes(slice[0..2].try_into().unwrap()),
            cliente_id: i16::from_ne_bytes(slice[2..4].try_into().unwrap()),
            valor: i32::from_ne_bytes(slice[4..8].try_into().unwrap()),
            tipo: slice[8] as char,
            descricao: String::from_utf8_lossy(&slice[9..19]).to_string(),
            realizada_em: u64::from_ne_bytes(slice[19..27].try_into().unwrap()),
            lock: slice[27] == 1,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(28);
        bytes.extend_from_slice(&self.id.to_ne_bytes());
        bytes.extend_from_slice(&self.cliente_id.to_ne_bytes());
        bytes.extend_from_slice(&self.valor.to_ne_bytes());
        bytes.push(self.tipo as u8);
        bytes.extend_from_slice(self.descricao.as_bytes());
        bytes.extend_from_slice(&self.realizada_em.to_ne_bytes());
        bytes.push(self.lock as u8);
        bytes
    }
}

#[derive(Debug)]
pub struct RinhaDatabase {
    pub controle_mmap: Mutex<MmapMut>,
    pub cliente_mmap: Mutex<MmapMut>,
    pub transacao_mmap: Mutex<MmapMut>,
}
impl RinhaDatabase {
    pub fn new() -> Self {

        fn abrir_e_inicializar_arquivo(path: &str, size: u64) -> File {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
                .unwrap();
            file.set_len(size).unwrap();
            file
        }

        fn inicializar_mmap(file: &File) -> Mutex<MmapMut> {
            unsafe { Mutex::new(MmapMut::map_mut(&file).unwrap()) }
        }

        let controle_file = abrir_e_inicializar_arquivo("./temp/controle.db", 1);
        let cliente_file = abrir_e_inicializar_arquivo("./temp/cliente.db", 1024);
        let transacao_file = abrir_e_inicializar_arquivo("./temp/transacao.db", 1024 * 10 * 100);

        Self {
            controle_mmap: inicializar_mmap(&controle_file),
            cliente_mmap: inicializar_mmap(&cliente_file),
            transacao_mmap: inicializar_mmap(&transacao_file),
        }
    }

    pub fn iniciar_trans(&self)  {
        loop {
            let mut controle = self.controle_mmap.lock().unwrap();
            if controle[0] == 0 { controle[0] = 1;return; }
            sleep(TIME_SLEEP);
        }
    }

    pub fn finalizar_trans(&self) {
        let mut controle = self.controle_mmap.lock().unwrap();
        controle[0] = 0;
    }

    pub fn adicionar_cliente(&self, cliente_row: &mut ClienteRow) {
        let mut cliente_mmap = self.cliente_mmap.lock().unwrap();

        let (id, proximo_id) = Self::proximo_id(&mut cliente_mmap);
        cliente_row.id = proximo_id;

        let offset = (id as usize) * TAMANHO_CLIENTE_ROW + TAMANHO_SEQ;
        cliente_mmap[offset..offset + TAMANHO_CLIENTE_ROW].copy_from_slice(&cliente_row.to_bytes());
    }

    fn proximo_id(mmap: &mut MutexGuard<MmapMut>) -> (u16, u16) {
        let mut id = [0u8; 2];
        id.copy_from_slice(&mmap[0..2]);
        let id = u16::from_ne_bytes(id);
        let proximo_id = id + 1;
        mmap[0..2].copy_from_slice(&proximo_id.to_ne_bytes());
        (id, proximo_id)
    }

    pub fn adicionar_transacao(&self, mut transacao_row: TransacaoRow) {
        let mut transacao_mmap = self.transacao_mmap.lock().unwrap();

        let (id, proximo_id) = Self::proximo_id(&mut transacao_mmap);

        transacao_row.id = proximo_id;
        transacao_row.descricao = format!("{: <10}", transacao_row.descricao).chars().take(10).collect();

        let offset = (id as usize) * TAMANHO_TRANSACAO_ROW + TAMANHO_SEQ;
        transacao_mmap[offset..offset + TAMANHO_TRANSACAO_ROW].copy_from_slice(&transacao_row.to_bytes());
    }

    pub fn recuperar_saldo(&self, id: i16) -> i32 {
        let cliente_mmap = self.cliente_mmap.lock().unwrap();
        let offset = (id as usize - 1) * TAMANHO_CLIENTE_ROW + TAMANHO_SEQ;
        ClienteRow::from_bytes(&cliente_mmap[offset..offset + TAMANHO_CLIENTE_ROW]).saldo
    }

    pub fn recuperar_transacoes(&self, cliente_id: i16) -> Vec<TransacaoRow> {
        let transacao_mmap = self.transacao_mmap.lock().unwrap();
        let num_transacoes = u16::from_ne_bytes([transacao_mmap[0], transacao_mmap[1]]);
        let mut transacoes = Vec::new();
        for i in (1..=num_transacoes).rev() {
            let offset = (i - 1) as usize * TAMANHO_TRANSACAO_ROW + TAMANHO_SEQ;
            let transacao = TransacaoRow::from_bytes(&transacao_mmap[offset..offset + TAMANHO_TRANSACAO_ROW]);
            if transacao.cliente_id == cliente_id {
                transacoes.push(transacao);
                if transacoes.len() == 10  { break; }
            }
        }
        transacoes
    }

    pub fn transacionar(&self, id: i16, novo_saldo: i32) -> i32 {
        let mut cliente_mmap = self.cliente_mmap.lock().unwrap();
        let offset = (id as usize - 1) * TAMANHO_CLIENTE_ROW + TAMANHO_SEQ + 2;
        cliente_mmap[offset..offset + 4].copy_from_slice(&(novo_saldo).to_ne_bytes());
        novo_saldo
    }
}

#[derive(Debug)]
pub struct AppState { pub pg_pool: Pool, pub db: RinhaDatabase}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    dotenv::from_path(".env.local").ok().unwrap_or_else(|| {dotenv::dotenv().ok();});

    let collector = tracing_subscriber::fmt().with_max_level(Level::INFO).finish();
    tracing::subscriber::set_global_default(collector).unwrap();

    info!("Iniciando servidor...");

    let cfg = Config::from_env()?;
    let pg_pool = cfg.pg.create_pool(Some(Tokio1), NoTls)?;

    let rinha_db = RinhaDatabase::new();

    rinha_db.iniciar_trans();
    rinha_db.adicionar_cliente(&mut ClienteRow::new(0));
    rinha_db.adicionar_cliente(&mut ClienteRow::new(0));
    rinha_db.adicionar_cliente(&mut ClienteRow::new(0));
    rinha_db.adicionar_cliente(&mut ClienteRow::new(0));
    rinha_db.adicionar_cliente(&mut ClienteRow::new(0));
    rinha_db.finalizar_trans();

    let app = Router::new()
        .route("/clientes/:id/transacoes", post(post_transacoes))
        .route("/clientes/:id/extrato", get(get_extrato))
        .with_state(Arc::new(AppState { pg_pool , db: rinha_db }));

    let http_port = env::var("HTTP_PORT").unwrap_or_else(|_| "80".to_string());
    let listener = TcpListener::bind(format!("0.0.0.0:{http_port}")).await?;

    info!("Escutando na porta {}...", http_port);

    axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await?;
    Ok(())
}


pub async fn post_transacoes(State(s): State<Arc<AppState>>, Path(cliente_id): Path<i16>, transacao: body::Bytes) -> impl IntoResponse  {

    if cliente_id > 5 { return (StatusCode::NOT_FOUND, String::new()); }

    let t = match from_slice::<TransacaoPayload>(&transacao) {
        Ok(p) if p.descricao.len() >= 1 && p.descricao.len() <= 10 && (p.tipo == 'd' || p.tipo == 'c') => p,
        _ => return (StatusCode::UNPROCESSABLE_ENTITY, String::new())
    };

    let limite_cliente = LIMITES[(cliente_id - 1) as usize];

    s.db.iniciar_trans();
    let saldo_antigo = s.db.recuperar_saldo(cliente_id);
    if t.tipo == 'd' && saldo_antigo - t.valor < -limite_cliente {
        return (StatusCode::UNPROCESSABLE_ENTITY, String::new());
    }
    let novo_saldo = s.db.transacionar(cliente_id, saldo_antigo + if t.tipo == 'd' { -t.valor } else { t.valor });
    s.db.adicionar_transacao(TransacaoRow::new(cliente_id, t.valor, t.tipo, t.descricao.as_str(), now()));
    s.db.finalizar_trans();

    (StatusCode::OK, to_string(&SaldoLimite { saldo: novo_saldo, limite: limite_cliente }).unwrap())
}

pub async fn get_extrato(State(s): State<Arc<AppState>>, Path(cliente_id): Path<i16>) -> impl IntoResponse {
    if cliente_id > 5 { return (StatusCode::NOT_FOUND, String::new()) }
    (StatusCode::OK, to_string(
        &Extrato {
            saldo: Saldo{
                total: s.db.recuperar_saldo(cliente_id),
                data_extrato: now(),
                limite: LIMITES[(cliente_id -1) as usize]
            },
            ultimas_transacoes: s.db.recuperar_transacoes(cliente_id).iter()
                .map(|row| Transacao {
                    valor: row.valor,
                    tipo: row.tipo.to_string(),
                    descricao: str::trim(&row.descricao),
                    realizada_em: row.realizada_em
            }).collect()}
        ).unwrap()
    )
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
pub struct Extrato<'a> { pub saldo: Saldo, pub ultimas_transacoes: Vec<Transacao<'a>> }
#[derive(serde::Serialize)]
pub struct Saldo { pub total: i32, pub data_extrato: u64, pub limite: i32 }
#[derive(serde::Serialize)]
pub struct Transacao<'a> { pub valor: i32, pub tipo: String, pub descricao: &'a str, pub realizada_em: u64 }
pub fn now() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64 }

async fn shutdown_signal() {
    let ctrl_c = async {
        std::fs::remove_file("/temp/controle.db").ok();
        std::fs::remove_file("/temp/cliente.db").ok();
        std::fs::remove_file("/temp/transacao.db").ok();
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
        let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

