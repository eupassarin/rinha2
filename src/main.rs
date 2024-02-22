use std::{env, sync::Arc};
use std::error::Error;
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::sync::{Mutex, MutexGuard};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{body, extract::{Path, State}, http::StatusCode, response::IntoResponse, Router, routing::{get, post}};
use deadpool_postgres::{Pool, Runtime::Tokio1, tokio_postgres::NoTls};
use memmap::MmapMut;
use serde_json::{from_slice, to_string};
use tokio::net::TcpListener;
use tracing::{debug, info, Level};
use rand::random;

const TIME_SLEEP: Duration = Duration::from_nanos(1);

static LIMITES: &'static [i32] = &[1000_00, 800_00,10000_00,100000_00,5000_00];

const TAMANHO_SEQ: usize = 2;
pub trait Row { fn from_bytes(slice: &[u8]) -> Self; fn to_bytes(&self) -> Vec<u8>; }

const TAMANHO_CLIENTE_ROW: usize = 7;
#[derive(Debug)]
pub struct ClienteRow { pub id: u16, pub saldo: i32, pub lock: bool }
impl ClienteRow { pub fn new( saldo: i32) -> Self { Self { id: 0,  saldo, lock: false } } }
impl Row for ClienteRow {
    fn from_bytes(slice: &[u8]) -> Self {
        let mut id = [0u8; 2];
        id.copy_from_slice(&slice[0..2]);
        let mut saldo = [0u8; 4];
        saldo.copy_from_slice(&slice[2..6]);
        let mut lock = [0u8; 1];
        lock.copy_from_slice(&slice[6..7]);
        ClienteRow { id: u16::from_ne_bytes(id), saldo: i32::from_ne_bytes(saldo), lock: lock[0] == 1}
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(7);
        bytes.extend_from_slice(&self.id.to_ne_bytes());
        bytes.extend_from_slice(&self.saldo.to_ne_bytes());
        bytes.extend_from_slice(&[self.lock as u8]);
        bytes
    }
}
impl Display for ClienteRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClienteRow {{ id: {}, saldo: {}, lock: {} }}", self.id, self.saldo, self.lock)
    }
}

const TAMANHO_TRANSACAO_ROW: usize = 28;
#[derive(Debug)]
pub struct TransacaoRow { pub id: u16, pub cliente_id: i16, pub valor: i32, pub tipo: char, pub descricao: String, pub realizada_em: u64, pub lock: bool }
impl TransacaoRow {
    pub fn new(cliente_id: i16 , valor: i32, tipo: char, descricao: &str, realizada_em: u64) -> Self {
        Self { id: 0, cliente_id, valor, tipo, descricao: descricao.to_string(), realizada_em, lock: false }
    }
}
impl Row for TransacaoRow {
    fn from_bytes(slice: &[u8]) -> Self {
        let mut id = [0u8; 2];
        id.copy_from_slice(&slice[0..2]);
        let mut cliente_id = [0u8; 2];
        cliente_id.copy_from_slice(&slice[2..4]);
        let mut valor = [0u8; 4];
        valor.copy_from_slice(&slice[4..8]);
        let tipo = slice[8] as char;
        let descricao = String::from_utf8_lossy(&slice[9..19]).to_string();
        let mut realizada_em = [0u8; 8];
        realizada_em.copy_from_slice(&slice[19..27]);
        let mut lock = [0u8; 1];
        lock.copy_from_slice(&slice[27..28]);
        TransacaoRow {
            id: u16::from_ne_bytes(id),
            cliente_id: i16::from_ne_bytes(cliente_id),
            valor: i32::from_ne_bytes(valor),
            tipo,
            descricao,
            realizada_em: u64::from_ne_bytes(realizada_em),
            lock: lock[0] == 1
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(28);
        bytes.extend_from_slice(&self.id.to_ne_bytes());
        bytes.extend_from_slice(&self.cliente_id.to_ne_bytes());
        bytes.extend_from_slice(&self.valor.to_ne_bytes());
        bytes.extend_from_slice(&[self.tipo as u8]);
        bytes.extend_from_slice(self.descricao.as_bytes());
        bytes.extend_from_slice(&self.realizada_em.to_ne_bytes());
        bytes.extend_from_slice(&[self.lock as u8]);
        bytes
    }
}
impl Display for TransacaoRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TransacaoRow {{ id: {}, cliente_id: {}, valor: {}, tipo: {}, descricao: {}, realizada_em: {}, lock: {} }}",
               self.id, self.cliente_id, self.valor, self.tipo, self.descricao, self.realizada_em, self.lock)
    }
}

#[derive(Debug)]
pub struct RinhaDatabase { pub controle: Mutex<MmapMut>, pub cliente: Mutex<MmapMut>, pub transacao: Mutex<MmapMut>, }
impl RinhaDatabase {
    pub fn new(controle_file: &File, cliente_file: &File, transacao_file: &File) -> Self {
        Self {
            controle: inicializar_mmap(&controle_file),
            cliente: inicializar_mmap(&cliente_file),
            transacao: inicializar_mmap(&transacao_file),
        }
    }

    pub fn imprimir_clientes(&self) {
        for i in 1..=5 {
            println!("{:?}", self.recuperar_cliente(i));
        }
    }

    pub fn imprimir_transacoes(&self, cliente_id: i16) {
        let transacoes = self.recuperar_transacoes(cliente_id);
        for transacao in transacoes {
            println!("{:?}", transacao);
        }
    }

    pub fn iniciar_trans(&self)  {
        let mut t = 10;
        loop {
            let mut controle = self.controle.lock().unwrap();
            if controle[0] == 0 || t == 0 {
                controle[0] = 1;
                return;
            }
            debug!("Esperando...");
            t = t - 1;
            sleep(Duration::from_nanos(random::<u64>() % 10));

        }
    }

    pub fn finalizar_trans(&self) {
        let mut controle = self.controle.lock().unwrap();
        controle[0] = 0;
    }

    pub fn adicionar_cliente(&self, cliente_row: &mut ClienteRow) {
        let mut cliente_mmap = self.cliente.lock().unwrap();

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
        let mut transacao_mmap = self.transacao.lock().unwrap();

        let (id, proximo_id) = Self::proximo_id(&mut transacao_mmap);

        transacao_row.id = proximo_id;
        transacao_row.descricao = format!("{: <10}", transacao_row.descricao).chars().take(10).collect();

        let offset = (id as usize) * TAMANHO_TRANSACAO_ROW + TAMANHO_SEQ;
        transacao_mmap[offset..offset + TAMANHO_TRANSACAO_ROW].copy_from_slice(&transacao_row.to_bytes());
    }

    pub fn recuperar_cliente(&self, id: i16) -> ClienteRow {
        let cliente_mmap = self.cliente.lock().unwrap();
        let offset = (id as usize - 1) * TAMANHO_CLIENTE_ROW + TAMANHO_SEQ;
        ClienteRow::from_bytes(&cliente_mmap[offset..offset + TAMANHO_CLIENTE_ROW])
    }

    pub fn recuperar_saldo(&self, id: i16) -> i32 {
        let cliente_mmap = self.cliente.lock().unwrap();
        let offset = (id as usize - 1) * TAMANHO_CLIENTE_ROW + TAMANHO_SEQ;
        ClienteRow::from_bytes(&cliente_mmap[offset..offset + TAMANHO_CLIENTE_ROW]).saldo
    }

    pub fn recuperar_transacoes(&self, cliente_id: i16) -> Vec<TransacaoRow> {
        let transacao_mmap = self.transacao.lock().unwrap();
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
        let mut cliente_mmap = self.cliente.lock().unwrap();
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

    let collector = tracing_subscriber::fmt().with_max_level(Level::DEBUG).finish();
    tracing::subscriber::set_global_default(collector).unwrap();

    info!("Iniciando servidor...");
    let http_port = env::var("HTTP_PORT").unwrap_or_else(|_| "80".to_string());
    debug!("Porta HTTP: {}", http_port);

    let cfg = Config::from_env()?;
    let pg_pool = cfg.pg.create_pool(Some(Tokio1), NoTls)?;

    let db_path = env::var("DB_PATH").unwrap_or_else(|_| "./temp".to_string());
    debug!("Caminho do banco de dados: {}", db_path);
    let controle_file = abrir_e_inicializar_arquivo(format!("{db_path}/controle.db").as_str(), 3);
    let cliente_file = abrir_e_inicializar_arquivo(format!("{db_path}/cliente.db").as_str(), 1024 * 10);
    let transacao_file = abrir_e_inicializar_arquivo(format!("{db_path}/transacao.db").as_str(), 1024 * 10 * 100);

    let rinha_db = RinhaDatabase::new(&controle_file, &cliente_file, &transacao_file);

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
        .route("/lock", get(get_lock).post(post_lock))
        .route("/unlock", post(post_unlock))
        .with_state(Arc::new(AppState { pg_pool , db: rinha_db }));

    let listener = TcpListener::bind(format!("0.0.0.0:{http_port}")).await?;
    info!("Escutando na porta {}...", http_port);
    axum::serve(listener, app).await?;
    Ok(())
}

pub async fn post_transacoes(State(s): State<Arc<AppState>>, Path(cliente_id): Path<i16>, transacao: body::Bytes) -> impl IntoResponse  {

    debug!("Recebendo transação do cliente {}", cliente_id);
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
    ).unwrap())
}

pub async fn get_lock(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    (StatusCode::OK, s.db.controle.lock().unwrap()[0].to_string())
}

pub async fn post_lock(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    s.db.controle.lock().unwrap()[0] = 1;
    (StatusCode::OK, s.db.controle.lock().unwrap()[0].to_string())
}

pub async fn post_unlock(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    s.db.controle.lock().unwrap()[0] = 0;
    (StatusCode::OK, s.db.controle.lock().unwrap()[0].to_string())
}

pub fn  abrir_e_inicializar_arquivo(path: &str, size: u64) -> File {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .unwrap();
    file.set_len(size).unwrap();
    file
}

pub fn inicializar_mmap(file: &File) -> Mutex<MmapMut> {
    unsafe { Mutex::new(MmapMut::map_mut(&file).unwrap()) }
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
