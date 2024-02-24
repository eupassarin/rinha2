use std::{env, sync::Arc, error::Error, fs::{File, OpenOptions}, thread::sleep, time::{Duration, SystemTime, UNIX_EPOCH}};
use axum::{body, extract::{Path, State}, http::StatusCode, response::IntoResponse, Router, routing::{get, post}};
use deadpool_postgres::Runtime::Tokio1;
use deadpool_postgres::tokio_postgres::NoTls;
use memmap::MmapMut;
use serde_json::{from_slice, to_string};
use spin::{Mutex, RwLock, RwLockWriteGuard};
use tokio::net::TcpListener;
use tokio::task;

pub struct RinhaDatabase { pub controle: Mutex<MmapMut>, pub cliente: RwLock<MmapMut>, pub transacao: Vec<RwLock<MmapMut>> }
impl RinhaDatabase {
    pub fn new(controle_file: &File, cliente_file: &File, transacoes_file: Vec<File>) -> Self {
        Self {
            controle: unsafe { Mutex::new(MmapMut::map_mut(&controle_file).unwrap()) },
            cliente: unsafe { RwLock::new(MmapMut::map_mut(&cliente_file).unwrap()) },
            transacao: unsafe { transacoes_file.iter().map(|f| RwLock::new(MmapMut::map_mut(f).unwrap())).collect() },
        }
    }
    pub fn abrir_trans(&mut self) {
        let mut controle = self.controle.lock();
        let mut tentativas = 10;
        loop {
            if controle[0] == 0 { controle[0] = 1; return; }
            tentativas -= 1; if tentativas == 0 { controle[0] = 0; continue; }
            sleep(Duration::from_nanos(1));
        }
    }
    pub fn abrir_trans_cliente(&self, cliente_id: u16) {
        let mut cliente = self.cliente.write();
        let offset = (cliente_id as usize - 1) * TAMANHO_CLIENTE_ROW + TAMANHO_SEQ;
        let mut tentativas = 10;
        loop {
            let cliente_row = &mut cliente[offset..offset + TAMANHO_CLIENTE_ROW];
            if cliente_row[6] == 0 {  cliente_row[6] = 1; return; }
            tentativas -= 1; if tentativas == 0 { cliente_row[6] = 0; continue; }
            sleep(Duration::from_nanos(1));
        }
    }
    pub fn fechar_trans(&self) { self.controle.lock()[0] = 0; }
    pub fn fechar_trans_cliente(&self, cliente_id: u16) {
        let mut cliente = self.cliente.write();
        let offset = (cliente_id as usize - 1) * TAMANHO_CLIENTE_ROW + TAMANHO_SEQ;
        cliente[offset..offset + TAMANHO_CLIENTE_ROW][6] = 0;
    }
    pub fn adicionar_cliente(&self, cliente_row: &mut ClienteRow) {
        let cliente = &mut self.cliente.write();
        let (id, proximo_id) = Self::proximo_id(cliente);
        cliente_row.id = proximo_id;
        let offset = (id as usize) * TAMANHO_CLIENTE_ROW + TAMANHO_SEQ;
        let _ = cliente[offset..offset + TAMANHO_CLIENTE_ROW].copy_from_slice(&cliente_row.to_bytes());
    }
    fn proximo_id(mmap: &mut RwLockWriteGuard<MmapMut>) -> (u16, u16) {
        let id = u16::from_ne_bytes([mmap[0], mmap[1]]);
        let proximo_id = id + 1;
        mmap[0] = proximo_id.to_ne_bytes()[0];
        mmap[1] = proximo_id.to_ne_bytes()[1];
        (id, proximo_id)
    }
    pub fn adicionar_transacao(&self, mut transacao_row: TransacaoRow) {
        let transacao = &mut self.transacao[transacao_row.cliente_id as usize - 1].write();
        let (id, proximo_id) = Self::proximo_id(transacao);
        transacao_row.id = proximo_id;
        transacao_row.descricao = format!("{: <10}", transacao_row.descricao).chars().take(10).collect();
        let offset = (id as usize) * TAMANHO_TRANSACAO_ROW + TAMANHO_SEQ;
        transacao[offset..offset + TAMANHO_TRANSACAO_ROW].copy_from_slice(&transacao_row.to_bytes());
    }
    pub fn saldo(&self, cliente_id: u16) -> i32 {
        let cliente = &self.cliente.read();
        let offset = (cliente_id as usize - 1) * TAMANHO_CLIENTE_ROW + TAMANHO_SEQ;
        ClienteRow::from_bytes(&cliente[offset..offset + TAMANHO_CLIENTE_ROW]).saldo
    }
    pub fn recuperar_transacoes(&self, cliente_id: u16) -> Vec<TransacaoRow> {
        let transacao = &self.transacao[cliente_id as usize - 1].read();
        let mut transacoes = Vec::new();
        let mut i = u16::from_ne_bytes([transacao[0], transacao[1]]);
        let f = if i > 10 { i - 10 } else { 0 };
        while i > f {
            i -= 1;
            let offset = (i as usize) * TAMANHO_TRANSACAO_ROW + TAMANHO_SEQ;
            transacoes.push(TransacaoRow::from_bytes(&transacao[offset..offset + TAMANHO_TRANSACAO_ROW]));
        }
        transacoes
    }
    pub fn transacionar(&self, cliente_id: u16, novo_saldo: i32)  {
        let cliente = &mut self.cliente.write();
        let offset = (cliente_id as usize - 1) * TAMANHO_CLIENTE_ROW + TAMANHO_SEQ + 2;
        let _ = &cliente[offset..offset + 4].copy_from_slice(&novo_saldo.to_ne_bytes());
    }
}
pub struct AppState { pub db: RinhaDatabase, pub pg_pool: deadpool_postgres::Pool }
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv::from_path(".env.local").ok().unwrap_or_else(|| {dotenv::dotenv().ok();});
    let pg_pool = Config::from_env()?.pg.create_pool(Some(Tokio1), NoTls)?;
    let app = Router::new()
        .route("/clientes/:id/transacoes", post(post_transacoes))
        .route("/clientes/:id/extrato", get(get_extrato))
        .with_state(Arc::new(AppState { db: criar_db(), pg_pool }));
    let http_port = env::var("HTTP_PORT").unwrap_or_else(|_| "80".to_string());
    let listener = TcpListener::bind(format!("0.0.0.0:{http_port}")).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
pub async fn post_transacoes(State(s): State<Arc<AppState>>, Path(cliente_id): Path<u16>, transacao: body::Bytes) -> impl IntoResponse  {
    if cliente_id > 5 { return (StatusCode::NOT_FOUND, String::new()); }
    let t = match from_slice::<TransacaoPayload>(&transacao) {
        Ok(p) if p.descricao.len() >= 1 && p.descricao.len() <= 10 && (p.tipo == 'd' || p.tipo == 'c') => p,
        _ => return (StatusCode::UNPROCESSABLE_ENTITY, String::new())
    };
    s.db.abrir_trans_cliente(cliente_id);
    let saldo_antigo = s.db.saldo(cliente_id);
    if t.tipo == 'd' && saldo_antigo - t.valor < -LIMITES[(cliente_id - 1) as usize] {
        s.db.fechar_trans_cliente(cliente_id);
        return (StatusCode::UNPROCESSABLE_ENTITY, String::new());
    }
    let valor = if t.tipo == 'd' { -t.valor } else { t.valor };
    let novo_saldo = saldo_antigo + valor;
    s.db.transacionar(cliente_id, novo_saldo);
    s.db.adicionar_transacao(TransacaoRow::new(cliente_id, t.valor, t.tipo, t.descricao.as_str(), now()));
    s.db.fechar_trans_cliente(cliente_id);
    task::spawn(async move {
        let conn = s.pg_pool.get().await.unwrap();
        conn.query(UPDATE_SALDO, &[&valor, &(cliente_id as i16)]).await.unwrap();
        conn.query(INSERT_TRANSACAO, &[&(cliente_id as i16), &t.valor, &t.tipo.to_string(), &t.descricao]).await.unwrap();
    });
    (StatusCode::OK, to_string(&SaldoLimite { saldo: novo_saldo, limite: LIMITES[(cliente_id - 1) as usize] }).unwrap())
}
pub async fn get_extrato(State(s): State<Arc<AppState>>, Path(cliente_id): Path<u16>) -> impl IntoResponse {
    if cliente_id > 5 { return (StatusCode::NOT_FOUND, String::new()) }
    (StatusCode::OK, to_string(
        &Extrato {
            saldo: Saldo{ total: s.db.saldo(cliente_id), data_extrato: now(), limite: LIMITES[(cliente_id -1) as usize] },
            ultimas_transacoes: s.db.recuperar_transacoes(cliente_id).iter()
                .map(|row| Transacao {
                    valor: row.valor,
                    tipo: row.tipo.to_string(),
                    descricao: str::trim(&row.descricao),
                    realizada_em: row.realizada_em
                }).collect()}
    ).unwrap())
}
pub fn criar_arquivo(path: &str, size: u64) -> File {
    let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path).unwrap();
    file.set_len(size).unwrap();
    file
}
fn criar_db() -> RinhaDatabase {
    let db_path = env::var("DB_PATH").unwrap_or_else(|_| "./temp".to_string());
    let controle_file = criar_arquivo(format!("{db_path}/controle.db").as_str(), 3);
    let cliente_file = criar_arquivo(format!("{db_path}/cliente.db").as_str(), 1024 * 10);
    let transacoes_file = (1..=5).map(|i| criar_arquivo(format!("{db_path}/transacao_{i}.db").as_str(), 1024 * 10 * 200)).collect();
    let mut rinha_db = RinhaDatabase::new(&controle_file, &cliente_file, transacoes_file);
    rinha_db.abrir_trans();
    for _ in 1..=5 { rinha_db.adicionar_cliente(&mut ClienteRow::new(0));}
    rinha_db.fechar_trans();
    rinha_db
}
pub trait Row { fn from_bytes(slice: &[u8]) -> Self; fn to_bytes(&self) -> Vec<u8>; }
const TAMANHO_SEQ: usize = 2;
const TAMANHO_CLIENTE_ROW: usize = 7;
pub struct ClienteRow { pub id: u16, pub saldo: i32, pub lock: bool }
impl ClienteRow { pub fn new( saldo: i32) -> Self { Self { id: 0,  saldo, lock: false } } }
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
const TAMANHO_TRANSACAO_ROW: usize = 27;
pub struct TransacaoRow { pub id: u16, pub cliente_id: u16, pub valor: i32, pub tipo: char, pub descricao: String, pub realizada_em: u64}
impl TransacaoRow {
    pub fn new(cliente_id: u16 , valor: i32, tipo: char, descricao: &str, realizada_em: u64) -> Self {
        Self { id: 0, cliente_id, valor, tipo, descricao: descricao.to_string(), realizada_em }
    }
}
impl Row for TransacaoRow {
    fn from_bytes(slice: &[u8]) -> Self {
        Self {
            id: u16::from_ne_bytes(slice[0..2].try_into().unwrap()),
            cliente_id: u16::from_ne_bytes(slice[2..4].try_into().unwrap()),
            valor: i32::from_ne_bytes(slice[4..8].try_into().unwrap()),
            tipo: slice[8] as char,
            descricao: String::from_utf8_lossy(&slice[9..19]).to_string(),
            realizada_em: u64::from_ne_bytes(slice[19..27].try_into().unwrap())
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
        bytes
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
static LIMITES: &'static [i32] = &[1000_00, 800_00,10000_00,100000_00,5000_00];
const UPDATE_SALDO: &str = "UPDATE C SET S = S + $1 WHERE I = $2";
const INSERT_TRANSACAO: &str = "INSERT INTO T(I, V, P, D) VALUES($1, $2, $3, $4)";
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
