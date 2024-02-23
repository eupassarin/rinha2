use std::{env, sync::Arc};
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::sync::Mutex;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use axum::{body, extract::{Path, State}, http::StatusCode, response::IntoResponse, Router, routing::{get, post}};
use deadpool_postgres::{Pool, Runtime::Tokio1, tokio_postgres::NoTls};
use memmap::MmapMut;
use serde_json::{from_slice, to_string};
use tokio::net::TcpListener;

static LIMITES: &'static [i32] = &[1000_00, 800_00,10000_00,100000_00,5000_00];

const TAMANHO_SEQ: usize = 2;
pub trait Row { fn from_bytes(slice: &[u8]) -> Self; fn to_bytes(&self) -> Vec<u8>; }

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

const TAMANHO_TRANSACAO_ROW: usize = 28;
pub struct TransacaoRow { pub id: u16, pub cliente_id: i16, pub valor: i32, pub tipo: char, pub descricao: String, pub realizada_em: u64, pub lock: bool }
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

pub struct RinhaDatabase { pub controle: MmapMut, pub cliente: MmapMut, pub transacao: Vec<MmapMut> }
impl RinhaDatabase {
    pub fn new(controle_file: &File, cliente_file: &File, transacoes_file: Vec<File>) -> Self {
        Self {
            controle: unsafe { MmapMut::map_mut(&controle_file).unwrap() },
            cliente: unsafe { MmapMut::map_mut(&cliente_file).unwrap() },
            transacao: unsafe {
                vec![
                    MmapMut::map_mut(&transacoes_file[0]).unwrap(),
                    MmapMut::map_mut(&transacoes_file[1]).unwrap(),
                    MmapMut::map_mut(&transacoes_file[2]).unwrap(),
                    MmapMut::map_mut(&transacoes_file[3]).unwrap(),
                    MmapMut::map_mut(&transacoes_file[4]).unwrap(),
                ]
            },
        }
    }
    pub fn abrir_trans(&mut self)  {
        let mut t = 3;
        loop {
            if self.controle[0] == 0 { self.controle[0] = 1; return; }
            t -= 1;
            if t == 0 { self.controle[0] = 0; continue; }
            sleep(Duration::from_nanos(1));
        }
    }
    pub fn fechar_trans(&mut self) {
        self.controle[0] = 0;
    }
    pub fn adicionar_cliente(&mut self, cliente_row: &mut ClienteRow) {
        let (id, proximo_id) = Self::proximo_id(&mut self.cliente);
        cliente_row.id = proximo_id;
        let offset = (id as usize) * TAMANHO_CLIENTE_ROW + TAMANHO_SEQ;
        let _ = &self.cliente[offset..offset + TAMANHO_CLIENTE_ROW].copy_from_slice(&cliente_row.to_bytes());
    }
    fn proximo_id(mmap: &mut MmapMut) -> (u16, u16) {
        let id = u16::from_ne_bytes([mmap[0], mmap[1]]);
        let proximo_id = id + 1;
        mmap[0] = proximo_id.to_ne_bytes()[0];
        mmap[1] = proximo_id.to_ne_bytes()[1];
        (id, proximo_id)
    }
    pub fn adicionar_transacao(&mut self, mut transacao_row: TransacaoRow) {
        let (id, proximo_id) = Self::proximo_id(&mut self.transacao[transacao_row.cliente_id as usize - 1]);
        transacao_row.id = proximo_id;
        transacao_row.descricao = format!("{: <10}", transacao_row.descricao).chars().take(10).collect();
        let offset = (id as usize) * TAMANHO_TRANSACAO_ROW + TAMANHO_SEQ;
        self.transacao[transacao_row.cliente_id as usize - 1][offset..offset + TAMANHO_TRANSACAO_ROW].copy_from_slice(&transacao_row.to_bytes());
    }
    pub fn recuperar_saldo(&self, id: i16) -> i32 {
        let offset = (id as usize - 1) * TAMANHO_CLIENTE_ROW + TAMANHO_SEQ;
        ClienteRow::from_bytes(&self.cliente[offset..offset + TAMANHO_CLIENTE_ROW]).saldo
    }
    pub fn recuperar_transacoes(&self, cliente_id: i16) -> Vec<TransacaoRow> {
        let t_data = &self.transacao[cliente_id as usize - 1];
        let num_transacoes = u16::from_ne_bytes([t_data[0], t_data[1]]);
        let mut transacoes = Vec::new();
        for i in (num_transacoes-10.min(num_transacoes)..=num_transacoes).rev() {
            let offset = (i - 1) as usize * TAMANHO_TRANSACAO_ROW + TAMANHO_SEQ;
            transacoes.push(TransacaoRow::from_bytes(&t_data[offset..offset + TAMANHO_TRANSACAO_ROW]));
        }
        transacoes
    }
    pub fn transacionar(&mut self, id: i16, novo_saldo: i32)  {
        let offset = (id as usize - 1) * TAMANHO_CLIENTE_ROW + TAMANHO_SEQ + 2;
        let _ = &self.cliente[offset..offset + 4].copy_from_slice(&(novo_saldo).to_ne_bytes());
    }
}

pub struct AppState { pub pg_pool: Pool, pub db: Mutex<RinhaDatabase>}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv::from_path(".env.local").ok().unwrap_or_else(|| {dotenv::dotenv().ok();});

    let cfg = Config::from_env()?;
    let pg_pool = cfg.pg.create_pool(Some(Tokio1), NoTls)?;

    let rinha_db = criar_db();

    let app = Router::new()
        .route("/clientes/:id/transacoes", post(post_transacoes))
        .route("/clientes/:id/extrato", get(get_extrato))
        .with_state(Arc::new(AppState { pg_pool , db: Mutex::new(rinha_db) }));

    let http_port = env::var("HTTP_PORT").unwrap_or_else(|_| "80".to_string());
    let listener = TcpListener::bind(format!("0.0.0.0:{http_port}")).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

pub async fn post_transacoes(State(s): State<Arc<AppState>>, Path(cliente_id): Path<i16>, transacao: body::Bytes) -> impl IntoResponse  {
    if cliente_id > 5 { return (StatusCode::NOT_FOUND, String::new()); }

    let t = match from_slice::<TransacaoPayload>(&transacao) {
        Ok(p) if p.descricao.len() >= 1 && p.descricao.len() <= 10 && (p.tipo == 'd' || p.tipo == 'c') => p,
        _ => return (StatusCode::UNPROCESSABLE_ENTITY, String::new())
    };

    let limite = LIMITES[(cliente_id - 1) as usize];

    let mut db = s.db.lock().unwrap();
    db.abrir_trans();
    let saldo_antigo = db.recuperar_saldo(cliente_id);
    if t.tipo == 'd' && saldo_antigo - t.valor < -limite {
        return (StatusCode::UNPROCESSABLE_ENTITY, String::new());
    }
    let novo_saldo = saldo_antigo + if t.tipo == 'd' { -t.valor } else { t.valor };
    db.transacionar(cliente_id, novo_saldo);
    db.adicionar_transacao(TransacaoRow::new(cliente_id, t.valor, t.tipo, t.descricao.as_str(), now()));
    db.fechar_trans();

    (StatusCode::OK, to_string(&SaldoLimite { saldo: novo_saldo, limite }).unwrap())
}

pub async fn get_extrato(State(s): State<Arc<AppState>>, Path(cliente_id): Path<i16>) -> impl IntoResponse {
    if cliente_id > 5 { return (StatusCode::NOT_FOUND, String::new()) }
    let db = s.db.lock().unwrap();
    (StatusCode::OK, to_string(
        &Extrato {
            saldo: Saldo{
                total: db.recuperar_saldo(cliente_id),
                data_extrato: now(),
                limite: LIMITES[(cliente_id -1) as usize]
            },
            ultimas_transacoes: db.recuperar_transacoes(cliente_id).iter()
                .map(|row| Transacao {
                    valor: row.valor,
                    tipo: row.tipo.to_string(),
                    descricao: str::trim(&row.descricao),
                    realizada_em: row.realizada_em
                }).collect()}
    ).unwrap())
}

pub fn criar_arquivo(path: &str, size: u64) -> File {
    let file = OpenOptions::new().read(true).write(true).create(true)
        .truncate(true).open(path).unwrap();
    file.set_len(size).unwrap();
    file
}

fn criar_db() -> RinhaDatabase {
    let db_path = env::var("DB_PATH").unwrap_or_else(|_| "./temp".to_string());
    let controle_file = criar_arquivo(format!("{db_path}/controle.db").as_str(), 3);
    let cliente_file = criar_arquivo(format!("{db_path}/cliente.db").as_str(), 1024 * 10);
    let transacoes_file =
        vec![criar_arquivo(format!("{db_path}/transacao_1.db").as_str(), 1024 * 10 * 200),
             criar_arquivo(format!("{db_path}/transacao_2.db").as_str(), 1024 * 10 * 200),
             criar_arquivo(format!("{db_path}/transacao_3.db").as_str(), 1024 * 10 * 200),
             criar_arquivo(format!("{db_path}/transacao_4.db").as_str(), 1024 * 10 * 200),
             criar_arquivo(format!("{db_path}/transacao_5.db").as_str(), 1024 * 10 * 200)
        ];

    let mut rinha_db = RinhaDatabase::new(&controle_file, &cliente_file, transacoes_file);
    rinha_db.abrir_trans();
    rinha_db.adicionar_cliente(&mut ClienteRow::new(0));
    rinha_db.adicionar_cliente(&mut ClienteRow::new(0));
    rinha_db.adicionar_cliente(&mut ClienteRow::new(0));
    rinha_db.adicionar_cliente(&mut ClienteRow::new(0));
    rinha_db.adicionar_cliente(&mut ClienteRow::new(0));
    rinha_db.fechar_trans();
    rinha_db
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
