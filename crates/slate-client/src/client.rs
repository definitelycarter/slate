use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};

use slate_db::Datasource;
use slate_query::Query;
use slate_server::protocol::{Request, Response};
use slate_store::Record;

#[derive(Debug)]
pub enum ClientError {
    Io(std::io::Error),
    Serialization(String),
    Server(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Io(e) => write!(f, "io error: {e}"),
            ClientError::Serialization(msg) => write!(f, "serialization error: {msg}"),
            ClientError::Server(msg) => write!(f, "server error: {msg}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        ClientError::Io(e)
    }
}

impl From<bincode::Error> for ClientError {
    fn from(e: bincode::Error) -> Self {
        ClientError::Serialization(e.to_string())
    }
}

pub struct Client {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl Client {
    pub fn connect(addr: impl ToSocketAddrs) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(addr)?;
        let reader = BufReader::new(stream.try_clone()?);
        let writer = BufWriter::new(stream);
        Ok(Self { reader, writer })
    }

    fn request(&mut self, request: Request) -> Result<Response, ClientError> {
        let bytes = bincode::serialize(&request)?;
        let len = (bytes.len() as u32).to_be_bytes();
        self.writer.write_all(&len)?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;

        let mut len_buf = [0u8; 4];
        self.reader.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut msg_buf = vec![0u8; len];
        self.reader.read_exact(&mut msg_buf)?;

        let response: Response = bincode::deserialize(&msg_buf)?;
        Ok(response)
    }

    fn expect_ok(&mut self, request: Request) -> Result<(), ClientError> {
        match self.request(request)? {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    // Data operations

    pub fn insert(&mut self, record: Record) -> Result<(), ClientError> {
        self.expect_ok(Request::Insert(record))
    }

    pub fn insert_batch(&mut self, records: Vec<Record>) -> Result<(), ClientError> {
        self.expect_ok(Request::InsertBatch(records))
    }

    pub fn delete(&mut self, id: &str) -> Result<(), ClientError> {
        self.expect_ok(Request::Delete(id.to_string()))
    }

    pub fn get_by_id(&mut self, id: &str) -> Result<Option<Record>, ClientError> {
        match self.request(Request::GetById(id.to_string()))? {
            Response::Record(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    pub fn query(&mut self, query: &Query) -> Result<Vec<Record>, ClientError> {
        match self.request(Request::Query(query.clone()))? {
            Response::Records(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    // Catalog operations

    pub fn save_datasource(&mut self, ds: &Datasource) -> Result<(), ClientError> {
        self.expect_ok(Request::SaveDatasource(ds.clone()))
    }

    pub fn get_datasource(&mut self, id: &str) -> Result<Option<Datasource>, ClientError> {
        match self.request(Request::GetDatasource(id.to_string()))? {
            Response::Datasource(ds) => Ok(ds),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    pub fn list_datasources(&mut self) -> Result<Vec<Datasource>, ClientError> {
        match self.request(Request::ListDatasources)? {
            Response::Datasources(list) => Ok(list),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    pub fn delete_datasource(&mut self, id: &str) -> Result<(), ClientError> {
        self.expect_ok(Request::DeleteDatasource(id.to_string()))
    }
}
