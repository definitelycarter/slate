use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};

use slate_db::{CollectionConfig, DeleteResult, InsertResult, UpdateResult, UpsertResult};
use slate_query::{DistinctQuery, FilterGroup, Query};
use slate_server::protocol::{Request, Response};

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

impl From<rmp_serde::encode::Error> for ClientError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        ClientError::Serialization(e.to_string())
    }
}

impl From<rmp_serde::decode::Error> for ClientError {
    fn from(e: rmp_serde::decode::Error) -> Self {
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
        let bytes = rmp_serde::to_vec(&request)?;
        let len = (bytes.len() as u32).to_be_bytes();
        self.writer.write_all(&len)?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;

        let mut len_buf = [0u8; 4];
        self.reader.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut msg_buf = vec![0u8; len];
        self.reader.read_exact(&mut msg_buf)?;

        let response: Response = rmp_serde::from_slice(&msg_buf)?;
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

    // ── Insert operations ───────────────────────────────────────

    pub fn insert_one(
        &mut self,
        collection: &str,
        doc: bson::Document,
    ) -> Result<InsertResult, ClientError> {
        match self.request(Request::InsertOne {
            collection: collection.to_string(),
            doc,
        })? {
            Response::Insert(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    pub fn insert_many(
        &mut self,
        collection: &str,
        docs: Vec<bson::Document>,
    ) -> Result<Vec<InsertResult>, ClientError> {
        match self.request(Request::InsertMany {
            collection: collection.to_string(),
            docs,
        })? {
            Response::Inserts(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    // ── Query operations ────────────────────────────────────────

    pub fn find(
        &mut self,
        collection: &str,
        query: &Query,
    ) -> Result<Vec<bson::Document>, ClientError> {
        match self.request(Request::Find {
            collection: collection.to_string(),
            query: query.clone(),
        })? {
            Response::Records(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    pub fn find_by_id(
        &mut self,
        collection: &str,
        id: &str,
        columns: Option<&[&str]>,
    ) -> Result<Option<bson::Document>, ClientError> {
        match self.request(Request::FindById {
            collection: collection.to_string(),
            id: id.to_string(),
            columns: columns.map(|c| c.iter().map(|s| s.to_string()).collect()),
        })? {
            Response::Record(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    pub fn find_one(
        &mut self,
        collection: &str,
        query: &Query,
    ) -> Result<Option<bson::Document>, ClientError> {
        match self.request(Request::FindOne {
            collection: collection.to_string(),
            query: query.clone(),
        })? {
            Response::Record(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    // ── Update operations ───────────────────────────────────────

    pub fn update_one(
        &mut self,
        collection: &str,
        filter: &FilterGroup,
        update: bson::Document,
        upsert: bool,
    ) -> Result<UpdateResult, ClientError> {
        match self.request(Request::UpdateOne {
            collection: collection.to_string(),
            filter: filter.clone(),
            update,
            upsert,
        })? {
            Response::Update(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    pub fn update_many(
        &mut self,
        collection: &str,
        filter: &FilterGroup,
        update: bson::Document,
    ) -> Result<UpdateResult, ClientError> {
        match self.request(Request::UpdateMany {
            collection: collection.to_string(),
            filter: filter.clone(),
            update,
        })? {
            Response::Update(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    pub fn replace_one(
        &mut self,
        collection: &str,
        filter: &FilterGroup,
        doc: bson::Document,
    ) -> Result<UpdateResult, ClientError> {
        match self.request(Request::ReplaceOne {
            collection: collection.to_string(),
            filter: filter.clone(),
            doc,
        })? {
            Response::Update(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    // ── Bulk upsert / merge operations ────────────────────────────

    pub fn upsert_many(
        &mut self,
        collection: &str,
        docs: Vec<bson::Document>,
    ) -> Result<UpsertResult, ClientError> {
        match self.request(Request::UpsertMany {
            collection: collection.to_string(),
            docs,
        })? {
            Response::Upsert(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    pub fn merge_many(
        &mut self,
        collection: &str,
        docs: Vec<bson::Document>,
    ) -> Result<UpsertResult, ClientError> {
        match self.request(Request::MergeMany {
            collection: collection.to_string(),
            docs,
        })? {
            Response::Upsert(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    // ── Delete operations ───────────────────────────────────────

    pub fn delete_one(
        &mut self,
        collection: &str,
        filter: &FilterGroup,
    ) -> Result<DeleteResult, ClientError> {
        match self.request(Request::DeleteOne {
            collection: collection.to_string(),
            filter: filter.clone(),
        })? {
            Response::Delete(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    pub fn delete_many(
        &mut self,
        collection: &str,
        filter: &FilterGroup,
    ) -> Result<DeleteResult, ClientError> {
        match self.request(Request::DeleteMany {
            collection: collection.to_string(),
            filter: filter.clone(),
        })? {
            Response::Delete(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    // ── Count ───────────────────────────────────────────────────

    pub fn count(
        &mut self,
        collection: &str,
        filter: Option<&FilterGroup>,
    ) -> Result<u64, ClientError> {
        match self.request(Request::Count {
            collection: collection.to_string(),
            filter: filter.cloned(),
        })? {
            Response::Count(n) => Ok(n),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    // ── Distinct ─────────────────────────────────────────────────

    pub fn distinct(
        &mut self,
        collection: &str,
        query: &DistinctQuery,
    ) -> Result<bson::RawBson, ClientError> {
        match self.request(Request::Distinct {
            collection: collection.to_string(),
            query: query.clone(),
        })? {
            Response::Values(v) => Ok(v),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    // ── Index operations ────────────────────────────────────────

    pub fn create_index(&mut self, collection: &str, field: &str) -> Result<(), ClientError> {
        self.expect_ok(Request::CreateIndex {
            collection: collection.to_string(),
            field: field.to_string(),
        })
    }

    pub fn drop_index(&mut self, collection: &str, field: &str) -> Result<(), ClientError> {
        self.expect_ok(Request::DropIndex {
            collection: collection.to_string(),
            field: field.to_string(),
        })
    }

    pub fn list_indexes(&mut self, collection: &str) -> Result<Vec<String>, ClientError> {
        match self.request(Request::ListIndexes {
            collection: collection.to_string(),
        })? {
            Response::Indexes(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    // ── Collection operations ───────────────────────────────────

    pub fn create_collection(&mut self, config: &CollectionConfig) -> Result<(), ClientError> {
        self.expect_ok(Request::CreateCollection {
            config: config.clone(),
        })
    }

    pub fn list_collections(&mut self) -> Result<Vec<String>, ClientError> {
        match self.request(Request::ListCollections)? {
            Response::Collections(r) => Ok(r),
            Response::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Server(format!(
                "unexpected response: {other:?}"
            ))),
        }
    }

    pub fn drop_collection(&mut self, collection: &str) -> Result<(), ClientError> {
        self.expect_ok(Request::DropCollection {
            collection: collection.to_string(),
        })
    }
}
