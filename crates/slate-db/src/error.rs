use std::fmt;

use slate_store::StoreError;

#[derive(Debug)]
pub enum DbError {
    Store(StoreError),
    NotFound(String),
    DatasourceNotFound(String),
    InvalidQuery(String),
    InvalidKey(String),
    Serialization(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Store(e) => write!(f, "store error: {e}"),
            DbError::NotFound(id) => write!(f, "not found: {id}"),
            DbError::DatasourceNotFound(id) => write!(f, "datasource not found: {id}"),
            DbError::InvalidQuery(msg) => write!(f, "invalid query: {msg}"),
            DbError::InvalidKey(msg) => write!(f, "invalid key: {msg}"),
            DbError::Serialization(msg) => write!(f, "serialization error: {msg}"),
        }
    }
}

impl std::error::Error for DbError {}

impl From<StoreError> for DbError {
    fn from(e: StoreError) -> Self {
        DbError::Store(e)
    }
}

impl From<bincode::Error> for DbError {
    fn from(e: bincode::Error) -> Self {
        DbError::Serialization(e.to_string())
    }
}
