use std::fmt;

use slate_store::StoreError;

#[derive(Debug)]
pub enum EngineError {
    Store(StoreError),
    InvalidKey(String),
    Encoding(String),
    CollectionNotFound(String),
    DuplicateKey(String),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(e) => write!(f, "store error: {e}"),
            Self::InvalidKey(msg) => write!(f, "invalid key: {msg}"),
            Self::Encoding(msg) => write!(f, "encoding error: {msg}"),
            Self::CollectionNotFound(name) => write!(f, "collection not found: {name}"),
            Self::DuplicateKey(id) => write!(f, "duplicate key: {id}"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<StoreError> for EngineError {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}
