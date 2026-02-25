use std::fmt;

use slate_store::StoreError;

#[derive(Debug)]
pub enum DbError {
    Store(StoreError),
    NotFound(String),
    CollectionNotFound(String),
    DuplicateKey(String),
    InvalidQuery(String),
    InvalidKey(String),
    Serialization(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Store(e) => write!(f, "store error: {e}"),
            DbError::NotFound(id) => write!(f, "not found: {id}"),
            DbError::CollectionNotFound(name) => write!(f, "collection not found: {name}"),
            DbError::DuplicateKey(id) => write!(f, "duplicate key: {id}"),
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

impl From<bson::ser::Error> for DbError {
    fn from(e: bson::ser::Error) -> Self {
        DbError::Serialization(e.to_string())
    }
}

impl From<bson::de::Error> for DbError {
    fn from(e: bson::de::Error) -> Self {
        DbError::Serialization(e.to_string())
    }
}

impl From<bson::raw::Error> for DbError {
    fn from(e: bson::raw::Error) -> Self {
        DbError::Serialization(e.to_string())
    }
}

impl From<slate_query::ParseError> for DbError {
    fn from(e: slate_query::ParseError) -> Self {
        DbError::InvalidQuery(e.to_string())
    }
}

impl From<crate::parse_filter::FilterParseError> for DbError {
    fn from(e: crate::parse_filter::FilterParseError) -> Self {
        DbError::InvalidQuery(e.to_string())
    }
}

impl From<slate_engine::EngineError> for DbError {
    fn from(e: slate_engine::EngineError) -> Self {
        match e {
            slate_engine::EngineError::Store(se) => DbError::Store(se),
            slate_engine::EngineError::CollectionNotFound(name) => {
                DbError::CollectionNotFound(name)
            }
            other => DbError::InvalidQuery(other.to_string()),
        }
    }
}
