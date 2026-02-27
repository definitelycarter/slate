use std::fmt;

use slate_db::DbError;

#[derive(Debug, uniffi::Error)]
pub enum SlateError {
    NotFound { message: String },
    DuplicateKey { message: String },
    InvalidQuery { message: String },
    Store { message: String },
    Serialization { message: String },
}

impl fmt::Display for SlateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SlateError::NotFound { message } => write!(f, "not found: {message}"),
            SlateError::DuplicateKey { message } => write!(f, "duplicate key: {message}"),
            SlateError::InvalidQuery { message } => write!(f, "invalid query: {message}"),
            SlateError::Store { message } => write!(f, "store error: {message}"),
            SlateError::Serialization { message } => write!(f, "serialization error: {message}"),
        }
    }
}

impl std::error::Error for SlateError {}

impl From<DbError> for SlateError {
    fn from(e: DbError) -> Self {
        match e {
            DbError::NotFound(msg) => SlateError::NotFound { message: msg },
            DbError::CollectionNotFound(msg) => SlateError::NotFound { message: msg },
            DbError::DuplicateKey(msg) => SlateError::DuplicateKey { message: msg },
            DbError::InvalidQuery(msg) => SlateError::InvalidQuery { message: msg },
            DbError::InvalidKey(msg) => SlateError::InvalidQuery { message: msg },
            DbError::Store(e) => SlateError::Store {
                message: e.to_string(),
            },
            DbError::InvalidDocument(msg) => SlateError::Serialization { message: msg },
            DbError::Serialization(msg) => SlateError::Serialization { message: msg },
        }
    }
}