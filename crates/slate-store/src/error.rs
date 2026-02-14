use std::fmt;

#[derive(Debug)]
pub enum StoreError {
    TransactionConsumed,
    ReadOnly,
    Serialization(String),
    Storage(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::TransactionConsumed => write!(f, "transaction already consumed"),
            StoreError::ReadOnly => write!(f, "cannot write in a read-only transaction"),
            StoreError::Serialization(msg) => write!(f, "serialization error: {msg}"),
            StoreError::Storage(msg) => write!(f, "storage error: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<std::str::Utf8Error> for StoreError {
    fn from(e: std::str::Utf8Error) -> Self {
        StoreError::Serialization(e.to_string())
    }
}

impl From<std::array::TryFromSliceError> for StoreError {
    fn from(e: std::array::TryFromSliceError) -> Self {
        StoreError::Serialization(e.to_string())
    }
}
