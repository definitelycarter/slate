use std::fmt;

#[derive(Debug)]
pub enum StoreError {
    TransactionConsumed,
    ReadOnly,
    Storage(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::TransactionConsumed => write!(f, "transaction already consumed"),
            StoreError::ReadOnly => write!(f, "cannot write in a read-only transaction"),
            StoreError::Storage(msg) => write!(f, "storage error: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}
