use std::fmt;

#[derive(Debug)]
pub enum StoreError {
    TransactionConsumed,
    ReadOnly,
    /// A write transaction failed to commit because a concurrent transaction
    /// modified one of the same keys (an optimistic write-write conflict). The
    /// transaction staged no changes and may be retried from `begin`.
    ///
    /// Only the optimistic backend (RocksDB) raises this today; the
    /// serialize-writers backends (memory, redb) can't conflict by
    /// construction. The variant is the shared seam onto which any future busy
    /// condition those backends might surface should also map, so callers can
    /// detect "retryable conflict" the same way on every backend.
    Conflict,
    Storage(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::TransactionConsumed => write!(f, "transaction already consumed"),
            StoreError::ReadOnly => write!(f, "cannot write in a read-only transaction"),
            StoreError::Conflict => write!(
                f,
                "write conflict: a concurrent transaction modified the same data"
            ),
            StoreError::Storage(msg) => write!(f, "storage error: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}
