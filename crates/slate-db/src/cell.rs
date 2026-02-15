use serde::{Deserialize, Serialize};

use crate::record::Value;

/// A single column value at a specific timestamp, as returned by reads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cell {
    pub value: Value,
    pub timestamp: i64,
}

/// What the caller sends when writing cells.
/// expire_at is derived internally from the datasource field's ttl_seconds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CellWrite {
    pub column: String,
    pub value: Value,
    pub timestamp: i64,
}

/// The serialized form stored as a value in RocksDB.
/// Timestamp lives here (not in the key) — last write wins.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CellValue {
    pub value: Value,
    pub timestamp: i64,
    pub expire_at: Option<i64>,
}
