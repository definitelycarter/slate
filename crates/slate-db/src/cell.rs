use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::record::Value;

/// A single column value as returned by reads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cell {
    pub value: Value,
}

/// What the caller sends when writing cells.
/// expire_at is derived internally from the datasource field's ttl_seconds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CellWrite {
    pub column: String,
    pub value: Value,
}

/// A single column stored inside a RecordValue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredCell {
    pub value: Value,
    pub expire_at: Option<i64>,
}

/// The serialized form for an entire record stored as a single RocksDB value.
/// Key: `d:{datasource_id}:{record_id}`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RecordValue {
    /// Record-level expiry (from _id field TTL).
    pub expire_at: Option<i64>,
    /// Column name → stored cell.
    pub cells: HashMap<String, StoredCell>,
}
