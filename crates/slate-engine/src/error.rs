use std::fmt;

use slate_store::StoreError;

// ── EncodingError ─────────────────────────────────────────────

#[derive(Debug)]
pub enum EncodingError {
    Bson(bson::error::Error),
    MalformedRecord(String),
    MalformedIndexMeta(String),
}

impl fmt::Display for EncodingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bson(e) => write!(f, "bson: {e}"),
            Self::MalformedRecord(msg) => write!(f, "malformed record: {msg}"),
            Self::MalformedIndexMeta(msg) => write!(f, "malformed index metadata: {msg}"),
        }
    }
}

impl std::error::Error for EncodingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bson(e) => Some(e),
            _ => None,
        }
    }
}

impl From<bson::error::Error> for EncodingError {
    fn from(e: bson::error::Error) -> Self {
        Self::Bson(e)
    }
}

// ── EngineError ───────────────────────────────────────────────

#[derive(Debug)]
pub enum EngineError {
    Store(StoreError),
    InvalidKey(String),
    Encoding(EncodingError),
    CollectionNotFound(String),
    DuplicateKey(String),
    InvalidDocument(String),
    IndexExists(String),
    FunctionExists(String),
    /// A unique index already holds `value` for a different document.
    UniqueViolation {
        index: String,
        value: String,
        existing_id: String,
    },
    /// An index scan was given a bound the index cannot represent as a key —
    /// null, undefined, or a non-scalar value. Sparse indexes store only scalar
    /// values, so such a bound is meaningless; this is surfaced loudly rather
    /// than silently returning no rows (or, for a range, the wrong rows).
    UnscannableBound {
        field: String,
        value_type: &'static str,
    },
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(e) => write!(f, "store error: {e}"),
            Self::InvalidKey(msg) => write!(f, "invalid key: {msg}"),
            Self::Encoding(e) => write!(f, "encoding error: {e}"),
            Self::CollectionNotFound(name) => write!(f, "collection not found: {name}"),
            Self::DuplicateKey(id) => write!(f, "duplicate key: {id}"),
            Self::InvalidDocument(msg) => write!(f, "invalid document: {msg}"),
            Self::IndexExists(desc) => write!(f, "index already exists: {desc}"),
            Self::FunctionExists(desc) => write!(f, "function already exists: {desc}"),
            Self::UniqueViolation {
                index,
                value,
                existing_id,
            } => write!(
                f,
                "unique constraint violation on {index}: value {value} already exists for document {existing_id}"
            ),
            Self::UnscannableBound { field, value_type } => write!(
                f,
                "cannot scan index `{field}` for a {value_type} bound: the index stores only scalar values"
            ),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<StoreError> for EngineError {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}

impl From<EncodingError> for EngineError {
    fn from(e: EncodingError) -> Self {
        Self::Encoding(e)
    }
}
