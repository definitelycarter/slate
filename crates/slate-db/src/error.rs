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
    InvalidDocument(String),
    Serialization(String),
    IndexExists(String),
    FunctionExists(String),
    UniqueViolation {
        index: String,
        value: String,
        existing_id: String,
    },
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
            DbError::InvalidDocument(msg) => write!(f, "invalid document: {msg}"),
            DbError::Serialization(msg) => write!(f, "serialization error: {msg}"),
            DbError::IndexExists(desc) => write!(f, "index already exists: {desc}"),
            DbError::FunctionExists(desc) => write!(f, "function already exists: {desc}"),
            DbError::UniqueViolation {
                index,
                value,
                existing_id,
            } => write!(
                f,
                "unique constraint violation on {index}: value {value} already exists for document {existing_id}"
            ),
        }
    }
}

impl std::error::Error for DbError {}

impl From<StoreError> for DbError {
    fn from(e: StoreError) -> Self {
        DbError::Store(e)
    }
}

impl From<bson::error::Error> for DbError {
    fn from(e: bson::error::Error) -> Self {
        DbError::Serialization(e.to_string())
    }
}

impl From<slate_query::TranslateError> for DbError {
    fn from(e: slate_query::TranslateError) -> Self {
        DbError::InvalidQuery(e.to_string())
    }
}

impl From<slate_sql::SqlError> for DbError {
    fn from(e: slate_sql::SqlError) -> Self {
        DbError::InvalidQuery(e.to_string())
    }
}

impl From<slate_planner::PlanError> for DbError {
    fn from(e: slate_planner::PlanError) -> Self {
        DbError::InvalidQuery(e.message)
    }
}

impl From<slate_engine::EngineError> for DbError {
    fn from(e: slate_engine::EngineError) -> Self {
        match e {
            slate_engine::EngineError::Store(se) => DbError::Store(se),
            slate_engine::EngineError::CollectionNotFound(name) => {
                DbError::CollectionNotFound(name)
            }
            slate_engine::EngineError::DuplicateKey(id) => DbError::DuplicateKey(id),
            slate_engine::EngineError::InvalidDocument(msg) => DbError::InvalidDocument(msg),
            // A vector whose dimensionality disagrees with its index is a data
            // error (a malformed document), not a malformed query.
            err @ slate_engine::EngineError::VectorDimsMismatch { .. } => {
                DbError::InvalidDocument(err.to_string())
            }
            slate_engine::EngineError::IndexExists(desc) => DbError::IndexExists(desc),
            slate_engine::EngineError::FunctionExists(desc) => DbError::FunctionExists(desc),
            slate_engine::EngineError::UniqueViolation {
                index,
                value,
                existing_id,
            } => DbError::UniqueViolation {
                index,
                value,
                existing_id,
            },
            other => DbError::InvalidQuery(other.to_string()),
        }
    }
}

impl From<slate_executor::ExecError> for DbError {
    fn from(e: slate_executor::ExecError) -> Self {
        use slate_executor::ExecError as E;
        match e {
            E::Eval(ev) => DbError::InvalidQuery(ev.to_string()),
            E::Engine(en) => en.into(),
            // `ExecError::Mutation` wraps a `slate_rawbson::RawMergeError` from the
            // upsert-merge path; surface its message without naming the type.
            E::Mutation(m) => DbError::Serialization(m.to_string()),
            // A rejected validation or a failed trigger is a write-path abort —
            // surface it as a document error (the write was refused).
            E::Validation(msg) => DbError::InvalidDocument(msg),
            E::Trigger(msg) => DbError::InvalidDocument(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_dims_mismatch_maps_to_invalid_document() {
        // A dims mismatch is a data error, not a query error — it must surface as
        // `InvalidDocument`, not fall through the wildcard to `InvalidQuery`.
        let err = slate_engine::EngineError::VectorDimsMismatch {
            field: "embedding".to_string(),
            expected: 3,
            found: 2,
        };
        let mapped: DbError = err.into();
        match mapped {
            DbError::InvalidDocument(msg) => {
                assert!(msg.contains("embedding"), "message lost detail: {msg}");
            }
            other => panic!("expected InvalidDocument, got {other:?}"),
        }
    }
}
