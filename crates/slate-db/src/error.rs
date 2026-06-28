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
    /// A write transaction could not commit because a concurrent transaction
    /// modified the same data (an optimistic write-write conflict). The
    /// transaction made no changes and the operation can be retried from the
    /// top — [`Database::transact`](crate::Database::transact) does this
    /// automatically. Part of the concurrency contract on every backend (see
    /// `book/src/architecture-database.md`), even those that serialize writers
    /// and so cannot raise it today.
    Conflict,
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
            DbError::Conflict => write!(
                f,
                "transaction conflict: a concurrent write touched the same data; retry the transaction"
            ),
        }
    }
}

impl std::error::Error for DbError {}

impl DbError {
    /// Whether this is a retryable optimistic write-write conflict
    /// ([`DbError::Conflict`]). The retry predicate
    /// [`Database::transact`](crate::Database::transact) loops on.
    pub fn is_conflict(&self) -> bool {
        matches!(self, DbError::Conflict)
    }
}

impl From<StoreError> for DbError {
    fn from(e: StoreError) -> Self {
        match e {
            // A commit-time conflict is its own first-class, retryable shape —
            // not a generic store/I-O error a caller can't act on.
            StoreError::Conflict => DbError::Conflict,
            other => DbError::Store(other),
        }
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
            // Route through `From<StoreError>` so a commit-time `Conflict`
            // surfaces as `DbError::Conflict` rather than an opaque `Store`.
            slate_engine::EngineError::Store(se) => se.into(),
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
            // A malformed plan node is a query-construction fault.
            E::InvalidPlan(msg) => DbError::InvalidQuery(msg),
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

    #[test]
    fn store_conflict_maps_to_db_conflict() {
        // A commit-time `StoreError::Conflict` must become `DbError::Conflict`,
        // not get buried inside the opaque `DbError::Store`.
        let mapped: DbError = StoreError::Conflict.into();
        assert!(mapped.is_conflict(), "expected Conflict, got {mapped:?}");
    }

    #[test]
    fn engine_store_conflict_propagates_as_db_conflict() {
        // The same conflict reaching the db layer wrapped in `EngineError::Store`
        // (the actual commit path) must still surface as `DbError::Conflict`.
        let err = slate_engine::EngineError::Store(StoreError::Conflict);
        let mapped: DbError = err.into();
        assert!(mapped.is_conflict(), "expected Conflict, got {mapped:?}");
    }

    #[test]
    fn non_conflict_store_error_stays_store() {
        // A plain storage error must not be mistaken for a retryable conflict.
        let mapped: DbError = StoreError::Storage("disk gone".to_string()).into();
        assert!(!mapped.is_conflict(), "must not be a conflict: {mapped:?}");
        assert!(matches!(mapped, DbError::Store(_)));
    }
}
