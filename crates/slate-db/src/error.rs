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
    Vm(slate_vm::VmError),
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
            DbError::Vm(e) => write!(f, "vm error: {e}"),
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

impl From<slate_mutation::ParseError> for DbError {
    fn from(e: slate_mutation::ParseError) -> Self {
        DbError::InvalidQuery(e.to_string())
    }
}

impl From<slate_mutation::MutationError> for DbError {
    fn from(e: slate_mutation::MutationError) -> Self {
        match e {
            slate_mutation::MutationError::Invalid(m) => DbError::InvalidQuery(m),
            slate_mutation::MutationError::Serialization(m) => DbError::Serialization(m),
        }
    }
}

impl From<crate::parser::FilterParseError> for DbError {
    fn from(e: crate::parser::FilterParseError) -> Self {
        DbError::InvalidQuery(e.to_string())
    }
}

impl From<slate_sql::SqlError> for DbError {
    fn from(e: slate_sql::SqlError) -> Self {
        DbError::InvalidQuery(e.to_string())
    }
}

impl From<slate_vm::VmError> for DbError {
    fn from(e: slate_vm::VmError) -> Self {
        DbError::Vm(e)
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
            E::Mutation(m) => m.into(),
            E::Vm(v) => DbError::Vm(v),
            E::Validation(msg) => DbError::InvalidDocument(msg),
        }
    }
}
