mod catalog;
mod collection;
mod database;
mod encoding;
mod error;
mod executor;
#[allow(dead_code)]
pub mod executor_v2;
mod planner;
mod result;

pub use bson::{Bson, Document, RawBson, RawDocumentBuf};
pub use collection::CollectionConfig;
pub use database::{Database, DatabaseConfig, DatabaseTransaction};
pub use error::DbError;
pub use result::{DeleteResult, InsertResult, UpdateResult, UpsertResult};

#[cfg(feature = "bench-internals")]
pub mod bench {
    pub use crate::executor::{ExecutionResult, Executor, RawIter, RawValue};
    pub use crate::executor_v2;
    pub use crate::planner::PlanNode;
}
