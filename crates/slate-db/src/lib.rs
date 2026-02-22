mod catalog;
mod collection;
mod convert;
mod cursor;
mod database;
mod encoding;
mod error;
mod executor;
mod planner;
mod result;
mod sweep;

pub use bson::{Bson, Document, RawBson, RawDocumentBuf};
pub use collection::CollectionConfig;
pub use convert::IntoRawDocumentBuf;
pub use cursor::{Cursor, CursorIter};
pub use database::{Database, DatabaseConfig, DatabaseTransaction};
pub use error::DbError;
pub use result::{DeleteResult, InsertResult, UpdateResult, UpsertResult};

#[cfg(feature = "bench-internals")]
pub mod bench {
    pub use crate::executor::{ExecutionResult, Executor, RawIter, RawValue};
    pub use crate::planner::{PlanNode, UpsertMode};
}
