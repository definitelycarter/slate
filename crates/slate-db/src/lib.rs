mod collection;
mod convert;
mod cursor;
mod database;
#[cfg(not(feature = "bench-internals"))]
pub(crate) mod engine;
#[cfg(feature = "bench-internals")]
pub mod engine;
mod error;
mod executor;
mod expression;
pub(crate) mod parse_filter;
mod planner;
mod result;
mod statement;
mod sweep;

pub use bson::{Bson, Document, RawBson, RawDocumentBuf};
pub use collection::CollectionConfig;
pub use convert::IntoRawDocumentBuf;
pub use cursor::{Cursor, CursorIter};
pub use database::{Database, DatabaseConfig};
pub use engine::Transaction as DatabaseTransaction;
pub use error::DbError;
pub use result::{DeleteResult, InsertResult, UpdateResult, UpsertResult};

#[cfg(feature = "bench-internals")]
pub mod bench {
    pub use crate::engine::SlateEngine;
    pub use crate::executor::{Executor, RawIter, RawValue};
    pub use crate::expression::{Expression, LogicalOp};
    pub use crate::planner::plan::{IndexScanRange, Node, Plan, ScanDirection};
    pub use crate::planner::planner::Planner;
    pub use crate::statement::Statement;
}
