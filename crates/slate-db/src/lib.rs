mod collection;
mod convert;
mod cursor;
pub(crate) mod database;
mod error;
mod executor;
mod expression;
pub(crate) mod parser;
mod planner;
mod statement;
mod sweep;

pub use bson::{Bson, Document, RawBson, RawDocumentBuf};
pub use collection::CollectionConfig;
pub use convert::IntoRawDocumentBuf;
pub use cursor::{Cursor, CursorIter};
pub use database::{Database, DatabaseConfig, Transaction as DatabaseTransaction};
pub use error::DbError;

#[cfg(feature = "bench-internals")]
pub mod bench {
    pub use crate::database::Database;
    pub use crate::executor::{Executor, RawIter};
    pub use crate::expression::{Expression, LogicalOp};
    pub use crate::planner::plan::{IndexScanRange, Node, Plan, ScanDirection};
    pub use crate::planner::planner::Planner;
    pub use crate::statement::Statement;
}
