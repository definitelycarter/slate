mod collection;
mod cursor;
pub(crate) mod database;
mod error;
mod executor;
mod expression;
pub(crate) mod hooks;
pub(crate) mod parser;
mod planner;
#[cfg(feature = "runtime")]
pub(crate) mod runtime;
mod statement;

pub use bson::{Bson, Document, RawBson, RawDocumentBuf};
pub use collection::CollectionConfig;
pub use cursor::{Cursor, CursorIter, RawCursorIter};
pub use database::{Database, DatabaseBuilder, Transaction as DatabaseTransaction};
pub use error::DbError;
pub use hooks::{HookRegistry, HookSnapshot, ResolvedHook};
pub use slate_engine::{DEFAULT_CF, FunctionKind};
pub use slate_vm::VmError;
pub use slate_vm::pool::{RuntimeRegistry, VmPool};

#[cfg(feature = "bench-internals")]
pub mod bench {
    pub use crate::database::Database;
    pub use crate::executor::{Executor, RawIter};
    pub use crate::expression::{Expression, LogicalOp};
    pub use crate::planner::plan::{IndexScanRange, Node, Plan, ScanDirection};
    pub use crate::planner::planner::Planner;
    pub use crate::statement::Statement;
    pub use slate_mutation::{Mutation, parse_mutation};
}
