mod collection;
mod convert;
mod cursor;
pub(crate) mod database;
mod error;
mod executor;
mod expression;
pub(crate) mod hooks;
pub(crate) mod mutation;
pub(crate) mod parser;
mod planner;
#[cfg(feature = "runtime")]
pub(crate) mod runtime;
mod statement;

pub use bson::{Bson, Document, RawBson, RawDocumentBuf};
pub use collection::CollectionConfig;
pub use slate_engine::{FunctionKind, DEFAULT_CF};
pub use slate_vm::VmError;
pub use slate_vm::pool::{RuntimeRegistry, VmPool};
pub use convert::IntoRawDocumentBuf;
pub use cursor::{Cursor, CursorIter};
pub use database::{Database, DatabaseBuilder, Transaction as DatabaseTransaction};
pub use error::DbError;
pub use hooks::{HookRegistry, HookSnapshot, ResolvedHook};

#[cfg(feature = "bench-internals")]
pub mod bench {
    pub use crate::database::Database;
    pub use crate::executor::{Executor, RawIter};
    pub use crate::expression::{Expression, LogicalOp};
    pub use crate::mutation::{Mutation, parse_mutation};
    pub use crate::planner::plan::{IndexScanRange, Node, Plan, ScanDirection};
    pub use crate::planner::planner::Planner;
    pub use crate::statement::Statement;
}
