mod collection;
mod cursor;
pub(crate) mod database;
mod error;
pub(crate) mod hooks;
#[cfg(feature = "runtime")]
pub(crate) mod runtime;

pub use bson::{Bson, Document, RawBson, RawDocumentBuf};
pub use collection::{CollectionConfig, CollectionSchema};
pub use cursor::{Cursor, CursorIter, RawCursorIter, RawValuesIter, ValuesIter};
pub use database::{Database, DatabaseBuilder, Transaction as DatabaseTransaction};
pub use error::DbError;
pub use hooks::{HookRegistry, HookSnapshot, ResolvedHook};
pub use slate_engine::{DEFAULT_CF, FunctionKind, IntegrityIssue, IntegrityReport};
pub use slate_query::{DistinctOptions, FindOptions, Sort, SortDirection};
pub use slate_store::Durability;
pub use slate_vm::VmError;
pub use slate_vm::pool::{RuntimeRegistry, VmPool};

#[cfg(feature = "bench-internals")]
pub mod bench {
    pub use crate::database::Database;
}
