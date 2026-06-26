//! The v2 public API surface (Phase 0).
//!
//! A self-contained second implementation of slate's collection-scoped API,
//! built alongside the flat v1 methods and vetted before anything moves onto it
//! (see `book/src/rfcs/db-api-cleanup.md`). v2 carries its own **real bodies**:
//! each terminal lowers the request and builds a [`Cursor`](crate::Cursor)
//! itself rather than calling a v1 verb. The only v1 internals it shares are the
//! engine transaction and the catalog read (`collection_meta`), reached through
//! crate-internal accessors on [`Transaction`](crate::DatabaseTransaction).
//!
//! Slice A: the [`Collection`] handle plus the `find` read builder
//! ([`FindBuilder`]) and its cursor terminals.

mod collection;
mod distinct;
mod query;
mod read;

pub use collection::{CfScope, Collection};
pub use distinct::DistinctBuilder;
pub use query::QueryBuilder;
pub use read::FindBuilder;
