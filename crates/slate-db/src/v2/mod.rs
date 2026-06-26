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
//! Slice A: the [`Collection`] handle plus the `find`/`query`/`distinct` read
//! builders and their cursor terminals.
//!
//! Slice B: the write builders — `find(f).update/.delete/.replace` and the
//! direct `insert_*`/`upsert_many`/`merge_many` — finished with
//! `.execute(&txn)` → [`WriteResult`] (or `.explain`/`.analyze`).
//!
//! Slice C: the [`Indexes`] sub-handle — `indexes().create(paths, opts)` (one
//! constructor for all index kinds) / `.remove(field)` / `.list(&txn)`.
//!
//! Slice D: the per-kind script sub-handles — [`Triggers`] / [`Validators`] /
//! [`Functions`] — each `create(name, src)` / `.remove(name)` / `.list(&txn)`.

mod collection;
mod distinct;
mod exec;
mod index;
mod query;
mod read;
mod scripts;
mod write;

pub use collection::{CfScope, Collection};
pub use distinct::DistinctBuilder;
pub use index::{
    CreateIndex, IndexBuild, IndexOptions, IndexPaths, Indexes, RemoveIndex, VectorIndexOptions,
};
pub use query::QueryBuilder;
pub use read::FindBuilder;
pub use scripts::{
    CreateFunction, CreateTrigger, CreateValidator, Functions, RemoveScript, Triggers, Validators,
};
pub use write::{
    DeleteBuilder, InsertBuilder, ReplaceBuilder, UpdateBuilder, UpsertBuilder, WriteResult,
};
