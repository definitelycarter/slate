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
//! [`Functions`] — each `create(name, src)` / `.remove(name)` / `.list(&txn)`;
//! the [`Collections`] namespace (`db.collections()` / `db.cf(cf).collections()`)
//! with `create(name)` / `.list` / `.remove`; and the handle metadata terminals
//! `stats` / `schema` / `purge`.
//!
//! Slice E: the reactive terminals — `find(filter).watch(cb)` / `.stream()` and
//! `query(sql).watch(cb)` / `.stream()` — which register a DB-lifetime
//! subscription on the watch registry (carried as an `Arc` on the handle) and
//! take no transaction. Filter-only in phase 1.

mod collection;
mod collections;
mod distinct;
mod exec;
mod index;
mod meta;
mod query;
mod read;
mod scripts;
mod write;

pub use collection::{CfScope, Collection};
pub use collections::{Collections, CreateCollection, RemoveCollection};
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

// Shared read/write cores, used both by the builders above and by the inverted
// flat `Transaction` methods (so the two surfaces run one body). `pub(crate)`,
// not public — they are an internal seam, not part of the v2 API.
pub(crate) use collections::create_collection_core;
pub(crate) use exec::write_cursor;
pub(crate) use meta::{collection_schema_core, collection_stats_core, purge_core};
pub(crate) use query::query_cursor;
pub(crate) use read::find_cursor;
pub(crate) use write::{insert_plan, upsert_plan};
