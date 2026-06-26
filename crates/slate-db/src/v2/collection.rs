//! The v2 collection handle — the entry point every collection-scoped operation
//! hangs off.
//!
//! Phase 0, slice A: [`Database::collection`] / [`Database::cf`] build a
//! lightweight [`Collection`]; [`Collection::find`] opens a read builder. The
//! handle owns only its `(cf, collection)` names — the transaction supplied at
//! the terminal carries the execution state. (Reactive roots — the `Arc`s into
//! `db` that `.watch`/`.stream` need — arrive with slice E.)

use serde::Serialize;
use slate_store::Store;

use super::index::Indexes;
use super::query::QueryBuilder;
use super::read::FindBuilder;
use super::scripts::{Functions, Triggers, Validators};
use super::write::{InsertBuilder, UpsertBuilder};
use crate::DEFAULT_CF;
use crate::database::Database;
use slate_planner::UpsertMode;

/// A lightweight handle to one collection. Build it with
/// [`Database::collection`] (default column family) or
/// [`Database::cf`]`(cf).collection(name)`.
pub struct Collection {
    cf: String,
    collection: String,
}

impl Collection {
    /// Open a `find` read over a Mongo-style `filter`. Returns a builder; nothing
    /// runs until a terminal (`.iter`/`.collect`/`.count`/`.first`) is called with
    /// a transaction.
    pub fn find<F: Serialize>(&self, filter: F) -> FindBuilder<'_, F> {
        FindBuilder::new(&self.cf, &self.collection, filter)
    }

    /// Open a CosmosDB-style SQL `query` over this collection. Returns a builder;
    /// nothing runs until a terminal (`.iter`/`.collect`/`.explain`) runs it.
    pub fn query<'a>(&'a self, sql: &'a str) -> QueryBuilder<'a> {
        QueryBuilder::new(&self.cf, &self.collection, sql)
    }

    /// Insert a single document (generating an `_id` when absent). Returns a
    /// builder; nothing runs until `.execute(&txn)`.
    pub fn insert_one<D: Serialize>(&self, doc: D) -> InsertBuilder<'_, D> {
        InsertBuilder::new(&self.cf, &self.collection, vec![doc])
    }

    /// Insert a batch of documents. Returns a builder; nothing runs until
    /// `.execute(&txn)`.
    pub fn insert_many<D: Serialize>(
        &self,
        docs: impl IntoIterator<Item = D>,
    ) -> InsertBuilder<'_, D> {
        InsertBuilder::new(&self.cf, &self.collection, docs.into_iter().collect())
    }

    /// Upsert (insert-or-replace) a batch of documents by `_id`. Returns a
    /// builder; nothing runs until `.execute(&txn)`.
    pub fn upsert_many<D: Serialize>(
        &self,
        docs: impl IntoIterator<Item = D>,
    ) -> UpsertBuilder<'_, D> {
        UpsertBuilder::new(
            &self.cf,
            &self.collection,
            docs.into_iter().collect(),
            UpsertMode::Replace,
        )
    }

    /// Merge (insert-or-patch) a batch of partial documents by `_id`. Returns a
    /// builder; nothing runs until `.execute(&txn)`.
    pub fn merge_many<D: Serialize>(
        &self,
        docs: impl IntoIterator<Item = D>,
    ) -> UpsertBuilder<'_, D> {
        UpsertBuilder::new(
            &self.cf,
            &self.collection,
            docs.into_iter().collect(),
            UpsertMode::Merge,
        )
    }

    /// The collection's index-management sub-handle:
    /// `indexes().create(...)` / `.remove(...)` / `.list(&txn)`.
    pub fn indexes(&self) -> Indexes<'_> {
        Indexes::new(&self.cf, &self.collection)
    }

    /// The collection's trigger sub-handle:
    /// `triggers().create(name, src)` / `.remove(name)` / `.list(&txn)`.
    pub fn triggers(&self) -> Triggers<'_> {
        Triggers::new(&self.cf, &self.collection)
    }

    /// The collection's validator sub-handle:
    /// `validators().create(name, src)` / `.remove(name)` / `.list(&txn)`.
    pub fn validators(&self) -> Validators<'_> {
        Validators::new(&self.cf, &self.collection)
    }

    /// The collection's user-defined-function sub-handle:
    /// `functions().create(name, src)` / `.remove(name)` / `.list(&txn)`.
    pub fn functions(&self) -> Functions<'_> {
        Functions::new(&self.cf, &self.collection)
    }
}

/// A column-family sub-scope, from [`Database::cf`]. Transient — name a
/// collection on it to get a [`Collection`].
pub struct CfScope {
    cf: String,
}

impl CfScope {
    /// The collection `name` within this column family.
    pub fn collection(self, name: &str) -> Collection {
        Collection {
            cf: self.cf,
            collection: name.to_string(),
        }
    }
}

impl<S: Store> Database<S> {
    /// A handle to `name` in the default column family.
    pub fn collection(&self, name: &str) -> Collection {
        Collection {
            cf: DEFAULT_CF.to_string(),
            collection: name.to_string(),
        }
    }

    /// Scope a subsequent `collection(...)` to the column family `cf`.
    pub fn cf(&self, cf: &str) -> CfScope {
        CfScope { cf: cf.to_string() }
    }
}
