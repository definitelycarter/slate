//! v2 write surface: the mutation builders and their `.execute` terminal.
//!
//! Phase 0, slice B. A write is a builder finished with a terminal: `.execute(&txn)`
//! runs it and returns a [`WriteResult`] (the affected count), while
//! `.iter_raw(&txn)` / `.iter::<T>(&txn)` run it and stream back the affected
//! *documents* (mirroring the read terminals). `.explain(&txn)` renders the
//! mutation plan without running it, and `.analyze(&txn)` runs it under
//! `EXPLAIN ANALYZE`. Each builder carries a **real, self-contained body** — it
//! lowers the request and plans it itself (via [`super::exec`]) rather than
//! calling `Transaction::update_*`/`delete_*`/`insert_many`/etc.
//!
//! Two families:
//! - **filter writes** chain off [`FindBuilder`](super::FindBuilder):
//!   `find(f).update(spec)` / `.delete()` / `.replace(doc)`. `.update`/`.delete`
//!   default to *all* matching rows; `.one()` restricts to the first.
//!   `replace` has only a single-row form (v1 has no `replace_many`).
//! - **document writes** hang directly off the [`Collection`](super::Collection):
//!   `insert_one`/`insert_many` and the bulk `upsert_many`/`merge_many` — they
//!   take documents, not a filter.

use serde::Serialize;
use serde::de::DeserializeOwned;
use slate_store::Store;

use crate::RawBson;
use crate::cursor::{CursorIter, RawCursorIter};
use crate::database::Transaction;
use crate::error::DbError;
use slate_planner::UpsertMode;

/// The outcome of a write. Carries the number of documents the mutation
/// affected — inserted, updated, replaced, deleted, or upserted — which is the
/// row count the mutation plan emits (v1 surfaces the same number by `.drain`ing
/// the cursor a mutation returns).
///
/// `#[non_exhaustive]`: future slices may surface more (e.g. the generated `_id`s
/// of an insert, which the mutation plan already yields) without a breaking
/// change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriteResult {
    /// The number of documents the write affected.
    pub affected: u64,
}

// ── Shared write cores ───────────────────────────────────────────────
//
// Each lowers one mutation to a plan. Called by both the v2 write builders
// above and the (inverted) flat `Transaction` mutation methods, so the two
// surfaces run one body. The flat method then wraps the plan in a cursor via
// [`super::exec::write_cursor`]; the builders drain it for a count.

/// Lower an `update`: select the matched rows (`take`-limited when `one`) and
/// apply the Mongo update document's assignments.
pub(crate) fn update_plan<F: Serialize, U: Serialize, S: Store>(
    cf: &str,
    collection: &str,
    filter: &F,
    update: &U,
    one: bool,
    txn: &Transaction<'_, S>,
) -> Result<slate_planner::Plan, DbError> {
    let filter_raw = bson::serialize_to_raw_document_buf(filter)?;
    let update_raw = bson::serialize_to_raw_document_buf(update)?;
    let assignments = slate_query::update_to_assignments(&update_raw)?;
    let query = super::exec::write_query(&filter_raw, one.then_some(1))?;
    let ctx = super::exec::write_context(txn, cf, collection, txn.collection_meta(cf, collection)?);
    Ok(slate_planner::plan(
        slate_planner::Statement::Update { query, assignments },
        &ctx,
    )?)
}

/// Lower a `delete`: select the matched rows (`take`-limited when `one`).
pub(crate) fn delete_plan<F: Serialize, S: Store>(
    cf: &str,
    collection: &str,
    filter: &F,
    one: bool,
    txn: &Transaction<'_, S>,
) -> Result<slate_planner::Plan, DbError> {
    let filter_raw = bson::serialize_to_raw_document_buf(filter)?;
    let query = super::exec::write_query(&filter_raw, one.then_some(1))?;
    let ctx = super::exec::write_context(txn, cf, collection, txn.collection_meta(cf, collection)?);
    Ok(slate_planner::plan(
        slate_planner::Statement::Delete { query },
        &ctx,
    )?)
}

/// Lower a `replace`: select the first matched row and swap it for `replacement`.
pub(crate) fn replace_plan<F: Serialize, R: Serialize, S: Store>(
    cf: &str,
    collection: &str,
    filter: &F,
    replacement: &R,
    txn: &Transaction<'_, S>,
) -> Result<slate_planner::Plan, DbError> {
    let filter_raw = bson::serialize_to_raw_document_buf(filter)?;
    let replacement = bson::serialize_to_raw_document_buf(replacement)?;
    // Replace targets a single document, mirroring v1's `replace_one`.
    let query = super::exec::write_query(&filter_raw, Some(1))?;
    let ctx = super::exec::write_context(txn, cf, collection, txn.collection_meta(cf, collection)?);
    Ok(slate_planner::plan(
        slate_planner::Statement::Replace { query, replacement },
        &ctx,
    )?)
}

/// Lower an `insert`: a scan-free write, so an empty meta suffices.
pub(crate) fn insert_plan<D: Serialize, S: Store>(
    cf: &str,
    collection: &str,
    docs: &[D],
    txn: &Transaction<'_, S>,
) -> Result<slate_planner::Plan, DbError> {
    let docs = serialize_docs(docs)?;
    let ctx = super::exec::write_context(
        txn,
        cf,
        collection,
        slate_planner::CollectionMeta::default(),
    );
    Ok(slate_planner::plan(
        slate_planner::Statement::Insert { docs },
        &ctx,
    )?)
}

/// Lower an `upsert`/`merge`: keyed by `_id`, so an empty meta suffices.
pub(crate) fn upsert_plan<D: Serialize, S: Store>(
    cf: &str,
    collection: &str,
    docs: &[D],
    mode: UpsertMode,
    txn: &Transaction<'_, S>,
) -> Result<slate_planner::Plan, DbError> {
    let docs = serialize_docs(docs)?;
    let ctx = super::exec::write_context(
        txn,
        cf,
        collection,
        slate_planner::CollectionMeta::default(),
    );
    Ok(slate_planner::plan(
        slate_planner::Statement::Upsert { docs, mode },
        &ctx,
    )?)
}

// ── Filter writes: update / delete / replace ─────────────────────────

/// An `update` write, built by [`FindBuilder::update`](super::FindBuilder::update).
/// Applies `spec` (a Mongo-style update document) to the matched rows. Defaults
/// to *all* matches; [`one`](Self::one) restricts it to the first.
#[must_use = "an update builder does nothing until a terminal (.execute/.explain/.analyze) runs it"]
pub struct UpdateBuilder<'a, F, U> {
    cf: &'a str,
    collection: &'a str,
    filter: F,
    update: U,
    one: bool,
}

impl<'a, F, U> UpdateBuilder<'a, F, U> {
    pub(super) fn new(cf: &'a str, collection: &'a str, filter: F, update: U) -> Self {
        Self {
            cf,
            collection,
            filter,
            update,
            one: false,
        }
    }

    /// Update only the first matching document (`= update_one`).
    pub fn one(mut self) -> Self {
        self.one = true;
        self
    }
}

impl<F: Serialize, U: Serialize> UpdateBuilder<'_, F, U> {
    fn build_plan<S: Store>(
        &self,
        txn: &Transaction<'_, S>,
    ) -> Result<slate_planner::Plan, DbError> {
        update_plan(
            self.cf,
            self.collection,
            &self.filter,
            &self.update,
            self.one,
            txn,
        )
    }

    /// Run the update, returning how many documents it changed.
    pub fn execute<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<WriteResult, DbError> {
        Ok(WriteResult {
            affected: super::exec::execute_write(self.build_plan(txn)?, txn)?,
        })
    }

    /// Run the update and stream the affected documents as raw BSON
    /// (`RawDocumentBuf`) — the documents this mutation wrote, the raw counterpart
    /// of [`iter`](Self::iter). Like the read terminals it's just an [`Iterator`],
    /// so `collect`/`count`/`next` come from the standard library.
    pub fn iter_raw<'t, 'db, S>(
        &self,
        txn: &'t Transaction<'db, S>,
    ) -> Result<RawCursorIter<'t>, DbError>
    where
        S: Store + 'db,
    {
        super::exec::write_cursor(self.build_plan(txn)?, txn)?.iter_raw()
    }

    /// Run the update and stream the affected documents deserialized into `T` (the
    /// typed counterpart of [`iter_raw`](Self::iter_raw)).
    pub fn iter<'t, 'db, T>(
        &self,
        txn: &'t Transaction<'db, impl Store + 'db>,
    ) -> Result<CursorIter<'t, T>, DbError>
    where
        T: DeserializeOwned,
    {
        super::exec::write_cursor(self.build_plan(txn)?, txn)?.iter::<T>()
    }

    /// Render the mutation plan without running it.
    pub fn explain<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<String, DbError> {
        Ok(self.build_plan(txn)?.explain())
    }

    /// Run the update and render its plan annotated with per-node actuals.
    pub fn analyze<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<String, DbError> {
        super::exec::analyze_plan(self.build_plan(txn)?, None, txn)
    }
}

/// A `delete` write, built by [`FindBuilder::delete`](super::FindBuilder::delete).
/// Removes the matched rows. Defaults to *all* matches; [`one`](Self::one)
/// restricts it to the first.
#[must_use = "a delete builder does nothing until a terminal (.execute/.explain/.analyze) runs it"]
pub struct DeleteBuilder<'a, F> {
    cf: &'a str,
    collection: &'a str,
    filter: F,
    one: bool,
}

impl<'a, F> DeleteBuilder<'a, F> {
    pub(super) fn new(cf: &'a str, collection: &'a str, filter: F) -> Self {
        Self {
            cf,
            collection,
            filter,
            one: false,
        }
    }

    /// Delete only the first matching document (`= delete_one`).
    pub fn one(mut self) -> Self {
        self.one = true;
        self
    }
}

impl<F: Serialize> DeleteBuilder<'_, F> {
    fn build_plan<S: Store>(
        &self,
        txn: &Transaction<'_, S>,
    ) -> Result<slate_planner::Plan, DbError> {
        delete_plan(self.cf, self.collection, &self.filter, self.one, txn)
    }

    /// Run the delete, returning how many documents it removed.
    pub fn execute<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<WriteResult, DbError> {
        Ok(WriteResult {
            affected: super::exec::execute_write(self.build_plan(txn)?, txn)?,
        })
    }

    /// Run the delete and stream the removed documents as raw BSON
    /// (`RawDocumentBuf`) — the raw counterpart of [`iter`](Self::iter). Like the
    /// read terminals it's just an [`Iterator`], so `collect`/`count`/`next` come
    /// from the standard library.
    pub fn iter_raw<'t, 'db, S>(
        &self,
        txn: &'t Transaction<'db, S>,
    ) -> Result<RawCursorIter<'t>, DbError>
    where
        S: Store + 'db,
    {
        super::exec::write_cursor(self.build_plan(txn)?, txn)?.iter_raw()
    }

    /// Run the delete and stream the removed documents deserialized into `T` (the
    /// typed counterpart of [`iter_raw`](Self::iter_raw)).
    pub fn iter<'t, 'db, T>(
        &self,
        txn: &'t Transaction<'db, impl Store + 'db>,
    ) -> Result<CursorIter<'t, T>, DbError>
    where
        T: DeserializeOwned,
    {
        super::exec::write_cursor(self.build_plan(txn)?, txn)?.iter::<T>()
    }

    /// Render the mutation plan without running it.
    pub fn explain<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<String, DbError> {
        Ok(self.build_plan(txn)?.explain())
    }

    /// Run the delete and render its plan annotated with per-node actuals.
    pub fn analyze<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<String, DbError> {
        super::exec::analyze_plan(self.build_plan(txn)?, None, txn)
    }
}

/// A `replace` write, built by
/// [`FindBuilder::replace`](super::FindBuilder::replace). Replaces the first
/// matched document with `replacement` entirely (no field merge), preserving its
/// primary key. There is only a single-row form, matching v1's `replace_one`, so
/// it has no `.one()`.
#[must_use = "a replace builder does nothing until a terminal (.execute/.explain/.analyze) runs it"]
pub struct ReplaceBuilder<'a, F, R> {
    cf: &'a str,
    collection: &'a str,
    filter: F,
    replacement: R,
}

impl<'a, F, R> ReplaceBuilder<'a, F, R> {
    pub(super) fn new(cf: &'a str, collection: &'a str, filter: F, replacement: R) -> Self {
        Self {
            cf,
            collection,
            filter,
            replacement,
        }
    }
}

impl<F: Serialize, R: Serialize> ReplaceBuilder<'_, F, R> {
    fn build_plan<S: Store>(
        &self,
        txn: &Transaction<'_, S>,
    ) -> Result<slate_planner::Plan, DbError> {
        replace_plan(
            self.cf,
            self.collection,
            &self.filter,
            &self.replacement,
            txn,
        )
    }

    /// Run the replace, returning how many documents it replaced (0 or 1).
    pub fn execute<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<WriteResult, DbError> {
        Ok(WriteResult {
            affected: super::exec::execute_write(self.build_plan(txn)?, txn)?,
        })
    }

    /// Run the replace and stream the replaced document as raw BSON
    /// (`RawDocumentBuf`) — the raw counterpart of [`iter`](Self::iter). Like the
    /// read terminals it's just an [`Iterator`], so `collect`/`count`/`next` come
    /// from the standard library.
    pub fn iter_raw<'t, 'db, S>(
        &self,
        txn: &'t Transaction<'db, S>,
    ) -> Result<RawCursorIter<'t>, DbError>
    where
        S: Store + 'db,
    {
        super::exec::write_cursor(self.build_plan(txn)?, txn)?.iter_raw()
    }

    /// Run the replace and stream the replaced document deserialized into `T` (the
    /// typed counterpart of [`iter_raw`](Self::iter_raw)).
    pub fn iter<'t, 'db, T>(
        &self,
        txn: &'t Transaction<'db, impl Store + 'db>,
    ) -> Result<CursorIter<'t, T>, DbError>
    where
        T: DeserializeOwned,
    {
        super::exec::write_cursor(self.build_plan(txn)?, txn)?.iter::<T>()
    }

    /// Render the mutation plan without running it.
    pub fn explain<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<String, DbError> {
        Ok(self.build_plan(txn)?.explain())
    }

    /// Run the replace and render its plan annotated with per-node actuals.
    pub fn analyze<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<String, DbError> {
        super::exec::analyze_plan(self.build_plan(txn)?, None, txn)
    }
}

// ── Document writes: insert / upsert / merge ─────────────────────────

/// An `insert` write, built by [`Collection::insert_one`](super::Collection::insert_one)
/// / [`insert_many`](super::Collection::insert_many). Inserts each document
/// (generating an `_id` when absent), failing on a duplicate key.
#[must_use = "an insert builder does nothing until a terminal (.execute/.explain/.analyze) runs it"]
pub struct InsertBuilder<'a, D> {
    cf: &'a str,
    collection: &'a str,
    docs: Vec<D>,
}

impl<'a, D> InsertBuilder<'a, D> {
    pub(super) fn new(cf: &'a str, collection: &'a str, docs: Vec<D>) -> Self {
        Self {
            cf,
            collection,
            docs,
        }
    }
}

impl<D: Serialize> InsertBuilder<'_, D> {
    fn build_plan<S: Store>(
        &self,
        txn: &Transaction<'_, S>,
    ) -> Result<slate_planner::Plan, DbError> {
        insert_plan(self.cf, self.collection, &self.docs, txn)
    }

    /// Run the insert, returning how many documents it inserted.
    pub fn execute<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<WriteResult, DbError> {
        Ok(WriteResult {
            affected: super::exec::execute_write(self.build_plan(txn)?, txn)?,
        })
    }

    /// Run the insert and stream the inserted documents as raw BSON
    /// (`RawDocumentBuf`), each carrying its `_id` (generated when absent) — the
    /// raw counterpart of [`iter`](Self::iter). Like the read terminals it's just
    /// an [`Iterator`], so `collect`/`count`/`next` come from the standard library.
    pub fn iter_raw<'t, 'db, S>(
        &self,
        txn: &'t Transaction<'db, S>,
    ) -> Result<RawCursorIter<'t>, DbError>
    where
        S: Store + 'db,
    {
        super::exec::write_cursor(self.build_plan(txn)?, txn)?.iter_raw()
    }

    /// Run the insert and stream the inserted documents deserialized into `T` (the
    /// typed counterpart of [`iter_raw`](Self::iter_raw)).
    pub fn iter<'t, 'db, T>(
        &self,
        txn: &'t Transaction<'db, impl Store + 'db>,
    ) -> Result<CursorIter<'t, T>, DbError>
    where
        T: DeserializeOwned,
    {
        super::exec::write_cursor(self.build_plan(txn)?, txn)?.iter::<T>()
    }

    /// Render the mutation plan without running it.
    pub fn explain<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<String, DbError> {
        Ok(self.build_plan(txn)?.explain())
    }

    /// Run the insert and render its plan annotated with per-node actuals.
    pub fn analyze<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<String, DbError> {
        super::exec::analyze_plan(self.build_plan(txn)?, None, txn)
    }
}

/// An `upsert`/`merge` write, built by
/// [`Collection::upsert_many`](super::Collection::upsert_many) /
/// [`merge_many`](super::Collection::merge_many). Each document is inserted if
/// its `_id` is absent, otherwise the existing document is replaced
/// ([`UpsertMode::Replace`], from `upsert_many`) or field-merged
/// ([`UpsertMode::Merge`], from `merge_many`).
#[must_use = "an upsert builder does nothing until a terminal (.execute/.explain/.analyze) runs it"]
pub struct UpsertBuilder<'a, D> {
    cf: &'a str,
    collection: &'a str,
    docs: Vec<D>,
    mode: UpsertMode,
}

impl<'a, D> UpsertBuilder<'a, D> {
    pub(super) fn new(cf: &'a str, collection: &'a str, docs: Vec<D>, mode: UpsertMode) -> Self {
        Self {
            cf,
            collection,
            docs,
            mode,
        }
    }
}

impl<D: Serialize> UpsertBuilder<'_, D> {
    fn build_plan<S: Store>(
        &self,
        txn: &Transaction<'_, S>,
    ) -> Result<slate_planner::Plan, DbError> {
        upsert_plan(self.cf, self.collection, &self.docs, self.mode, txn)
    }

    /// Run the upsert/merge, returning how many documents it wrote.
    pub fn execute<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<WriteResult, DbError> {
        Ok(WriteResult {
            affected: super::exec::execute_write(self.build_plan(txn)?, txn)?,
        })
    }

    /// Run the upsert/merge and stream the written documents as raw BSON
    /// (`RawDocumentBuf`) — the raw counterpart of [`iter`](Self::iter). Like the
    /// read terminals it's just an [`Iterator`], so `collect`/`count`/`next` come
    /// from the standard library.
    pub fn iter_raw<'t, 'db, S>(
        &self,
        txn: &'t Transaction<'db, S>,
    ) -> Result<RawCursorIter<'t>, DbError>
    where
        S: Store + 'db,
    {
        super::exec::write_cursor(self.build_plan(txn)?, txn)?.iter_raw()
    }

    /// Run the upsert/merge and stream the written documents deserialized into `T`
    /// (the typed counterpart of [`iter_raw`](Self::iter_raw)).
    pub fn iter<'t, 'db, T>(
        &self,
        txn: &'t Transaction<'db, impl Store + 'db>,
    ) -> Result<CursorIter<'t, T>, DbError>
    where
        T: DeserializeOwned,
    {
        super::exec::write_cursor(self.build_plan(txn)?, txn)?.iter::<T>()
    }

    /// Render the mutation plan without running it.
    pub fn explain<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<String, DbError> {
        Ok(self.build_plan(txn)?.explain())
    }

    /// Run the upsert/merge and render its plan annotated with per-node actuals.
    pub fn analyze<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<String, DbError> {
        super::exec::analyze_plan(self.build_plan(txn)?, None, txn)
    }
}

/// Serialize a batch of input documents into the `RawBson::Document` list the
/// `Insert`/`Upsert` statements take.
fn serialize_docs<D: Serialize>(docs: &[D]) -> Result<Vec<RawBson>, DbError> {
    docs.iter()
        .map(|doc| {
            bson::serialize_to_raw_document_buf(doc)
                .map(RawBson::Document)
                .map_err(DbError::from)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use slate_store::MemoryStore;

    use crate::{Database, DatabaseBuilder};

    fn db_with_users() -> Database<MemoryStore> {
        let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
        let txn = db.begin(false).unwrap();
        db.collections().create("users").execute(&txn).unwrap();
        txn.commit().unwrap();
        db
    }

    fn seed_three(db: &Database<MemoryStore>) {
        let txn = db.begin(false).unwrap();
        db.collection("users")
            .insert_many(vec![
                doc! { "_id": 1, "name": "ana", "age": 30 },
                doc! { "_id": 2, "name": "bo", "age": 20 },
                doc! { "_id": 3, "name": "cy", "age": 40 },
            ])
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();
    }

    #[test]
    fn insert_then_read_back() {
        let db = db_with_users();
        let txn = db.begin(false).unwrap();
        let users = db.collection("users");

        let res = users
            .insert_many(vec![
                doc! { "_id": 1, "name": "ana" },
                doc! { "_id": 2, "name": "bo" },
            ])
            .execute(&txn)
            .unwrap();
        assert_eq!(res.affected, 2);

        // insert_one
        let res1 = users
            .insert_one(doc! { "_id": 3, "name": "cy" })
            .execute(&txn)
            .unwrap();
        assert_eq!(res1.affected, 1);

        assert_eq!(users.find(doc! {}).iter_raw(&txn).unwrap().count(), 3);
        txn.commit().unwrap();
    }

    #[test]
    fn update_many_and_one() {
        let db = db_with_users();
        seed_three(&db);
        let txn = db.begin(false).unwrap();
        let users = db.collection("users");

        // update_many: everyone over 25 gets active=true → ana(30), cy(40)
        let many = users
            .find(doc! { "age": { "$gt": 25 } })
            .update(doc! { "$set": { "active": true } })
            .execute(&txn)
            .unwrap();
        assert_eq!(many.affected, 2);

        // update_one: only the first match flips
        let one = users
            .find(doc! { "age": { "$gt": 25 } })
            .update(doc! { "$set": { "vip": true } })
            .one()
            .execute(&txn)
            .unwrap();
        assert_eq!(one.affected, 1);

        txn.commit().unwrap();
    }

    #[test]
    fn delete_one_and_many() {
        let db = db_with_users();
        seed_three(&db);
        let txn = db.begin(false).unwrap();
        let users = db.collection("users");

        // delete_one
        let one = users
            .find(doc! { "age": { "$gt": 25 } })
            .delete()
            .one()
            .execute(&txn)
            .unwrap();
        assert_eq!(one.affected, 1);
        assert_eq!(users.find(doc! {}).iter_raw(&txn).unwrap().count(), 2);

        // delete_many: remove the rest
        let many = users.find(doc! {}).delete().execute(&txn).unwrap();
        assert_eq!(many.affected, 2);
        assert_eq!(users.find(doc! {}).iter_raw(&txn).unwrap().count(), 0);

        txn.commit().unwrap();
    }

    #[test]
    fn replace_one_swaps_the_document() {
        let db = db_with_users();
        seed_three(&db);
        let txn = db.begin(false).unwrap();
        let users = db.collection("users");

        let res = users
            .find(doc! { "_id": 2 })
            .replace(doc! { "name": "bohdan", "age": 21 })
            .execute(&txn)
            .unwrap();
        assert_eq!(res.affected, 1);

        let bo = users
            .find(doc! { "_id": 2 })
            .iter_raw(&txn)
            .unwrap()
            .next()
            .transpose()
            .unwrap()
            .expect("a row");
        assert_eq!(bo.get_str("name").unwrap(), "bohdan");
        // replace is a full swap — the old `name`/`age` are gone, pk preserved
        assert_eq!(bo.get_i32("age").unwrap(), 21);

        txn.commit().unwrap();
    }

    #[test]
    fn upsert_and_merge() {
        let db = db_with_users();
        seed_three(&db);
        let txn = db.begin(false).unwrap();
        let users = db.collection("users");

        // upsert: insert id=4, replace id=1 entirely
        let up = users
            .upsert_many(vec![
                doc! { "_id": 4, "name": "di", "age": 50 },
                doc! { "_id": 1, "name": "ana2" },
            ])
            .execute(&txn)
            .unwrap();
        assert_eq!(up.affected, 2);
        // replace semantics: id=1 lost its `age`
        let ana = users
            .find(doc! { "_id": 1 })
            .iter_raw(&txn)
            .unwrap()
            .next()
            .transpose()
            .unwrap()
            .unwrap();
        assert!(ana.get("age").unwrap().is_none());

        // merge: patch id=4's name, keep its age
        let mg = users
            .merge_many(vec![doc! { "_id": 4, "name": "dina" }])
            .execute(&txn)
            .unwrap();
        assert_eq!(mg.affected, 1);
        let di = users
            .find(doc! { "_id": 4 })
            .iter_raw(&txn)
            .unwrap()
            .next()
            .transpose()
            .unwrap()
            .unwrap();
        assert_eq!(di.get_str("name").unwrap(), "dina");
        assert_eq!(di.get_i32("age").unwrap(), 50); // merge kept it

        txn.commit().unwrap();
    }

    #[test]
    fn write_explain_does_not_mutate_but_analyze_does() {
        let db = db_with_users();
        seed_three(&db);
        let txn = db.begin(false).unwrap();
        let users = db.collection("users");

        // explain renders the plan without running it — the row is untouched.
        let explained = users
            .find(doc! { "age": 30 })
            .update(doc! { "$set": { "x": 1 } })
            .explain(&txn)
            .unwrap();
        assert!(!explained.is_empty());
        let row = users
            .find(doc! { "age": 30 })
            .iter_raw(&txn)
            .unwrap()
            .next()
            .transpose()
            .unwrap()
            .unwrap();
        assert!(row.get("x").unwrap().is_none(), "explain must not mutate");

        // analyze runs the plan to capture actuals, so on a write it *executes*
        // the mutation (EXPLAIN ANALYZE on DML, like Postgres) — the change lands.
        let analyzed = users
            .find(doc! { "age": 30 })
            .update(doc! { "$set": { "x": 1 } })
            .analyze(&txn)
            .unwrap();
        assert!(!analyzed.is_empty());
        assert_ne!(explained, analyzed);
        let row = users
            .find(doc! { "age": 30 })
            .iter_raw(&txn)
            .unwrap()
            .next()
            .transpose()
            .unwrap()
            .unwrap();
        assert_eq!(row.get_i32("x").unwrap(), 1, "analyze executes the write");

        txn.commit().unwrap();
    }

    #[test]
    fn write_terminals_return_affected_docs() {
        let db = db_with_users();
        let txn = db.begin(false).unwrap();
        let users = db.collection("users");

        // insert: iter_raw streams back the inserted documents
        let inserted: Vec<bson::RawDocumentBuf> = users
            .insert_many(vec![
                doc! { "_id": 1, "name": "ana", "age": 30 },
                doc! { "_id": 2, "name": "bo", "age": 20 },
            ])
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(inserted.len(), 2);

        // insert_one without an `_id`: the returned doc carries the generated one
        let one = users
            .insert_one(doc! { "name": "cy", "age": 40 })
            .iter_raw(&txn)
            .unwrap()
            .next()
            .transpose()
            .unwrap()
            .expect("the inserted doc");
        assert!(
            one.get("_id").unwrap().is_some(),
            "insert generates an _id and hands it back"
        );

        // update: iter::<T> streams the updated documents, deserialized
        #[derive(serde::Deserialize)]
        struct User {
            active: bool,
        }
        let updated: Vec<User> = users
            .find(doc! { "age": { "$gt": 25 } })
            .update(doc! { "$set": { "active": true } })
            .iter::<User>(&txn)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(updated.len(), 2); // ana(30), cy(40)
        assert!(updated.iter().all(|u| u.active));

        // delete: iter_raw streams the removed documents
        let removed = users
            .find(doc! { "_id": 2 })
            .delete()
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].get_str("name").unwrap(), "bo");

        // the delete actually landed — bo is gone, ana + cy remain
        assert_eq!(users.find(doc! {}).iter_raw(&txn).unwrap().count(), 2);

        txn.commit().unwrap();
    }
}
