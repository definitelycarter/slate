//! v2 read surface: the `find` builder and its cursor terminals.
//!
//! Phase 0, slice A. Each terminal carries a **real, self-contained body** — it
//! lowers the filter, plans it, and constructs a [`Cursor`] directly; it does
//! *not* call `Transaction::find`/`count`/`run_plan`. Reads are raw BSON for now
//! (`RawDocumentBuf`); typed reads (generic `T`) are Decision 9 / phase 2.

use std::sync::Arc;

use serde::Serialize;
use slate_store::Store;

use crate::cursor::{Cursor, RawCursorIter};
use crate::database::Transaction;
use crate::error::DbError;
use crate::watch::{DEFAULT_STREAM_CAPACITY, WatchStream};
use crate::{ChangeEvent, FindOptions, RawDocumentBuf, Sort, SortDirection};
use crate::{WatchHandle, WatchRegistry};

/// A lazily-built `find` read: stages reshape it, a terminal runs it.
///
/// Built by [`Collection::find`](super::Collection::find). Inert until a terminal
/// is called — `.iter` / `.collect` / `.count` / `.first`, each taking the
/// transaction the read runs in, or the reactive `.watch` / `.stream`, which
/// register a subscription with no transaction.
#[must_use = "a find builder does nothing until a terminal (.iter/.collect/.count/.first/.watch/.stream) runs it"]
pub struct FindBuilder<'a, F> {
    cf: &'a str,
    collection: &'a str,
    /// The database's watch registry (borrowed from the [`Collection`]), used only
    /// by the reactive terminals.
    watch: &'a Arc<WatchRegistry>,
    filter: F,
    options: FindOptions,
}

impl<'a, F> FindBuilder<'a, F> {
    pub(super) fn new(
        cf: &'a str,
        collection: &'a str,
        watch: &'a Arc<WatchRegistry>,
        filter: F,
    ) -> Self {
        Self {
            cf,
            collection,
            watch,
            filter,
            options: FindOptions::default(),
        }
    }

    /// Order the result by `field` (chainable; later calls append further keys).
    pub fn sort(mut self, field: &str, direction: SortDirection) -> Self {
        self.options.sort.push(Sort {
            field: field.to_string(),
            direction,
        });
        self
    }

    /// Skip the first `n` rows (SQL `OFFSET`).
    pub fn offset(mut self, n: usize) -> Self {
        self.options.skip = Some(n);
        self
    }

    /// Cap the result at `n` rows (SQL `LIMIT`).
    pub fn limit(mut self, n: usize) -> Self {
        self.options.take = Some(n);
        self
    }

    /// Return only `fields` from each matched document.
    pub fn project(mut self, fields: impl IntoIterator<Item = String>) -> Self {
        self.options.columns = Some(fields.into_iter().collect());
        self
    }

    /// Narrow the read to the distinct values of `field`. The filter becomes the
    /// predicate; reshape the values with the returned builder's own
    /// `.sort`/`.offset`/`.limit` (find-level stages don't carry over).
    pub fn distinct(self, field: &str) -> super::DistinctBuilder<'a, F> {
        super::DistinctBuilder::new(self.cf, self.collection, field, self.filter)
    }

    /// Apply a Mongo-style `update` to the matched documents. Defaults to *all*
    /// matches; chain [`one`](super::UpdateBuilder::one) for the first only. Any
    /// read stages set above (sort/limit/project) don't apply to a write — the
    /// filter is what carries over.
    pub fn update<U>(self, update: U) -> super::UpdateBuilder<'a, F, U> {
        super::UpdateBuilder::new(self.cf, self.collection, self.filter, update)
    }

    /// Delete the matched documents. Defaults to *all* matches; chain
    /// [`one`](super::DeleteBuilder::one) for the first only.
    pub fn delete(self) -> super::DeleteBuilder<'a, F> {
        super::DeleteBuilder::new(self.cf, self.collection, self.filter)
    }

    /// Replace the first matched document with `replacement` entirely (no merge),
    /// preserving its primary key.
    pub fn replace<R>(self, replacement: R) -> super::ReplaceBuilder<'a, F, R> {
        super::ReplaceBuilder::new(self.cf, self.collection, self.filter, replacement)
    }
}

/// The shared `find` read core: lower the filter to a query plan. Called by both
/// the v2 [`FindBuilder`] terminals and the (inverted) flat `Transaction::find`,
/// so the two surfaces run one body. Shares only `collection_meta` (a catalog
/// read) with the rest of the transaction.
pub(crate) fn find_plan<F: Serialize, S: Store>(
    cf: &str,
    collection: &str,
    filter: &F,
    options: &FindOptions,
    txn: &Transaction<'_, S>,
) -> Result<slate_planner::Plan, DbError> {
    let filter_raw = bson::serialize_to_raw_document_buf(filter)?;
    let query = slate_query::find_to_query(&filter_raw, options)?;
    let ctx = slate_planner::PlanContext {
        container: slate_planner::CollectionRef {
            cf: cf.to_string(),
            collection: collection.to_string(),
        },
        meta: txn.collection_meta(cf, collection)?,
        validators: Vec::new(),
        triggers: Vec::new(),
    };
    Ok(slate_planner::plan(
        slate_planner::Statement::Query(query),
        &ctx,
    )?)
}

/// The shared `find` read core: lower and wrap in a [`Cursor`]. The cursor's
/// `.cloned()` rand/watch handles are `Arc`/`Rc` refcount bumps, the same handoff
/// the v1 read path made.
pub(crate) fn find_cursor<'t, 'db, F, S>(
    cf: &str,
    collection: &str,
    filter: &F,
    options: &FindOptions,
    txn: &'t Transaction<'db, S>,
) -> Result<Cursor<'db, 't, S>, DbError>
where
    F: Serialize,
    S: Store + 'db,
{
    let plan = find_plan(cf, collection, filter, options, txn)?;
    Ok(Cursor::new(
        txn.engine_txn(),
        plan,
        txn.pool(),
        txn.rand().cloned(),
        txn.watch_sink().cloned(),
    ))
}

impl<F: Serialize> FindBuilder<'_, F> {
    fn build_plan<S: Store>(
        &self,
        txn: &Transaction<'_, S>,
    ) -> Result<slate_planner::Plan, DbError> {
        find_plan(self.cf, self.collection, &self.filter, &self.options, txn)
    }

    fn build_cursor<'t, 'db, S>(
        &self,
        txn: &'t Transaction<'db, S>,
    ) -> Result<Cursor<'db, 't, S>, DbError>
    where
        S: Store + 'db,
    {
        find_cursor(self.cf, self.collection, &self.filter, &self.options, txn)
    }

    /// Stream the matching documents lazily.
    pub fn iter<'t, 'db, S>(
        self,
        txn: &'t Transaction<'db, S>,
    ) -> Result<RawCursorIter<'t>, DbError>
    where
        S: Store + 'db,
    {
        self.build_cursor(txn)?.iter_raw()
    }

    /// Collect all matching documents.
    pub fn collect<'db, S>(self, txn: &Transaction<'db, S>) -> Result<Vec<RawDocumentBuf>, DbError>
    where
        S: Store + 'db,
    {
        self.build_cursor(txn)?.iter_raw()?.collect()
    }

    /// Count the matching documents (honors `limit`/`offset` if set).
    pub fn count<'db, S>(self, txn: &Transaction<'db, S>) -> Result<u64, DbError>
    where
        S: Store + 'db,
    {
        self.build_cursor(txn)?.drain()
    }

    /// Return the first matching document, if any (`find_one`). Honors any
    /// `sort` set on the builder.
    pub fn first<'db, S>(
        mut self,
        txn: &Transaction<'db, S>,
    ) -> Result<Option<RawDocumentBuf>, DbError>
    where
        S: Store + 'db,
    {
        self.options.take = Some(1);
        self.build_cursor(txn)?.iter_raw()?.next().transpose()
    }

    /// Render the physical plan without running it. There is no `EXPLAIN`
    /// keyword for a Mongo-style `find`, so this is the only way to see its plan
    /// (v1 can only explain SQL strings).
    pub fn explain<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<String, DbError> {
        Ok(self.build_plan(txn)?.explain())
    }

    /// Run the read and render its plan annotated with per-node actuals
    /// (`EXPLAIN ANALYZE`). Like [`explain`](Self::explain), the only way to get
    /// it for a `find`.
    pub fn analyze<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<String, DbError> {
        super::exec::analyze_plan(self.build_plan(txn)?, None, txn)
    }

    /// Reactive terminals are **filter-only**: a `find` reshaped with
    /// `sort`/`offset`/`limit`/`project` has no meaning as a change subscription,
    /// so reject it at runtime (phase 1). An untouched `find(filter)` passes.
    fn ensure_filter_only(&self) -> Result<(), DbError> {
        if self.options.sort.is_empty()
            && self.options.skip.is_none()
            && self.options.take.is_none()
            && self.options.columns.is_none()
        {
            Ok(())
        } else {
            Err(DbError::InvalidQuery(
                "watch/stream are filter-only: drop sort/offset/limit/project".to_string(),
            ))
        }
    }

    /// Register a **push** subscription: `callback` fires once per committed
    /// transaction with the batch of changes matching this filter, recast against
    /// the filter's set boundary (enter → `Insert`, leave → `Delete`, modified
    /// in-set → `Update`). Takes no transaction — the returned [`WatchHandle`]
    /// unregisters on drop. Filter-only (see [`ensure_filter_only`](Self::ensure_filter_only)).
    pub fn watch(
        self,
        callback: impl Fn(&[ChangeEvent]) + Send + Sync + 'static,
    ) -> Result<WatchHandle, DbError> {
        self.ensure_filter_only()?;
        let filter_raw = bson::serialize_to_raw_document_buf(&self.filter)?;
        WatchRegistry::watch_bson(
            self.watch,
            self.cf,
            self.collection,
            &filter_raw,
            Arc::new(callback),
        )
    }

    /// Open a **pull** subscription: a long-lived [`WatchStream`] the consumer
    /// drains on its own thread (non-blocking lag-drop, so a slow consumer never
    /// blocks the writer). Same filter/set-transition semantics as
    /// [`watch`](Self::watch); takes no transaction, unregisters on drop.
    /// Filter-only.
    pub fn stream(self) -> Result<WatchStream, DbError> {
        self.ensure_filter_only()?;
        let filter_raw = bson::serialize_to_raw_document_buf(&self.filter)?;
        WatchRegistry::stream_bson(
            self.watch,
            self.cf,
            self.collection,
            &filter_raw,
            DEFAULT_STREAM_CAPACITY,
        )
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use slate_store::MemoryStore;

    use crate::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder, SortDirection};

    fn seed() -> Database<MemoryStore> {
        let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
        let txn = db.begin(false).unwrap();
        txn.create_collection(&CollectionConfig {
            name: "users".to_string(),
            ..Default::default()
        })
        .unwrap();
        txn.insert_many(
            DEFAULT_CF,
            "users",
            vec![
                doc! { "_id": 1, "name": "ana", "age": 30 },
                doc! { "_id": 2, "name": "bo", "age": 20 },
                doc! { "_id": 3, "name": "cy", "age": 40 },
            ],
        )
        .unwrap()
        .drain()
        .unwrap();
        txn.commit().unwrap();
        db
    }

    #[test]
    fn collect_count_first_and_stages() {
        let db = seed();
        let txn = db.begin(true).unwrap();
        let users = db.collection("users");

        // collect all
        assert_eq!(users.find(doc! {}).collect(&txn).unwrap().len(), 3);

        // count with a filter
        let n = users
            .find(doc! { "age": { "$gt": 25 } })
            .count(&txn)
            .unwrap();
        assert_eq!(n, 2);

        // first, honoring sort
        let oldest = users
            .find(doc! {})
            .sort("age", SortDirection::Desc)
            .first(&txn)
            .unwrap()
            .expect("a row");
        assert_eq!(oldest.get_str("name").unwrap(), "cy");

        // sort + limit
        let two = users
            .find(doc! {})
            .sort("age", SortDirection::Asc)
            .limit(2)
            .collect(&txn)
            .unwrap();
        assert_eq!(two.len(), 2);
        assert_eq!(two[0].get_str("name").unwrap(), "bo");

        // offset
        let skipped = users
            .find(doc! {})
            .sort("age", SortDirection::Asc)
            .offset(1)
            .collect(&txn)
            .unwrap();
        assert_eq!(skipped.len(), 2);
        assert_eq!(skipped[0].get_str("name").unwrap(), "ana");
    }

    #[test]
    fn find_explain_and_analyze_render_a_plan() {
        let db = seed();
        let txn = db.begin(true).unwrap();
        let users = db.collection("users");

        let explained = users
            .find(doc! { "age": { "$gt": 25 } })
            .explain(&txn)
            .unwrap();
        assert!(!explained.is_empty());

        let analyzed = users
            .find(doc! { "age": { "$gt": 25 } })
            .analyze(&txn)
            .unwrap();
        assert!(!analyzed.is_empty());
        // analyze annotates with actual counts that plain explain lacks
        assert_ne!(explained, analyzed);
    }

    #[test]
    fn find_watch_fires_on_matching_commit() {
        use std::sync::{Arc, Mutex};

        use crate::ChangeEvent;

        let db = seed();
        let batches: Arc<Mutex<Vec<Vec<ChangeEvent>>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&batches);
        // register on the db-rooted handle — no transaction
        let _handle = db
            .collection("users")
            .find(doc! { "age": { "$gt": 25 } })
            .watch(move |events| sink.lock().unwrap().push(events.to_vec()))
            .unwrap();

        // a matching insert in a later commit fires the callback inline
        let txn = db.begin(false).unwrap();
        db.collection("users")
            .insert_one(doc! { "_id": 9, "name": "zoe", "age": 50 })
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();

        let b = batches.lock().unwrap();
        assert_eq!(b.len(), 1, "one matching commit → one batch");
        match &b[0][0] {
            ChangeEvent::Insert { doc } => assert_eq!(doc.get_str("name").unwrap(), "zoe"),
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn find_stream_drains_matching_batches() {
        let db = seed();
        let stream = db
            .collection("users")
            .find(doc! { "age": { "$gt": 25 } })
            .stream()
            .unwrap();

        let txn = db.begin(false).unwrap();
        db.collection("users")
            .insert_one(doc! { "_id": 9, "age": 50 })
            .execute(&txn)
            .unwrap();
        // a non-matching write contributes no batch
        db.collection("users")
            .insert_one(doc! { "_id": 10, "age": 5 })
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();

        let mut batches = Vec::new();
        while let Some(batch) = stream.try_next() {
            batches.push(batch);
        }
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        assert!(!stream.lagged());
    }

    #[test]
    fn watch_and_stream_reject_non_filter_stages() {
        use crate::ChangeEvent;

        let db = seed();
        // sort/offset/limit/project make no sense for a subscription → runtime error
        let w = db
            .collection("users")
            .find(doc! {})
            .sort("age", SortDirection::Asc)
            .watch(|_: &[ChangeEvent]| {});
        assert!(w.is_err());

        let s = db.collection("users").find(doc! {}).limit(5).stream();
        assert!(s.is_err());
    }
}
