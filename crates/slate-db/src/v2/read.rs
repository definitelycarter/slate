//! v2 read surface: the `find` builder and its cursor terminals.
//!
//! Phase 0, slice A. Each terminal carries a **real, self-contained body** — it
//! lowers the filter, plans it, and constructs a [`Cursor`] directly; it does
//! *not* call `Transaction::find`/`count`/`run_plan`. Reads are raw BSON for now
//! (`RawDocumentBuf`); typed reads (generic `T`) are Decision 9 / phase 2.

use serde::Serialize;
use slate_store::Store;

use crate::cursor::{Cursor, RawCursorIter};
use crate::database::Transaction;
use crate::error::DbError;
use crate::{FindOptions, RawDocumentBuf, Sort, SortDirection};

/// A lazily-built `find` read: stages reshape it, a terminal runs it.
///
/// Built by [`Collection::find`](super::Collection::find). Inert until a terminal
/// is called — `.iter` / `.collect` / `.count` / `.first`, each taking the
/// transaction the read runs in.
#[must_use = "a find builder does nothing until a terminal (.iter/.collect/.count/.first) runs it"]
pub struct FindBuilder<'a, F> {
    cf: &'a str,
    collection: &'a str,
    filter: F,
    options: FindOptions,
}

impl<'a, F> FindBuilder<'a, F> {
    pub(super) fn new(cf: &'a str, collection: &'a str, filter: F) -> Self {
        Self {
            cf,
            collection,
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
}

impl<F: Serialize> FindBuilder<'_, F> {
    /// Lower the filter, plan it, and build the cursor — v2's own read body, not
    /// a call into `Transaction::find`. Shares only the engine transaction and
    /// `collection_meta` (a catalog read).
    fn build_cursor<'t, 'db, S>(
        &self,
        txn: &'t Transaction<'db, S>,
    ) -> Result<Cursor<'db, 't, S>, DbError>
    where
        S: Store + 'db,
    {
        let filter_raw = bson::serialize_to_raw_document_buf(&self.filter)?;
        let query = slate_query::find_to_query(&filter_raw, &self.options)?;
        let ctx = slate_planner::PlanContext {
            container: slate_planner::CollectionRef {
                cf: self.cf.to_string(),
                collection: self.collection.to_string(),
            },
            meta: txn.collection_meta(self.cf, self.collection)?,
            validators: Vec::new(),
            triggers: Vec::new(),
        };
        let plan = slate_planner::plan(slate_planner::Statement::Query(query), &ctx)?;
        // `.cloned()` is an `Arc`/`Rc` refcount bump so the cursor owns its rand
        // and watch handles — the same handoff `Transaction::run_plan` makes.
        Ok(Cursor::new(
            txn.engine_txn(),
            plan,
            txn.pool(),
            txn.rand().cloned(),
            txn.watch_sink().cloned(),
        ))
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
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use slate_store::MemoryStore;

    use crate::{
        CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder, FindOptions, Sort, SortDirection,
    };

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
    fn v2_find_matches_v1() {
        let db = seed();
        let txn = db.begin(true).unwrap();

        let v2 = db
            .collection("users")
            .find(doc! { "age": { "$gte": 30 } })
            .sort("age", SortDirection::Asc)
            .collect(&txn)
            .unwrap();

        let v1 = txn
            .find(
                DEFAULT_CF,
                "users",
                doc! { "age": { "$gte": 30 } },
                FindOptions {
                    sort: vec![Sort {
                        field: "age".to_string(),
                        direction: SortDirection::Asc,
                    }],
                    ..Default::default()
                },
            )
            .unwrap()
            .iter_raw()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(v2, v1);
    }

    #[test]
    fn project_narrows_fields_and_matches_v1() {
        let db = seed();
        let txn = db.begin(true).unwrap();

        let v2 = db
            .collection("users")
            .find(doc! {})
            .sort("age", SortDirection::Asc)
            .project(["name".to_string()])
            .collect(&txn)
            .unwrap();

        let v1 = txn
            .find(
                DEFAULT_CF,
                "users",
                doc! {},
                FindOptions {
                    sort: vec![Sort {
                        field: "age".to_string(),
                        direction: SortDirection::Asc,
                    }],
                    columns: Some(vec!["name".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap()
            .iter_raw()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(v2, v1);
        // projecting "name" keeps it and drops the unprojected "age"
        assert_eq!(v2.len(), 3);
        assert!(v2[0].get_str("name").is_ok());
        assert!(v2[0].get("age").unwrap().is_none());
    }
}
