//! v2 read surface: the `distinct` builder.
//!
//! Phase 0, slice A. Reached as a `find` stage —
//! [`FindBuilder::distinct`](super::FindBuilder::distinct) — it narrows the read
//! to the distinct values of one field (the find filter becomes the predicate).
//! It is value-oriented, so its `.sort` takes a [`SortDirection`] (the values are
//! scalars), and `.offset`/`.limit` page the value list; the terminals
//! (`.iter`/`.collect`/`.count`/`.first`) yield [`RawBson`](crate::RawBson).
//!
//! Real, self-contained body: builds the `Distinct` statement and cursor itself.

use serde::Serialize;
use slate_store::Store;

use crate::cursor::{Cursor, RawValuesIter};
use crate::database::Transaction;
use crate::error::DbError;
use crate::{RawBson, SortDirection};

/// A lazily-built distinct-values read. Built by
/// [`FindBuilder::distinct`](super::FindBuilder::distinct); inert until a
/// terminal runs it.
#[must_use = "a distinct builder does nothing until a terminal (.iter/.collect/.count/.first) runs it"]
pub struct DistinctBuilder<'a, F> {
    cf: &'a str,
    collection: &'a str,
    field: String,
    filter: F,
    sort: Option<SortDirection>,
    skip: Option<usize>,
    take: Option<usize>,
}

impl<'a, F> DistinctBuilder<'a, F> {
    pub(super) fn new(cf: &'a str, collection: &'a str, field: &str, filter: F) -> Self {
        Self {
            cf,
            collection,
            field: field.to_string(),
            filter,
            sort: None,
            skip: None,
            take: None,
        }
    }

    /// Order the distinct values ascending or descending.
    pub fn sort(mut self, direction: SortDirection) -> Self {
        self.sort = Some(direction);
        self
    }

    /// Skip the first `n` distinct values (SQL `OFFSET`).
    pub fn offset(mut self, n: usize) -> Self {
        self.skip = Some(n);
        self
    }

    /// Cap the result at `n` distinct values (SQL `LIMIT`).
    pub fn limit(mut self, n: usize) -> Self {
        self.take = Some(n);
        self
    }
}

impl<F: Serialize> DistinctBuilder<'_, F> {
    /// Build the `Distinct` plan and cursor — v2's own body, not a call into
    /// `Transaction::distinct`. Distinct scans the container directly, so it needs
    /// no index metadata (`CollectionMeta::default()`).
    fn build_cursor<'t, 'db, S>(
        self,
        txn: &'t Transaction<'db, S>,
    ) -> Result<Cursor<'db, 't, S>, DbError>
    where
        S: Store + 'db,
    {
        let filter_raw = bson::serialize_to_raw_document_buf(&self.filter)?;
        let predicate = slate_query::translate_filter(&filter_raw)?;
        let sort = self.sort.map(|dir| match dir {
            SortDirection::Asc => slate_ast::SortDirection::Asc,
            SortDirection::Desc => slate_ast::SortDirection::Desc,
        });
        let stmt = slate_planner::Statement::Distinct {
            alias: slate_query::ALIAS.to_string(),
            field: self.field,
            predicate,
            sort,
            skip: self.skip.map(|n| n as u64),
            take: self.take.map(|n| n as u64),
        };
        let ctx = slate_planner::PlanContext {
            container: slate_planner::CollectionRef {
                cf: self.cf.to_string(),
                collection: self.collection.to_string(),
            },
            meta: slate_planner::CollectionMeta::default(),
            validators: Vec::new(),
            triggers: Vec::new(),
        };
        let plan = slate_planner::plan(stmt, &ctx)?;
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

    /// Stream the distinct values lazily.
    pub fn iter<'t, 'db, S>(
        self,
        txn: &'t Transaction<'db, S>,
    ) -> Result<RawValuesIter<'t>, DbError>
    where
        S: Store + 'db,
    {
        self.build_cursor(txn)?.iter_raw_values()
    }

    /// Collect all distinct values.
    pub fn collect<'db, S>(self, txn: &Transaction<'db, S>) -> Result<Vec<RawBson>, DbError>
    where
        S: Store + 'db,
    {
        self.build_cursor(txn)?.iter_raw_values()?.collect()
    }

    /// Count the distinct values.
    pub fn count<'db, S>(self, txn: &Transaction<'db, S>) -> Result<u64, DbError>
    where
        S: Store + 'db,
    {
        self.build_cursor(txn)?.drain()
    }

    /// Return the first distinct value, if any (honors `sort`).
    pub fn first<'db, S>(mut self, txn: &Transaction<'db, S>) -> Result<Option<RawBson>, DbError>
    where
        S: Store + 'db,
    {
        self.take = Some(1);
        self.build_cursor(txn)?
            .iter_raw_values()?
            .next()
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use bson::{RawBson, doc};
    use slate_store::MemoryStore;

    use crate::{
        CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder, DistinctOptions, SortDirection,
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
                doc! { "_id": 4, "name": "di", "age": 30 },
            ],
        )
        .unwrap()
        .drain()
        .unwrap();
        txn.commit().unwrap();
        db
    }

    #[test]
    fn distinct_values_sort_count_first() {
        let db = seed();
        let txn = db.begin(true).unwrap();
        let users = db.collection("users");

        // distinct ages, sorted ascending: {20, 30, 40}
        let ages = users
            .find(doc! {})
            .distinct("age")
            .sort(SortDirection::Asc)
            .collect(&txn)
            .unwrap();
        assert_eq!(
            ages,
            vec![RawBson::Int32(20), RawBson::Int32(30), RawBson::Int32(40)]
        );

        // count of distinct values
        let n = users.find(doc! {}).distinct("age").count(&txn).unwrap();
        assert_eq!(n, 3);

        // first, descending
        let top = users
            .find(doc! {})
            .distinct("age")
            .sort(SortDirection::Desc)
            .first(&txn)
            .unwrap();
        assert_eq!(top, Some(RawBson::Int32(40)));

        // with a filter
        let filtered = users
            .find(doc! { "age": { "$gte": 30 } })
            .distinct("age")
            .sort(SortDirection::Asc)
            .collect(&txn)
            .unwrap();
        assert_eq!(filtered, vec![RawBson::Int32(30), RawBson::Int32(40)]);
    }

    #[test]
    fn distinct_count_matches_v1() {
        let db = seed();
        let txn = db.begin(true).unwrap();

        let v2 = db
            .collection("users")
            .find(doc! {})
            .distinct("age")
            .count(&txn)
            .unwrap();

        // v1 gathers distinct values into one array; its length is the count.
        let v1 = txn
            .distinct(
                DEFAULT_CF,
                "users",
                "age",
                doc! {},
                DistinctOptions::default(),
            )
            .unwrap();
        let v1_len = match v1 {
            RawBson::Array(arr) => arr.into_iter().count() as u64,
            _ => panic!("expected an array"),
        };
        assert_eq!(v2, v1_len);
    }
}
