//! v2 read surface: the `query` (SQL) builder and its terminals.
//!
//! Phase 0, slice A. Like [`FindBuilder`](super::FindBuilder), each terminal
//! carries a **real, self-contained body** — it parses, validates parameters,
//! plans, and builds the [`Cursor`] itself rather than calling
//! `Transaction::query`/`lower_sql`. SQL can `SELECT VALUE <scalar>`, so the raw
//! terminals iterate result *values* ([`RawBson`](crate::RawBson)), not just
//! documents. Per the RFC the SQL builder carries no `count`/`first`/`distinct`
//! — those live inline in the SQL.

use serde::Serialize;
use slate_store::Store;

use crate::cursor::{Cursor, RawValuesIter};
use crate::database::Transaction;
use crate::error::DbError;
use crate::{RawBson, RawDocumentBuf};

/// A lazily-built SQL read: parameters bind via [`params`](Self::params); a
/// terminal runs it.
///
/// Built by [`Collection::query`](super::Collection::query). Inert until a
/// terminal — `.iter` / `.collect` consume the values, `.explain` renders the
/// plan without running it.
#[must_use = "a query builder does nothing until a terminal (.iter/.collect/.explain) runs it"]
pub struct QueryBuilder<'a, P = ()> {
    cf: &'a str,
    collection: &'a str,
    sql: &'a str,
    params: Option<P>,
}

impl<'a> QueryBuilder<'a, ()> {
    pub(super) fn new(cf: &'a str, collection: &'a str, sql: &'a str) -> Self {
        Self {
            cf,
            collection,
            sql,
            params: None,
        }
    }

    /// Bind values for the query's `@name` parameters. `params` serializes to a
    /// document whose keys are the bare names (no leading `@`).
    pub fn params<P: Serialize>(self, params: P) -> QueryBuilder<'a, P> {
        QueryBuilder {
            cf: self.cf,
            collection: self.collection,
            sql: self.sql,
            params: Some(params),
        }
    }
}

impl<P: Serialize> QueryBuilder<'_, P> {
    /// Parse, validate parameters, and plan the query — v2's own SQL body, not a
    /// call into `Transaction::query`/`lower_sql`. Returns the plan plus the
    /// serialized parameter document (if any) for the cursor.
    fn lower<S>(
        &self,
        txn: &Transaction<'_, S>,
    ) -> Result<(slate_planner::Plan, Option<RawDocumentBuf>), DbError>
    where
        S: Store,
    {
        let params_raw = match &self.params {
            Some(p) => Some(bson::serialize_to_raw_document_buf(p)?),
            None => None,
        };

        let query = slate_sql::parse(self.sql)?;

        // Every referenced `@param` must have a supplied value; the plan shape
        // never depends on a parameter's value, only its presence.
        let mut supplied: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        if let Some(doc) = &params_raw {
            for entry in doc.iter() {
                let (name, _) = entry.map_err(|err| DbError::InvalidQuery(err.to_string()))?;
                supplied.insert(name.to_string());
            }
        }
        for name in query.parameter_names() {
            if !supplied.contains(name) {
                return Err(DbError::InvalidQuery(format!(
                    "query references parameter @{name}, which was not supplied"
                )));
            }
        }

        // A FROM-less query (`SELECT VALUE 1`) reads no container, so it needs no
        // catalog metadata.
        let meta = if query.from.is_some() {
            txn.collection_meta(self.cf, self.collection)?
        } else {
            slate_planner::CollectionMeta {
                indexes: Vec::new(),
                compound_indexes: Vec::new(),
                vector_indexes: Vec::new(),
                pk_path: "_id".to_string(),
            }
        };
        let ctx = slate_planner::PlanContext {
            container: slate_planner::CollectionRef {
                cf: self.cf.to_string(),
                collection: self.collection.to_string(),
            },
            meta,
            validators: Vec::new(),
            triggers: Vec::new(),
        };
        let plan = slate_planner::plan(slate_planner::Statement::Query(query), &ctx)?;
        Ok((plan, params_raw))
    }

    fn build_cursor<'t, 'db, S>(
        &self,
        txn: &'t Transaction<'db, S>,
    ) -> Result<Cursor<'db, 't, S>, DbError>
    where
        S: Store + 'db,
    {
        let (plan, params_raw) = self.lower(txn)?;
        // `.cloned()` is an `Arc`/`Rc` refcount bump so the cursor owns its rand
        // and watch handles — the same handoff `Transaction::run_plan` makes.
        Ok(match params_raw {
            Some(params) => Cursor::new_with_params(
                txn.engine_txn(),
                plan,
                txn.pool(),
                params,
                txn.rand().cloned(),
                txn.watch_sink().cloned(),
            ),
            None => Cursor::new(
                txn.engine_txn(),
                plan,
                txn.pool(),
                txn.rand().cloned(),
                txn.watch_sink().cloned(),
            ),
        })
    }

    /// Stream the result values lazily.
    pub fn iter<'t, 'db, S>(
        self,
        txn: &'t Transaction<'db, S>,
    ) -> Result<RawValuesIter<'t>, DbError>
    where
        S: Store + 'db,
    {
        self.build_cursor(txn)?.iter_raw_values()
    }

    /// Collect all result values.
    pub fn collect<'db, S>(self, txn: &Transaction<'db, S>) -> Result<Vec<RawBson>, DbError>
    where
        S: Store + 'db,
    {
        self.build_cursor(txn)?.iter_raw_values()?.collect()
    }

    /// Render the physical plan without running the query.
    pub fn explain<S>(&self, txn: &Transaction<'_, S>) -> Result<String, DbError>
    where
        S: Store,
    {
        let (plan, _params) = self.lower(txn)?;
        Ok(plan.explain())
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use slate_store::MemoryStore;

    use crate::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};

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
    fn query_values_and_params_match_v1() {
        let db = seed();
        let txn = db.begin(true).unwrap();

        // SELECT VALUE scalar
        let sql = "SELECT VALUE c.name FROM c ORDER BY c.age";
        let v2 = db.collection("users").query(sql).collect(&txn).unwrap();
        let v1 = txn
            .query(DEFAULT_CF, "users", sql)
            .unwrap()
            .iter_raw_values()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(v2, v1);
        assert_eq!(v2.len(), 3);

        // parameterized
        let psql = "SELECT VALUE c.name FROM c WHERE c.age > @min ORDER BY c.age";
        let v2p = db
            .collection("users")
            .query(psql)
            .params(doc! { "min": 25 })
            .collect(&txn)
            .unwrap();
        let v1p = txn
            .query_with_params(DEFAULT_CF, "users", psql, doc! { "min": 25 })
            .unwrap()
            .iter_raw_values()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(v2p, v1p);
        assert_eq!(v2p.len(), 2);
    }

    #[test]
    fn query_explain_matches_v1() {
        let db = seed();
        let txn = db.begin(true).unwrap();
        let sql = "SELECT VALUE c.name FROM c WHERE c.age > 25";

        let v2 = db.collection("users").query(sql).explain(&txn).unwrap();
        let v1 = txn.explain(DEFAULT_CF, "users", sql).unwrap();
        assert_eq!(v2, v1);
        assert!(!v2.is_empty());
    }

    #[test]
    fn missing_param_is_rejected() {
        let db = seed();
        let txn = db.begin(true).unwrap();
        let err = db
            .collection("users")
            .query("SELECT VALUE c.name FROM c WHERE c.age > @min")
            .collect(&txn);
        assert!(err.is_err());
    }
}
