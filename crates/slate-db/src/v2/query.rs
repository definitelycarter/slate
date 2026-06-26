//! v2 read surface: the `query` (SQL) builder and its terminals.
//!
//! Phase 0, slice A. Like [`FindBuilder`](super::FindBuilder), each terminal
//! carries a **real, self-contained body** — it parses, validates parameters,
//! plans, and builds the [`Cursor`] itself rather than calling
//! `Transaction::query`/`lower_sql`. SQL can `SELECT VALUE <scalar>`, so the raw
//! terminals iterate result *values* ([`RawBson`](crate::RawBson)), not just
//! documents. Per the RFC the SQL builder carries no `count`/`first`/`distinct`
//! — those live inline in the SQL.

use std::sync::Arc;

use serde::Serialize;
use slate_store::Store;

use crate::cursor::{Cursor, RawValuesIter};
use crate::database::Transaction;
use crate::error::DbError;
use crate::watch::{DEFAULT_STREAM_CAPACITY, WatchStream};
use crate::{ChangeEvent, RawBson, RawDocumentBuf, WatchHandle, WatchRegistry};

/// A lazily-built SQL read: parameters bind via [`params`](Self::params); a
/// terminal runs it.
///
/// Built by [`Collection::query`](super::Collection::query). Inert until a
/// terminal — `.iter` / `.collect` consume the values, `.explain` renders the
/// plan without running it, `.watch` / `.stream` register a subscription.
#[must_use = "a query builder does nothing until a terminal (.iter/.collect/.explain/.watch/.stream) runs it"]
pub struct QueryBuilder<'a, P = ()> {
    cf: &'a str,
    collection: &'a str,
    /// The database's watch registry (borrowed from the [`Collection`]), used only
    /// by the reactive terminals.
    watch: &'a Arc<WatchRegistry>,
    sql: &'a str,
    params: Option<P>,
}

impl<'a> QueryBuilder<'a, ()> {
    pub(super) fn new(
        cf: &'a str,
        collection: &'a str,
        watch: &'a Arc<WatchRegistry>,
        sql: &'a str,
    ) -> Self {
        Self {
            cf,
            collection,
            watch,
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
            watch: self.watch,
            sql: self.sql,
            params: Some(params),
        }
    }

    /// Register a **push** subscription on this SQL `WHERE` filter: `callback`
    /// fires once per committed transaction with the batch of changes matching
    /// it, recast against the filter's set boundary. The SQL is a filter `SELECT`
    /// (`SELECT * FROM c WHERE …`); set operations (`ORDER BY`/`GROUP BY`/`LIMIT`/
    /// aggregates/joins) are rejected by the registry. Takes no transaction; the
    /// returned [`WatchHandle`] unregisters on drop.
    ///
    /// Only on the no-parameter builder — reactive SQL binds no `@params` (as in
    /// v1), so `query(sql).params(..).watch(..)` does not compile.
    pub fn watch(
        self,
        callback: impl Fn(&[ChangeEvent]) + Send + Sync + 'static,
    ) -> Result<WatchHandle, DbError> {
        WatchRegistry::watch_sql(
            self.watch,
            self.cf,
            self.collection,
            self.sql,
            Arc::new(callback),
        )
    }

    /// Open a **pull** subscription: a long-lived [`WatchStream`] with the same
    /// SQL filter semantics as [`watch`](Self::watch) and the non-blocking
    /// lag-drop delivery. Takes no transaction; unregisters on drop.
    pub fn stream(self) -> Result<WatchStream, DbError> {
        WatchRegistry::stream_sql(
            self.watch,
            self.cf,
            self.collection,
            self.sql,
            DEFAULT_STREAM_CAPACITY,
        )
    }
}

/// The shared SQL read core: parse, validate parameters, and plan. Called by
/// both the v2 [`QueryBuilder`] terminals and the (inverted) flat
/// `Transaction::query`/`query_with_params`/`explain`, so the two surfaces run
/// one body. `params` is the bound parameter document (`None` for the
/// no-parameter forms); a referenced `@param` with no value is rejected.
pub(crate) fn query_plan<S: Store>(
    cf: &str,
    collection: &str,
    sql: &str,
    params: Option<&bson::RawDocument>,
    txn: &Transaction<'_, S>,
) -> Result<slate_planner::Plan, DbError> {
    let query = slate_sql::parse(sql)?;

    // Every referenced `@param` must have a supplied value; the plan shape never
    // depends on a parameter's value, only its presence.
    let mut supplied: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    if let Some(doc) = params {
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
        txn.collection_meta(cf, collection)?
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
            cf: cf.to_string(),
            collection: collection.to_string(),
        },
        meta,
        validators: Vec::new(),
        triggers: Vec::new(),
    };
    Ok(slate_planner::plan(
        slate_planner::Statement::Query(query),
        &ctx,
    )?)
}

/// The shared SQL read core: lower and wrap in a [`Cursor`], binding `params`.
pub(crate) fn query_cursor<'t, 'db, S>(
    cf: &str,
    collection: &str,
    sql: &str,
    params: Option<RawDocumentBuf>,
    txn: &'t Transaction<'db, S>,
) -> Result<Cursor<'db, 't, S>, DbError>
where
    S: Store + 'db,
{
    let plan = query_plan(cf, collection, sql, params.as_deref(), txn)?;
    // `.cloned()` rand/watch handles are `Arc`/`Rc` refcount bumps, the same
    // handoff the v1 read path made.
    Ok(match params {
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

impl<P: Serialize> QueryBuilder<'_, P> {
    /// Serialize the bound parameters (if any) once, for the terminals below.
    fn params_raw(&self) -> Result<Option<RawDocumentBuf>, DbError> {
        match &self.params {
            Some(p) => Ok(Some(bson::serialize_to_raw_document_buf(p)?)),
            None => Ok(None),
        }
    }

    fn lower<S: Store>(
        &self,
        txn: &Transaction<'_, S>,
    ) -> Result<(slate_planner::Plan, Option<RawDocumentBuf>), DbError> {
        let params_raw = self.params_raw()?;
        let plan = query_plan(
            self.cf,
            self.collection,
            self.sql,
            params_raw.as_deref(),
            txn,
        )?;
        Ok((plan, params_raw))
    }

    fn build_cursor<'t, 'db, S>(
        &self,
        txn: &'t Transaction<'db, S>,
    ) -> Result<Cursor<'db, 't, S>, DbError>
    where
        S: Store + 'db,
    {
        query_cursor(self.cf, self.collection, self.sql, self.params_raw()?, txn)
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

    /// Run the query and render its plan annotated with per-node actuals
    /// (`EXPLAIN ANALYZE`). Bound parameters are honored, so a parameterized
    /// query can be analyzed (v1's `explain_analyze` binds none).
    pub fn analyze<S>(&self, txn: &Transaction<'_, S>) -> Result<String, DbError>
    where
        S: Store,
    {
        let (plan, params) = self.lower(txn)?;
        super::exec::analyze_plan(plan, params, txn)
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

    #[test]
    fn query_analyze_matches_v1() {
        let db = seed();
        let txn = db.begin(true).unwrap();
        let sql = "SELECT VALUE c.name FROM c WHERE c.age > 25";

        let v2 = db.collection("users").query(sql).analyze(&txn).unwrap();
        let v1 = txn.explain_analyze(DEFAULT_CF, "users", sql).unwrap();
        assert_eq!(v2, v1);
        assert!(!v2.is_empty());
    }

    #[test]
    fn parameterized_query_analyzes() {
        let db = seed();
        let txn = db.begin(true).unwrap();
        // v1's explain_analyze binds no parameters; v2's threads them through.
        let analyzed = db
            .collection("users")
            .query("SELECT VALUE c.name FROM c WHERE c.age > @min")
            .params(doc! { "min": 25 })
            .analyze(&txn)
            .unwrap();
        assert!(!analyzed.is_empty());
    }

    #[test]
    fn query_watch_fires_on_matching_commit() {
        use std::sync::{Arc, Mutex};

        use crate::ChangeEvent;

        let db = seed();
        let batches: Arc<Mutex<Vec<Vec<ChangeEvent>>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&batches);
        // SQL WHERE filter as a subscription — no transaction, no params
        let _handle = db
            .collection("users")
            .query("SELECT * FROM c WHERE c.age > 25")
            .watch(move |events| sink.lock().unwrap().push(events.to_vec()))
            .unwrap();

        let txn = db.begin(false).unwrap();
        db.collection("users")
            .insert_one(doc! { "_id": 9, "name": "zoe", "age": 50 })
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();

        let b = batches.lock().unwrap();
        assert_eq!(b.len(), 1);
        match &b[0][0] {
            ChangeEvent::Insert { doc } => assert_eq!(doc.get_str("name").unwrap(), "zoe"),
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn query_stream_drains_matching() {
        let db = seed();
        let stream = db
            .collection("users")
            .query("SELECT * FROM c WHERE c.age > 25")
            .stream()
            .unwrap();

        let txn = db.begin(false).unwrap();
        db.collection("users")
            .insert_one(doc! { "_id": 9, "age": 50 })
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();

        let mut batches = Vec::new();
        while let Some(batch) = stream.try_next() {
            batches.push(batch);
        }
        assert_eq!(batches.len(), 1);
    }
}
