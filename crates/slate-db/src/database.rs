use std::cell::Cell;
use std::sync::Arc;

use bson::{RawBson, RawDocumentBuf};
use serde::Serialize;
use slate_engine::{Catalog, Engine, EngineTransaction, FunctionKind, KvEngine};
use slate_query::{DistinctOptions, FindOptions};
use slate_store::{BackupStore, Store};
use slate_vm::pool::VmPool;

use crate::collection::CollectionConfig;
use crate::cursor::Cursor;
use crate::error::DbError;
use crate::executor;
use crate::expression::Expression;
use crate::hooks::{HookRegistry, HookSnapshot, ResolvedHook};
use crate::parser;
use crate::planner::planner::Planner;
use crate::statement::Statement;

/// Build the distinct field projection as a [`slate_ast::ScalarExpr::PathGet`].
/// Unlike plain member access, `PathGet` resolves the dotted path with Mongo
/// array-path traversal (distributing over arrays of subdocuments), which v1's
/// `distinct` does. Filters/SQL use member access, which does not traverse.
fn distinct_field_expr(field: &str) -> slate_ast::ScalarExpr {
    slate_ast::ScalarExpr::PathGet {
        base: Box::new(slate_ast::ScalarExpr::Identifier("c".into())),
        path: field.split('.').map(|s| s.to_string()).collect(),
    }
}

// ── DatabaseBuilder ────────────────────────────────────────

/// Which query engine backs reads.
///
/// `V2` (the default) routes `find` through the new stack — the Mongo
/// front-end (`slate-query`) → shared AST → `slate-planner` → `slate-executor`.
/// `V1` is the original planner/executor in this crate, kept for a soak period
/// and as the differential oracle for v1↔v2 testing; untranslatable filters
/// under `V2` still fall back to it automatically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryEngine {
    V1,
    #[default]
    V2,
}

pub struct DatabaseBuilder {
    pool: Option<VmPool>,
    clock: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
    engine: QueryEngine,
    #[cfg(feature = "runtime")]
    sweep_interval: Option<std::time::Duration>,
}

impl DatabaseBuilder {
    pub fn new() -> Self {
        Self {
            pool: None,
            clock: None,
            engine: QueryEngine::V2,
            #[cfg(feature = "runtime")]
            sweep_interval: None,
        }
    }

    /// Select the query engine backing reads (default [`QueryEngine::V2`]).
    pub fn query_engine(mut self, engine: QueryEngine) -> Self {
        self.engine = engine;
        self
    }

    /// Attach a script execution pool.
    ///
    /// Without a pool, function source is still stored in the engine
    /// but no scripts will be executed.
    pub fn with_scripting(mut self, pool: VmPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Inject a custom clock function (returns epoch millis).
    ///
    /// Required on platforms where `SystemTime::now()` is unavailable (e.g. wasm32).
    pub fn with_clock(mut self, clock: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.clock = Some(Arc::new(clock));
        self
    }

    /// Enable background TTL sweep at the given interval.
    #[cfg(feature = "runtime")]
    pub fn with_sweep(mut self, interval: std::time::Duration) -> Self {
        self.sweep_interval = Some(interval);
        self
    }

    /// Open the database with the configured settings.
    ///
    /// When a script pool is configured, loads an initial hook snapshot
    /// from the engine so triggers and validators are available immediately.
    pub fn open<S: Store + Send + Sync + 'static>(self, store: S) -> Result<Database<S>, DbError> {
        let engine = match self.clock {
            Some(clock) => Arc::new(KvEngine::with_clock(store, move || clock())),
            None => Arc::new(KvEngine::new(store)),
        };

        // Load initial hook snapshot if scripting is enabled.
        let registry = if self.pool.is_some() {
            let txn = engine.begin(true)?;
            let snapshot = HookSnapshot::load_all(&txn)?;
            txn.rollback()?;
            Some(HookRegistry::new(snapshot))
        } else {
            None
        };

        #[cfg(feature = "runtime")]
        let ttl_handle = match self.sweep_interval {
            Some(d) => crate::runtime::sweep::spawn(Arc::clone(&engine), d.as_secs()),
            None => None,
        };

        Ok(Database {
            engine,
            query_engine: self.engine,
            pool: self.pool,
            registry,
            #[cfg(feature = "runtime")]
            ttl_handle,
        })
    }
}

// ── Database ───────────────────────────────────────────────

pub struct Database<S: Store> {
    engine: Arc<KvEngine<S>>,
    query_engine: QueryEngine,
    pool: Option<VmPool>,
    registry: Option<HookRegistry>,
    #[cfg(feature = "runtime")]
    ttl_handle: Option<crate::runtime::sweep::TtlHandle>,
}

impl<S: Store + BackupStore> Database<S> {
    /// Create a physical backup of the database at the given path.
    ///
    /// The backup can be opened with the same store backend as a new database.
    /// Safe to call while the database is live (online backup).
    pub fn backup(&self, dest: impl AsRef<std::path::Path>) -> Result<(), DbError> {
        self.engine.backup(dest.as_ref())?;
        Ok(())
    }
}

impl<S: Store> Database<S> {
    pub fn begin(&self, read_only: bool) -> Result<Transaction<'_, S>, DbError> {
        let txn = self.engine.begin(read_only)?;
        let snapshot = self.registry.as_ref().map(|r| r.snapshot());
        Ok(Transaction {
            txn,
            query_engine: self.query_engine,
            pool: self.pool.as_ref(),
            snapshot,
            registry: self.registry.as_ref(),
            hooks_dirty: Cell::new(false),
        })
    }

    /// Purge expired documents from a collection.
    pub fn purge_expired(&self, cf: &str, collection: &str) -> Result<u64, DbError> {
        let txn = self.begin(false)?;
        let deleted = txn.purge_expired(cf, collection)?;
        txn.commit()?;
        Ok(deleted)
    }

    /// List all known collections as `(cf, name)` pairs.
    pub fn list_collections(&self) -> Result<Vec<(String, String)>, DbError> {
        let txn = self.begin(true)?;
        let pairs = txn.list_collections()?;
        let _ = txn.rollback();
        Ok(pairs)
    }

    /// Gracefully stop background tasks.
    #[cfg(feature = "runtime")]
    pub fn shutdown(&mut self) {
        if let Some(mut handle) = self.ttl_handle.take() {
            handle.stop();
        }
    }

    #[cfg(feature = "bench-internals")]
    pub fn kv_engine(&self) -> &KvEngine<S> {
        &self.engine
    }
}

// ── Transaction ────────────────────────────────────────────

pub struct Transaction<'db, S: Store + 'db> {
    txn: <KvEngine<S> as Engine>::Txn<'db>,
    query_engine: QueryEngine,
    pool: Option<&'db VmPool>,
    snapshot: Option<Arc<HookSnapshot>>,
    registry: Option<&'db HookRegistry>,
    hooks_dirty: Cell<bool>,
}

impl<'db, S: Store + 'db> Transaction<'db, S> {
    // ── Insert operations ───────────────────────────────────────

    /// Insert a single document. Fails with DuplicateKey if `_id` already exists.
    /// If the document has no `_id`, an ObjectId is generated.
    pub fn insert_one<D: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        doc: D,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let raw = bson::serialize_to_raw_document_buf(&doc)?;
        self.insert_many(cf, collection, vec![raw])
    }

    /// Insert multiple documents. Fails per-doc on duplicate `_id`.
    pub fn insert_many<D: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        docs: impl IntoIterator<Item = D>,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| bson::serialize_to_raw_document_buf(&doc).map_err(DbError::from))
            .collect::<Result<Vec<_>, DbError>>()?;

        if self.query_engine == QueryEngine::V2 {
            let values = raw_docs.into_iter().map(RawBson::Document).collect();
            return Ok(self.insert_v2(cf, collection, values));
        }

        let stmt = Statement::Insert {
            cf,
            collection,
            docs: raw_docs,
        };
        self.prepare_cursor(stmt)
    }

    // ── Query operations ────────────────────────────────────────

    /// Find documents matching a filter with optional sort, skip, take, and projection.
    ///
    /// Returns a [`Cursor`] that can be iterated lazily via [`.iter()`](Cursor::iter)
    /// or drained via [`.drain()`](Cursor::drain) for a count.
    pub fn find<F: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
        options: FindOptions,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = bson::serialize_to_raw_document_buf(&filter)?;

        if self.query_engine == QueryEngine::V2 {
            return self.find_v2(cf, collection, &filter_raw, options);
        }

        let predicate = Self::parse_optional_filter(Some(&filter_raw))?;
        let stmt = Statement::Find {
            cf,
            collection,
            predicate,
            sort: options.sort,
            skip: options.skip,
            take: options.take,
            projection: options.columns,
        };
        self.prepare_cursor(stmt)
    }

    /// Execute a CosmosDB-style SQL query (`SELECT VALUE <expr> FROM <alias>
    /// [JOIN ...] [WHERE ...] [ORDER BY ...] [OFFSET/LIMIT]`) and return a
    /// [`Cursor`] over the resulting values.
    ///
    /// SQL is read-only and shares the v2 stack with `find` — it parses to the
    /// same AST (`slate-sql`), lowers with the same planner, and runs on the
    /// same executor, so the two surfaces can't drift. The `FROM` clause names
    /// only the row alias; the container is `(cf, collection)`, chosen here
    /// (matching Cosmos, where the container is external to the query text).
    ///
    /// ```ignore
    /// let cursor = txn.query(DEFAULT_CF, "users",
    ///     "SELECT VALUE c.name FROM c WHERE c.age > 21 ORDER BY c.age DESC")?;
    /// for name in cursor.iter::<String>()? { /* ... */ }
    /// ```
    ///
    /// Always uses [`QueryEngine::V2`] regardless of the builder setting — the
    /// legacy v1 engine has no SQL surface.
    pub fn query(
        &self,
        cf: &str,
        collection: &str,
        sql: &str,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let plan = self.lower_sql(cf, collection, sql, None)?;
        Ok(Cursor::new_v2(&self.txn, plan, self.pool))
    }

    /// Execute a SQL query with values for its `@name` parameters.
    ///
    /// `params` serializes to a document whose keys are the bare parameter names
    /// (no leading `@`) — e.g. `doc! { "minAge": 21 }` binds `@minAge`. A
    /// referenced parameter with no supplied value evaluates to undefined.
    ///
    /// ```ignore
    /// let cursor = txn.query_with_params(DEFAULT_CF, "users",
    ///     "SELECT VALUE c.name FROM c WHERE c.age > @minAge",
    ///     doc! { "minAge": 21 })?;
    /// ```
    pub fn query_with_params<P: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        sql: &str,
        params: P,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let params = bson::serialize_to_raw_document_buf(&params)?;
        let plan = self.lower_sql(cf, collection, sql, Some(&params))?;
        Ok(Cursor::new_v2_with_params(
            &self.txn, plan, self.pool, params,
        ))
    }

    /// Parse and lower a SQL string into a v2 plan (shared by `query` and
    /// `query_with_params`).
    ///
    /// Validates that every `@parameter` the query references has a value in
    /// `params` — an unsupplied parameter is a hard error rather than silently
    /// undefined (matching Cosmos), which catches a misspelled or forgotten
    /// name. `params` is `None` for the no-parameter `query` API, so any `@name`
    /// there is unsupplied.
    fn lower_sql(
        &self,
        cf: &str,
        collection: &str,
        sql: &str,
        params: Option<&bson::RawDocument>,
    ) -> Result<slate_planner::Plan, DbError> {
        let query = slate_sql::parse(sql)?;

        let mut supplied: std::collections::HashSet<String> = std::collections::HashSet::new();
        if let Some(doc) = params {
            for entry in doc.iter() {
                let (name, _) = entry.map_err(|e| DbError::InvalidQuery(e.to_string()))?;
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

        // Reject unqualified identifiers (Cosmos requires bound paths like `c.x`)
        // and an ungrouped non-aggregate column in a GROUP BY / aggregate query —
        // both are errors in Cosmos rather than silently undefined.
        slate_planner::validate_bindings(&query)?;
        slate_planner::validate_grouping(&query)?;

        // A FROM-less query (`SELECT VALUE 1`) reads no container, so it neither
        // needs nor requires the collection to exist — skip the metadata fetch.
        let meta = if query.from.is_some() {
            self.collection_meta(cf, collection)?
        } else {
            slate_planner::CollectionMeta {
                indexes: Vec::new(),
                pk_path: "_id".to_string(),
            }
        };
        let container = slate_planner::CollectionRef {
            cf: cf.to_string(),
            collection: collection.to_string(),
        };
        Ok(slate_planner::lower(query, container, &meta))
    }

    /// The v2 read path: Mongo find → shared AST → lower → a v2 plan run by
    /// `slate-executor`. Falls back to v1 if the filter isn't yet translatable.
    fn find_v2(
        &self,
        cf: &str,
        collection: &str,
        filter_raw: &RawDocumentBuf,
        options: FindOptions,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let query = match slate_query::find_to_query(filter_raw, &options) {
            Ok(q) => q,
            // A filter v2 can't express yet (e.g. `$in`): fall back to v1 so the
            // engine toggle never loses functionality.
            Err(_) => {
                let predicate = Self::parse_optional_filter(Some(filter_raw))?;
                let stmt = Statement::Find {
                    cf,
                    collection,
                    predicate,
                    sort: options.sort,
                    skip: options.skip,
                    take: options.take,
                    projection: options.columns,
                };
                return self.prepare_cursor(stmt);
            }
        };
        let meta = self.collection_meta(cf, collection)?;
        let container = slate_planner::CollectionRef {
            cf: cf.to_string(),
            collection: collection.to_string(),
        };
        let plan = slate_planner::lower(query, container, &meta);
        Ok(Cursor::new_v2(&self.txn, plan, self.pool))
    }

    /// Read the index/pk metadata `slate-planner` needs to choose a scan source.
    fn collection_meta(
        &self,
        cf: &str,
        collection: &str,
    ) -> Result<slate_planner::CollectionMeta, DbError> {
        let handle = self.txn.collection(cf, collection)?;
        Ok(slate_planner::CollectionMeta {
            indexes: handle.indexes().to_vec(),
            pk_path: handle.pk_path().to_string(),
        })
    }

    // ── v2 write planning ───────────────────────────────────────
    //
    // These mirror v1's `Planner` write methods (`wrap_before`/`wrap_after`,
    // `plan_read_source`) but build the handle-free `slate-planner` IR and route
    // the matched-document source through the Mongo front-end + `lower`, so the
    // filter is translated exactly as `find` translates it. Each returns `None`
    // when the filter isn't translatable yet, so the caller falls back to v1.

    fn v2_container(&self, cf: &str, collection: &str) -> slate_planner::CollectionRef {
        slate_planner::CollectionRef {
            cf: cf.to_string(),
            collection: collection.to_string(),
        }
    }

    fn v2_validators(&self, cf: &str, collection: &str) -> Vec<ResolvedHook> {
        self.snapshot
            .as_ref()
            .map(|s| s.validators_for(cf, collection).to_vec())
            .unwrap_or_default()
    }

    fn v2_triggers(&self, cf: &str, collection: &str) -> Vec<ResolvedHook> {
        self.snapshot
            .as_ref()
            .map(|s| s.triggers_for(cf, collection).to_vec())
            .unwrap_or_default()
    }

    /// Wrap a write source with the collection's validators then before-triggers
    /// (the order v1 uses): `Trigger(before) → Validate → source`.
    fn v2_wrap_before(
        &self,
        cf: &str,
        collection: &str,
        action: &str,
        source: slate_planner::Node,
    ) -> slate_planner::Node {
        let mut node = source;
        let validators = self.v2_validators(cf, collection);
        if !validators.is_empty() {
            node = slate_planner::Node::Validate {
                validators,
                source: Box::new(node),
            };
        }
        self.v2_wrap_before_triggers(cf, collection, action, node)
    }

    /// Wrap a write source with the collection's before-triggers only (no
    /// validators) — used by delete, which performs no write.
    fn v2_wrap_before_triggers(
        &self,
        cf: &str,
        collection: &str,
        action: &str,
        source: slate_planner::Node,
    ) -> slate_planner::Node {
        let triggers = self.v2_triggers(cf, collection);
        if triggers.is_empty() {
            source
        } else {
            slate_planner::Node::Trigger {
                cf: cf.into(),
                action: action.into(),
                hooks: triggers,
                source: Box::new(source),
            }
        }
    }

    /// Wrap a finished write plan with the collection's after-triggers.
    fn v2_wrap_after(
        &self,
        cf: &str,
        collection: &str,
        action: &str,
        plan: slate_planner::Plan,
    ) -> slate_planner::Plan {
        let triggers = self.v2_triggers(cf, collection);
        if triggers.is_empty() {
            plan
        } else {
            slate_planner::Plan::Trigger {
                cf: cf.into(),
                action: action.into(),
                hooks: triggers,
                plan: Box::new(plan),
            }
        }
    }

    /// The read-source node selecting the documents a write targets: the matched
    /// documents (identity projection) from the filter, optionally limited.
    /// `None` if the filter can't be translated to v2 yet.
    fn v2_write_source(
        &self,
        cf: &str,
        collection: &str,
        filter_raw: &RawDocumentBuf,
        take: Option<usize>,
    ) -> Result<Option<slate_planner::Node>, DbError> {
        let options = FindOptions {
            take,
            ..Default::default()
        };
        let query = match slate_query::find_to_query(filter_raw, &options) {
            Ok(q) => q,
            Err(_) => return Ok(None),
        };
        let meta = self.collection_meta(cf, collection)?;
        match slate_planner::lower(query, self.v2_container(cf, collection), &meta) {
            slate_planner::Plan::Query(node) => Ok(Some(node)),
            _ => Ok(None),
        }
    }

    fn insert_v2(&self, cf: &str, collection: &str, docs: Vec<RawBson>) -> Cursor<'db, '_, S> {
        let source = self.v2_wrap_before(
            cf,
            collection,
            "inserting",
            slate_planner::Node::Values(docs),
        );
        let plan = slate_planner::Plan::Insert {
            collection: self.v2_container(cf, collection),
            source,
        };
        let plan = self.v2_wrap_after(cf, collection, "inserted", plan);
        Cursor::new_v2(&self.txn, plan, self.pool)
    }

    /// Build the v2 update plan from an already-resolved read `source` (so a
    /// non-translatable filter can fall back to v1 without losing `mutation`).
    fn build_update_v2(
        &self,
        cf: &str,
        collection: &str,
        source: slate_planner::Node,
        mutation: slate_mutation::Mutation,
    ) -> Cursor<'db, '_, S> {
        let source = self.v2_wrap_before(cf, collection, "updating", source);
        let plan = slate_planner::Plan::Update {
            collection: self.v2_container(cf, collection),
            mutation,
            source,
        };
        let plan = self.v2_wrap_after(cf, collection, "updated", plan);
        Cursor::new_v2(&self.txn, plan, self.pool)
    }

    fn build_replace_v2(
        &self,
        cf: &str,
        collection: &str,
        source: slate_planner::Node,
        replacement: RawDocumentBuf,
    ) -> Cursor<'db, '_, S> {
        let source = self.v2_wrap_before(cf, collection, "updating", source);
        let plan = slate_planner::Plan::Replace {
            collection: self.v2_container(cf, collection),
            replacement,
            source,
        };
        let plan = self.v2_wrap_after(cf, collection, "updated", plan);
        Cursor::new_v2(&self.txn, plan, self.pool)
    }

    fn build_delete_v2(
        &self,
        cf: &str,
        collection: &str,
        source: slate_planner::Node,
    ) -> Cursor<'db, '_, S> {
        let source = self.v2_wrap_before_triggers(cf, collection, "deleting", source);
        let plan = slate_planner::Plan::Delete {
            collection: self.v2_container(cf, collection),
            source,
        };
        let plan = self.v2_wrap_after(cf, collection, "deleted", plan);
        Cursor::new_v2(&self.txn, plan, self.pool)
    }

    fn upsert_v2(
        &self,
        cf: &str,
        collection: &str,
        docs: Vec<RawBson>,
        mode: slate_planner::UpsertMode,
    ) -> Cursor<'db, '_, S> {
        let plan = slate_planner::Plan::Upsert {
            collection: self.v2_container(cf, collection),
            mode,
            hooks: self.v2_triggers(cf, collection),
            source: slate_planner::Node::Values(docs),
        };
        Cursor::new_v2(&self.txn, plan, self.pool)
    }

    /// v2 distinct: `Scan → [Filter] → Project(c.field) → Distinct → [Sort] →
    /// [Limit]`, run directly and collected into a single array (matching v1's
    /// return shape). `None` if the filter isn't translatable yet.
    fn distinct_v2(
        &self,
        cf: &str,
        collection: &str,
        field: &str,
        filter_raw: &RawDocumentBuf,
        options: &DistinctOptions,
    ) -> Result<Option<bson::RawBson>, DbError> {
        let binding = slate_planner::RowBinding::Alias("c".into());
        let mut node = slate_planner::Node::Scan {
            collection: self.v2_container(cf, collection),
        };
        match slate_query::translate_filter(filter_raw) {
            Ok(Some(predicate)) => {
                node = slate_planner::Node::Filter {
                    predicate,
                    binding: binding.clone(),
                    source: Box::new(node),
                };
            }
            Ok(None) => {} // match-all
            Err(_) => return Ok(None),
        }
        node = slate_planner::Node::Project {
            expr: distinct_field_expr(field),
            binding: binding.clone(),
            source: Box::new(node),
        };
        node = slate_planner::Node::Distinct {
            source: Box::new(node),
            // Mongo `distinct` flattens array values one level (multikey).
            flatten: true,
        };
        if let Some(dir) = options.sort {
            let direction = match dir {
                slate_query::SortDirection::Asc => slate_ast::SortDirection::Asc,
                slate_query::SortDirection::Desc => slate_ast::SortDirection::Desc,
            };
            // The distinct values are the bare rows, bound to `c`, so ordering
            // by `c` sorts the values themselves.
            node = slate_planner::Node::Sort {
                keys: vec![slate_ast::OrderByItem {
                    expr: slate_ast::ScalarExpr::Identifier("c".into()),
                    direction,
                }],
                binding,
                source: Box::new(node),
            };
        }
        if options.skip.is_some() || options.take.is_some() {
            node = slate_planner::Node::Limit {
                skip: options.skip.unwrap_or(0),
                take: options.take,
                source: Box::new(node),
            };
        }

        // Distinct yields bare scalar values, not documents, so run the plan
        // directly and gather them rather than going through `Cursor` (which
        // expects documents).
        let iter = slate_executor::Executor::with_pool(&self.txn, self.pool)
            .execute(slate_planner::Plan::Query(node))?;
        let mut arr = bson::RawArrayBuf::new();
        for item in iter {
            if let Some(value) = item.map_err(DbError::from)? {
                arr.push(value);
            }
        }
        Ok(Some(bson::RawBson::Array(arr)))
    }

    /// Find the first document matching a filter.
    pub fn find_one<F: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
    ) -> Result<Option<RawDocumentBuf>, DbError> {
        let options = FindOptions {
            take: Some(1),
            ..Default::default()
        };
        let cursor = self.find(cf, collection, filter, options)?;
        cursor.iter_raw()?.next().transpose()
    }

    // ── Update operations ───────────────────────────────────────

    /// Update the first document matching the filter.
    pub fn update_one<F: Serialize, U: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
        update: U,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = bson::serialize_to_raw_document_buf(&filter)?;
        let raw = bson::serialize_to_raw_document_buf(&update)?;
        let handle = self.txn.collection(cf, collection)?;
        let mutation = slate_mutation::parse_mutation(&raw, handle.pk_path())?;

        if self.query_engine == QueryEngine::V2
            && let Some(source) = self.v2_write_source(cf, collection, &filter_raw, Some(1))?
        {
            return Ok(self.build_update_v2(cf, collection, source, mutation));
        }

        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Update {
            cf,
            collection,
            predicate,
            mutation,
            limit: Some(1),
        };
        self.prepare_cursor(stmt)
    }

    /// Update all documents matching the filter.
    pub fn update_many<F: Serialize, U: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
        update: U,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = bson::serialize_to_raw_document_buf(&filter)?;
        let raw = bson::serialize_to_raw_document_buf(&update)?;
        let handle = self.txn.collection(cf, collection)?;
        let mutation = slate_mutation::parse_mutation(&raw, handle.pk_path())?;

        if self.query_engine == QueryEngine::V2
            && let Some(source) = self.v2_write_source(cf, collection, &filter_raw, None)?
        {
            return Ok(self.build_update_v2(cf, collection, source, mutation));
        }

        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Update {
            cf,
            collection,
            predicate,
            mutation,
            limit: None,
        };
        self.prepare_cursor(stmt)
    }

    /// Replace the first document matching the filter entirely (no merge).
    pub fn replace_one<F: Serialize, R: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
        replacement: R,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = bson::serialize_to_raw_document_buf(&filter)?;
        let raw = bson::serialize_to_raw_document_buf(&replacement)?;

        if self.query_engine == QueryEngine::V2
            && let Some(source) = self.v2_write_source(cf, collection, &filter_raw, Some(1))?
        {
            return Ok(self.build_replace_v2(cf, collection, source, raw));
        }

        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Replace {
            cf,
            collection,
            predicate,
            replacement: raw,
        };
        self.prepare_cursor(stmt)
    }

    // ── Delete operations ───────────────────────────────────────

    /// Delete the first document matching the filter.
    pub fn delete_one<F: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = bson::serialize_to_raw_document_buf(&filter)?;

        if self.query_engine == QueryEngine::V2
            && let Some(source) = self.v2_write_source(cf, collection, &filter_raw, Some(1))?
        {
            return Ok(self.build_delete_v2(cf, collection, source));
        }

        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Delete {
            cf,
            collection,
            predicate,
            limit: Some(1),
        };
        self.prepare_cursor(stmt)
    }

    /// Delete all documents matching the filter.
    pub fn delete_many<F: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = bson::serialize_to_raw_document_buf(&filter)?;

        if self.query_engine == QueryEngine::V2
            && let Some(source) = self.v2_write_source(cf, collection, &filter_raw, None)?
        {
            return Ok(self.build_delete_v2(cf, collection, source));
        }

        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Delete {
            cf,
            collection,
            predicate,
            limit: None,
        };
        self.prepare_cursor(stmt)
    }

    // ── Bulk upsert / merge operations ────────────────────────────

    /// Upsert (insert-or-replace) a batch of documents by `_id`.
    pub fn upsert_many<D: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        docs: impl IntoIterator<Item = D>,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| bson::serialize_to_raw_document_buf(&doc).map_err(DbError::from))
            .collect::<Result<Vec<_>, DbError>>()?;

        if self.query_engine == QueryEngine::V2 {
            let values = raw_docs.into_iter().map(RawBson::Document).collect();
            return Ok(self.upsert_v2(cf, collection, values, slate_planner::UpsertMode::Replace));
        }

        let stmt = Statement::Upsert {
            cf,
            collection,
            docs: raw_docs,
        };
        self.prepare_cursor(stmt)
    }

    /// Merge (insert-or-patch) a batch of partial documents by `_id`.
    pub fn merge_many<D: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        docs: impl IntoIterator<Item = D>,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| bson::serialize_to_raw_document_buf(&doc).map_err(DbError::from))
            .collect::<Result<Vec<_>, DbError>>()?;

        if self.query_engine == QueryEngine::V2 {
            let values = raw_docs.into_iter().map(RawBson::Document).collect();
            return Ok(self.upsert_v2(cf, collection, values, slate_planner::UpsertMode::Merge));
        }

        let stmt = Statement::Merge {
            cf,
            collection,
            docs: raw_docs,
        };
        self.prepare_cursor(stmt)
    }

    // ── Count ───────────────────────────────────────────────────

    /// Count documents matching a filter.
    pub fn count<F: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
    ) -> Result<u64, DbError> {
        self.find(cf, collection, filter, FindOptions::default())?
            .drain()
    }

    /// Return distinct values for a field, with optional filter and sort.
    pub fn distinct<F: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        field: &str,
        filter: F,
        options: DistinctOptions,
    ) -> Result<bson::RawBson, DbError> {
        let filter_raw = bson::serialize_to_raw_document_buf(&filter)?;

        if self.query_engine == QueryEngine::V2
            && let Some(array) = self.distinct_v2(cf, collection, field, &filter_raw, &options)?
        {
            return Ok(array);
        }

        let predicate = Self::parse_optional_filter(Some(&filter_raw))?;
        let stmt = Statement::Distinct {
            cf,
            collection,
            field: field.to_string(),
            predicate,
            sort: options.sort,
            skip: options.skip,
            take: options.take,
        };
        let plan = self.plan(stmt)?;
        let exec = executor::Executor::new(&self.txn, self.pool);
        let mut iter = exec.execute(plan)?;
        match iter.next() {
            Some(result) => {
                let opt_val: Option<RawBson> = result?;
                opt_val.ok_or_else(|| DbError::InvalidQuery("expected value".into()))
            }
            None => Ok(bson::RawBson::Array(bson::RawArrayBuf::new())),
        }
    }

    // ── TTL operations ──────────────────────────────────────────

    /// Purge expired documents from a collection.
    pub fn purge_expired(&self, cf: &str, collection: &str) -> Result<u64, DbError> {
        let handle = self.txn.collection(cf, collection)?;
        Ok(self.txn.purge(&handle)?)
    }

    // ── Index operations ────────────────────────────────────────

    /// Create an index on a field and backfill existing records.
    pub fn create_index(&self, cf: &str, collection: &str, field: &str) -> Result<(), DbError> {
        self.txn.create_index(cf, collection, field)?;
        Ok(())
    }

    /// Create a unique index on a field and backfill existing records.
    ///
    /// Enforces that no two live documents share the same value for `field`.
    /// Fails with [`DbError::UniqueViolation`] if existing data already
    /// contains a duplicate. Scalar paths only (no multikey `[]`).
    pub fn create_unique_index(
        &self,
        cf: &str,
        collection: &str,
        field: &str,
    ) -> Result<(), DbError> {
        self.txn.create_index_with_options(
            cf,
            collection,
            field,
            &slate_engine::IndexOptions { unique: true },
        )?;
        Ok(())
    }

    /// Drop an index and remove all its entries.
    pub fn drop_index(&self, cf: &str, collection: &str, field: &str) -> Result<(), DbError> {
        self.txn.drop_index(cf, collection, field)?;
        Ok(())
    }

    /// List indexed fields for a collection.
    pub fn list_indexes(&self, cf: &str, collection: &str) -> Result<Vec<String>, DbError> {
        let handle = self.txn.collection(cf, collection)?;
        Ok(handle.indexes().to_vec())
    }

    // ── Collection operations ───────────────────────────────────

    /// List all known collections as `(cf, name)` pairs.
    pub fn list_collections(&self) -> Result<Vec<(String, String)>, DbError> {
        let configs = self.txn.list_collections(None)?;
        Ok(configs
            .into_iter()
            .map(|c| (c.cf_name().to_string(), c.name().to_string()))
            .collect())
    }

    /// Drop a collection and all its data, indexes, and metadata.
    pub fn drop_collection(&self, cf: &str, collection: &str) -> Result<(), DbError> {
        self.txn.drop_collection(cf, collection)?;
        self.hooks_dirty.set(true);
        Ok(())
    }

    // ── Lifecycle ───────────────────────────────────────────────

    pub fn commit(self) -> Result<(), DbError> {
        // If hooks were modified, reload the snapshot before committing
        // so the new snapshot reflects the changes we're about to persist.
        let new_snapshot = if self.hooks_dirty.get() {
            Some(HookSnapshot::load_all(&self.txn)?)
        } else {
            None
        };

        self.txn.commit()?;

        // Swap the new snapshot into the registry after a successful commit.
        if let (Some(snapshot), Some(registry)) = (new_snapshot, self.registry) {
            registry.swap(snapshot);
        }

        Ok(())
    }

    pub fn rollback(self) -> Result<(), DbError> {
        self.txn.rollback()?;
        Ok(())
    }

    // ── Collection management ───────────────────────────────────

    /// Create a collection with the given config.
    pub fn create_collection(&self, config: &CollectionConfig) -> Result<(), DbError> {
        let options = slate_engine::CreateCollectionOptions {
            pk_path: Some(config.pk_path.clone()),
            ttl_path: Some(config.ttl_path.clone()),
        };
        self.txn
            .create_collection(&config.cf, &config.name, &options)?;

        // Auto-create TTL index; ignore IndexExists for idempotent re-creation.
        if let Err(e) = self
            .txn
            .create_index(&config.cf, &config.name, &config.ttl_path)
        {
            if !matches!(e, slate_engine::EngineError::IndexExists(_)) {
                return Err(e.into());
            }
        }
        Ok(())
    }

    // ── Function operations ──────────────────────────────────────

    /// Register a trigger function on a collection.
    pub fn register_trigger(
        &self,
        cf: &str,
        collection: &str,
        name: &str,
        source: &str,
    ) -> Result<(), DbError> {
        self.txn.create_function(
            cf,
            collection,
            FunctionKind::Trigger,
            name,
            slate_engine::runtime_tag::LUA,
            source.as_bytes(),
        )?;
        self.hooks_dirty.set(true);
        Ok(())
    }

    /// Register a validator function on a collection.
    pub fn register_validator(
        &self,
        cf: &str,
        collection: &str,
        name: &str,
        source: &str,
    ) -> Result<(), DbError> {
        self.txn.create_function(
            cf,
            collection,
            FunctionKind::Validator,
            name,
            slate_engine::runtime_tag::LUA,
            source.as_bytes(),
        )?;
        self.hooks_dirty.set(true);
        Ok(())
    }

    /// Register a user-defined function on a collection.
    pub fn register_udf(
        &self,
        cf: &str,
        collection: &str,
        name: &str,
        source: &str,
    ) -> Result<(), DbError> {
        self.txn.create_function(
            cf,
            collection,
            FunctionKind::Udf,
            name,
            slate_engine::runtime_tag::LUA,
            source.as_bytes(),
        )?;
        Ok(())
    }

    /// Drop a trigger function from a collection.
    pub fn drop_trigger(&self, cf: &str, collection: &str, name: &str) -> Result<(), DbError> {
        self.txn
            .drop_function(cf, collection, FunctionKind::Trigger, name)?;
        self.hooks_dirty.set(true);
        Ok(())
    }

    /// Drop a validator function from a collection.
    pub fn drop_validator(&self, cf: &str, collection: &str, name: &str) -> Result<(), DbError> {
        self.txn
            .drop_function(cf, collection, FunctionKind::Validator, name)?;
        self.hooks_dirty.set(true);
        Ok(())
    }

    /// Drop a user-defined function from a collection.
    pub fn drop_udf(&self, cf: &str, collection: &str, name: &str) -> Result<(), DbError> {
        self.txn
            .drop_function(cf, collection, FunctionKind::Udf, name)?;
        Ok(())
    }

    // ── Private helpers ─────────────────────────────────────────

    /// Build a planner and produce a plan for the given statement.
    fn plan(
        &self,
        stmt: Statement<'_>,
    ) -> Result<
        crate::planner::plan::Plan<<<KvEngine<S> as Engine>::Txn<'db> as EngineTransaction>::Cf>,
        DbError,
    > {
        let planner = Planner::with_snapshot(&self.txn, self.snapshot.as_deref());
        planner.plan(stmt)
    }

    /// Prepare a cursor for a query statement.
    fn prepare_cursor(&self, statement: Statement<'_>) -> Result<Cursor<'db, '_, S>, DbError> {
        let plan = self.plan(statement)?;
        Ok(Cursor::new(&self.txn, plan, self.pool))
    }

    /// Parse a required filter document into an Expression.
    fn parse_required_filter(doc: &RawDocumentBuf) -> Result<Expression, DbError> {
        Ok(parser::parse_filter(doc)?)
    }

    /// Parse an optional filter document into an Expression.
    /// None or empty doc → Expression::And(vec![]) (matches everything).
    fn parse_optional_filter(doc: Option<&RawDocumentBuf>) -> Result<Expression, DbError> {
        match doc {
            Some(d) if d.iter().next().is_some() => Ok(parser::parse_filter(d)?),
            _ => Ok(Expression::And(vec![])),
        }
    }
}
