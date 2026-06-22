use std::cell::Cell;
use std::sync::Arc;

use bson::{RawBson, RawDocumentBuf};
use serde::Serialize;
use slate_engine::{Catalog, Engine, EngineTransaction, FunctionKind, KvEngine};
use slate_query::{DistinctOptions, FindOptions};
use slate_store::{BackupStore, Store};
use slate_vm::pool::VmPool;

use crate::collection::{CollectionConfig, CollectionSchema};
use crate::cursor::Cursor;
use crate::error::DbError;
use crate::hooks::{HookRegistry, HookSnapshot, ResolvedHook};

/// The injected random source backing the SQL `RAND()` function: a callable
/// returning a fresh value in `[0, 1)` per call. Shared (`Arc`) so it outlives
/// each transaction; `Send + Sync` so the database stays thread-safe. Unlike the
/// clock (a *static* per-transaction value), `RAND()` needs a *different* value
/// per call, so the source owns its mutable PRNG state behind interior
/// mutability and the evaluator only ever calls it.
pub(crate) type RandFn = Arc<dyn Fn() -> f64 + Send + Sync>;

/// Bridge the thread-safe `Arc` random source (shared across the database) into
/// the executor's single-threaded `Rc` channel. The wrapper closure just
/// forwards the call; `None` (no source) leaves `RAND()` undefined.
pub(crate) fn rand_rc(rand: &Option<RandFn>) -> Option<std::rc::Rc<dyn Fn() -> f64>> {
    rand.as_ref().map(|arc| {
        let arc = Arc::clone(arc);
        std::rc::Rc::new(move || arc()) as std::rc::Rc<dyn Fn() -> f64>
    })
}

/// The native default random source: a per-thread seeded [`SmallRng`]. Gated by
/// the `runtime` feature so the `rand`/`getrandom` dependency never reaches the
/// wasm build — there the host injects `Math.random` via [`DatabaseBuilder::with_rand`].
///
/// [`SmallRng`]: rand::rngs::SmallRng
#[cfg(feature = "runtime")]
fn default_rand() -> f64 {
    use rand::{Rng, SeedableRng, rngs::SmallRng};
    use std::cell::RefCell;

    thread_local! {
        // Seeded from OS entropy once per thread; the state lives in the
        // thread-local, so this closure captures nothing and stays `Send + Sync`.
        static RNG: RefCell<SmallRng> = RefCell::new(SmallRng::from_entropy());
    }
    RNG.with(|rng| rng.borrow_mut().gen_range(0.0..1.0))
}

// ── DatabaseBuilder ────────────────────────────────────────

pub struct DatabaseBuilder {
    pool: Option<VmPool>,
    clock: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
    rand: Option<RandFn>,
    #[cfg(feature = "runtime")]
    sweep_interval: Option<std::time::Duration>,
}

impl Default for DatabaseBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseBuilder {
    pub fn new() -> Self {
        Self {
            pool: None,
            clock: None,
            rand: None,
            #[cfg(feature = "runtime")]
            sweep_interval: None,
        }
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

    /// Inject the random source backing the SQL `RAND()` function — a callable
    /// returning a value in `[0, 1)`, called afresh per `RAND()` evaluation.
    ///
    /// The parallel of [`with_clock`](Self::with_clock): required on platforms
    /// without the native default PRNG (e.g. wasm32, where the host passes
    /// `js_sys::Math::random`), and the escape hatch for determinism — inject a
    /// fixed sequence in tests. Without it, the native build defaults to a seeded
    /// per-thread PRNG; absent any source, `RAND()` evaluates to undefined.
    pub fn with_rand(mut self, rand: impl Fn() -> f64 + Send + Sync + 'static) -> Self {
        self.rand = Some(Arc::new(rand));
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

        // Resolve the `RAND()` source: an injected one wins; otherwise the native
        // build falls back to the seeded PRNG. With the `runtime` feature off
        // (the wasm build) and no injected source, `RAND()` stays undefined —
        // but the wasm host always injects `Math.random`, so it never is.
        let rand: Option<RandFn> = match self.rand {
            Some(r) => Some(r),
            #[cfg(feature = "runtime")]
            None => Some(Arc::new(default_rand)),
            #[cfg(not(feature = "runtime"))]
            None => None,
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
            pool: self.pool,
            registry,
            rand,
            #[cfg(feature = "runtime")]
            ttl_handle,
        })
    }
}

// ── Database ───────────────────────────────────────────────

pub struct Database<S: Store> {
    engine: Arc<KvEngine<S>>,
    pool: Option<VmPool>,
    registry: Option<HookRegistry>,
    /// Random source for `RAND()`, threaded into each transaction's cursors.
    rand: Option<RandFn>,
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
            pool: self.pool.as_ref(),
            snapshot,
            registry: self.registry.as_ref(),
            rand: self.rand.clone(),
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
    pool: Option<&'db VmPool>,
    snapshot: Option<Arc<HookSnapshot>>,
    registry: Option<&'db HookRegistry>,
    /// Random source for `RAND()`, handed to each cursor this transaction opens.
    rand: Option<RandFn>,
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

        let docs = raw_docs.into_iter().map(RawBson::Document).collect();
        let ctx = self.write_context(cf, collection, slate_planner::CollectionMeta::default());
        self.run_plan(slate_planner::Statement::Insert { docs }, ctx)
    }

    // ── Query operations ────────────────────────────────────────

    /// Find documents matching a filter with optional sort, skip, take, and projection.
    ///
    /// Returns a [`Cursor`] that can be iterated lazily via [`.iter()`](Cursor::iter)
    /// or drained via [`.drain()`](Cursor::drain) for a count.
    ///
    /// The Mongo filter is translated to the shared AST (`slate-query`), lowered
    /// (`slate-planner`), and run on `slate-executor`. A filter using an operator
    /// the front-end doesn't support yet is a hard error.
    pub fn find<F: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
        options: FindOptions,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = bson::serialize_to_raw_document_buf(&filter)?;
        let query = slate_query::find_to_query(&filter_raw, &options)?;
        let ctx = self.read_context(cf, collection, self.collection_meta(cf, collection)?);
        self.run_plan(slate_planner::Statement::Query(query), ctx)
    }

    /// Execute a CosmosDB-style SQL query (`SELECT VALUE <expr> FROM <alias>
    /// [JOIN ...] [WHERE ...] [ORDER BY ...] [OFFSET/LIMIT]`) and return a
    /// [`Cursor`] over the resulting values.
    ///
    /// SQL is read-only and shares the stack with `find` — it parses to the same
    /// AST (`slate-sql`), lowers with the same planner, and runs on the same
    /// executor, so the two surfaces can't drift. The `FROM` clause names only
    /// the row alias; the container is `(cf, collection)`, chosen here (matching
    /// Cosmos, where the container is external to the query text).
    ///
    /// ```ignore
    /// let cursor = txn.query(DEFAULT_CF, "users",
    ///     "SELECT VALUE c.name FROM c WHERE c.age > 21 ORDER BY c.age DESC")?;
    /// for name in cursor.iter::<String>()? { /* ... */ }
    /// ```
    pub fn query(
        &self,
        cf: &str,
        collection: &str,
        sql: &str,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let plan = self.lower_sql(cf, collection, sql, None)?;
        Ok(Cursor::new(&self.txn, plan, self.pool, self.rand.clone()))
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
        Ok(Cursor::new_with_params(
            &self.txn,
            plan,
            self.pool,
            params,
            self.rand.clone(),
        ))
    }

    /// Explain a query: lower it to a physical plan and render that plan as an
    /// indented operator tree, without running it.
    ///
    /// Lowering is exactly what [`query`](Self::query) does — same parse, same
    /// planner, same index choice — so the tree shows the plan the query would
    /// actually run. Like `query`, this binds no parameters, so a `sql` that
    /// references an `@parameter` is rejected (the plan shape never depends on a
    /// parameter's value, only on its presence in the query text).
    pub fn explain(&self, cf: &str, collection: &str, sql: &str) -> Result<String, DbError> {
        let plan = self.lower_sql(cf, collection, sql, None)?;
        Ok(plan.explain())
    }

    /// Parse and lower a SQL string into a plan (shared by `query` and
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

        // A FROM-less query (`SELECT VALUE 1`) reads no container, so it neither
        // needs nor requires the collection to exist — skip the metadata fetch.
        // `plan` validates bindings and grouping (rejecting unqualified
        // identifiers and ungrouped columns); the param check above is the one
        // validation it can't do, since it needs the supplied parameter set.
        let meta = if query.from.is_some() {
            self.collection_meta(cf, collection)?
        } else {
            slate_planner::CollectionMeta {
                indexes: Vec::new(),
                pk_path: "_id".to_string(),
            }
        };
        let ctx = self.read_context(cf, collection, meta);
        Ok(slate_planner::plan(
            slate_planner::Statement::Query(query),
            &ctx,
        )?)
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

    // ── Planning helpers ─────────────────────────────────────────
    //
    // The Mongo front-end (`slate-query`) translates a request into a
    // `slate_ast` query / statement, these helpers gather the collection's
    // catalog state into a `PlanContext`, and `slate_planner::plan` does all
    // plan shaping — validator/trigger wrapping included.

    fn container(&self, cf: &str, collection: &str) -> slate_planner::CollectionRef {
        slate_planner::CollectionRef {
            cf: cf.to_string(),
            collection: collection.to_string(),
        }
    }

    fn validators(&self, cf: &str, collection: &str) -> Vec<ResolvedHook> {
        self.snapshot
            .as_ref()
            .map(|s| s.validators_for(cf, collection).to_vec())
            .unwrap_or_default()
    }

    fn triggers(&self, cf: &str, collection: &str) -> Vec<ResolvedHook> {
        self.snapshot
            .as_ref()
            .map(|s| s.triggers_for(cf, collection).to_vec())
            .unwrap_or_default()
    }

    /// The find query selecting the documents a write targets (`take`-limited).
    /// The translation is the one `find` uses, so writes match identically; a
    /// filter the front-end can't translate yet is a hard error.
    fn write_query(
        &self,
        filter_raw: &RawDocumentBuf,
        take: Option<usize>,
    ) -> Result<slate_ast::Query, DbError> {
        let options = FindOptions {
            take,
            ..Default::default()
        };
        Ok(slate_query::find_to_query(filter_raw, &options)?)
    }

    /// Catalog context for a read: container + index metadata, no hooks.
    fn read_context(
        &self,
        cf: &str,
        collection: &str,
        meta: slate_planner::CollectionMeta,
    ) -> slate_planner::PlanContext {
        slate_planner::PlanContext {
            container: self.container(cf, collection),
            meta,
            validators: Vec::new(),
            triggers: Vec::new(),
        }
    }

    /// Catalog context for a write: container + validators + triggers. `meta` is
    /// the caller's choice — filter-bearing writes pass real index metadata (so
    /// the matched-document source can use an index); insert/upsert, which scan
    /// nothing, pass an empty one.
    fn write_context(
        &self,
        cf: &str,
        collection: &str,
        meta: slate_planner::CollectionMeta,
    ) -> slate_planner::PlanContext {
        slate_planner::PlanContext {
            container: self.container(cf, collection),
            meta,
            validators: self.validators(cf, collection),
            triggers: self.triggers(cf, collection),
        }
    }

    /// Plan `stmt` against `ctx` and wrap the result in a cursor.
    fn run_plan(
        &self,
        stmt: slate_planner::Statement,
        ctx: slate_planner::PlanContext,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let plan = slate_planner::plan(stmt, &ctx)?;
        Ok(Cursor::new(&self.txn, plan, self.pool, self.rand.clone()))
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
        let assignments = slate_query::update_to_assignments(&raw)?;

        let query = self.write_query(&filter_raw, Some(1))?;
        let ctx = self.write_context(cf, collection, self.collection_meta(cf, collection)?);
        self.run_plan(slate_planner::Statement::Update { query, assignments }, ctx)
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
        let assignments = slate_query::update_to_assignments(&raw)?;

        let query = self.write_query(&filter_raw, None)?;
        let ctx = self.write_context(cf, collection, self.collection_meta(cf, collection)?);
        self.run_plan(slate_planner::Statement::Update { query, assignments }, ctx)
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

        let query = self.write_query(&filter_raw, Some(1))?;
        let ctx = self.write_context(cf, collection, self.collection_meta(cf, collection)?);
        self.run_plan(
            slate_planner::Statement::Replace {
                query,
                replacement: raw,
            },
            ctx,
        )
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
        let query = self.write_query(&filter_raw, Some(1))?;
        let ctx = self.write_context(cf, collection, self.collection_meta(cf, collection)?);
        self.run_plan(slate_planner::Statement::Delete { query }, ctx)
    }

    /// Delete all documents matching the filter.
    pub fn delete_many<F: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = bson::serialize_to_raw_document_buf(&filter)?;
        let query = self.write_query(&filter_raw, None)?;
        let ctx = self.write_context(cf, collection, self.collection_meta(cf, collection)?);
        self.run_plan(slate_planner::Statement::Delete { query }, ctx)
    }

    // ── Bulk upsert / merge operations ────────────────────────────

    /// Upsert (insert-or-replace) a batch of documents by `_id`.
    pub fn upsert_many<D: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        docs: impl IntoIterator<Item = D>,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        self.upsert_with_mode(cf, collection, docs, slate_planner::UpsertMode::Replace)
    }

    /// Merge (insert-or-patch) a batch of partial documents by `_id`.
    pub fn merge_many<D: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        docs: impl IntoIterator<Item = D>,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        self.upsert_with_mode(cf, collection, docs, slate_planner::UpsertMode::Merge)
    }

    fn upsert_with_mode<D: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        docs: impl IntoIterator<Item = D>,
        mode: slate_planner::UpsertMode,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| bson::serialize_to_raw_document_buf(&doc).map_err(DbError::from))
            .collect::<Result<Vec<_>, DbError>>()?;

        let docs = raw_docs.into_iter().map(RawBson::Document).collect();
        let ctx = self.write_context(cf, collection, slate_planner::CollectionMeta::default());
        self.run_plan(slate_planner::Statement::Upsert { docs, mode }, ctx)
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
    ///
    /// Builds the Mongo `distinct` pipeline (`Scan → [Filter] → Project(path) →
    /// Distinct → [Sort] → [Limit]`), runs it directly, and gathers the bare
    /// values into a single array.
    pub fn distinct<F: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        field: &str,
        filter: F,
        options: DistinctOptions,
    ) -> Result<bson::RawBson, DbError> {
        let filter_raw = bson::serialize_to_raw_document_buf(&filter)?;
        let predicate = slate_query::translate_filter(&filter_raw)?;
        let sort = options.sort.map(|dir| match dir {
            slate_query::SortDirection::Asc => slate_ast::SortDirection::Asc,
            slate_query::SortDirection::Desc => slate_ast::SortDirection::Desc,
        });
        let stmt = slate_planner::Statement::Distinct {
            alias: slate_query::ALIAS.to_string(),
            field: field.to_string(),
            predicate,
            sort,
            skip: options.skip.map(|n| n as u64),
            take: options.take.map(|n| n as u64),
        };
        // Distinct scans the container directly (no index pushdown), so an empty
        // meta suffices; it's a read, so no validators/triggers.
        let ctx = self.read_context(cf, collection, slate_planner::CollectionMeta::default());
        let plan = slate_planner::plan(stmt, &ctx)?;

        // Distinct yields bare scalar values, not documents, so run the plan
        // directly and gather them rather than going through `Cursor` (which
        // expects documents).
        let iter = slate_executor::Executor::with_pool(&self.txn, self.pool)
            .with_rand(rand_rc(&self.rand))
            .execute(plan)?;
        let mut arr = bson::RawArrayBuf::new();
        for item in iter {
            if let Some(value) = item.map_err(DbError::from)? {
                arr.push(value);
            }
        }
        Ok(bson::RawBson::Array(arr))
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

    /// Read a collection's catalog metadata: its key paths and indexed fields
    /// (with the unique subset called out). Read-only; intended for schema
    /// introspection rather than planning.
    pub fn collection_schema(
        &self,
        cf: &str,
        collection: &str,
    ) -> Result<CollectionSchema, DbError> {
        let handle = self.txn.collection(cf, collection)?;
        Ok(CollectionSchema {
            cf: handle.cf_name().to_string(),
            name: handle.name().to_string(),
            pk_path: handle.pk_path().to_string(),
            ttl_path: handle.ttl_path().to_string(),
            indexes: handle.indexes().to_vec(),
            unique_indexes: handle.unique_indexes().to_vec(),
        })
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
            && !matches!(e, slate_engine::EngineError::IndexExists(_))
        {
            return Err(e.into());
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
}
