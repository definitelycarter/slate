use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;

use bson::RawDocumentBuf;
use serde::Serialize;
use slate_engine::{
    Catalog, Engine, EngineTransaction, IntegrityReport, KvEngine, VectorIndexSpec,
};
use slate_executor::watch::WatchSink;
use slate_query::{DistinctOptions, FindOptions};
use slate_store::{BackupStore, Durability, Store};
use slate_vm::pool::VmPool;

use crate::collection::{CollectionConfig, CollectionSchema};
use crate::cursor::Cursor;
use crate::error::DbError;
use crate::hooks::{HookRegistry, HookSnapshot, ResolvedHook};
use crate::watch::{WatchHandle, WatchRegistry, WatchSnapshot};

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
    durability: Option<Durability>,
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
            durability: None,
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

    /// Set the default durability level for every write transaction this
    /// database opens, overriding the store's own default.
    ///
    /// Three levels, each documented on [`Durability`]:
    /// - [`Durability::Strict`] — fsync per commit; survives power loss.
    /// - [`Durability::Buffered`] — the default; survives a process crash but
    ///   not power loss.
    /// - [`Durability::Relaxed`] — fastest; survives neither.
    ///
    /// A money-moving commit can still tighten this per-transaction via
    /// [`Database::begin_with`], which overrides the default for one transaction.
    pub fn with_durability(mut self, durability: Durability) -> Self {
        self.durability = Some(durability);
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

        // Validate and migrate every on-disk format before serving transactions:
        // an un-migrated string index would silently undercount, and a store
        // written by a newer binary is refused cleanly rather than mis-read.
        engine.check_and_migrate_formats()?;

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
            watch_registry: Arc::new(WatchRegistry::new()),
            rand,
            durability: self.durability,
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
    /// Ephemeral registry of watch queries. Always present (cheap when empty);
    /// behind an `Arc` so a [`WatchHandle`] outlives any database borrow and can
    /// unregister itself on `Drop`.
    watch_registry: Arc<WatchRegistry>,
    /// Random source for `RAND()`, threaded into each transaction's cursors.
    rand: Option<RandFn>,
    /// The builder-level durability default applied to every write transaction,
    /// overriding the store's own default. `None` falls back to the store's.
    durability: Option<Durability>,
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
        // A write transaction honors the builder-level durability default when
        // one was set; a read transaction makes no durability promise, so it
        // always takes the plain begin path. `None` falls through to the store's
        // own default.
        let txn = match (read_only, self.durability) {
            (false, Some(level)) => self.engine.begin_with_durability(level)?,
            _ => self.engine.begin(read_only)?,
        };
        self.wrap_txn(txn, read_only)
    }

    /// Begin a write transaction at an explicit durability level, overriding
    /// both the store and builder defaults for this one transaction.
    ///
    /// The granular knob the durability contract promises: a hot ingest path can
    /// run at the [`Durability::Buffered`] default while a money-moving commit
    /// asks for [`Durability::Strict`].
    pub fn begin_with(&self, durability: Durability) -> Result<Transaction<'_, S>, DbError> {
        let txn = self.engine.begin_with_durability(durability)?;
        self.wrap_txn(txn, false)
    }

    /// Wrap an engine transaction in a database [`Transaction`] (the shared tail
    /// of `begin` and `begin_with`).
    fn wrap_txn<'db>(
        &'db self,
        txn: <KvEngine<S> as Engine>::Txn<'db>,
        read_only: bool,
    ) -> Result<Transaction<'db, S>, DbError> {
        let snapshot = self.registry.as_ref().map(|r| r.snapshot());

        // Watch capture only applies to writes. Snapshot the watch registry at
        // `begin` (so the transaction sees a frozen set even if a watch is
        // registered/dropped mid-transaction) and build the executor sink only
        // when there is at least one watch — read transactions and the
        // no-watches common case pay nothing.
        let (watch_snapshot, watch_sink) = if read_only {
            (None, None)
        } else {
            let snap = self.watch_registry.snapshot();
            if snap.is_empty() {
                (None, None)
            } else {
                let sink = snap.build_sink();
                (Some(snap), sink)
            }
        };

        Ok(Transaction {
            txn,
            pool: self.pool.as_ref(),
            snapshot,
            registry: self.registry.as_ref(),
            rand: self.rand.clone(),
            hooks_dirty: Cell::new(false),
            watch_snapshot,
            watch_sink,
        })
    }

    /// Register a **watch** with a BSON (`find`-style) filter and a callback
    /// fired once per committed transaction with the batch of matching changes,
    /// in write order.
    ///
    /// This is the BSON counterpart of [`watch_query`](Self::watch_query): the
    /// `filter` is the same Mongo filter document `find` takes (translated by
    /// `slate_query`), so the two surfaces share semantics. The BSON surface is
    /// **filter-only** — there is no projection (use `watch_query` for that).
    /// An empty filter (`doc! {}`) is match-all.
    ///
    /// Each change is recast against the filter's set boundary: a document
    /// *entering* the filtered set surfaces as [`ChangeEvent::Insert`], one
    /// *leaving* as [`ChangeEvent::Delete`], and one modified while staying in
    /// as [`ChangeEvent::Update`] (carrying both old and new).
    ///
    /// The returned [`WatchHandle`] unregisters the watch on `Drop` (or via
    /// [`WatchHandle::unwatch`]). The callback runs **inline on the writer
    /// thread** after commit; it should be fast and non-panicking (a panic is
    /// caught and isolated) — offload heavy work via [`stream`](Self::stream).
    pub fn watch<F: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
        callback: impl Fn(&[crate::ChangeEvent]) + Send + Sync + 'static,
    ) -> Result<WatchHandle, DbError> {
        self.cf(cf)
            .collection(collection)
            .find(filter)
            .watch(callback)
    }

    /// Register a **watch query**: a SQL `WHERE` filter against `(cf,
    /// collection)` whose `callback` fires once per committed transaction with
    /// the batch of matching changes, in write order.
    ///
    /// This is the SQL counterpart of [`watch`](Self::watch) (bare name =
    /// BSON filter; `_query` = SQL, mirroring `find`/`query`). `sql_filter` is a
    /// `SELECT` whose `WHERE` clause is the filter (`SELECT * FROM c WHERE c.temp
    /// > 80`); only `WHERE` and an identity projection apply. Set operations
    /// (`ORDER BY` / `GROUP BY` / `HAVING` / aggregates / `LIMIT` / `OFFSET` /
    /// `DISTINCT`) and joins are rejected.
    ///
    /// Each change is recast against the filter's set boundary: a document
    /// *entering* the filtered set surfaces as [`ChangeEvent::Insert`], one
    /// *leaving* as [`ChangeEvent::Delete`], and one modified while staying in
    /// as [`ChangeEvent::Update`] (carrying both old and new).
    ///
    /// The returned [`WatchHandle`] unregisters the watch on `Drop` (or via
    /// [`WatchHandle::unwatch`]). The callback runs **inline on the writer
    /// thread** after commit; it should be fast and non-panicking (a panic is
    /// caught and isolated) — offload heavy work via
    /// [`stream_query`](Self::stream_query).
    pub fn watch_query(
        &self,
        cf: &str,
        collection: &str,
        sql_filter: &str,
        callback: impl Fn(&[crate::ChangeEvent]) + Send + Sync + 'static,
    ) -> Result<WatchHandle, DbError> {
        self.cf(cf)
            .collection(collection)
            .query(sql_filter)
            .watch(callback)
    }

    /// Open a **watch stream** with a BSON (`find`-style) filter: the pull
    /// (cursor) counterpart of [`watch`](Self::watch).
    ///
    /// Returns a long-lived [`WatchStream`] subscription the consumer drains on
    /// its own thread — decoupled from the writer, the FFI-clean delivery shape.
    /// The stream is backed by a bounded buffer with the **non-blocking lag-drop**
    /// policy: a slow consumer that fills the buffer causes further batches to be
    /// dropped and the stream marked [`lagged`](WatchStream::lagged), never
    /// blocking the writer. On a lag signal the consumer re-snapshots current
    /// state. Dropping the stream unregisters the watch.
    ///
    /// See [`watch`](Self::watch) for filter and set-transition semantics (they
    /// are identical — only the delivery differs).
    pub fn stream<F: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
    ) -> Result<crate::watch::WatchStream, DbError> {
        self.cf(cf).collection(collection).find(filter).stream()
    }

    /// Open a **watch stream** with a SQL `WHERE` filter: the pull (cursor)
    /// counterpart of [`watch_query`](Self::watch_query).
    ///
    /// Returns a long-lived [`WatchStream`] subscription with the same
    /// bounded-buffer, non-blocking lag-drop semantics as [`stream`](Self::stream)
    /// (see there). The SQL clause restrictions match
    /// [`watch_query`](Self::watch_query). Dropping the stream unregisters the
    /// watch.
    pub fn stream_query(
        &self,
        cf: &str,
        collection: &str,
        sql_filter: &str,
    ) -> Result<crate::watch::WatchStream, DbError> {
        self.cf(cf)
            .collection(collection)
            .query(sql_filter)
            .stream()
    }

    /// The ephemeral watch registry, behind its `Arc` — for v2's reactive
    /// terminals (`find(f).watch`/`.stream`, `query(sql).watch`/`.stream`), which
    /// register a DB-lifetime subscription with no transaction, exactly as the
    /// flat `watch`/`stream` methods above do.
    pub(crate) fn watch_registry(&self) -> &Arc<WatchRegistry> {
        &self.watch_registry
    }

    /// Walk a collection's records and index structures and report any integrity
    /// drift (missing / orphan / mismatched `i` / `u` / TTL entries).
    ///
    /// Read-only; safe on a live database. The oracle the crash harness asserts
    /// with after each kill.
    pub fn verify(&self, cf: &str, collection: &str) -> Result<IntegrityReport, DbError> {
        Ok(self.engine.verify(cf, collection)?)
    }

    /// Rebuild a collection's index entries from its records (the source of
    /// truth), bringing a drifted collection back to a clean
    /// [`verify`](Self::verify).
    pub fn repair(&self, cf: &str, collection: &str) -> Result<(), DbError> {
        self.engine.repair(cf, collection)?;
        Ok(())
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

    /// Database-wide size/cardinality statistics as of a fresh read snapshot.
    ///
    /// A convenience over [`Transaction::stats`](Transaction::stats) that opens
    /// and rolls back its own read transaction. See
    /// [`DatabaseStats`](crate::DatabaseStats) for the exact/approximate contract.
    pub fn stats(&self) -> Result<crate::stats::DatabaseStats, DbError> {
        let txn = self.begin(true)?;
        let stats = txn.stats();
        let _ = txn.rollback();
        stats
    }

    /// Size/cardinality statistics for one collection, as of a fresh read
    /// snapshot. A convenience over
    /// [`Transaction::collection_stats`](Transaction::collection_stats).
    pub fn collection_stats(
        &self,
        cf: &str,
        collection: &str,
    ) -> Result<crate::stats::CollectionStats, DbError> {
        let txn = self.begin(true)?;
        let stats = txn.collection_stats(cf, collection);
        let _ = txn.rollback();
        stats
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
    /// Watch registry snapshot frozen at `begin` — the source of callbacks at
    /// emit time. `None` for read transactions and when no watches are
    /// registered.
    watch_snapshot: Option<Arc<WatchSnapshot>>,
    /// The per-transaction capture sink, shared (`Rc`) into each cursor's
    /// executor so the mutation nodes buffer matching changes. Drained at
    /// commit. `None` when there is nothing to watch.
    watch_sink: Option<Rc<WatchSink>>,
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
        let docs: Vec<D> = docs.into_iter().collect();
        let plan = crate::v2::insert_plan(cf, collection, &docs, self)?;
        Ok(crate::v2::write_cursor(plan, self))
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
        crate::v2::find_cursor(cf, collection, &filter, &options, self)
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
        crate::v2::query_cursor(cf, collection, sql, None, self)
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
        crate::v2::query_cursor(cf, collection, sql, Some(params), self)
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
        Ok(crate::v2::query_plan(cf, collection, sql, None, self)?.explain())
    }

    /// Run a query and render its plan as an `EXPLAIN ANALYZE` tree — the same
    /// shape [`explain`](Self::explain) prints, annotated with per-node *actuals*
    /// (`rows=` emitted and, for non-source nodes, `examined=` rows that flowed
    /// in). Unlike `explain`, this *executes* the query (read-only), so it sees
    /// real cardinalities; the rows are collected and dropped — only the counts
    /// are returned.
    ///
    /// Lowering matches [`query`](Self::query) exactly (same parse, planner, index
    /// choice). Like `query` it binds no `@parameters`, so a parameterized query
    /// is rejected (the plan shape never depends on a parameter's value).
    pub fn explain_analyze(
        &self,
        cf: &str,
        collection: &str,
        sql: &str,
    ) -> Result<String, DbError> {
        let plan = crate::v2::query_plan(cf, collection, sql, None, self)?;
        crate::v2::analyze_plan(plan, None, self)
    }

    /// Gather size/cardinality statistics for one collection as of this
    /// transaction's read snapshot: live document count, plus per-index entry and
    /// distinct-value (cardinality) counts.
    ///
    /// Computed by scanning, so the numbers are **exact** but the call is
    /// O(documents + index entries) — an introspection surface, not a hot path.
    /// See [`CollectionStats`](crate::CollectionStats) for the exact/approximate
    /// contract.
    pub fn collection_stats(
        &self,
        cf: &str,
        collection: &str,
    ) -> Result<crate::stats::CollectionStats, DbError> {
        crate::v2::collection_stats_core(cf, collection, self)
    }

    /// Gather statistics for every collection visible to this transaction and
    /// roll them up into a [`DatabaseStats`](crate::DatabaseStats).
    ///
    /// On-disk size is `None` (a backend property not yet plumbed through the
    /// store trait); the per-collection counts are exact as of the snapshot.
    pub fn stats(&self) -> Result<crate::stats::DatabaseStats, DbError> {
        let mut collections = Vec::new();
        for (cf, name) in self.list_collections()? {
            collections.push(self.collection_stats(&cf, &name)?);
        }
        Ok(crate::stats::DatabaseStats::from_collections(
            collections,
            None,
        ))
    }

    /// Read the index/pk metadata `slate-planner` needs to choose a scan source.
    ///
    /// Index identities split into single-field (the flat `indexes`) and compound
    /// (`compound_indexes`, each carried as `(identity, components)` so the
    /// planner passes the engine's stored identity through opaquely).
    pub(crate) fn collection_meta(
        &self,
        cf: &str,
        collection: &str,
    ) -> Result<slate_planner::CollectionMeta, DbError> {
        let handle = self.txn.collection(cf, collection)?;
        let mut indexes = Vec::new();
        let mut compound_indexes = Vec::new();
        // `indexes()` yields identities borrowed from the catalog snapshot; the
        // planner's `CollectionMeta` owns its identity strings, so each is cloned
        // once here. This is per-plan setup (not a per-row path), so the cost is
        // negligible and the owned copy is required.
        for identity in handle.indexes() {
            let components = slate_engine::split_index_fields(identity);
            if components.len() > 1 {
                compound_indexes.push((identity.clone(), components));
            } else {
                indexes.push(identity.clone());
            }
        }
        // Flat vector indexes — the planner routes a `VECTORDISTANCE(c.<field>, …)`
        // kNN to a matching one. Map the engine's `VectorMetric` across the crate
        // boundary into the planner's mirror (the planner can't depend on
        // slate-engine); it carries only the field + metric the recogniser needs.
        let vector_indexes = handle
            .vector_indexes()
            .iter()
            .map(|spec| slate_planner::VectorIndexMeta {
                field: spec.path.clone(),
                metric: map_vector_metric(spec.metric),
            })
            .collect();
        Ok(slate_planner::CollectionMeta {
            indexes,
            compound_indexes,
            vector_indexes,
            pk_path: handle.pk_path().to_string(),
        })
    }

    // ── Hook snapshot accessors ──────────────────────────────────
    //
    // `pub(crate)` so v2's write builders can assemble their own write
    // `PlanContext` (container + validators + triggers) without calling a v1
    // verb — the hook snapshot is transaction state, reached like rand/watch.
    pub(crate) fn validators(&self, cf: &str, collection: &str) -> Vec<ResolvedHook> {
        self.snapshot
            .as_ref()
            .map(|s| s.validators_for(cf, collection).to_vec())
            .unwrap_or_default()
    }

    pub(crate) fn triggers(&self, cf: &str, collection: &str) -> Vec<ResolvedHook> {
        self.snapshot
            .as_ref()
            .map(|s| s.triggers_for(cf, collection).to_vec())
            .unwrap_or_default()
    }

    // ── v2 surface accessors ─────────────────────────────────────
    //
    // The `slate-db/src/v2` module builds its own read/write bodies (lower →
    // plan → cursor) instead of calling the public verbs above, so it reaches
    // this transaction's execution state through these crate-internal accessors.
    // It shares the engine transaction and `collection_meta` (catalog reads),
    // nothing else of v1's glue.

    pub(crate) fn engine_txn(&self) -> &<KvEngine<S> as Engine>::Txn<'db> {
        &self.txn
    }

    pub(crate) fn pool(&self) -> Option<&'db VmPool> {
        self.pool
    }

    pub(crate) fn rand(&self) -> Option<&RandFn> {
        self.rand.as_ref()
    }

    pub(crate) fn watch_sink(&self) -> Option<&Rc<WatchSink>> {
        self.watch_sink.as_ref()
    }

    pub(crate) fn now_millis(&self) -> i64 {
        self.txn.now_millis()
    }

    /// The transaction's random source in the executor's `Rc` form — for v2's
    /// `analyze`, which builds an `Executor` directly (the cursor path uses the
    /// `Arc`-based [`rand`](Self::rand) instead).
    pub(crate) fn exec_rand(&self) -> Option<std::rc::Rc<dyn Fn() -> f64>> {
        rand_rc(&self.rand)
    }

    /// Mark the trigger/validator hook snapshot stale — for v2's `triggers()` /
    /// `validators()` sub-handles, which register/remove hooks that mutations
    /// consult (UDFs don't, so `functions()` never calls this). Mirrors the
    /// `hooks_dirty` flip v1's `register_trigger`/`drop_validator`/etc. make.
    pub(crate) fn mark_hooks_dirty(&self) {
        self.hooks_dirty.set(true);
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
        let plan = crate::v2::update_plan(cf, collection, &filter, &update, true, self)?;
        Ok(crate::v2::write_cursor(plan, self))
    }

    /// Update all documents matching the filter.
    pub fn update_many<F: Serialize, U: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
        update: U,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let plan = crate::v2::update_plan(cf, collection, &filter, &update, false, self)?;
        Ok(crate::v2::write_cursor(plan, self))
    }

    /// Replace the first document matching the filter entirely (no merge).
    pub fn replace_one<F: Serialize, R: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
        replacement: R,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let plan = crate::v2::replace_plan(cf, collection, &filter, &replacement, self)?;
        Ok(crate::v2::write_cursor(plan, self))
    }

    // ── Delete operations ───────────────────────────────────────

    /// Delete the first document matching the filter.
    pub fn delete_one<F: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let plan = crate::v2::delete_plan(cf, collection, &filter, true, self)?;
        Ok(crate::v2::write_cursor(plan, self))
    }

    /// Delete all documents matching the filter.
    pub fn delete_many<F: Serialize>(
        &self,
        cf: &str,
        collection: &str,
        filter: F,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let plan = crate::v2::delete_plan(cf, collection, &filter, false, self)?;
        Ok(crate::v2::write_cursor(plan, self))
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
        let docs: Vec<D> = docs.into_iter().collect();
        let plan = crate::v2::upsert_plan(cf, collection, &docs, mode, self)?;
        Ok(crate::v2::write_cursor(plan, self))
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
        let sort = options.sort.map(|dir| match dir {
            slate_query::SortDirection::Asc => slate_ast::SortDirection::Asc,
            slate_query::SortDirection::Desc => slate_ast::SortDirection::Desc,
        });
        let cursor = crate::v2::distinct_cursor(
            cf,
            collection,
            field,
            &filter,
            sort,
            options.skip,
            options.take,
            self,
        )?;
        // Distinct yields bare scalar values; gather them into one array.
        let mut arr = bson::RawArrayBuf::new();
        for item in cursor.iter_raw_values()? {
            arr.push(item?);
        }
        Ok(bson::RawBson::Array(arr))
    }

    // ── TTL operations ──────────────────────────────────────────

    /// Purge expired documents from a collection.
    pub fn purge_expired(&self, cf: &str, collection: &str) -> Result<u64, DbError> {
        crate::v2::purge_core(cf, collection, self)
    }

    // ── Index operations ────────────────────────────────────────

    /// Create an index on a field and backfill existing records.
    pub fn create_index(&self, cf: &str, collection: &str, field: &str) -> Result<(), DbError> {
        crate::v2::Indexes::new(cf, collection)
            .create(field, crate::v2::IndexOptions::default())
            .execute(self)
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
        crate::v2::Indexes::new(cf, collection)
            .create(field, crate::v2::IndexOptions::unique())
            .execute(self)
    }

    /// Create a *compound* (multi-field) index and backfill existing records.
    ///
    /// The `fields` are matched left-to-right by the leftmost-prefix rule: an
    /// index on `["status", "created_at"]` can serve queries on `{status}` or
    /// `{status, created_at}`, but not `{created_at}` alone. A single-element
    /// `fields` is equivalent to [`create_index`](Self::create_index).
    pub fn create_compound_index(
        &self,
        cf: &str,
        collection: &str,
        fields: &[String],
    ) -> Result<(), DbError> {
        crate::v2::Indexes::new(cf, collection)
            .create(fields, crate::v2::IndexOptions::default())
            .execute(self)
    }

    /// Create a *unique* compound index and backfill existing records.
    ///
    /// Enforces that no two live documents share the same *combination* of
    /// values across `fields` (e.g. unique on `["org_id", "email"]` allows the
    /// same email across different orgs). Scalar components only (no multikey
    /// `[]`). Fails with [`DbError::UniqueViolation`] if existing data already
    /// holds a duplicate combination.
    pub fn create_unique_compound_index(
        &self,
        cf: &str,
        collection: &str,
        fields: &[String],
    ) -> Result<(), DbError> {
        crate::v2::Indexes::new(cf, collection)
            .create(fields, crate::v2::IndexOptions::unique())
            .execute(self)
    }

    /// Create a *flat vector index* from `spec` and backfill existing records.
    ///
    /// The `spec` carries the embedding field path, dimensionality, distance
    /// metric, and element dtype (see [`VectorIndexSpec`]). Every existing
    /// document with a well-formed embedding at `spec.path` is packed and
    /// indexed; the index then serves `ORDER BY VECTORDISTANCE(field, q) LIMIT k`
    /// top-k seeks. Fails with [`DbError::IndexExists`] if a vector index on
    /// that field already exists, and with [`DbError::InvalidDocument`] if any
    /// existing document's vector has the wrong dimensionality (the whole
    /// create rolls back).
    pub fn create_vector_index(
        &self,
        cf: &str,
        collection: &str,
        spec: &VectorIndexSpec,
    ) -> Result<(), DbError> {
        crate::v2::Indexes::new(cf, collection)
            .create(
                spec.path.as_str(),
                crate::v2::VectorIndexOptions::float32(spec.dims, spec.metric),
            )
            .execute(self)
    }

    /// Drop an index and remove all its entries. For a compound index, pass the
    /// joined identity returned by [`list_indexes`](Self::list_indexes).
    pub fn drop_index(&self, cf: &str, collection: &str, field: &str) -> Result<(), DbError> {
        crate::v2::Indexes::new(cf, collection)
            .remove(field)
            .execute(self)
    }

    /// List indexed fields for a collection.
    pub fn list_indexes(&self, cf: &str, collection: &str) -> Result<Vec<String>, DbError> {
        crate::v2::Indexes::new(cf, collection).list(self)
    }

    /// Read a collection's catalog metadata: its key paths and indexed fields
    /// (with the unique subset called out). Read-only; intended for schema
    /// introspection rather than planning.
    pub fn collection_schema(
        &self,
        cf: &str,
        collection: &str,
    ) -> Result<CollectionSchema, DbError> {
        crate::v2::collection_schema_core(cf, collection, self)
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
        crate::v2::drop_collection_core(cf, collection, self)
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
        crate::trace::trace_event!("transaction committed");

        // Swap the new snapshot into the registry after a successful commit.
        if let (Some(snapshot), Some(registry)) = (new_snapshot, self.registry) {
            registry.swap(snapshot);
        }

        // Fire watch callbacks on the just-committed state, in the same
        // post-commit side-effect position as the hook `registry.swap` above.
        // The commit has already succeeded, so a panicking callback is isolated
        // and cannot poison the transaction.
        if let (Some(snapshot), Some(sink)) = (&self.watch_snapshot, &self.watch_sink) {
            crate::watch::emit(snapshot, sink);
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
        crate::v2::create_collection_core(
            &config.cf,
            &config.name,
            &config.pk_path,
            &config.ttl_path,
            self,
        )
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
        crate::v2::Triggers::new(cf, collection)
            .create(name, source)
            .execute(self)
    }

    /// Register a validator function on a collection.
    pub fn register_validator(
        &self,
        cf: &str,
        collection: &str,
        name: &str,
        source: &str,
    ) -> Result<(), DbError> {
        crate::v2::Validators::new(cf, collection)
            .create(name, source)
            .execute(self)
    }

    /// Register a user-defined function on a collection.
    pub fn register_udf(
        &self,
        cf: &str,
        collection: &str,
        name: &str,
        source: &str,
    ) -> Result<(), DbError> {
        crate::v2::Functions::new(cf, collection)
            .create(name, source)
            .execute(self)
    }

    /// Drop a trigger function from a collection.
    pub fn drop_trigger(&self, cf: &str, collection: &str, name: &str) -> Result<(), DbError> {
        crate::v2::Triggers::new(cf, collection)
            .remove(name)
            .execute(self)
    }

    /// Drop a validator function from a collection.
    pub fn drop_validator(&self, cf: &str, collection: &str, name: &str) -> Result<(), DbError> {
        crate::v2::Validators::new(cf, collection)
            .remove(name)
            .execute(self)
    }

    /// Drop a user-defined function from a collection.
    pub fn drop_udf(&self, cf: &str, collection: &str, name: &str) -> Result<(), DbError> {
        crate::v2::Functions::new(cf, collection)
            .remove(name)
            .execute(self)
    }
}

/// Map the engine's [`VectorMetric`](slate_engine::VectorMetric) onto the
/// planner's mirror — the same three metrics, kept as separate types so the
/// planner needn't depend on slate-engine. The single crossing of that boundary.
fn map_vector_metric(metric: slate_engine::VectorMetric) -> slate_planner::VectorMetric {
    match metric {
        slate_engine::VectorMetric::Cosine => slate_planner::VectorMetric::Cosine,
        slate_engine::VectorMetric::DotProduct => slate_planner::VectorMetric::DotProduct,
        slate_engine::VectorMetric::Euclidean => slate_planner::VectorMetric::Euclidean,
    }
}
