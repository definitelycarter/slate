use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use slate_engine::{Catalog, Engine, EngineTransaction, IntegrityReport, KvEngine};
use slate_executor::ExecEnv;
use slate_executor::watch::WatchSink;
use slate_store::{BackupStore, Durability, Store};
use slate_udf::UdfBag;
use slate_validator::ValidatorBag;
use slate_vm::pool::VmPool;

use crate::error::DbError;
use crate::hooks::{HookRegistry, HookSnapshot, ResolvedHook};
use crate::watch::{WatchRegistry, WatchSnapshot};

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
    /// UDF bag accumulated before open via `with_udf`; moved into the database.
    udf_bag: Arc<UdfBag>,
    /// Validator bag accumulated before open via `with_validator`; moved into the
    /// database, mirroring `udf_bag`.
    validator_bag: Arc<ValidatorBag>,
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
            udf_bag: Arc::new(UdfBag::new()),
            validator_bag: Arc::new(ValidatorBag::new()),
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

    /// Register a native UDF in the database-scoped bag before open. Equivalent
    /// to `functions().register` but at build time, for compiled applications
    /// that carry their functions in the binary. A bare closure is accepted via
    /// the blanket [`Udf`](slate_udf::Udf) impl.
    pub fn with_udf<U: slate_udf::Udf + 'static>(self, name: &str, udf: U) -> Self {
        self.udf_bag.register(name, udf);
        self
    }

    /// Register a native validator in the database-scoped bag before open.
    /// Equivalent to `validators().register` but at build time, for compiled
    /// applications that carry their validators in the binary. A bare closure is
    /// accepted via the blanket [`Validator`](slate_validator::Validator) impl.
    pub fn with_validator<V: slate_validator::Validator + 'static>(
        self,
        name: &str,
        validator: V,
    ) -> Self {
        self.validator_bag.register(name, validator);
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

        // Load the initial catalog snapshot. It carries trigger/validator hooks
        // (fired only when scripting is enabled) *and* native UDF bindings
        // (resolved at query time with no VM), so it is needed even without a
        // pool — a UDF database has bindings but no scripting.
        let registry = {
            let txn = engine.begin(true)?;
            let snapshot = HookSnapshot::load_all(&txn)?;
            txn.rollback()?;
            Some(HookRegistry::new(snapshot))
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
            udf_bag: self.udf_bag,
            validator_bag: self.validator_bag,
            rand,
            durability: self.durability,
            #[cfg(feature = "runtime")]
            ttl_handle,
        })
    }
}

// ── Database ───────────────────────────────────────────────

/// Which role a [`DanglingBinding`] belongs to — they fail differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BindingKind {
    /// A `udf.<name>` reference (read path): a dangling one fails only the
    /// queries that call it; writes are never blocked.
    Udf,
    /// A validator (write path): a dangling one blocks **all** writes to its
    /// collection — fail-safe.
    Validator,
}

/// A binding whose target native function is not registered in its bag — an
/// unresolved symbol. Returned by [`Database::dangling_bindings`] so an app can
/// detect a misconfiguration at startup instead of at use time.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DanglingBinding {
    /// Which role the binding belongs to (they fail differently).
    pub kind: BindingKind,
    /// The column family the binding lives on.
    pub cf: String,
    /// The collection the binding lives on.
    pub collection: String,
    /// The binding's name (`udf.<name>` for a UDF, the validator name otherwise).
    pub name: String,
    /// The target native function name, which is absent from the bag.
    pub func: String,
}

pub struct Database<S: Store> {
    engine: Arc<KvEngine<S>>,
    pool: Option<VmPool>,
    registry: Option<HookRegistry>,
    /// Ephemeral registry of watch queries. Always present (cheap when empty);
    /// behind an `Arc` so a [`WatchHandle`] outlives any database borrow and can
    /// unregister itself on `Drop`.
    watch_registry: Arc<WatchRegistry>,
    /// The database-scoped UDF bag — the live `name -> Arc<dyn Udf>` registry the
    /// query path resolves `udf.*` calls against. Always present (cheap when
    /// empty); behind an `Arc` so collection handles can share it for no-txn
    /// `functions().register`, exactly like the watch registry.
    udf_bag: Arc<UdfBag>,
    /// The database-scoped validator bag — the live `name -> Arc<dyn Validator>`
    /// registry the write path resolves bound validators against. Always present
    /// (cheap when empty); behind an `Arc` so collection handles can share it for
    /// no-txn `validators().register`, exactly like the UDF bag.
    validator_bag: Arc<ValidatorBag>,
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
            udf_bag: self.udf_bag.as_ref(),
            validator_bag: self.validator_bag.as_ref(),
            snapshot,
            registry: self.registry.as_ref(),
            rand: self.rand.clone(),
            hooks_dirty: Cell::new(false),
            watch_snapshot,
            watch_sink,
        })
    }

    /// The ephemeral watch registry, behind its `Arc` — for v2's reactive
    /// terminals (`find(f).watch`/`.stream`, `query(sql).watch`/`.stream`), which
    /// register a DB-lifetime subscription with no transaction, exactly as the
    /// flat `watch`/`stream` methods above do.
    pub(crate) fn watch_registry(&self) -> &Arc<WatchRegistry> {
        &self.watch_registry
    }

    /// The database-scoped UDF bag, behind its `Arc` — for collection handles to
    /// share (no-txn `functions().register`), like the watch registry.
    pub(crate) fn udf_bag(&self) -> &Arc<UdfBag> {
        &self.udf_bag
    }

    /// The database-scoped validator bag, behind its `Arc` — for collection
    /// handles to share (no-txn `validators().register`), like the UDF bag.
    pub(crate) fn validator_bag(&self) -> &Arc<ValidatorBag> {
        &self.validator_bag
    }

    /// Every binding (UDF or validator) whose target native function is not
    /// registered in its bag — the unresolved symbols (bindings minus bag). Empty
    /// when every binding resolves. Call it at startup to surface a missing
    /// `register` before a query or write hits it. Reflects committed state (the
    /// current snapshot). The string clones are per-binding on an introspection
    /// path (not a hot path), and the result owns its strings independent of the
    /// snapshot `Arc`.
    pub fn dangling_bindings(&self) -> Vec<DanglingBinding> {
        let Some(registry) = &self.registry else {
            return Vec::new();
        };
        let snapshot = registry.snapshot();
        let mut out = Vec::new();
        for ((cf, collection), bindings) in snapshot.all_udf_bindings() {
            for (name, func) in bindings {
                if self.udf_bag.get(func).is_none() {
                    out.push(DanglingBinding {
                        kind: BindingKind::Udf,
                        cf: cf.clone(),
                        collection: collection.clone(),
                        name: name.clone(),
                        func: func.clone(),
                    });
                }
            }
        }
        for ((cf, collection), bindings) in snapshot.all_validator_bindings() {
            for (name, func) in bindings {
                if self.validator_bag.get(func).is_none() {
                    out.push(DanglingBinding {
                        kind: BindingKind::Validator,
                        cf: cf.clone(),
                        collection: collection.clone(),
                        name: name.clone(),
                        func: func.clone(),
                    });
                }
            }
        }
        out.sort();
        out
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
    /// The database's UDF bag, borrowed for the life of the transaction (like
    /// `pool`). Consulted at `compile` to resolve `udf.*` references.
    udf_bag: &'db UdfBag,
    /// The database's validator bag, borrowed for the life of the transaction
    /// (like `udf_bag`). The `Validate` node resolves each bound validator's
    /// native name against it at fire time.
    validator_bag: &'db ValidatorBag,
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
    pub(crate) fn validators(&self, cf: &str, collection: &str) -> Vec<(String, String)> {
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

    pub(crate) fn now_millis(&self) -> i64 {
        self.txn.now_millis()
    }

    /// The transaction's random source in the executor's `Rc` form — for v2's
    /// `analyze`, which builds an `Executor` directly (the cursor path uses the
    /// `Arc`-based [`rand`](Self::rand) instead).
    pub(crate) fn exec_rand(&self) -> Option<std::rc::Rc<dyn Fn() -> f64>> {
        rand_rc(&self.rand)
    }

    /// Build the per-query execution context ([`ExecEnv`]) for a plan run against
    /// this transaction: the scripting pool, the `RAND()` source, the watch sink,
    /// the clock reading (epoch ms, captured at `begin`, backing `GETCURRENT*` —
    /// consistent across the txn and wasm-clean, no syscall in the evaluator),
    /// and the query's `@`-parameters. The single translation point from a
    /// transaction's capabilities to an executor env — the cursor and `analyze`
    /// both go through it, so a new capability is wired here once.
    pub(crate) fn exec_env(&self, params: Option<bson::RawDocumentBuf>) -> ExecEnv<'db> {
        ExecEnv::new()
            .with_pool(self.pool)
            .with_udf(Some(self.udf_bag))
            .with_validator(Some(self.validator_bag))
            .with_params(params.map(Rc::new))
            .with_rand(self.exec_rand())
            .with_watch(self.watch_sink.clone())
            .with_clock(Some(self.now_millis()))
    }

    /// The collection's UDF bindings (`query_name -> native_name`) from this
    /// transaction's cached snapshot, cloned into an `Rc` for the query's
    /// `ExecEnv`. `None` when the collection has no bindings — the common case
    /// pays nothing. The small per-collection map is cloned once per query
    /// (it lives inside the shared snapshot; the executor needs an owned handle).
    pub(crate) fn udf_bindings(
        &self,
        cf: &str,
        collection: &str,
    ) -> Option<Rc<HashMap<String, String>>> {
        self.snapshot
            .as_ref()
            .and_then(|s| s.udf_bindings_for(cf, collection))
            .map(|m| Rc::new(m.clone()))
    }

    /// The collection's UDF bindings as an owned map, for the planner's
    /// `PlanContext` (it validates that every `udf.NAME` is bound). Empty when
    /// the collection has no bindings. Cloned from the shared snapshot once per
    /// plan — the planner needs an owned map, not a borrow into the snapshot.
    pub(crate) fn udf_bindings_map(&self, cf: &str, collection: &str) -> HashMap<String, String> {
        self.snapshot
            .as_ref()
            .and_then(|s| s.udf_bindings_for(cf, collection))
            .cloned()
            .unwrap_or_default()
    }

    /// Mark the trigger/validator hook snapshot stale — for v2's `triggers()` /
    /// `validators()` sub-handles, which register/remove hooks that mutations
    /// consult (UDFs don't, so `functions()` never calls this). Mirrors the
    /// `hooks_dirty` flip v1's `register_trigger`/`drop_validator`/etc. make.
    pub(crate) fn mark_hooks_dirty(&self) {
        self.hooks_dirty.set(true);
    }

    // ── TTL operations ──────────────────────────────────────────

    /// Purge expired documents from a collection.
    pub fn purge_expired(&self, cf: &str, collection: &str) -> Result<u64, DbError> {
        crate::v2::purge_core(cf, collection, self)
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
