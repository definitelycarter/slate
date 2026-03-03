use std::sync::Arc;

use bson::{RawBson, RawDocumentBuf};
use slate_engine::{Catalog, Engine, EngineTransaction, FunctionKind, KvEngine};
use slate_query::{DistinctOptions, FindOptions};
use slate_store::Store;
use slate_vm::pool::VmPool;

use crate::collection::CollectionConfig;
use crate::convert::IntoRawDocumentBuf;
use crate::cursor::Cursor;
use crate::error::DbError;
use crate::executor;
use crate::expression::Expression;
use crate::hooks::{HookRegistry, HookSnapshot};
use crate::parser;
use crate::planner::planner::Planner;
use crate::statement::Statement;

// ── DatabaseBuilder ────────────────────────────────────────

pub struct DatabaseBuilder {
    pool: Option<VmPool>,
    clock: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
    #[cfg(feature = "runtime")]
    sweep_interval: Option<std::time::Duration>,
}

impl DatabaseBuilder {
    pub fn new() -> Self {
        Self {
            pool: None,
            clock: None,
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
    pool: Option<VmPool>,
    registry: Option<HookRegistry>,
    #[cfg(feature = "runtime")]
    ttl_handle: Option<crate::runtime::sweep::TtlHandle>,
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
            hooks_dirty: false,
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
    hooks_dirty: bool,
}

impl<'db, S: Store + 'db> Transaction<'db, S> {
    // ── Insert operations ───────────────────────────────────────

    /// Insert a single document. Fails with DuplicateKey if `_id` already exists.
    /// If the document has no `_id`, an ObjectId is generated.
    pub fn insert_one(
        &mut self,
        cf: &str,
        collection: &str,
        doc: impl IntoRawDocumentBuf,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let raw = doc.into_raw_document_buf()?;
        self.insert_many(cf, collection, vec![raw])
    }

    /// Insert multiple documents. Fails per-doc on duplicate `_id`.
    pub fn insert_many(
        &mut self,
        cf: &str,
        collection: &str,
        docs: impl IntoIterator<Item = impl IntoRawDocumentBuf>,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| doc.into_raw_document_buf())
            .collect::<Result<Vec<_>, DbError>>()?;

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
    pub fn find(
        &self,
        cf: &str,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
        options: FindOptions,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
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

    /// Find the first document matching a filter.
    pub fn find_one(
        &self,
        cf: &str,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
    ) -> Result<Option<RawDocumentBuf>, DbError> {
        let options = FindOptions {
            take: Some(1),
            ..Default::default()
        };
        let cursor = self.find(cf, collection, filter, options)?;
        cursor.iter()?.next().transpose()
    }

    // ── Update operations ───────────────────────────────────────

    /// Update the first document matching the filter.
    pub fn update_one(
        &self,
        cf: &str,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
        update: impl IntoRawDocumentBuf,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let raw = update.into_raw_document_buf()?;
        let handle = self.txn.collection(cf, collection)?;
        let mutation = crate::mutation::parse_mutation(&raw, handle.pk_path())?;
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
    pub fn update_many(
        &self,
        cf: &str,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
        update: impl IntoRawDocumentBuf,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let raw = update.into_raw_document_buf()?;
        let handle = self.txn.collection(cf, collection)?;
        let mutation = crate::mutation::parse_mutation(&raw, handle.pk_path())?;
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
    pub fn replace_one(
        &self,
        cf: &str,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
        replacement: impl IntoRawDocumentBuf,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let raw = replacement.into_raw_document_buf()?;
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
    pub fn delete_one(
        &self,
        cf: &str,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
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
    pub fn delete_many(
        &self,
        cf: &str,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
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
    pub fn upsert_many(
        &self,
        cf: &str,
        collection: &str,
        docs: impl IntoIterator<Item = impl IntoRawDocumentBuf>,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| doc.into_raw_document_buf())
            .collect::<Result<Vec<_>, DbError>>()?;

        let stmt = Statement::Upsert {
            cf,
            collection,
            docs: raw_docs,
        };
        self.prepare_cursor(stmt)
    }

    /// Merge (insert-or-patch) a batch of partial documents by `_id`.
    pub fn merge_many(
        &self,
        cf: &str,
        collection: &str,
        docs: impl IntoIterator<Item = impl IntoRawDocumentBuf>,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| doc.into_raw_document_buf())
            .collect::<Result<Vec<_>, DbError>>()?;

        let stmt = Statement::Merge {
            cf,
            collection,
            docs: raw_docs,
        };
        self.prepare_cursor(stmt)
    }

    // ── Count ───────────────────────────────────────────────────

    /// Count documents matching a filter.
    pub fn count(
        &self,
        cf: &str,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
    ) -> Result<u64, DbError> {
        self.find(cf, collection, filter, FindOptions::default())?
            .drain()
    }

    /// Return distinct values for a field, with optional filter and sort.
    pub fn distinct(
        &self,
        cf: &str,
        collection: &str,
        field: &str,
        filter: impl IntoRawDocumentBuf,
        options: DistinctOptions,
    ) -> Result<bson::RawBson, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
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
    pub fn create_index(&mut self, cf: &str, collection: &str, field: &str) -> Result<(), DbError> {
        self.txn.create_index(cf, collection, field)?;
        Ok(())
    }

    /// Drop an index and remove all its entries.
    pub fn drop_index(&mut self, cf: &str, collection: &str, field: &str) -> Result<(), DbError> {
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
    pub fn drop_collection(&mut self, cf: &str, collection: &str) -> Result<(), DbError> {
        self.txn.drop_collection(cf, collection)?;
        self.hooks_dirty = true;
        Ok(())
    }

    // ── Lifecycle ───────────────────────────────────────────────

    pub fn commit(self) -> Result<(), DbError> {
        // If hooks were modified, reload the snapshot before committing
        // so the new snapshot reflects the changes we're about to persist.
        let new_snapshot = if self.hooks_dirty {
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
    pub fn create_collection(&mut self, config: &CollectionConfig) -> Result<(), DbError> {
        let options = slate_engine::CreateCollectionOptions {
            pk_path: Some(config.pk_path.clone()),
            ttl_path: Some(config.ttl_path.clone()),
        };
        self.txn
            .create_collection(&config.cf, &config.name, &options)?;

        // Auto-create TTL index; ignore IndexExists for idempotent re-creation.
        if let Err(e) = self.txn.create_index(&config.cf, &config.name, &config.ttl_path) {
            if !matches!(e, slate_engine::EngineError::IndexExists(_)) {
                return Err(e.into());
            }
        }
        Ok(())
    }

    // ── Function operations ──────────────────────────────────────

    /// Register a trigger function on a collection.
    pub fn register_trigger(
        &mut self,
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
        self.hooks_dirty = true;
        Ok(())
    }

    /// Register a validator function on a collection.
    pub fn register_validator(
        &mut self,
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
        self.hooks_dirty = true;
        Ok(())
    }

    /// Register a user-defined function on a collection.
    pub fn register_udf(
        &mut self,
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
    pub fn drop_trigger(
        &mut self,
        cf: &str,
        collection: &str,
        name: &str,
    ) -> Result<(), DbError> {
        self.txn
            .drop_function(cf, collection, FunctionKind::Trigger, name)?;
        self.hooks_dirty = true;
        Ok(())
    }

    /// Drop a validator function from a collection.
    pub fn drop_validator(
        &mut self,
        cf: &str,
        collection: &str,
        name: &str,
    ) -> Result<(), DbError> {
        self.txn
            .drop_function(cf, collection, FunctionKind::Validator, name)?;
        self.hooks_dirty = true;
        Ok(())
    }

    /// Drop a user-defined function from a collection.
    pub fn drop_udf(
        &mut self,
        cf: &str,
        collection: &str,
        name: &str,
    ) -> Result<(), DbError> {
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
