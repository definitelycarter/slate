use std::sync::Arc;

use bson::{RawBson, RawDocumentBuf};
use slate_engine::{Catalog, Engine, EngineTransaction, KvEngine};
use slate_query::{DistinctOptions, FindOptions};
use slate_store::Store;

use crate::collection::CollectionConfig;
use crate::convert::IntoRawDocumentBuf;
use crate::cursor::Cursor;
use crate::error::DbError;
use crate::executor;
use crate::expression::Expression;
use crate::parser;
use crate::planner::planner::Planner;
use crate::statement::Statement;
use crate::sweep::{self, TtlHandle};

pub struct DatabaseConfig {
    /// Interval in seconds between TTL sweep runs.
    pub ttl_sweep_interval_secs: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            ttl_sweep_interval_secs: u64::MAX,
        }
    }
}

pub struct Database<S: Store> {
    engine: Arc<KvEngine<S>>,
    ttl_handle: Option<TtlHandle>,
}

impl<S: Store> Database<S> {
    pub fn begin(&self, read_only: bool) -> Result<Transaction<'_, S>, DbError> {
        let txn = self.engine.begin(read_only)?;
        Ok(Transaction { txn })
    }

    /// Purge expired documents from a collection.
    pub fn purge_expired(&self, collection: &str) -> Result<u64, DbError> {
        let txn = self.begin(false)?;
        let deleted = txn.purge_expired(collection)?;
        txn.commit()?;
        Ok(deleted)
    }

    /// List all known collection names.
    pub fn list_collections(&self) -> Result<Vec<String>, DbError> {
        let txn = self.begin(true)?;
        let names = txn.list_collections()?;
        let _ = txn.rollback();
        Ok(names)
    }

    /// Gracefully stop background tasks.
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

impl<S: Store + Send + Sync + 'static> Database<S> {
    pub fn open(store: S, config: DatabaseConfig) -> Self {
        let engine = Arc::new(KvEngine::new(store));
        let ttl_handle = sweep::spawn(Arc::clone(&engine), config.ttl_sweep_interval_secs);
        Self { engine, ttl_handle }
    }
}

// ── Transaction ─────────────────────────────────────────────

pub struct Transaction<'db, S: Store + 'db> {
    txn: <KvEngine<S> as Engine>::Txn<'db>,
}

impl<'db, S: Store + 'db> Transaction<'db, S> {
    // ── Insert operations ───────────────────────────────────────

    /// Insert a single document. Fails with DuplicateKey if `_id` already exists.
    /// If the document has no `_id`, an ObjectId is generated.
    pub fn insert_one(
        &mut self,
        collection: &str,
        doc: impl IntoRawDocumentBuf,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let raw = doc.into_raw_document_buf()?;
        self.insert_many(collection, vec![raw])
    }

    /// Insert multiple documents. Fails per-doc on duplicate `_id`.
    pub fn insert_many(
        &mut self,
        collection: &str,
        docs: impl IntoIterator<Item = impl IntoRawDocumentBuf>,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| doc.into_raw_document_buf())
            .collect::<Result<Vec<_>, DbError>>()?;

        let stmt = Statement::Insert {
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
        collection: &str,
        filter: impl IntoRawDocumentBuf,
        options: FindOptions,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let predicate = Self::parse_optional_filter(Some(&filter_raw))?;
        let stmt = Statement::Find {
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
        collection: &str,
        filter: impl IntoRawDocumentBuf,
    ) -> Result<Option<RawDocumentBuf>, DbError> {
        let options = FindOptions {
            take: Some(1),
            ..Default::default()
        };
        let cursor = self.find(collection, filter, options)?;
        cursor.iter()?.next().transpose()
    }

    // ── Update operations ───────────────────────────────────────

    /// Update the first document matching the filter.
    pub fn update_one(
        &self,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
        update: impl IntoRawDocumentBuf,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let raw = update.into_raw_document_buf()?;
        let mutation = crate::mutation::parse_mutation(&raw)?;
        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Update {
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
        collection: &str,
        filter: impl IntoRawDocumentBuf,
        update: impl IntoRawDocumentBuf,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let raw = update.into_raw_document_buf()?;
        let mutation = crate::mutation::parse_mutation(&raw)?;
        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Update {
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
        collection: &str,
        filter: impl IntoRawDocumentBuf,
        replacement: impl IntoRawDocumentBuf,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let raw = replacement.into_raw_document_buf()?;
        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Replace {
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
        collection: &str,
        filter: impl IntoRawDocumentBuf,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Delete {
            collection,
            predicate,
            limit: Some(1),
        };
        self.prepare_cursor(stmt)
    }

    /// Delete all documents matching the filter.
    pub fn delete_many(
        &self,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Delete {
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
        collection: &str,
        docs: impl IntoIterator<Item = impl IntoRawDocumentBuf>,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| doc.into_raw_document_buf())
            .collect::<Result<Vec<_>, DbError>>()?;

        let stmt = Statement::Upsert {
            collection,
            docs: raw_docs,
        };
        self.prepare_cursor(stmt)
    }

    /// Merge (insert-or-patch) a batch of partial documents by `_id`.
    pub fn merge_many(
        &self,
        collection: &str,
        docs: impl IntoIterator<Item = impl IntoRawDocumentBuf>,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| doc.into_raw_document_buf())
            .collect::<Result<Vec<_>, DbError>>()?;

        let stmt = Statement::Merge {
            collection,
            docs: raw_docs,
        };
        self.prepare_cursor(stmt)
    }

    // ── Count ───────────────────────────────────────────────────

    /// Count documents matching a filter.
    pub fn count(&self, collection: &str, filter: impl IntoRawDocumentBuf) -> Result<u64, DbError> {
        self.find(collection, filter, FindOptions::default())?
            .drain()
    }

    /// Return distinct values for a field, with optional filter and sort.
    pub fn distinct(
        &self,
        collection: &str,
        field: &str,
        filter: impl IntoRawDocumentBuf,
        options: DistinctOptions,
    ) -> Result<bson::RawBson, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let predicate = Self::parse_optional_filter(Some(&filter_raw))?;
        let stmt = Statement::Distinct {
            collection,
            field: field.to_string(),
            predicate,
            sort: options.sort,
            skip: options.skip,
            take: options.take,
        };
        let plan = self.plan(stmt)?;
        let exec = executor::Executor::new(&self.txn);
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
    pub fn purge_expired(&self, collection: &str) -> Result<u64, DbError> {
        let handle = self.txn.collection(collection)?;
        Ok(self.txn.purge(&handle)?)
    }

    // ── Index operations ────────────────────────────────────────

    /// Create an index on a field and backfill existing records.
    pub fn create_index(&mut self, collection: &str, field: &str) -> Result<(), DbError> {
        self.txn.create_index(collection, field)?;
        Ok(())
    }

    /// Drop an index and remove all its entries.
    pub fn drop_index(&mut self, collection: &str, field: &str) -> Result<(), DbError> {
        self.txn.drop_index(collection, field)?;
        Ok(())
    }

    /// List indexed fields for a collection.
    pub fn list_indexes(&self, collection: &str) -> Result<Vec<String>, DbError> {
        let handle = self.txn.collection(collection)?;
        Ok(handle.indexes().to_vec())
    }

    // ── Collection operations ───────────────────────────────────

    /// List all known collection names.
    pub fn list_collections(&self) -> Result<Vec<String>, DbError> {
        let configs = self.txn.list_collections()?;
        Ok(configs.into_iter().map(|c| c.name().to_string()).collect())
    }

    /// Drop a collection and all its data, indexes, and metadata.
    pub fn drop_collection(&mut self, collection: &str) -> Result<(), DbError> {
        self.txn.drop_collection(collection)?;
        Ok(())
    }

    // ── Lifecycle ───────────────────────────────────────────────

    pub fn commit(self) -> Result<(), DbError> {
        self.txn.commit()?;
        Ok(())
    }

    pub fn rollback(self) -> Result<(), DbError> {
        self.txn.rollback()?;
        Ok(())
    }

    // ── Collection management ───────────────────────────────────

    /// Create a collection with the given config and ensure its indexes exist.
    pub fn create_collection(&mut self, config: &CollectionConfig) -> Result<(), DbError> {
        // Create the collection in the engine catalog
        let options = slate_engine::CreateCollectionOptions {
            cf: None,
            pk_path: Some(config.pk_path.clone()),
            ttl_path: Some(config.ttl_path.clone()),
        };
        self.txn.create_collection(&config.name, &options)?;

        // Ensure required indexes exist
        self.ensure_indexes(config)?;
        Ok(())
    }

    /// Ensure all indexes declared in the config exist, creating any that are missing.
    pub fn ensure_indexes(&mut self, config: &CollectionConfig) -> Result<(), DbError> {
        let handle = self.txn.collection(&config.name)?;
        let existing = handle.indexes().to_vec();
        let mut fields: Vec<&str> = config.indexes.iter().map(|s| s.as_str()).collect();
        if !fields.contains(&config.ttl_path.as_str()) {
            fields.push(&config.ttl_path);
        }
        for field in &fields {
            if !existing.iter().any(|e| e == field) {
                self.create_index(&config.name, field)?;
            }
        }
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
        let planner =
            Planner::new(|name: &str| self.txn.collection(name).map_err(DbError::from));
        planner.plan(stmt)
    }

    /// Prepare a cursor for a query statement.
    fn prepare_cursor(&self, statement: Statement<'_>) -> Result<Cursor<'db, '_, S>, DbError> {
        let plan = self.plan(statement)?;
        Ok(Cursor::new(&self.txn, plan))
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
