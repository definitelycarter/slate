use bson::{RawBson, RawDocumentBuf};
use slate_engine::{BsonValue, Catalog, Engine, EngineTransaction, KvEngine};
use slate_query::{DistinctQuery, Query};
use slate_store::Store;

use crate::collection::CollectionConfig;
use crate::convert::IntoRawDocumentBuf;
use crate::cursor::Cursor;
use crate::error::DbError;
use crate::executor;
use crate::executor::exec;
use crate::expression::Expression;
use crate::parser;
use crate::planner::planner::Planner;
use crate::result::{DeleteResult, InsertResult, UpdateResult, UpsertResult};
use crate::statement::Statement;

pub struct SlateEngine<S: Store> {
    engine: KvEngine<S>,
}

impl<S: Store> SlateEngine<S> {
    pub fn new(store: S) -> Self {
        Self {
            engine: KvEngine::new(store),
        }
    }

    pub fn begin(&self, read_only: bool) -> Result<Transaction<'_, S>, DbError> {
        let txn = self.engine.begin(read_only)?;
        Ok(Transaction { txn })
    }

    /// Purge expired documents from a collection.
    pub fn purge_expired(&self, collection: &str) -> Result<u64, DbError> {
        let mut txn = self.begin(false)?;
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

    #[cfg(any(test, feature = "bench-internals"))]
    pub fn kv_engine(&self) -> &KvEngine<S> {
        &self.engine
    }
}

pub struct Transaction<'db, S: Store + 'db> {
    txn: <KvEngine<S> as Engine>::Txn<'db>,
}

impl<'db, S: Store + 'db> Transaction<'db, S> {
    // ── Insert operations ───────────────────────────────────────

    /// Insert a single document. Fails with DuplicateKey if `_id` already exists.
    /// If the document has no `_id`, a UUID is generated.
    pub fn insert_one(
        &mut self,
        collection: &str,
        doc: impl IntoRawDocumentBuf,
    ) -> Result<InsertResult, DbError> {
        let raw = doc.into_raw_document_buf()?;
        let mut results = self.insert_many(collection, vec![raw])?;
        Ok(results.remove(0))
    }

    /// Insert multiple documents. Fails per-doc on duplicate `_id`.
    pub fn insert_many(
        &mut self,
        collection: &str,
        docs: impl IntoIterator<Item = impl IntoRawDocumentBuf>,
    ) -> Result<Vec<InsertResult>, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| doc.into_raw_document_buf())
            .collect::<Result<Vec<_>, DbError>>()?;

        let stmt = Statement::Insert {
            collection,
            docs: raw_docs,
        };
        let plan = self.plan(stmt)?;
        let exec = executor::Executor::new(&self.txn);
        let iter = exec.execute(plan)?;
        let mut ids = Vec::new();
        for result in iter {
            if let Some(val) = result? {
                if let Some(raw) = val.as_document() {
                    if let Some(id_str) = exec::raw_extract_id(raw)? {
                        ids.push(id_str.to_string());
                    }
                }
            }
        }
        Ok(ids.into_iter().map(|id| InsertResult { id }).collect())
    }

    // ── Query operations ────────────────────────────────────────

    /// Find documents matching a query (filter, sort, skip, take, projection).
    ///
    /// Returns a [`Cursor`] that can be iterated lazily via [`.iter()`](Cursor::iter)
    /// or drained via [`.execute()`](Cursor::execute) for a count.
    pub fn find(&self, collection: &str, query: Query) -> Result<Cursor<'db, '_, S>, DbError> {
        let predicate = Self::parse_optional_filter(query.filter.as_ref())?;
        let stmt = Statement::Find {
            collection,
            predicate,
            sort: query.sort,
            skip: query.skip,
            take: query.take,
            columns: query.columns,
        };
        self.prepare_cursor(stmt)
    }

    /// Get a single document by `_id`. Direct key lookup — O(1).
    pub fn find_by_id(
        &self,
        collection: &str,
        id: &str,
        columns: Option<&[&str]>,
    ) -> Result<Option<bson::Document>, DbError> {
        let handle = self.txn.collection(collection)?;
        let doc_id = BsonValue::from_raw_bson_ref(bson::raw::RawBsonRef::String(id))
            .expect("string is always a valid BsonValue");
        let now = bson::DateTime::now().timestamp_millis();
        let raw = match self.txn.get(&handle, &doc_id, now)? {
            Some(r) => r,
            None => return Ok(None),
        };

        let mut doc: bson::Document = bson::from_slice(raw.as_bytes())?;

        if let Some(wanted) = columns {
            let cols: Vec<String> = wanted.iter().map(|s| s.to_string()).collect();
            exec::apply_projection(&mut doc, &cols);
        }

        Ok(Some(doc))
    }

    /// Find the first document matching a query.
    pub fn find_one(
        &self,
        collection: &str,
        mut query: Query,
    ) -> Result<Option<RawDocumentBuf>, DbError> {
        query.take = Some(1);
        let cursor = self.find(collection, query)?;
        cursor.iter()?.next().transpose()
    }

    // ── Update operations ───────────────────────────────────────

    /// Update the first document matching the filter. Merges fields.
    pub fn update_one(
        &mut self,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
        update: impl IntoRawDocumentBuf,
        upsert: bool,
    ) -> Result<UpdateResult, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let raw = update.into_raw_document_buf()?;
        let mutation = slate_query::parse_mutation(&raw)?;
        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Update {
            collection,
            predicate,
            mutation,
            limit: Some(1),
        };
        let plan = self.plan(stmt)?;
        let exec = executor::Executor::new(&self.txn);
        let iter = exec.execute(plan)?;
        let mut matched = 0u64;
        let mut modified = 0u64;
        for result in iter {
            let opt_val = result?;
            matched += 1;
            if opt_val.is_some() {
                modified += 1;
            }
        }

        if matched == 0 && upsert {
            let result = self.insert_one(collection, raw)?;
            Ok(UpdateResult {
                matched: 0,
                modified: 0,
                upserted_id: Some(result.id),
            })
        } else {
            Ok(UpdateResult {
                matched,
                modified,
                upserted_id: None,
            })
        }
    }

    /// Update all documents matching the filter. Merges fields.
    pub fn update_many(
        &mut self,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
        update: impl IntoRawDocumentBuf,
    ) -> Result<UpdateResult, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let raw = update.into_raw_document_buf()?;
        let mutation = slate_query::parse_mutation(&raw)?;
        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Update {
            collection,
            predicate,
            mutation,
            limit: None,
        };
        let plan = self.plan(stmt)?;
        let exec = executor::Executor::new(&self.txn);
        let iter = exec.execute(plan)?;
        let mut matched = 0u64;
        let mut modified = 0u64;
        for result in iter {
            let opt_val = result?;
            matched += 1;
            if opt_val.is_some() {
                modified += 1;
            }
        }
        Ok(UpdateResult {
            matched,
            modified,
            upserted_id: None,
        })
    }

    /// Replace the first document matching the filter entirely (no merge).
    pub fn replace_one(
        &mut self,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
        replacement: impl IntoRawDocumentBuf,
    ) -> Result<UpdateResult, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let raw = replacement.into_raw_document_buf()?;
        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Replace {
            collection,
            predicate,
            replacement: raw,
        };
        let plan = self.plan(stmt)?;
        let exec = executor::Executor::new(&self.txn);
        let iter = exec.execute(plan)?;
        let mut matched = 0u64;
        let mut modified = 0u64;
        for result in iter {
            let opt_val = result?;
            matched += 1;
            if opt_val.is_some() {
                modified += 1;
            }
        }
        Ok(UpdateResult {
            matched,
            modified,
            upserted_id: None,
        })
    }

    // ── Delete operations ───────────────────────────────────────

    /// Delete the first document matching the filter.
    pub fn delete_one(
        &mut self,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
    ) -> Result<DeleteResult, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Delete {
            collection,
            predicate,
            limit: Some(1),
        };
        let plan = self.plan(stmt)?;
        let exec = executor::Executor::new(&self.txn);
        let iter = exec.execute(plan)?;
        let mut deleted = 0u64;
        for result in iter {
            result?;
            deleted += 1;
        }
        Ok(DeleteResult { deleted })
    }

    /// Delete all documents matching the filter.
    pub fn delete_many(
        &mut self,
        collection: &str,
        filter: impl IntoRawDocumentBuf,
    ) -> Result<DeleteResult, DbError> {
        let filter_raw = filter.into_raw_document_buf()?;
        let predicate = Self::parse_required_filter(&filter_raw)?;
        let stmt = Statement::Delete {
            collection,
            predicate,
            limit: None,
        };
        let plan = self.plan(stmt)?;
        let exec = executor::Executor::new(&self.txn);
        let iter = exec.execute(plan)?;
        let mut deleted = 0u64;
        for result in iter {
            result?;
            deleted += 1;
        }
        Ok(DeleteResult { deleted })
    }

    // ── Bulk upsert / merge operations ────────────────────────────

    /// Upsert (insert-or-replace) a batch of documents by `_id`.
    pub fn upsert_many(
        &mut self,
        collection: &str,
        docs: impl IntoIterator<Item = impl IntoRawDocumentBuf>,
    ) -> Result<UpsertResult, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| doc.into_raw_document_buf())
            .collect::<Result<Vec<_>, DbError>>()?;

        let stmt = Statement::Upsert {
            collection,
            docs: raw_docs,
        };
        let plan = self.plan(stmt)?;
        let exec = executor::Executor::new(&self.txn);
        let (iter, inserted, updated) = exec.execute_upsert_plan(plan)?;
        for result in iter {
            result?;
        }
        Ok(UpsertResult {
            inserted: inserted.get(),
            updated: updated.get(),
        })
    }

    /// Merge (insert-or-patch) a batch of partial documents by `_id`.
    pub fn merge_many(
        &mut self,
        collection: &str,
        docs: impl IntoIterator<Item = impl IntoRawDocumentBuf>,
    ) -> Result<UpsertResult, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| doc.into_raw_document_buf())
            .collect::<Result<Vec<_>, DbError>>()?;

        let stmt = Statement::Merge {
            collection,
            docs: raw_docs,
        };
        let plan = self.plan(stmt)?;
        let exec = executor::Executor::new(&self.txn);
        let (iter, inserted, updated) = exec.execute_upsert_plan(plan)?;
        for result in iter {
            result?;
        }
        Ok(UpsertResult {
            inserted: inserted.get(),
            updated: updated.get(),
        })
    }

    // ── Count ───────────────────────────────────────────────────

    /// Count documents matching a filter.
    pub fn count(&self, collection: &str, filter: Option<RawDocumentBuf>) -> Result<u64, DbError> {
        let query = Query {
            filter,
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        self.find(collection, query)?.execute()
    }

    /// Return distinct values for a field, with optional filter and sort.
    pub fn distinct(
        &self,
        collection: &str,
        query: DistinctQuery,
    ) -> Result<bson::RawBson, DbError> {
        let predicate = Self::parse_optional_filter(query.filter.as_ref())?;
        let stmt = Statement::Distinct {
            collection,
            field: query.field,
            predicate,
            sort: query.sort,
            skip: query.skip,
            take: query.take,
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
    pub fn purge_expired(&mut self, collection: &str) -> Result<u64, DbError> {
        let filter_doc = bson::rawdoc! { "ttl": { "$lt": bson::DateTime::now() } };
        let predicate = Self::parse_required_filter(&filter_doc)?;
        let stmt = Statement::Delete {
            collection,
            predicate,
            limit: None,
        };
        let plan = self.plan(stmt)?;
        // Use no-TTL-filter executor so the delete pipeline can read expired docs
        let exec = executor::Executor::new_no_ttl_filter(&self.txn);
        let iter = exec.execute(plan)?;
        let mut deleted = 0u64;
        for result in iter {
            result?;
            deleted += 1;
        }
        Ok(deleted)
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
        Ok(handle.indexes)
    }

    // ── Collection operations ───────────────────────────────────

    /// List all known collection names.
    pub fn list_collections(&self) -> Result<Vec<String>, DbError> {
        let configs = self.txn.list_collections()?;
        Ok(configs.into_iter().map(|c| c.name).collect())
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
        self.txn.create_collection(None, &config.name)?;

        // Ensure required indexes exist
        self.ensure_indexes(config)?;
        Ok(())
    }

    /// Ensure all indexes declared in the config exist, creating any that are missing.
    pub fn ensure_indexes(&mut self, config: &CollectionConfig) -> Result<(), DbError> {
        let handle = self.txn.collection(&config.name)?;
        let existing = handle.indexes;
        let mut fields: Vec<&str> = config.indexes.iter().map(|s| s.as_str()).collect();
        if !fields.iter().any(|&f| f == "ttl") {
            fields.push("ttl");
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
            Planner::new(|name: &str| self.txn.collection(name).map_err(|e| DbError::from(e)));
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
            Some(d) => Ok(parser::parse_filter(d)?),
            None => Ok(Expression::And(vec![])),
        }
    }
}
