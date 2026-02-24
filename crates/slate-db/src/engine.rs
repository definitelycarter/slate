use bson::RawDocumentBuf;
use slate_query::{DistinctQuery, Query};
use slate_store::{Store, Transaction as StoreTxn};

use crate::catalog::Catalog;
use crate::collection::CollectionConfig;
use crate::convert::IntoRawDocumentBuf;
use crate::cursor::Cursor;
use crate::encoding;
use crate::error::DbError;
use crate::executor;
use crate::executor::exec;
use crate::executor::RawValue;
use crate::planner::{Planner, Statement};
use crate::result::{DeleteResult, InsertResult, UpdateResult, UpsertResult};

const SYS_CF: &str = "_sys";

pub struct Engine<S: Store> {
    store: S,
    catalog: Catalog,
}

impl<S: Store> Engine<S> {
    pub fn new(store: S) -> Self {
        let _ = store.create_cf(SYS_CF);
        Self {
            store,
            catalog: Catalog,
        }
    }

    pub fn begin(&self, read_only: bool) -> Result<Transaction<'_, S>, DbError> {
        let txn = self.store.begin(read_only)?;
        Ok(Transaction {
            txn,
            catalog: &self.catalog,
        })
    }

    /// Purge expired documents from a collection.
    pub fn purge_expired(&self, collection: &str) -> Result<u64, DbError> {
        let mut txn = self.begin(false)?;
        let deleted = txn.purge_expired(collection)?;
        txn.commit()?;
        Ok(deleted)
    }

    #[cfg(any(test, feature = "bench-internals"))]
    pub fn store(&self) -> &S {
        &self.store
    }
}

pub struct Transaction<'db, S: Store + 'db> {
    txn: S::Txn<'db>,
    catalog: &'db Catalog,
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

        let stmt = Statement::Insert { docs: raw_docs };
        let cf = self.collection_cf(collection)?;
        let plan = Planner::new(&self.txn, self.catalog).plan(collection, stmt)?;
        let iter = executor::Executor::new(&self.txn, &cf).execute(plan)?;
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
        let stmt = Statement::Find {
            filter: query.filter,
            sort: query.sort,
            skip: query.skip,
            take: query.take,
            columns: query.columns,
        };
        self.prepare_cursor(collection, stmt)
    }

    /// Get a single document by `_id`. Direct key lookup — O(1).
    pub fn find_by_id(
        &self,
        collection: &str,
        id: &str,
        columns: Option<&[&str]>,
    ) -> Result<Option<bson::Document>, DbError> {
        let cf = self.collection_cf(collection)?;
        let key = encoding::record_key(id);
        let bytes = match self.txn.get(&cf, &key)? {
            Some(b) => b,
            None => return Ok(None),
        };

        // O(1) TTL check on envelope prefix
        if encoding::is_record_expired(&bytes, bson::DateTime::now().timestamp_millis()) {
            return Ok(None);
        }

        let (_, bson_slice) = encoding::decode_record(&bytes)?;
        let mut doc: bson::Document = bson::from_slice(bson_slice)?;

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
        let stmt = Statement::Update {
            filter: filter_raw,
            mutation,
            limit: Some(1),
        };
        let cf = self.collection_cf(collection)?;
        let plan = Planner::new(&self.txn, self.catalog).plan(collection, stmt)?;
        let iter = executor::Executor::new(&self.txn, &cf).execute(plan)?;
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
        let stmt = Statement::Update {
            filter: filter_raw,
            mutation,
            limit: None,
        };
        let cf = self.collection_cf(collection)?;
        let plan = Planner::new(&self.txn, self.catalog).plan(collection, stmt)?;
        let iter = executor::Executor::new(&self.txn, &cf).execute(plan)?;
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
        let stmt = Statement::Replace {
            filter: filter_raw,
            replacement: raw,
        };
        let cf = self.collection_cf(collection)?;
        let plan = Planner::new(&self.txn, self.catalog).plan(collection, stmt)?;
        let iter = executor::Executor::new(&self.txn, &cf).execute(plan)?;
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
        let stmt = Statement::Delete {
            filter: filter_raw,
            limit: Some(1),
        };
        let cf = self.collection_cf(collection)?;
        let plan = Planner::new(&self.txn, self.catalog).plan(collection, stmt)?;
        let iter = executor::Executor::new(&self.txn, &cf).execute(plan)?;
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
        let stmt = Statement::Delete {
            filter: filter_raw,
            limit: None,
        };
        let cf = self.collection_cf(collection)?;
        let plan = Planner::new(&self.txn, self.catalog).plan(collection, stmt)?;
        let iter = executor::Executor::new(&self.txn, &cf).execute(plan)?;
        let mut deleted = 0u64;
        for result in iter {
            result?;
            deleted += 1;
        }
        Ok(DeleteResult { deleted })
    }

    // ── Bulk upsert / merge operations ────────────────────────────

    /// Upsert (insert-or-replace) a batch of documents by `_id`.
    /// Each document must have an `_id`. If a document with that `_id` exists,
    /// it is fully replaced. Otherwise it is inserted.
    pub fn upsert_many(
        &mut self,
        collection: &str,
        docs: impl IntoIterator<Item = impl IntoRawDocumentBuf>,
    ) -> Result<UpsertResult, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| doc.into_raw_document_buf())
            .collect::<Result<Vec<_>, DbError>>()?;

        let stmt = Statement::UpsertMany { docs: raw_docs };
        let cf = self.collection_cf(collection)?;
        let planner = Planner::new(&self.txn, self.catalog);
        let plan = planner.plan(collection, stmt)?;
        let exec = executor::Executor::new(&self.txn, &cf);
        let (iter, inserted, updated) = exec.execute_upsert(plan)?;
        for result in iter {
            result?;
        }
        Ok(UpsertResult {
            inserted: inserted.get(),
            updated: updated.get(),
        })
    }

    /// Merge (insert-or-patch) a batch of partial documents by `_id`.
    /// Each document must have an `_id`. If a document with that `_id` exists,
    /// the provided fields are merged into it (existing fields not in the update
    /// are preserved). Otherwise the document is inserted as-is.
    pub fn merge_many(
        &mut self,
        collection: &str,
        docs: impl IntoIterator<Item = impl IntoRawDocumentBuf>,
    ) -> Result<UpsertResult, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| doc.into_raw_document_buf())
            .collect::<Result<Vec<_>, DbError>>()?;

        let stmt = Statement::MergeMany { docs: raw_docs };
        let cf = self.collection_cf(collection)?;
        let planner = Planner::new(&self.txn, self.catalog);
        let plan = planner.plan(collection, stmt)?;
        let exec = executor::Executor::new(&self.txn, &cf);
        let (iter, inserted, updated) = exec.execute_upsert(plan)?;
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
    /// Streams results without materializing all documents into memory.
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
        let stmt = Statement::Distinct {
            field: query.field,
            filter: query.filter,
            sort: query.sort,
            skip: query.skip,
            take: query.take,
        };
        let cf = self.collection_cf(collection)?;
        let planner = Planner::new(&self.txn, self.catalog);
        let plan = planner.plan(collection, stmt)?;
        let exec = executor::Executor::new(&self.txn, &cf);
        let mut iter = exec.execute(plan)?;
        match iter.next() {
            Some(result) => {
                let opt_val: Option<RawValue> = result?;
                opt_val
                    .ok_or_else(|| DbError::InvalidQuery("expected value".into()))?
                    .into_raw_bson()
                    .ok_or_else(|| DbError::InvalidQuery("unsupported bson type".into()))
            }
            None => Ok(bson::RawBson::Array(bson::RawArrayBuf::new())),
        }
    }

    // ── TTL operations ──────────────────────────────────────────

    /// Purge expired documents from a collection.
    /// Uses `FlushExpired` which retains the TTL index for efficient scanning.
    pub fn purge_expired(&mut self, collection: &str) -> Result<u64, DbError> {
        let stmt = Statement::FlushExpired {
            filter: bson::rawdoc! { "ttl": { "$lt": bson::DateTime::now() } },
        };
        let cf = self.collection_cf(collection)?;
        let planner = Planner::new(&self.txn, self.catalog);
        let plan = planner.plan(collection, stmt)?;
        // Use no-TTL-filter executor so the delete pipeline can read expired docs
        let iter = executor::Executor::new_no_ttl_filter(&self.txn, &cf).execute(plan)?;
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
        let cf = self.collection_cf(collection)?;
        self.catalog
            .create_index(&mut self.txn, collection, field)?;

        // Backfill: scan all records and index the field
        let scan_prefix = encoding::data_scan_prefix("");
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .txn
            .scan_prefix(&cf, &scan_prefix)?
            .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect::<Result<_, _>>()
            .map_err(DbError::Store)?;

        for (key, value) in entries {
            let record_id = match encoding::parse_record_key(&key) {
                Some(id) => id.to_string(),
                None => continue,
            };
            let (_, bson_slice) = encoding::decode_record(&value)?;
            let doc: bson::Document = bson::from_slice(bson_slice)?;
            let ttl_millis = doc.get_datetime("ttl").ok().map(|dt| dt.timestamp_millis());
            for val in exec::get_path_values(&doc, field) {
                let idx_key = encoding::index_key(field, val, &record_id);
                let idx_val =
                    encoding::encode_index_value(encoding::bson_type_byte(val)[0], ttl_millis);
                self.txn.put(&cf, &idx_key, &idx_val)?;
            }
        }

        Ok(())
    }

    /// Drop an index and remove all its entries.
    pub fn drop_index(&mut self, collection: &str, field: &str) -> Result<(), DbError> {
        let cf = self.collection_cf(collection)?;
        let prefix = encoding::index_scan_field_prefix(field);
        let keys: Vec<Vec<u8>> = self
            .txn
            .scan_prefix(&cf, &prefix)?
            .map(|r| r.map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()
            .map_err(DbError::Store)?;
        for key in keys {
            self.txn.delete(&cf, &key)?;
        }

        self.catalog.drop_index(&mut self.txn, collection, field)?;
        Ok(())
    }

    /// List indexed fields for a collection.
    pub fn list_indexes(&self, collection: &str) -> Result<Vec<String>, DbError> {
        let sys = self.txn.cf(SYS_CF)?;
        self.catalog.list_indexes(&self.txn, &sys, collection)
    }

    // ── Collection operations ───────────────────────────────────

    /// List all known collection names.
    pub fn list_collections(&self) -> Result<Vec<String>, DbError> {
        let sys = self.txn.cf(SYS_CF)?;
        self.catalog.list_collections(&self.txn, &sys)
    }

    /// Drop a collection and all its data, indexes, and metadata.
    pub fn drop_collection(&mut self, collection: &str) -> Result<(), DbError> {
        self.txn.drop_cf(collection)?;
        self.catalog.drop_collection(&mut self.txn, collection)?;
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
    /// Idempotent — if the collection already exists, this is a no-op.
    pub fn create_collection(&mut self, config: &CollectionConfig) -> Result<(), DbError> {
        self.catalog.create_collection(&mut self.txn, config)?;
        self.ensure_indexes(config)?;
        Ok(())
    }

    /// Ensure all indexes declared in the config exist, creating any that are missing.
    /// Always adds a "ttl" index so the planner can use IndexScan for TTL purges.
    pub fn ensure_indexes(&mut self, config: &CollectionConfig) -> Result<(), DbError> {
        let sys = self.txn.cf(SYS_CF)?;
        let existing = self.catalog.list_indexes(&self.txn, &sys, &config.name)?;
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

    /// Prepare a cursor for a query statement. The Cursor owns the
    /// catalog ref, collection name, and statement — planning is deferred.
    fn prepare_cursor(
        &self,
        collection: &str,
        statement: Statement,
    ) -> Result<Cursor<'db, '_, S>, DbError> {
        let cf = self.collection_cf(collection)?;
        Ok(Cursor::new(
            &self.txn,
            cf,
            self.catalog,
            collection.to_string(),
            statement,
        ))
    }

    /// Resolve a collection CF, returning CollectionNotFound if it doesn't exist.
    fn collection_cf(
        &self,
        collection: &str,
    ) -> Result<<S::Txn<'db> as slate_store::Transaction>::Cf, DbError> {
        self.txn
            .cf(collection)
            .map_err(|_| DbError::CollectionNotFound(collection.to_string()))
    }
}
