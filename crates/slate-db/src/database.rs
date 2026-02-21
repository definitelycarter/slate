use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::Duration;

use bson::{Bson, RawDocumentBuf};
use slate_query::{DistinctQuery, FilterGroup, Query};
use slate_store::{Store, Transaction};

use crate::catalog::Catalog;
use crate::collection::CollectionConfig;
use crate::encoding;
use crate::error::DbError;
use crate::executor::exec;
use crate::executor::{ExecutionResult, RawValue};
use crate::executor_v2;
use crate::planner;
use crate::result::{DeleteResult, InsertResult, UpdateResult, UpsertResult};

const SYS_CF: &str = "_sys";
const ID_COLUMN: &str = "_id";

pub struct DatabaseConfig {
    /// Interval in seconds between TTL sweep runs.
    pub ttl_sweep_interval_secs: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            ttl_sweep_interval_secs: 10,
        }
    }
}

struct StoreInner<S: Store> {
    store: S,
    catalog: Catalog,
}

pub struct Database<S: Store> {
    inner: Arc<StoreInner<S>>,
    ttl_handle: Option<TtlHandle>,
}

impl<S: Store> Database<S> {
    #[cfg(any(test, feature = "bench-internals"))]
    pub fn store(&self) -> &S {
        &self.inner.store
    }

    pub fn begin(&self, read_only: bool) -> Result<DatabaseTransaction<'_, S>, DbError> {
        let txn = self.inner.store.begin(read_only)?;
        Ok(DatabaseTransaction {
            txn,
            catalog: &self.inner.catalog,
        })
    }

    /// Purge expired documents from a collection.
    pub fn purge_expired(&self, collection: &str) -> Result<u64, DbError> {
        purge_expired_inner(&self.inner, collection)
    }

    /// Gracefully stop background tasks.
    pub fn shutdown(&mut self) {
        if let Some(mut handle) = self.ttl_handle.take() {
            handle.stop();
        }
    }
}

impl<S: Store + Send + Sync + 'static> Database<S> {
    pub fn open(store: S, config: DatabaseConfig) -> Self {
        let _ = store.create_cf(SYS_CF);
        let inner = Arc::new(StoreInner {
            store,
            catalog: Catalog,
        });

        let shutdown = Arc::new(AtomicBool::new(false));
        let notify = Arc::new((Mutex::new(()), Condvar::new()));
        let sweep_inner = Arc::clone(&inner);
        let sweep_flag = Arc::clone(&shutdown);
        let sweep_notify = Arc::clone(&notify);
        let interval_secs = config.ttl_sweep_interval_secs;
        let handle = thread::spawn(move || {
            let interval = Duration::from_secs(interval_secs);
            loop {
                let (lock, cvar) = &*sweep_notify;
                let guard = lock.lock().unwrap();
                let _ = cvar.wait_timeout(guard, interval).unwrap();
                if sweep_flag.load(Ordering::Relaxed) {
                    break;
                }
                let collections = match sweep_inner.store.begin(true) {
                    Ok(mut txn) => match txn.cf(SYS_CF) {
                        Ok(sys) => match Catalog.list_collections(&txn, &sys) {
                            Ok(c) => {
                                let _ = txn.rollback();
                                c
                            }
                            Err(_) => continue,
                        },
                        Err(_) => continue,
                    },
                    Err(_) => continue,
                };
                for col in &collections {
                    let _ = purge_expired_inner(&sweep_inner, col);
                }
            }
        });

        Self {
            inner,
            ttl_handle: Some(TtlHandle {
                shutdown,
                notify,
                handle: Some(handle),
            }),
        }
    }
}

/// Standalone purge function that works with Arc<StoreInner> for the sweep thread.
fn purge_expired_inner<S: Store>(inner: &StoreInner<S>, collection: &str) -> Result<u64, DbError> {
    let mut txn = inner.store.begin(false).map_err(DbError::Store)?;
    let now_bytes = encoding::encode_datetime_millis(bson::DateTime::now().timestamp_millis());

    let cf = txn.cf(collection).map_err(DbError::Store)?;
    let prefix = encoding::index_scan_field_prefix("ttl");
    let val_start = prefix.len();
    let val_end = val_start + 8;
    let id_start = val_end + 1;

    let mut entries: Vec<(Vec<u8>, String)> = Vec::new();
    for result in txn.scan_prefix(&cf, &prefix).map_err(DbError::Store)? {
        let (key, _) = result.map_err(DbError::Store)?;
        if key.len() < id_start {
            continue;
        }
        let value_bytes = &key[val_start..val_end];
        if value_bytes >= now_bytes.as_slice() {
            break;
        }
        let record_id = match std::str::from_utf8(&key[id_start..]) {
            Ok(s) => s,
            Err(_) => continue,
        };
        entries.push((key.to_vec(), record_id.to_string()));
    }

    let mut deleted = 0u64;
    let sys = txn.cf(SYS_CF).map_err(DbError::Store)?;
    let indexed_fields = inner.catalog.list_indexes(&txn, &sys, collection)?;

    for (ttl_key, record_id) in &entries {
        let data_key = encoding::record_key(record_id);
        if let Some(bytes) = txn.get(&cf, &data_key).map_err(DbError::Store)? {
            let raw = bson::RawDocument::from_bytes(&bytes)?;
            for field in &indexed_fields {
                for value in exec::raw_get_path_values(raw, field)? {
                    let idx_key = encoding::raw_index_key(field, value, record_id);
                    txn.delete(&cf, &idx_key).map_err(DbError::Store)?;
                }
            }
            txn.delete(&cf, &data_key).map_err(DbError::Store)?;
        }
        txn.delete(&cf, ttl_key).map_err(DbError::Store)?;
        deleted += 1;
    }

    txn.commit().map_err(DbError::Store)?;
    Ok(deleted)
}

struct TtlHandle {
    shutdown: Arc<AtomicBool>,
    notify: Arc<(Mutex<()>, Condvar)>,
    handle: Option<thread::JoinHandle<()>>,
}

impl TtlHandle {
    fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.notify.1.notify_one();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for TtlHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

pub struct DatabaseTransaction<'db, S: Store + 'db> {
    txn: S::Txn<'db>,
    catalog: &'db Catalog,
}

impl<'db, S: Store + 'db> DatabaseTransaction<'db, S> {
    // ── Insert operations ───────────────────────────────────────

    /// Insert a single document. Fails with DuplicateKey if `_id` already exists.
    /// If the document has no `_id`, a UUID is generated.
    pub fn insert_one(
        &mut self,
        collection: &str,
        doc: bson::Document,
    ) -> Result<InsertResult, DbError> {
        let mut results = self.insert_many(collection, vec![doc])?;
        Ok(results.remove(0))
    }

    /// Insert multiple documents. Fails per-doc on duplicate `_id`.
    pub fn insert_many(
        &mut self,
        collection: &str,
        docs: Vec<bson::Document>,
    ) -> Result<Vec<InsertResult>, DbError> {
        let raw_docs: Vec<RawDocumentBuf> = docs
            .into_iter()
            .map(|doc| Ok(RawDocumentBuf::from_document(&doc)?))
            .collect::<Result<Vec<_>, DbError>>()?;

        let stmt = planner::Statement::Insert { docs: raw_docs };
        let (plan, cf) = self.plan_statement(collection, stmt)?;
        let ids = match executor_v2::Executor::new(&self.txn, &cf).execute(&plan)? {
            ExecutionResult::Insert { ids } => ids,
            _ => unreachable!(),
        };

        Ok(ids.into_iter().map(|id| InsertResult { id }).collect())
    }

    // ── Query operations ────────────────────────────────────────

    /// Find documents matching a query (filter, sort, skip, take, projection).
    pub fn find(
        &mut self,
        collection: &str,
        query: &Query,
    ) -> Result<Vec<RawDocumentBuf>, DbError> {
        let stmt = planner::Statement::Find(query.clone());
        let (plan, cf) = self.plan_statement(collection, stmt)?;
        match executor_v2::Executor::new(&self.txn, &cf).execute(&plan)? {
            ExecutionResult::Rows(iter) => iter
                .map(|r| {
                    let opt_val: Option<RawValue> = r?;
                    let val =
                        opt_val.ok_or_else(|| DbError::InvalidQuery("expected value".into()))?;
                    val.into_document_buf()
                        .ok_or_else(|| DbError::InvalidQuery("expected document".into()))
                })
                .collect(),
            _ => unreachable!(),
        }
    }

    /// Get a single document by `_id`. Direct key lookup — O(1).
    pub fn find_by_id(
        &mut self,
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

        let mut doc: bson::Document = bson::from_slice(&bytes)?;
        if let Some(wanted) = columns {
            let cols: Vec<String> = wanted.iter().map(|s| s.to_string()).collect();
            exec::apply_projection(&mut doc, &cols);
        }

        Ok(Some(doc))
    }

    /// Find the first document matching a query.
    pub fn find_one(
        &mut self,
        collection: &str,
        query: &Query,
    ) -> Result<Option<RawDocumentBuf>, DbError> {
        let mut q = query.clone();
        q.take = Some(1);
        let results = self.find(collection, &q)?;
        Ok(results.into_iter().next())
    }

    // ── Update operations ───────────────────────────────────────

    /// Update the first document matching the filter. Merges fields.
    pub fn update_one(
        &mut self,
        collection: &str,
        filter: &FilterGroup,
        update: bson::Document,
        upsert: bool,
    ) -> Result<UpdateResult, DbError> {
        let stmt = planner::Statement::Update {
            filter: filter.clone(),
            update: update.clone(),
            limit: Some(1),
        };
        let (plan, cf) = self.plan_statement(collection, stmt)?;
        let (matched, modified) = match executor_v2::Executor::new(&self.txn, &cf).execute(&plan)? {
            ExecutionResult::Update { matched, modified } => (matched, modified),
            _ => unreachable!(),
        };

        if matched == 0 && upsert {
            let result = self.insert_one(collection, update)?;
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
        filter: &FilterGroup,
        update: bson::Document,
    ) -> Result<UpdateResult, DbError> {
        let stmt = planner::Statement::Update {
            filter: filter.clone(),
            update,
            limit: None,
        };
        let (plan, cf) = self.plan_statement(collection, stmt)?;
        let (matched, modified) = match executor_v2::Executor::new(&self.txn, &cf).execute(&plan)? {
            ExecutionResult::Update { matched, modified } => (matched, modified),
            _ => unreachable!(),
        };
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
        filter: &FilterGroup,
        replacement: bson::Document,
    ) -> Result<UpdateResult, DbError> {
        let stmt = planner::Statement::Replace {
            filter: filter.clone(),
            replacement,
        };
        let (plan, cf) = self.plan_statement(collection, stmt)?;
        let (matched, modified) = match executor_v2::Executor::new(&self.txn, &cf).execute(&plan)? {
            ExecutionResult::Update { matched, modified } => (matched, modified),
            _ => unreachable!(),
        };
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
        filter: &FilterGroup,
    ) -> Result<DeleteResult, DbError> {
        let stmt = planner::Statement::Delete {
            filter: filter.clone(),
            limit: Some(1),
        };
        let (plan, cf) = self.plan_statement(collection, stmt)?;
        let deleted = match executor_v2::Executor::new(&self.txn, &cf).execute(&plan)? {
            ExecutionResult::Delete { deleted } => deleted,
            _ => unreachable!(),
        };
        Ok(DeleteResult { deleted })
    }

    /// Delete all documents matching the filter.
    pub fn delete_many(
        &mut self,
        collection: &str,
        filter: &FilterGroup,
    ) -> Result<DeleteResult, DbError> {
        let stmt = planner::Statement::Delete {
            filter: filter.clone(),
            limit: None,
        };
        let (plan, cf) = self.plan_statement(collection, stmt)?;
        let deleted = match executor_v2::Executor::new(&self.txn, &cf).execute(&plan)? {
            ExecutionResult::Delete { deleted } => deleted,
            _ => unreachable!(),
        };
        Ok(DeleteResult { deleted })
    }

    // ── Bulk upsert / merge operations ────────────────────────────

    /// Upsert (insert-or-replace) a batch of documents by `_id`.
    /// Each document must have an `_id`. If a document with that `_id` exists,
    /// it is fully replaced. Otherwise it is inserted.
    pub fn upsert_many(
        &mut self,
        collection: &str,
        docs: Vec<bson::Document>,
    ) -> Result<UpsertResult, DbError> {
        let cf = self.collection_cf(collection)?;
        let sys = self.txn.cf(SYS_CF)?;
        let indexed_fields = self.catalog.list_indexes(&self.txn, &sys, collection)?;
        let mut inserted = 0u64;
        let mut updated = 0u64;

        for mut doc in docs {
            let id = extract_or_generate_id(&mut doc);
            let key = encoding::record_key(&id);

            if self.txn.get(&cf, &key)?.is_some() {
                self.replace_by_id(&id, doc, &cf, &indexed_fields)?;
                updated += 1;
            } else {
                self.insert_with_id(&cf, &id, doc, &indexed_fields)?;
                inserted += 1;
            }
        }

        Ok(UpsertResult { inserted, updated })
    }

    /// Merge (insert-or-patch) a batch of partial documents by `_id`.
    /// Each document must have an `_id`. If a document with that `_id` exists,
    /// the provided fields are merged into it (existing fields not in the update
    /// are preserved). Otherwise the document is inserted as-is.
    pub fn merge_many(
        &mut self,
        collection: &str,
        docs: Vec<bson::Document>,
    ) -> Result<UpsertResult, DbError> {
        let cf = self.collection_cf(collection)?;
        let sys = self.txn.cf(SYS_CF)?;
        let indexed_fields = self.catalog.list_indexes(&self.txn, &sys, collection)?;
        let mut inserted = 0u64;
        let mut updated = 0u64;

        for mut doc in docs {
            let id = extract_or_generate_id(&mut doc);
            let key = encoding::record_key(&id);

            if self.txn.get(&cf, &key)?.is_some() {
                self.raw_merge_update(&id, &doc, &cf, &indexed_fields)?;
                updated += 1;
            } else {
                self.insert_with_id(&cf, &id, doc, &indexed_fields)?;
                inserted += 1;
            }
        }

        Ok(UpsertResult { inserted, updated })
    }

    // ── Count ───────────────────────────────────────────────────

    /// Count documents matching a filter.
    /// Streams results without materializing all documents into memory.
    pub fn count(
        &mut self,
        collection: &str,
        filter: Option<&FilterGroup>,
    ) -> Result<u64, DbError> {
        let stmt = planner::Statement::Find(Query {
            filter: filter.cloned(),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        });
        let (plan, cf) = self.plan_statement(collection, stmt)?;
        match executor_v2::Executor::new(&self.txn, &cf).execute(&plan)? {
            ExecutionResult::Rows(iter) => {
                let mut n = 0u64;
                for result in iter {
                    result?;
                    n += 1;
                }
                Ok(n)
            }
            _ => unreachable!(),
        }
    }

    /// Return distinct values for a field, with optional filter and sort.
    pub fn distinct(
        &mut self,
        collection: &str,
        query: &DistinctQuery,
    ) -> Result<bson::RawBson, DbError> {
        let stmt = planner::Statement::Distinct(query.clone());
        let (plan, cf) = self.plan_statement(collection, stmt)?;
        match executor_v2::Executor::new(&self.txn, &cf).execute(&plan)? {
            ExecutionResult::Rows(mut iter) => match iter.next() {
                Some(result) => {
                    let opt_val: Option<RawValue> = result?;
                    opt_val
                        .ok_or_else(|| DbError::InvalidQuery("expected value".into()))?
                        .into_raw_bson()
                        .ok_or_else(|| DbError::InvalidQuery("unsupported bson type".into()))
                }
                None => Ok(bson::RawBson::Array(bson::RawArrayBuf::new())),
            },
            _ => unreachable!(),
        }
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
            let doc: bson::Document = bson::from_slice(&value)?;
            for val in exec::get_path_values(&doc, field) {
                let idx_key = encoding::index_key(field, val, &record_id);
                self.txn
                    .put(&cf, &idx_key, &encoding::bson_type_byte(val))?;
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
    pub fn list_indexes(&mut self, collection: &str) -> Result<Vec<String>, DbError> {
        let sys = self.txn.cf(SYS_CF)?;
        self.catalog.list_indexes(&self.txn, &sys, collection)
    }

    // ── Collection operations ───────────────────────────────────

    /// List all known collection names.
    pub fn list_collections(&mut self) -> Result<Vec<String>, DbError> {
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
    pub fn ensure_indexes(&mut self, config: &CollectionConfig) -> Result<(), DbError> {
        let sys = self.txn.cf(SYS_CF)?;
        let existing = self.catalog.list_indexes(&self.txn, &sys, &config.name)?;
        for field in &config.indexes {
            if !existing.contains(field) {
                self.create_index(&config.name, field)?;
            }
        }
        Ok(())
    }

    // ── Private helpers ─────────────────────────────────────────

    /// Resolve catalog metadata and plan a statement through the planner.
    fn plan_statement(
        &mut self,
        collection: &str,
        statement: planner::Statement,
    ) -> Result<(planner::PlanNode, <S::Txn<'db> as Transaction>::Cf), DbError> {
        let cf = self.collection_cf(collection)?;
        let sys = self.txn.cf(SYS_CF)?;
        let indexed_fields = self.catalog.list_indexes(&self.txn, &sys, collection)?;
        let plan = planner::plan(collection, indexed_fields, statement);
        Ok((plan, cf))
    }

    /// Resolve a collection CF, returning CollectionNotFound if it doesn't exist.
    fn collection_cf(
        &mut self,
        collection: &str,
    ) -> Result<<S::Txn<'db> as Transaction>::Cf, DbError> {
        self.txn
            .cf(collection)
            .map_err(|_| DbError::CollectionNotFound(collection.to_string()))
    }

    /// Write index entries for a document. Handles multi-key paths.
    fn write_index_entries(
        &mut self,
        cf: &<S::Txn<'db> as Transaction>::Cf,
        id: &str,
        doc: &bson::Document,
        indexed_fields: &[String],
    ) -> Result<(), DbError> {
        for field in indexed_fields {
            for value in exec::get_path_values(doc, field) {
                let idx_key = encoding::index_key(field, value, id);
                self.txn
                    .put(cf, &idx_key, &encoding::bson_type_byte(value))?;
            }
        }
        Ok(())
    }

    /// Write a TTL index entry if the document has a `ttl` DateTime field.
    fn write_ttl_index_entry(
        &mut self,
        cf: &<S::Txn<'db> as Transaction>::Cf,
        id: &str,
        doc: &bson::Document,
    ) -> Result<(), DbError> {
        if let Some(Bson::DateTime(dt)) = doc.get("ttl") {
            let val = Bson::DateTime(*dt);
            let idx_key = encoding::index_key("ttl", &val, id);
            self.txn
                .put(cf, &idx_key, &encoding::bson_type_byte(&val))?;
        }
        Ok(())
    }

    /// Delete index entries using raw BSON document (avoids deserialization).
    fn delete_raw_index_entries(
        &mut self,
        cf: &<S::Txn<'db> as Transaction>::Cf,
        id: &str,
        raw: &bson::RawDocument,
        indexed_fields: &[String],
    ) -> Result<(), DbError> {
        for field in indexed_fields {
            for value in exec::raw_get_path_values(raw, field)? {
                let idx_key = encoding::raw_index_key(field, value, id);
                self.txn.delete(cf, &idx_key)?;
            }
        }
        Ok(())
    }

    /// Delete a TTL index entry using raw BSON document.
    fn delete_raw_ttl_index_entry(
        &mut self,
        cf: &<S::Txn<'db> as Transaction>::Cf,
        id: &str,
        raw: &bson::RawDocument,
    ) -> Result<(), DbError> {
        if let Some(bson::raw::RawBsonRef::DateTime(dt)) = raw.get("ttl")? {
            let idx_key = encoding::raw_index_key("ttl", bson::raw::RawBsonRef::DateTime(dt), id);
            self.txn.delete(cf, &idx_key)?;
        }
        Ok(())
    }

    /// Insert a document with a known `_id` (already extracted from the doc).
    fn insert_with_id(
        &mut self,
        cf: &<S::Txn<'db> as Transaction>::Cf,
        id: &str,
        doc: bson::Document,
        indexed_fields: &[String],
    ) -> Result<(), DbError> {
        let key = encoding::record_key(id);
        self.txn.put(cf, &key, &bson::to_vec(&doc)?)?;
        self.write_index_entries(cf, id, &doc, indexed_fields)?;
        self.write_ttl_index_entry(cf, id, &doc)?;
        Ok(())
    }

    /// Replace a document by `_id`: delete old indexes, write new doc + indexes.
    fn replace_by_id(
        &mut self,
        id: &str,
        doc: bson::Document,
        cf: &<S::Txn<'db> as Transaction>::Cf,
        indexed_fields: &[String],
    ) -> Result<(), DbError> {
        let key = encoding::record_key(id);

        // Clean up old index entries from raw bytes (no deserialization)
        let old_bytes = self.txn.get(cf, &key)?.map(|b| b.to_vec());
        if let Some(ref bytes) = old_bytes {
            let raw = bson::RawDocument::from_bytes(bytes)?;
            self.delete_raw_index_entries(cf, id, raw, indexed_fields)?;
            self.delete_raw_ttl_index_entry(cf, id, raw)?;
        }

        // Write new document and indexes
        self.txn.put(cf, &key, &bson::to_vec(&doc)?)?;
        self.write_index_entries(cf, id, &doc, indexed_fields)?;
        self.write_ttl_index_entry(cf, id, &doc)?;
        Ok(())
    }

    /// Merge fields into an existing document using raw BSON (no full deserialization).
    /// Unchanged fields are copied as raw bytes via `append_ref()`.
    /// Returns true if the document was actually modified.
    fn raw_merge_update(
        &mut self,
        id: &str,
        update: &bson::Document,
        cf: &<S::Txn<'db> as Transaction>::Cf,
        indexed_fields: &[String],
    ) -> Result<bool, DbError> {
        let key = encoding::record_key(id);

        let old_bytes = match self.txn.get(cf, &key)? {
            Some(b) => b.to_vec(),
            None => return Ok(false),
        };
        let old_raw = bson::RawDocument::from_bytes(&old_bytes)?;

        // Collect old indexed values from raw bytes
        let old_indexed: Vec<(&str, Vec<bson::raw::RawBsonRef<'_>>)> = indexed_fields
            .iter()
            .map(|f| {
                let vals = exec::raw_get_path_values(old_raw, f).unwrap_or_default();
                (f.as_str(), vals)
            })
            .collect();

        // Track old TTL
        let old_ttl_raw = old_raw.get("ttl")?;

        // Build update key set for fast lookup
        let update_keys: std::collections::HashSet<&str> = update
            .keys()
            .filter(|k| *k != ID_COLUMN)
            .map(|k| k.as_str())
            .collect();

        // Check if anything actually changed
        let mut changed = false;
        for (ukey, uval) in update {
            if ukey == ID_COLUMN {
                continue;
            }
            match old_raw.get(ukey)? {
                Some(old_val) => {
                    if !exec::raw_values_eq(&old_val, uval) {
                        changed = true;
                        break;
                    }
                }
                None => {
                    changed = true;
                    break;
                }
            }
        }

        if !changed {
            return Ok(false);
        }

        // Build merged RawDocumentBuf
        let mut merged = RawDocumentBuf::new();

        // Copy old fields not in the update
        for result in old_raw.iter() {
            let (field_key, field_val) = result?;
            if !update_keys.contains(field_key) {
                merged.append_ref(field_key, field_val);
            }
        }

        // Append update fields
        for (ukey, uval) in update {
            if ukey == ID_COLUMN {
                continue;
            }
            let raw_val = bson::RawBson::try_from(uval.clone())
                .map_err(|e| DbError::Serialization(e.to_string()))?;
            merged.append(ukey, raw_val);
        }

        // Write merged document
        self.txn.put(cf, &key, merged.as_bytes())?;

        // Index maintenance: diff old vs new
        let new_raw = bson::RawDocument::from_bytes(merged.as_bytes())?;
        for (field, old_vals) in &old_indexed {
            let new_vals = exec::raw_get_path_values(new_raw, field)?;

            // Delete removed index entries
            for old_val in old_vals {
                if !new_vals.iter().any(|nv| exec::raw_refs_eq(nv, old_val)) {
                    let idx_key = encoding::raw_index_key(field, *old_val, id);
                    self.txn.delete(cf, &idx_key)?;
                }
            }

            // Add new index entries
            for new_val in &new_vals {
                if !old_vals.iter().any(|ov| exec::raw_refs_eq(new_val, ov)) {
                    let idx_key = encoding::raw_index_key(field, *new_val, id);
                    let type_byte = encoding::raw_bson_ref_type_byte(*new_val);
                    self.txn.put(cf, &idx_key, &type_byte)?;
                }
            }
        }

        // TTL index maintenance
        let new_ttl_raw = new_raw.get("ttl")?;
        if old_ttl_raw != new_ttl_raw {
            if let Some(bson::raw::RawBsonRef::DateTime(dt)) = old_ttl_raw {
                let idx_key =
                    encoding::raw_index_key("ttl", bson::raw::RawBsonRef::DateTime(dt), id);
                self.txn.delete(cf, &idx_key)?;
            }
            if let Some(bson::raw::RawBsonRef::DateTime(dt)) = new_ttl_raw {
                let idx_key =
                    encoding::raw_index_key("ttl", bson::raw::RawBsonRef::DateTime(dt), id);
                let type_byte =
                    encoding::raw_bson_ref_type_byte(bson::raw::RawBsonRef::DateTime(dt));
                self.txn.put(cf, &idx_key, &type_byte)?;
            }
        }

        Ok(true)
    }
}

/// Extract `_id` from a document, or generate a UUID if not present.
/// Keeps `_id` in the document — it is stored in both the key and the value.
fn extract_or_generate_id(doc: &mut bson::Document) -> String {
    match doc.get(ID_COLUMN) {
        Some(Bson::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => {
            let id = uuid::Uuid::new_v4().to_string();
            doc.insert(ID_COLUMN, &id);
            id
        }
    }
}
