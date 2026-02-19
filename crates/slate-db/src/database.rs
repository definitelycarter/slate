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
use crate::exec;
use crate::executor;
use crate::planner;
use crate::result::{DeleteResult, InsertResult, UpdateResult};

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
                    Ok(mut txn) => match Catalog.list_collections(&mut txn) {
                        Ok(c) => {
                            let _ = txn.rollback();
                            c
                        }
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

    let prefix = encoding::index_scan_field_prefix("ttl");
    let val_start = prefix.len();
    let val_end = val_start + 8;
    let id_start = val_end + 1;

    let mut entries: Vec<(Vec<u8>, String)> = Vec::new();
    for result in txn
        .scan_prefix(collection, &prefix)
        .map_err(DbError::Store)?
    {
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
    let indexed_fields = inner.catalog.list_indexes(&mut txn, collection)?;

    for (ttl_key, record_id) in &entries {
        let data_key = encoding::record_key(record_id);
        if let Some(bytes) = txn.get(collection, &data_key).map_err(DbError::Store)? {
            let doc: bson::Document = bson::from_slice(&bytes)?;
            for field in &indexed_fields {
                for value in exec::get_path_values(&doc, field) {
                    let idx_key = encoding::index_key(field, value, record_id);
                    txn.delete(collection, &idx_key).map_err(DbError::Store)?;
                }
            }
            txn.delete(collection, &data_key).map_err(DbError::Store)?;
        }
        txn.delete(collection, ttl_key).map_err(DbError::Store)?;
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
        mut doc: bson::Document,
    ) -> Result<InsertResult, DbError> {
        self.require_collection(collection)?;

        // Extract or generate _id
        let id = extract_or_generate_id(&mut doc);
        let key = encoding::record_key(&id);

        // Check for duplicate
        if self.txn.get(collection, &key)?.is_some() {
            return Err(DbError::DuplicateKey(id));
        }

        // Write document (without _id — it's in the key)
        self.txn.put(collection, &key, &bson::to_vec(&doc)?)?;

        // Index maintenance
        let indexed_fields = self.catalog.list_indexes(&mut self.txn, collection)?;
        self.write_index_entries(collection, &id, &doc, &indexed_fields)?;
        self.write_ttl_index_entry(collection, &id, &doc)?;

        Ok(InsertResult { id })
    }

    /// Insert multiple documents. Fails per-doc on duplicate `_id`.
    pub fn insert_many(
        &mut self,
        collection: &str,
        docs: Vec<bson::Document>,
    ) -> Result<Vec<InsertResult>, DbError> {
        self.require_collection(collection)?;

        let indexed_fields = self.catalog.list_indexes(&mut self.txn, collection)?;
        let mut results = Vec::with_capacity(docs.len());

        for mut doc in docs {
            let id = extract_or_generate_id(&mut doc);
            let key = encoding::record_key(&id);

            if self.txn.get(collection, &key)?.is_some() {
                return Err(DbError::DuplicateKey(id));
            }

            self.txn.put(collection, &key, &bson::to_vec(&doc)?)?;

            self.write_index_entries(collection, &id, &doc, &indexed_fields)?;
            self.write_ttl_index_entry(collection, &id, &doc)?;

            results.push(InsertResult { id });
        }

        Ok(results)
    }

    // ── Query operations ────────────────────────────────────────

    /// Find documents matching a query (filter, sort, skip, take, projection).
    /// Returns an empty result set if the collection doesn't exist.
    pub fn find(
        &mut self,
        collection: &str,
        query: &Query,
    ) -> Result<Vec<RawDocumentBuf>, DbError> {
        let indexed_fields = self.catalog.list_indexes(&mut self.txn, collection)?;
        let plan = planner::plan(collection, &indexed_fields, query);
        match executor::execute(&mut self.txn, &plan) {
            Ok(iter) => iter
                .map(|r| {
                    let (_id, opt_val) = r?;
                    let val =
                        opt_val.ok_or_else(|| DbError::InvalidQuery("expected value".into()))?;
                    val.into_document_buf()
                        .ok_or_else(|| DbError::InvalidQuery("expected document".into()))
                })
                .collect(),
            Err(DbError::Store(ref e)) if e.to_string().contains("column family not found") => {
                Ok(vec![])
            }
            Err(e) => Err(e),
        }
    }

    /// Get a single document by `_id`. Direct key lookup — O(1).
    pub fn find_by_id(
        &mut self,
        collection: &str,
        id: &str,
        columns: Option<&[&str]>,
    ) -> Result<Option<bson::Document>, DbError> {
        let key = encoding::record_key(id);
        let bytes = match self.txn.get(collection, &key) {
            Ok(Some(b)) => b,
            Ok(None) => return Ok(None),
            Err(ref e) if e.to_string().contains("column family not found") => return Ok(None),
            Err(e) => return Err(DbError::Store(e)),
        };

        let mut doc: bson::Document = bson::from_slice(&bytes)?;
        doc.insert("_id", id);

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
        let query = Query {
            filter: Some(filter.clone()),
            sort: vec![],
            skip: None,
            take: Some(1),
            columns: None,
        };
        let matches = self.find(collection, &query)?;

        if let Some(matched_doc) = matches.into_iter().next() {
            let id = matched_doc.get_str(ID_COLUMN).ok().unwrap_or_default();
            let modified = self.merge_update(collection, id, &update)?;
            Ok(UpdateResult {
                matched: 1,
                modified: if modified { 1 } else { 0 },
                upserted_id: None,
            })
        } else if upsert {
            let result = self.insert_one(collection, update)?;
            Ok(UpdateResult {
                matched: 0,
                modified: 0,
                upserted_id: Some(result.id),
            })
        } else {
            Ok(UpdateResult {
                matched: 0,
                modified: 0,
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
        let query = Query {
            filter: Some(filter.clone()),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        let matches = self.find(collection, &query)?;
        let matched = matches.len() as u64;
        let mut modified = 0u64;

        for doc in &matches {
            let id = doc.get_str(ID_COLUMN).ok().unwrap_or_default();
            if self.merge_update(collection, id, &update)? {
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
        filter: &FilterGroup,
        mut replacement: bson::Document,
    ) -> Result<UpdateResult, DbError> {
        let query = Query {
            filter: Some(filter.clone()),
            sort: vec![],
            skip: None,
            take: Some(1),
            columns: None,
        };
        let matches = self.find(collection, &query)?;

        if let Some(matched_doc) = matches.into_iter().next() {
            let id = matched_doc.get_str(ID_COLUMN).ok().unwrap_or_default();

            let key = encoding::record_key(id);
            let indexed_fields = self.catalog.list_indexes(&mut self.txn, collection)?;

            // Read stored raw bytes for index cleanup (no deserialization)
            let old_bytes = self.txn.get(collection, &key)?.map(|b| b.to_vec());
            if let Some(ref bytes) = old_bytes {
                let raw = bson::RawDocument::from_bytes(bytes)?;
                self.delete_raw_index_entries(collection, id, raw, &indexed_fields)?;
                self.delete_raw_ttl_index_entry(collection, id, raw)?;
            }

            // Strip _id from replacement if present
            replacement.remove(ID_COLUMN);

            // Write new document
            self.txn
                .put(collection, &key, &bson::to_vec(&replacement)?)?;

            // Insert new index entries
            self.write_index_entries(collection, id, &replacement, &indexed_fields)?;
            self.write_ttl_index_entry(collection, id, &replacement)?;

            Ok(UpdateResult {
                matched: 1,
                modified: 1,
                upserted_id: None,
            })
        } else {
            Ok(UpdateResult {
                matched: 0,
                modified: 0,
                upserted_id: None,
            })
        }
    }

    // ── Delete operations ───────────────────────────────────────

    /// Delete the first document matching the filter.
    pub fn delete_one(
        &mut self,
        collection: &str,
        filter: &FilterGroup,
    ) -> Result<DeleteResult, DbError> {
        let query = Query {
            filter: Some(filter.clone()),
            sort: vec![],
            skip: None,
            take: Some(1),
            columns: None,
        };
        let matches = self.find(collection, &query)?;

        if let Some(doc) = matches.into_iter().next() {
            let id = doc.get_str(ID_COLUMN).ok().unwrap_or_default();
            self.delete_by_id(collection, id)?;
            Ok(DeleteResult { deleted: 1 })
        } else {
            Ok(DeleteResult { deleted: 0 })
        }
    }

    /// Delete all documents matching the filter.
    pub fn delete_many(
        &mut self,
        collection: &str,
        filter: &FilterGroup,
    ) -> Result<DeleteResult, DbError> {
        let query = Query {
            filter: Some(filter.clone()),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        let matches = self.find(collection, &query)?;
        let count = matches.len() as u64;

        for doc in &matches {
            let id = doc.get_str(ID_COLUMN).ok().unwrap_or_default();
            self.delete_by_id(collection, id)?;
        }

        Ok(DeleteResult { deleted: count })
    }

    // ── Count ───────────────────────────────────────────────────

    /// Count documents matching a filter.
    pub fn count(
        &mut self,
        collection: &str,
        filter: Option<&FilterGroup>,
    ) -> Result<u64, DbError> {
        let query = Query {
            filter: filter.cloned(),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        let results = self.find(collection, &query)?;
        Ok(results.len() as u64)
    }

    /// Return distinct values for a field, with optional filter and sort.
    pub fn distinct(
        &mut self,
        collection: &str,
        query: &DistinctQuery,
    ) -> Result<bson::RawBson, DbError> {
        let indexed_fields = self.catalog.list_indexes(&mut self.txn, collection)?;
        let plan = planner::plan_distinct(collection, &indexed_fields, query);
        match executor::execute(&mut self.txn, &plan) {
            Ok(mut iter) => match iter.next() {
                Some(result) => {
                    let (_id, opt_val) = result?;
                    opt_val
                        .ok_or_else(|| DbError::InvalidQuery("expected value".into()))?
                        .into_raw_bson()
                        .ok_or_else(|| DbError::InvalidQuery("unsupported bson type".into()))
                }
                None => Ok(bson::RawBson::Array(bson::RawArrayBuf::new())),
            },
            Err(DbError::Store(ref e)) if e.to_string().contains("column family not found") => {
                Ok(bson::RawBson::Array(bson::RawArrayBuf::new()))
            }
            Err(e) => Err(e),
        }
    }

    // ── Index operations ────────────────────────────────────────

    /// Create an index on a field and backfill existing records.
    pub fn create_index(&mut self, collection: &str, field: &str) -> Result<(), DbError> {
        self.require_collection(collection)?;
        self.catalog
            .create_index(&mut self.txn, collection, field)?;

        // Backfill: scan all records and index the field
        let scan_prefix = encoding::data_scan_prefix("");
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .txn
            .scan_prefix(collection, &scan_prefix)?
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
                self.txn.put(collection, &idx_key, &[])?;
            }
        }

        Ok(())
    }

    /// Drop an index and remove all its entries.
    pub fn drop_index(&mut self, collection: &str, field: &str) -> Result<(), DbError> {
        // Remove all index entries for this field
        let prefix = encoding::index_scan_field_prefix(field);
        let keys: Vec<Vec<u8>> = self
            .txn
            .scan_prefix(collection, &prefix)?
            .map(|r| r.map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()
            .map_err(DbError::Store)?;
        for key in keys {
            self.txn.delete(collection, &key)?;
        }

        self.catalog.drop_index(&mut self.txn, collection, field)?;
        Ok(())
    }

    /// List indexed fields for a collection.
    pub fn list_indexes(&mut self, collection: &str) -> Result<Vec<String>, DbError> {
        self.catalog.list_indexes(&mut self.txn, collection)
    }

    // ── Collection operations ───────────────────────────────────

    /// List all known collection names.
    pub fn list_collections(&mut self) -> Result<Vec<String>, DbError> {
        self.catalog.list_collections(&mut self.txn)
    }

    /// Drop a collection and all its data, indexes, and metadata.
    pub fn drop_collection(&mut self, collection: &str) -> Result<(), DbError> {
        // Delete all data keys
        let data_prefix = encoding::data_scan_prefix("");
        let data_keys: Vec<Vec<u8>> = self
            .txn
            .scan_prefix(collection, &data_prefix)?
            .map(|r| r.map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()
            .map_err(DbError::Store)?;
        for key in data_keys {
            self.txn.delete(collection, &key)?;
        }

        // Delete all index keys
        let idx_prefix = b"i:".to_vec();
        let idx_keys: Vec<Vec<u8>> = self
            .txn
            .scan_prefix(collection, &idx_prefix)?
            .map(|r| r.map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()
            .map_err(DbError::Store)?;
        for key in idx_keys {
            self.txn.delete(collection, &key)?;
        }

        // Remove catalog metadata
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
        let existing = self.catalog.list_indexes(&mut self.txn, &config.name)?;
        for field in &config.indexes {
            if !existing.contains(field) {
                self.create_index(&config.name, field)?;
            }
        }
        Ok(())
    }

    // ── Private helpers ─────────────────────────────────────────

    /// Verify a collection exists, returning CollectionNotFound if not.
    fn require_collection(&mut self, collection: &str) -> Result<(), DbError> {
        if !self.catalog.collection_exists(&mut self.txn, collection)? {
            return Err(DbError::CollectionNotFound(collection.to_string()));
        }
        Ok(())
    }

    /// Merge update fields into an existing document by _id.
    /// Returns true if the document was actually modified.
    fn merge_update(
        &mut self,
        collection: &str,
        id: &str,
        update: &bson::Document,
    ) -> Result<bool, DbError> {
        let key = encoding::record_key(id);
        let indexed_fields = self.catalog.list_indexes(&mut self.txn, collection)?;

        let mut existing = match self.txn.get(collection, &key)? {
            Some(bytes) => bson::from_slice::<bson::Document>(&bytes)?,
            None => return Ok(false),
        };

        // Track old TTL value before merge
        let old_ttl = existing.get("ttl").cloned();

        // Track old indexed values (multi-key aware)
        let old_indexed: Vec<(String, Vec<Bson>)> = indexed_fields
            .iter()
            .map(|f| {
                let values = exec::get_path_values(&existing, f)
                    .into_iter()
                    .cloned()
                    .collect();
                (f.clone(), values)
            })
            .collect();

        // Merge
        let mut changed = false;
        for (column, value) in update {
            if column == ID_COLUMN {
                continue;
            }
            if existing.get(column) != Some(value) {
                changed = true;
            }
            existing.insert(column.clone(), value.clone());
        }

        if !changed {
            return Ok(false);
        }

        // Write
        self.txn.put(collection, &key, &bson::to_vec(&existing)?)?;

        // Index maintenance: diff old vs new value sets
        for (field, old_values) in &old_indexed {
            let new_values: Vec<&Bson> = exec::get_path_values(&existing, field);

            for old_val in old_values {
                if !new_values.contains(&old_val) {
                    let idx_key = encoding::index_key(field, old_val, id);
                    self.txn.delete(collection, &idx_key)?;
                }
            }

            for new_val in &new_values {
                if !old_values.contains(new_val) {
                    let idx_key = encoding::index_key(field, new_val, id);
                    self.txn.put(collection, &idx_key, &[])?;
                }
            }
        }

        // TTL index maintenance: diff old vs new ttl value
        let new_ttl = existing.get("ttl").cloned();
        if old_ttl != new_ttl {
            if let Some(Bson::DateTime(dt)) = &old_ttl {
                let idx_key = encoding::index_key("ttl", &Bson::DateTime(*dt), id);
                self.txn.delete(collection, &idx_key)?;
            }
            if let Some(Bson::DateTime(dt)) = &new_ttl {
                let idx_key = encoding::index_key("ttl", &Bson::DateTime(*dt), id);
                self.txn.put(collection, &idx_key, &[])?;
            }
        }

        Ok(true)
    }

    /// Delete a document by _id, cleaning up its index entries.
    fn delete_by_id(&mut self, collection: &str, id: &str) -> Result<(), DbError> {
        let key = encoding::record_key(id);
        let indexed_fields = self.catalog.list_indexes(&mut self.txn, collection)?;

        let old_bytes = self.txn.get(collection, &key)?.map(|b| b.to_vec());
        if let Some(ref bytes) = old_bytes {
            let raw = bson::RawDocument::from_bytes(bytes)?;
            self.delete_raw_index_entries(collection, id, raw, &indexed_fields)?;
            self.delete_raw_ttl_index_entry(collection, id, raw)?;
        }

        self.txn.delete(collection, &key)?;
        Ok(())
    }

    /// Write index entries for a document. Handles multi-key paths.
    fn write_index_entries(
        &mut self,
        collection: &str,
        id: &str,
        doc: &bson::Document,
        indexed_fields: &[String],
    ) -> Result<(), DbError> {
        for field in indexed_fields {
            for value in exec::get_path_values(doc, field) {
                let idx_key = encoding::index_key(field, value, id);
                self.txn.put(collection, &idx_key, &[])?;
            }
        }
        Ok(())
    }

    /// Write a TTL index entry if the document has a `ttl` DateTime field.
    fn write_ttl_index_entry(
        &mut self,
        collection: &str,
        id: &str,
        doc: &bson::Document,
    ) -> Result<(), DbError> {
        if let Some(Bson::DateTime(dt)) = doc.get("ttl") {
            let idx_key = encoding::index_key("ttl", &Bson::DateTime(*dt), id);
            self.txn.put(collection, &idx_key, &[])?;
        }
        Ok(())
    }

    /// Delete index entries using raw BSON document (avoids deserialization).
    fn delete_raw_index_entries(
        &mut self,
        collection: &str,
        id: &str,
        raw: &bson::RawDocument,
        indexed_fields: &[String],
    ) -> Result<(), DbError> {
        for field in indexed_fields {
            for value in exec::raw_get_path_values(raw, field)? {
                let idx_key = encoding::raw_index_key(field, value, id);
                self.txn.delete(collection, &idx_key)?;
            }
        }
        Ok(())
    }

    /// Delete a TTL index entry using raw BSON document.
    fn delete_raw_ttl_index_entry(
        &mut self,
        collection: &str,
        id: &str,
        raw: &bson::RawDocument,
    ) -> Result<(), DbError> {
        if let Some(bson::raw::RawBsonRef::DateTime(dt)) = raw.get("ttl")? {
            let idx_key = encoding::raw_index_key("ttl", bson::raw::RawBsonRef::DateTime(dt), id);
            self.txn.delete(collection, &idx_key)?;
        }
        Ok(())
    }
}

/// Extract `_id` from a document, or generate a UUID if not present.
/// Removes `_id` from the document (it's stored in the key, not the value).
fn extract_or_generate_id(doc: &mut bson::Document) -> String {
    match doc.remove(ID_COLUMN) {
        Some(Bson::String(s)) => s,
        Some(other) => other.to_string(),
        None => uuid::Uuid::new_v4().to_string(),
    }
}
