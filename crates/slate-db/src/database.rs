use bson::Bson;
use slate_query::{FilterGroup, Query};
use slate_store::{Store, Transaction};

use crate::catalog::Catalog;
use crate::encoding;
use crate::error::DbError;
use crate::exec;
use crate::executor;
use crate::planner;
use crate::result::{DeleteResult, InsertResult, UpdateResult};

const SYS_CF: &str = "_sys";
const ID_COLUMN: &str = "_id";

pub struct Database<S: Store> {
    store: S,
    catalog: Catalog,
}

impl<S: Store> Database<S> {
    pub fn new(store: S) -> Self {
        let _ = store.create_cf(SYS_CF);
        Self {
            store,
            catalog: Catalog,
        }
    }

    pub fn begin(&self, read_only: bool) -> Result<DatabaseTransaction<'_, S>, DbError> {
        let txn = self.store.begin(read_only)?;
        Ok(DatabaseTransaction {
            txn,
            catalog: &self.catalog,
        })
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
        self.catalog.ensure_collection(&mut self.txn, collection)?;

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
        let indexed_fields = self.catalog.list_indexes(&self.txn, collection)?;
        for field in &indexed_fields {
            if let Some(value) = exec::get_path(&doc, field) {
                let idx_key = encoding::index_key(field, value, &id);
                self.txn.put(collection, &idx_key, &[])?;
            }
        }

        Ok(InsertResult { id })
    }

    /// Insert multiple documents. Fails per-doc on duplicate `_id`.
    pub fn insert_many(
        &mut self,
        collection: &str,
        docs: Vec<bson::Document>,
    ) -> Result<Vec<InsertResult>, DbError> {
        self.catalog.ensure_collection(&mut self.txn, collection)?;

        let indexed_fields = self.catalog.list_indexes(&self.txn, collection)?;
        let mut results = Vec::with_capacity(docs.len());

        for mut doc in docs {
            let id = extract_or_generate_id(&mut doc);
            let key = encoding::record_key(&id);

            if self.txn.get(collection, &key)?.is_some() {
                return Err(DbError::DuplicateKey(id));
            }

            self.txn.put(collection, &key, &bson::to_vec(&doc)?)?;

            for field in &indexed_fields {
                if let Some(value) = exec::get_path(&doc, field) {
                    let idx_key = encoding::index_key(field, value, &id);
                    self.txn.put(collection, &idx_key, &[])?;
                }
            }

            results.push(InsertResult { id });
        }

        Ok(results)
    }

    // ── Query operations ────────────────────────────────────────

    /// Find documents matching a query (filter, sort, skip, take, projection).
    /// Returns an empty result set if the collection doesn't exist.
    pub fn find(&self, collection: &str, query: &Query) -> Result<Vec<bson::Document>, DbError> {
        let indexed_fields = self.catalog.list_indexes(&self.txn, collection)?;
        let plan = planner::plan(collection, &indexed_fields, query);
        match executor::execute(&self.txn, &plan) {
            Ok(iter) => iter.collect(),
            Err(DbError::Store(ref e)) if e.to_string().contains("column family not found") => {
                Ok(vec![])
            }
            Err(e) => Err(e),
        }
    }

    /// Get a single document by `_id`. Direct key lookup — O(1).
    pub fn find_by_id(
        &self,
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
        &self,
        collection: &str,
        query: &Query,
    ) -> Result<Option<bson::Document>, DbError> {
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
            let id = matched_doc
                .get_str(ID_COLUMN)
                .map(|s| s.to_string())
                .unwrap_or_default();
            let modified = self.merge_update(collection, &id, &update)?;
            Ok(UpdateResult {
                matched: 1,
                modified: if modified { 1 } else { 0 },
                upserted_id: None,
            })
        } else if upsert {
            self.catalog.ensure_collection(&mut self.txn, collection)?;
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

        for doc in matches {
            let id = doc
                .get_str(ID_COLUMN)
                .map(|s| s.to_string())
                .unwrap_or_default();
            if self.merge_update(collection, &id, &update)? {
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
            let id = matched_doc
                .get_str(ID_COLUMN)
                .map(|s| s.to_string())
                .unwrap_or_default();

            let key = encoding::record_key(&id);
            let indexed_fields = self.catalog.list_indexes(&self.txn, collection)?;

            // Delete old index entries
            self.delete_index_entries(collection, &id, &matched_doc, &indexed_fields)?;

            // Strip _id from replacement if present
            replacement.remove(ID_COLUMN);

            // Write new document
            self.txn
                .put(collection, &key, &bson::to_vec(&replacement)?)?;

            // Insert new index entries
            for field in &indexed_fields {
                if let Some(value) = exec::get_path(&replacement, field) {
                    let idx_key = encoding::index_key(field, value, &id);
                    self.txn.put(collection, &idx_key, &[])?;
                }
            }

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
            let id = doc
                .get_str(ID_COLUMN)
                .map(|s| s.to_string())
                .unwrap_or_default();
            self.delete_by_id(collection, &id, &doc)?;
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

        for doc in matches {
            let id = doc
                .get_str(ID_COLUMN)
                .map(|s| s.to_string())
                .unwrap_or_default();
            self.delete_by_id(collection, &id, &doc)?;
        }

        Ok(DeleteResult { deleted: count })
    }

    // ── Count ───────────────────────────────────────────────────

    /// Count documents matching a filter.
    pub fn count(&self, collection: &str, filter: Option<&FilterGroup>) -> Result<u64, DbError> {
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

    // ── Index operations ────────────────────────────────────────

    /// Create an index on a field and backfill existing records.
    pub fn create_index(&mut self, collection: &str, field: &str) -> Result<(), DbError> {
        self.catalog.ensure_collection(&mut self.txn, collection)?;
        self.catalog
            .create_index(&mut self.txn, collection, field)?;

        // Backfill: scan all records and index the field
        let scan_prefix = encoding::data_scan_prefix("");
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .txn
            .scan_prefix(collection, &scan_prefix)?
            .filter_map(|r| r.ok().map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect();

        for (key, value) in entries {
            let record_id = match encoding::parse_record_key(&key) {
                Some(id) => id.to_string(),
                None => continue,
            };
            let doc: bson::Document = bson::from_slice(&value)?;
            if let Some(val) = exec::get_path(&doc, field) {
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
            .filter_map(|r| r.ok().map(|(k, _)| k.to_vec()))
            .collect();
        for key in keys {
            self.txn.delete(collection, &key)?;
        }

        self.catalog.drop_index(&mut self.txn, collection, field)?;
        Ok(())
    }

    /// List indexed fields for a collection.
    pub fn list_indexes(&self, collection: &str) -> Result<Vec<String>, DbError> {
        self.catalog.list_indexes(&self.txn, collection)
    }

    // ── Collection operations ───────────────────────────────────

    /// List all known collection names.
    pub fn list_collections(&self) -> Result<Vec<String>, DbError> {
        self.catalog.list_collections(&self.txn)
    }

    /// Drop a collection and all its data, indexes, and metadata.
    pub fn drop_collection(&mut self, collection: &str) -> Result<(), DbError> {
        // Delete all data keys
        let data_prefix = encoding::data_scan_prefix("");
        let data_keys: Vec<Vec<u8>> = self
            .txn
            .scan_prefix(collection, &data_prefix)?
            .filter_map(|r| r.ok().map(|(k, _)| k.to_vec()))
            .collect();
        for key in data_keys {
            self.txn.delete(collection, &key)?;
        }

        // Delete all index keys
        let idx_prefix = b"i:".to_vec();
        let idx_keys: Vec<Vec<u8>> = self
            .txn
            .scan_prefix(collection, &idx_prefix)?
            .filter_map(|r| r.ok().map(|(k, _)| k.to_vec()))
            .collect();
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

    // ── Private helpers ─────────────────────────────────────────

    /// Merge update fields into an existing document by _id.
    /// Returns true if the document was actually modified.
    fn merge_update(
        &mut self,
        collection: &str,
        id: &str,
        update: &bson::Document,
    ) -> Result<bool, DbError> {
        let key = encoding::record_key(id);
        let indexed_fields = self.catalog.list_indexes(&self.txn, collection)?;

        let mut existing = match self.txn.get(collection, &key)? {
            Some(bytes) => bson::from_slice::<bson::Document>(&bytes)?,
            None => return Ok(false),
        };

        // Track old indexed values
        let old_indexed: Vec<(String, Option<Bson>)> = indexed_fields
            .iter()
            .map(|f| (f.clone(), exec::get_path(&existing, f).cloned()))
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

        // Index maintenance
        for (field, old_val) in &old_indexed {
            let new_val = exec::get_path(&existing, field).cloned();

            if let Some(old) = old_val {
                if new_val.as_ref() != Some(old) {
                    let old_idx_key = encoding::index_key(field, old, id);
                    self.txn.delete(collection, &old_idx_key)?;
                }
            }

            if let Some(new) = &new_val {
                if old_val.as_ref() != Some(new) {
                    let idx_key = encoding::index_key(field, new, id);
                    self.txn.put(collection, &idx_key, &[])?;
                }
            }
        }

        Ok(true)
    }

    /// Delete a document by _id, cleaning up its index entries.
    fn delete_by_id(
        &mut self,
        collection: &str,
        id: &str,
        doc: &bson::Document,
    ) -> Result<(), DbError> {
        let key = encoding::record_key(id);
        let indexed_fields = self.catalog.list_indexes(&self.txn, collection)?;

        // The doc from find() has _id but the stored doc doesn't, so we need
        // to read the stored doc for accurate index values
        if let Some(bytes) = self.txn.get(collection, &key)? {
            let stored: bson::Document = bson::from_slice(&bytes)?;
            self.delete_index_entries(collection, id, &stored, &indexed_fields)?;
        } else {
            // Fallback: use the doc from find() (skip _id field)
            self.delete_index_entries(collection, id, doc, &indexed_fields)?;
        }

        self.txn.delete(collection, &key)?;
        Ok(())
    }

    /// Delete index entries for a document.
    fn delete_index_entries(
        &mut self,
        collection: &str,
        id: &str,
        doc: &bson::Document,
        indexed_fields: &[String],
    ) -> Result<(), DbError> {
        for field in indexed_fields {
            if let Some(value) = exec::get_path(doc, field) {
                let idx_key = encoding::index_key(field, value, id);
                self.txn.delete(collection, &idx_key)?;
            }
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
