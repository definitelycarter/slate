use std::collections::HashMap;

use bson::Bson;
use slate_query::Query;
use slate_store::{Store, Transaction};

use crate::catalog::Catalog;
use crate::datasource::Datasource;
use crate::encoding;
use crate::error::DbError;
use crate::exec;
use crate::executor;
use crate::planner;
use crate::record;

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
            store: &self.store,
            catalog: &self.catalog,
        })
    }
}

pub struct DatabaseTransaction<'db, S: Store + 'db> {
    txn: S::Txn<'db>,
    store: &'db S,
    catalog: &'db Catalog,
}

impl<'db, S: Store + 'db> DatabaseTransaction<'db, S> {
    // ── Catalog operations ──────────────────────────────────────────

    pub fn save_datasource(&mut self, datasource: &Datasource) -> Result<(), DbError> {
        let _ = self.store.create_cf(&datasource.partition);
        self.catalog.save(&mut self.txn, datasource)
    }

    pub fn get_datasource(&self, id: &str) -> Result<Option<Datasource>, DbError> {
        self.catalog.get(&self.txn, id)
    }

    pub fn list_datasources(&self) -> Result<Vec<Datasource>, DbError> {
        self.catalog.list(&self.txn)
    }

    pub fn delete_datasource(&mut self, id: &str) -> Result<(), DbError> {
        self.catalog.delete(&mut self.txn, id)
    }

    // ── Datasource lookup helper ────────────────────────────────────

    fn require_datasource(&self, datasource_id: &str) -> Result<Datasource, DbError> {
        self.catalog
            .get(&self.txn, datasource_id)?
            .ok_or_else(|| DbError::DatasourceNotFound(datasource_id.to_string()))
    }

    /// Build the full record key: {datasource_id}:{record_id}
    fn full_record_id(datasource_id: &str, record_id: &str) -> String {
        format!("{datasource_id}:{record_id}")
    }

    // ── Write operations ────────────────────────────────────────────

    /// Write a document for a record. Uses read-modify-write to merge with existing data.
    pub fn write_record(
        &mut self,
        datasource_id: &str,
        record_id: &str,
        doc: bson::Document,
    ) -> Result<(), DbError> {
        record::validate_document(&doc)?;
        let ds = self.require_datasource(datasource_id)?;
        let full_id = Self::full_record_id(datasource_id, record_id);
        let key = encoding::record_key(&full_id);

        // Read existing record for merge (partial writes)
        let mut existing = match self.txn.get(&ds.partition, &key)? {
            Some(bytes) => bson::from_slice::<bson::Document>(&bytes)?,
            None => bson::Document::new(),
        };

        // Track old indexed values for cleanup
        let old_indexed: HashMap<String, Bson> = ds
            .fields
            .iter()
            .filter(|f| f.indexed)
            .filter_map(|f| exec::get_path(&existing, &f.name).map(|v| (f.name.clone(), v.clone())))
            .collect();

        // Merge new fields
        for (column, value) in &doc {
            if column == ID_COLUMN {
                continue;
            }
            existing.insert(column.clone(), value.clone());
        }

        // Write raw BSON bytes
        self.txn
            .put(&ds.partition, &key, &bson::to_vec(&existing)?)?;

        // Index maintenance
        for field in &ds.fields {
            if !field.indexed {
                continue;
            }
            let new_value = exec::get_path(&existing, &field.name);
            let old_value = old_indexed.get(&field.name);

            // Delete old index entry if value changed
            if let Some(old_val) = old_value {
                if new_value != Some(old_val) {
                    let old_idx_key = encoding::index_key(&field.name, old_val, &full_id);
                    self.txn.delete(&ds.partition, &old_idx_key)?;
                }
            }

            // Add new index entry
            if let Some(new_val) = new_value {
                let idx_key = encoding::index_key(&field.name, new_val, &full_id);
                self.txn.put(&ds.partition, &idx_key, &[])?;
            }
        }

        Ok(())
    }

    /// Write multiple records in a single transaction.
    pub fn write_batch(
        &mut self,
        datasource_id: &str,
        writes: Vec<(String, bson::Document)>,
    ) -> Result<(), DbError> {
        let ds = self.require_datasource(datasource_id)?;

        // Validate all documents first
        for (_, doc) in &writes {
            record::validate_document(doc)?;
        }

        // Batch read existing records
        let full_ids: Vec<String> = writes
            .iter()
            .map(|(id, _)| Self::full_record_id(datasource_id, id))
            .collect();
        let keys: Vec<Vec<u8>> = full_ids.iter().map(|id| encoding::record_key(id)).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let existing_records = self.txn.multi_get(&ds.partition, &key_refs)?;

        for (i, (_, doc)) in writes.into_iter().enumerate() {
            let full_id = &full_ids[i];

            let mut existing = match &existing_records[i] {
                Some(bytes) => bson::from_slice::<bson::Document>(bytes)?,
                None => bson::Document::new(),
            };

            // Track old indexed values for cleanup
            let old_indexed: HashMap<String, Bson> = ds
                .fields
                .iter()
                .filter(|f| f.indexed)
                .filter_map(|f| {
                    exec::get_path(&existing, &f.name).map(|v| (f.name.clone(), v.clone()))
                })
                .collect();

            // Merge new fields
            for (column, value) in &doc {
                if column == ID_COLUMN {
                    continue;
                }
                existing.insert(column.clone(), value.clone());
            }

            // Write raw BSON bytes
            self.txn
                .put(&ds.partition, &keys[i], &bson::to_vec(&existing)?)?;

            // Index maintenance
            for field in &ds.fields {
                if !field.indexed {
                    continue;
                }
                let new_value = exec::get_path(&existing, &field.name);
                let old_value = old_indexed.get(&field.name);

                if let Some(old_val) = old_value {
                    if new_value != Some(old_val) {
                        let old_idx_key = encoding::index_key(&field.name, old_val, full_id);
                        self.txn.delete(&ds.partition, &old_idx_key)?;
                    }
                }

                if let Some(new_val) = new_value {
                    let idx_key = encoding::index_key(&field.name, new_val, full_id);
                    self.txn.put(&ds.partition, &idx_key, &[])?;
                }
            }
        }

        Ok(())
    }

    /// Delete a record and its index entries.
    pub fn delete_record(&mut self, datasource_id: &str, record_id: &str) -> Result<(), DbError> {
        let ds = self.require_datasource(datasource_id)?;
        let full_id = Self::full_record_id(datasource_id, record_id);
        let key = encoding::record_key(&full_id);

        // Read record to get indexed values for cleanup
        if let Some(bytes) = self.txn.get(&ds.partition, &key)? {
            let doc: bson::Document = bson::from_slice(&bytes)?;
            for field in &ds.fields {
                if field.indexed {
                    if let Some(value) = exec::get_path(&doc, &field.name) {
                        let idx_key = encoding::index_key(&field.name, value, &full_id);
                        self.txn.delete(&ds.partition, &idx_key)?;
                    }
                }
            }
        }

        self.txn.delete(&ds.partition, &key)?;
        Ok(())
    }

    // ── Read operations ─────────────────────────────────────────────

    /// Get a single record by ID, optionally projecting specific columns.
    pub fn get_by_id(
        &self,
        datasource_id: &str,
        record_id: &str,
        columns: Option<&[&str]>,
    ) -> Result<Option<bson::Document>, DbError> {
        let ds = self.require_datasource(datasource_id)?;
        let full_id = Self::full_record_id(datasource_id, record_id);
        let key = encoding::record_key(&full_id);

        let bytes = match self.txn.get(&ds.partition, &key)? {
            Some(b) => b,
            None => return Ok(None),
        };

        let mut doc: bson::Document = bson::from_slice(&bytes)?;
        doc.insert("_id", record_id);

        // Apply projection
        if let Some(wanted) = columns {
            let cols: Vec<String> = wanted.iter().map(|s| s.to_string()).collect();
            exec::apply_projection(&mut doc, &cols);
        }

        Ok(Some(doc))
    }

    // ── Query ───────────────────────────────────────────────────────

    /// Query records in a datasource.
    ///
    /// Builds a plan tree from the query, then executes it lazily:
    /// Scan/IndexScan → Filter → Sort → Limit → Projection
    pub fn query(
        &self,
        datasource_id: &str,
        query: &Query,
    ) -> Result<Vec<bson::Document>, DbError> {
        let ds = self.require_datasource(datasource_id)?;
        let ds_prefix = format!("{datasource_id}:");

        let plan = planner::plan(&ds, query);
        let iter = executor::execute(&self.txn, &plan, &ds_prefix)?;
        iter.collect()
    }

    // ── Lifecycle ───────────────────────────────────────────────────

    pub fn commit(self) -> Result<(), DbError> {
        self.txn.commit()?;
        Ok(())
    }

    pub fn rollback(self) -> Result<(), DbError> {
        self.txn.rollback()?;
        Ok(())
    }
}
