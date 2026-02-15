use std::collections::HashMap;

use slate_query::Query;
use slate_store::{Store, Transaction};

use crate::catalog::Catalog;
use crate::cell::{Cell, CellWrite, RecordValue, StoredCell};
use crate::datasource::Datasource;
use crate::encoding;
use crate::error::DbError;
use crate::executor;
use crate::planner;
use crate::record::{Record, Value};

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

    /// Deduplicate cells: last write per column wins.
    fn dedup_cells(cells: Vec<CellWrite>) -> Vec<CellWrite> {
        let mut best: HashMap<String, CellWrite> = HashMap::new();
        for cell in cells {
            best.insert(cell.column.clone(), cell);
        }
        best.into_values().collect()
    }

    /// Compute expire_at for a cell based on the datasource field TTL config.
    fn compute_expire_at(ds: &Datasource, column: &str) -> Option<i64> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        ds.fields
            .iter()
            .find(|f| f.name == column)
            .and_then(|f| f.ttl_seconds)
            .map(|ttl| now + ttl)
    }

    // ── Write operations ────────────────────────────────────────────

    /// Write cells for a record. Uses read-modify-write to merge with existing data.
    /// Auto-sets record-level expire_at from _id field TTL.
    pub fn write_record(
        &mut self,
        datasource_id: &str,
        record_id: &str,
        cells: Vec<CellWrite>,
    ) -> Result<(), DbError> {
        let ds = self.require_datasource(datasource_id)?;
        let full_id = Self::full_record_id(datasource_id, record_id);
        let key = encoding::record_key(&full_id);

        let cells = Self::dedup_cells(cells);

        // Read existing record for merge (partial writes)
        let mut record_value = match self.txn.get(&ds.partition, &key)? {
            Some(bytes) => bincode::deserialize(&bytes)?,
            None => RecordValue {
                expire_at: None,
                cells: HashMap::new(),
            },
        };

        // Track old indexed values for cleanup
        let old_indexed: HashMap<String, Value> = ds
            .fields
            .iter()
            .filter(|f| f.indexed)
            .filter_map(|f| {
                record_value
                    .cells
                    .get(&f.name)
                    .map(|sc| (f.name.clone(), sc.value.clone()))
            })
            .collect();

        // Merge new cells
        for cell in &cells {
            if cell.column == ID_COLUMN {
                record_value.expire_at = Self::compute_expire_at(&ds, ID_COLUMN);
                continue;
            }
            let expire_at = Self::compute_expire_at(&ds, &cell.column);
            record_value.cells.insert(
                cell.column.clone(),
                StoredCell {
                    value: cell.value.clone(),
                    expire_at,
                },
            );
        }

        // Auto-set record expire_at if no _id was explicitly written
        if !cells.iter().any(|c| c.column == ID_COLUMN) && record_value.expire_at.is_none() {
            record_value.expire_at = Self::compute_expire_at(&ds, ID_COLUMN);
        }

        // Write the single record key
        self.txn
            .put(&ds.partition, &key, &bincode::serialize(&record_value)?)?;

        // Index maintenance
        for field in &ds.fields {
            if !field.indexed {
                continue;
            }
            let new_value = record_value.cells.get(&field.name).map(|sc| &sc.value);
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
        writes: Vec<(String, Vec<CellWrite>)>,
    ) -> Result<(), DbError> {
        let ds = self.require_datasource(datasource_id)?;

        // Batch read existing records
        let full_ids: Vec<String> = writes
            .iter()
            .map(|(id, _)| Self::full_record_id(datasource_id, id))
            .collect();
        let keys: Vec<Vec<u8>> = full_ids.iter().map(|id| encoding::record_key(id)).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let existing = self.txn.multi_get(&ds.partition, &key_refs)?;

        for (i, (_, cells)) in writes.into_iter().enumerate() {
            let full_id = &full_ids[i];
            let cells = Self::dedup_cells(cells);

            let mut record_value = match &existing[i] {
                Some(bytes) => bincode::deserialize(bytes)?,
                None => RecordValue {
                    expire_at: None,
                    cells: HashMap::new(),
                },
            };

            // Track old indexed values for cleanup
            let old_indexed: HashMap<String, Value> = ds
                .fields
                .iter()
                .filter(|f| f.indexed)
                .filter_map(|f| {
                    record_value
                        .cells
                        .get(&f.name)
                        .map(|sc| (f.name.clone(), sc.value.clone()))
                })
                .collect();

            // Merge new cells
            for cell in &cells {
                if cell.column == ID_COLUMN {
                    record_value.expire_at = Self::compute_expire_at(&ds, ID_COLUMN);
                    continue;
                }
                let expire_at = Self::compute_expire_at(&ds, &cell.column);
                record_value.cells.insert(
                    cell.column.clone(),
                    StoredCell {
                        value: cell.value.clone(),
                        expire_at,
                    },
                );
            }

            // Auto-set record expire_at
            if !cells.iter().any(|c| c.column == ID_COLUMN) && record_value.expire_at.is_none() {
                record_value.expire_at = Self::compute_expire_at(&ds, ID_COLUMN);
            }

            // Write the single record key
            self.txn
                .put(&ds.partition, &keys[i], &bincode::serialize(&record_value)?)?;

            // Index maintenance
            for field in &ds.fields {
                if !field.indexed {
                    continue;
                }
                let new_value = record_value.cells.get(&field.name).map(|sc| &sc.value);
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
            let record_value: RecordValue = bincode::deserialize(&bytes)?;
            for field in &ds.fields {
                if field.indexed {
                    if let Some(stored_cell) = record_value.cells.get(&field.name) {
                        let idx_key =
                            encoding::index_key(&field.name, &stored_cell.value, &full_id);
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
    ) -> Result<Option<Record>, DbError> {
        let ds = self.require_datasource(datasource_id)?;
        let full_id = Self::full_record_id(datasource_id, record_id);
        let key = encoding::record_key(&full_id);

        let bytes = match self.txn.get(&ds.partition, &key)? {
            Some(b) => b,
            None => return Ok(None),
        };

        let record_value: RecordValue = bincode::deserialize(&bytes)?;

        // Check record-level expiry
        if is_expired(record_value.expire_at) {
            return Ok(None);
        }

        // Build cells, filtering expired and applying projection
        let mut cells = HashMap::new();
        for (col, stored_cell) in record_value.cells {
            if is_expired(stored_cell.expire_at) {
                continue;
            }
            if let Some(wanted) = columns {
                if !wanted.contains(&col.as_str()) {
                    continue;
                }
            }
            cells.insert(
                col,
                Cell {
                    value: stored_cell.value,
                },
            );
        }

        Ok(Some(Record {
            id: record_id.to_string(),
            cells,
        }))
    }

    // ── Query ───────────────────────────────────────────────────────

    /// Query records in a datasource.
    ///
    /// Builds a plan tree from the query, then executes it lazily:
    /// Scan/IndexScan → Filter → Sort → Limit → Projection
    pub fn query(&self, datasource_id: &str, query: &Query) -> Result<Vec<Record>, DbError> {
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

/// Check if an expire_at timestamp has passed.
pub(crate) fn is_expired(expire_at: Option<i64>) -> bool {
    match expire_at {
        Some(expire_at) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            expire_at <= now
        }
        None => false,
    }
}
