use std::collections::HashMap;

use slate_query::Query;
use slate_store::{Store, Transaction};

use crate::catalog::Catalog;
use crate::cell::{Cell, CellValue, CellWrite};
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

    /// Check if a field is indexed in the datasource.
    fn is_field_indexed(ds: &Datasource, column: &str) -> bool {
        ds.fields.iter().any(|f| f.name == column && f.indexed)
    }

    /// Deduplicate cells: keep only the highest timestamp per column.
    fn dedup_cells(cells: Vec<CellWrite>) -> Vec<CellWrite> {
        let mut best: HashMap<String, CellWrite> = HashMap::new();
        for cell in cells {
            best.entry(cell.column.clone())
                .and_modify(|existing| {
                    if cell.timestamp > existing.timestamp {
                        *existing = cell.clone();
                    }
                })
                .or_insert(cell);
        }
        best.into_values().collect()
    }

    /// Compute expire_at for a cell based on the datasource field TTL config.
    fn compute_expire_at(ds: &Datasource, column: &str, timestamp: i64) -> Option<i64> {
        ds.fields
            .iter()
            .find(|f| f.name == column)
            .and_then(|f| f.ttl_seconds)
            .map(|ttl| timestamp + ttl)
    }

    // ── Write operations ────────────────────────────────────────────

    /// Write cells for a record. Looks up the datasource to derive partition
    /// and per-column TTL. Auto-creates an `_id` cell if not present.
    pub fn write_record(
        &mut self,
        datasource_id: &str,
        record_id: &str,
        cells: Vec<CellWrite>,
    ) -> Result<(), DbError> {
        let ds = self.require_datasource(datasource_id)?;
        let full_id = Self::full_record_id(datasource_id, record_id);

        // Deduplicate: keep highest timestamp per column
        let cells = Self::dedup_cells(cells);

        // Auto-create _id cell if not provided
        let has_id = cells.iter().any(|c| c.column == ID_COLUMN);
        if !has_id {
            let max_ts = cells.iter().map(|c| c.timestamp).max().unwrap_or(0);
            let key = encoding::cell_key(&full_id, ID_COLUMN);
            let expire_at = Self::compute_expire_at(&ds, ID_COLUMN, max_ts);
            let cv = CellValue {
                value: Value::Bool(true),
                timestamp: max_ts,
                expire_at,
            };
            self.txn
                .put(&ds.partition, &key, &bincode::serialize(&cv)?)?;
        }

        // Write each cell with TTL derived from datasource field config
        for cell in &cells {
            let key = encoding::cell_key(&full_id, &cell.column);
            let expire_at = Self::compute_expire_at(&ds, &cell.column, cell.timestamp);
            let cv = CellValue {
                value: cell.value.clone(),
                timestamp: cell.timestamp,
                expire_at,
            };
            self.txn
                .put(&ds.partition, &key, &bincode::serialize(&cv)?)?;

            // Maintain index for indexed fields
            if Self::is_field_indexed(&ds, &cell.column) {
                let idx_key = encoding::index_key(&cell.column, &cell.value, &full_id);
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
        // Look up datasource once for the batch
        let ds = self.require_datasource(datasource_id)?;

        for (record_id, cells) in writes {
            let full_id = Self::full_record_id(datasource_id, &record_id);

            // Deduplicate: keep highest timestamp per column
            let cells = Self::dedup_cells(cells);

            let has_id = cells.iter().any(|c| c.column == ID_COLUMN);
            if !has_id {
                let max_ts = cells.iter().map(|c| c.timestamp).max().unwrap_or(0);
                let key = encoding::cell_key(&full_id, ID_COLUMN);
                let expire_at = Self::compute_expire_at(&ds, ID_COLUMN, max_ts);
                let cv = CellValue {
                    value: Value::Bool(true),
                    timestamp: max_ts,
                    expire_at,
                };
                self.txn
                    .put(&ds.partition, &key, &bincode::serialize(&cv)?)?;
            }

            for cell in &cells {
                let key = encoding::cell_key(&full_id, &cell.column);
                let expire_at = Self::compute_expire_at(&ds, &cell.column, cell.timestamp);
                let cv = CellValue {
                    value: cell.value.clone(),
                    timestamp: cell.timestamp,
                    expire_at,
                };
                self.txn
                    .put(&ds.partition, &key, &bincode::serialize(&cv)?)?;

                // Maintain index for indexed fields
                if Self::is_field_indexed(&ds, &cell.column) {
                    let idx_key = encoding::index_key(&cell.column, &cell.value, &full_id);
                    self.txn.put(&ds.partition, &idx_key, &[])?;
                }
            }
        }

        Ok(())
    }

    /// Delete all cells for a record.
    pub fn delete_record(&mut self, datasource_id: &str, record_id: &str) -> Result<(), DbError> {
        let ds = self.require_datasource(datasource_id)?;
        let full_id = Self::full_record_id(datasource_id, record_id);
        let prefix = encoding::record_prefix(&full_id);
        let iter = self.txn.scan_prefix(&ds.partition, &prefix)?;
        let keys: Vec<Box<[u8]>> = iter
            .map(|r| r.map(|(k, _)| k))
            .collect::<Result<Vec<_>, _>>()?;
        for key in keys {
            self.txn.delete(&ds.partition, &key)?;
        }
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

        // Check _id cell first
        match self.read_cell_value(&ds.partition, &full_id, ID_COLUMN)? {
            None => return Ok(None),
            Some(cv) => {
                if is_expired(cv.expire_at) {
                    return Ok(None);
                }
            }
        }

        let cells = match columns {
            Some(cols) => {
                let wanted: Vec<&str> = cols.iter().filter(|c| **c != ID_COLUMN).copied().collect();
                let keys: Vec<Vec<u8>> = wanted
                    .iter()
                    .map(|col| encoding::cell_key(&full_id, col))
                    .collect();
                let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                let values = self.txn.multi_get(&ds.partition, &key_refs)?;
                let mut cells = HashMap::new();
                for (col, value_opt) in wanted.iter().zip(values) {
                    if let Some(bytes) = value_opt {
                        let cv: CellValue = bincode::deserialize(&bytes)?;
                        cells.insert(
                            col.to_string(),
                            Cell {
                                value: cv.value,
                                timestamp: cv.timestamp,
                            },
                        );
                    }
                }
                cells
            }
            None => self.read_all_latest_cells(&ds.partition, &full_id)?,
        };

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

        // Build the plan tree
        let plan = planner::plan(&ds, query);

        // Execute — returns a lazy iterator, collect into Vec
        let iter = executor::execute(&self.txn, &plan, &ds_prefix)?;
        iter.collect()
    }

    // ── Internal helpers ────────────────────────────────────────────

    /// Read the raw CellValue by exact key lookup.
    fn read_cell_value(
        &self,
        partition: &str,
        full_record_id: &str,
        column: &str,
    ) -> Result<Option<CellValue>, DbError> {
        let key = encoding::cell_key(full_record_id, column);
        match self.txn.get(partition, &key)? {
            None => Ok(None),
            Some(value) => Ok(Some(bincode::deserialize(&value)?)),
        }
    }

    /// Read all cells for a record (one key per column).
    fn read_all_latest_cells(
        &self,
        partition: &str,
        full_record_id: &str,
    ) -> Result<HashMap<String, Cell>, DbError> {
        let prefix = encoding::record_prefix(full_record_id);
        let iter = self.txn.scan_prefix(partition, &prefix)?;

        let mut cells: HashMap<String, Cell> = HashMap::new();
        for result in iter {
            let (key, value) = result?;
            let (_, column) = encoding::parse_cell_key(&key)
                .ok_or_else(|| DbError::InvalidKey("malformed cell key".into()))?;

            if column == ID_COLUMN {
                continue;
            }

            let cv: CellValue = bincode::deserialize(&value)?;
            cells.insert(
                column.to_string(),
                Cell {
                    value: cv.value,
                    timestamp: cv.timestamp,
                },
            );
        }

        Ok(cells)
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
fn is_expired(expire_at: Option<i64>) -> bool {
    match expire_at {
        Some(expire_at) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            expire_at < now
        }
        None => false,
    }
}
