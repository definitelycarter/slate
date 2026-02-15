use std::collections::HashSet;

use slate_store::Transaction;

use crate::cell::{Cell, CellValue};
use crate::encoding;
use crate::error::DbError;
use crate::exec;
use crate::planner::PlanNode;
use crate::record::Record;

const ID_COLUMN: &str = "_id";

/// Reads cells on demand from the transaction, used for lazy filter evaluation.
struct TxnCellReader<'a, T: Transaction> {
    txn: &'a T,
    partition: String,
    ds_prefix: &'a str,
}

impl<'a, T: Transaction> exec::CellReader for TxnCellReader<'a, T> {
    fn read_cell(&self, record: &mut Record, column: &str) -> Result<(), DbError> {
        let full_id = format!("{}{}", self.ds_prefix, record.id);
        if let Some(cell) = read_latest_cell(self.txn, &self.partition, &full_id, column)? {
            record.cells.insert(column.to_string(), cell);
        }
        Ok(())
    }
}

type RecordIter<'a> = Box<dyn Iterator<Item = Result<Record, DbError>> + 'a>;

/// Executes a query plan tree, returning a lazy record iterator.
///
/// Each node pulls from the node below it. Records flow from leaves
/// (Scan/IndexScan) through Filter, Sort, Limit, Projection to the caller.
///
/// When the plan uses IndexScan (which yields lightweight records with only
/// the indexed column) and there's no Projection node to fill in remaining
/// columns, this wrapper reads all remaining columns per record.
pub fn execute<'a, T: Transaction + 'a>(
    txn: &'a T,
    node: &'a PlanNode,
    ds_prefix: &'a str,
) -> Result<RecordIter<'a>, DbError> {
    let source = execute_node(txn, node, ds_prefix)?;

    // If the plan uses IndexScan and has no Projection, records will be
    // incomplete — fill in all remaining columns.
    if uses_index_scan(node) && !has_projection(node) {
        let partition = partition_from_node(node).to_string();
        Ok(Box::new(source.map(move |result| {
            let mut record = result?;
            let full_id = format!("{ds_prefix}{}", record.id);
            read_remaining_cells(txn, &partition, &full_id, &mut record)?;
            Ok(record)
        })))
    } else {
        Ok(source)
    }
}

fn execute_node<'a, T: Transaction + 'a>(
    txn: &'a T,
    node: &'a PlanNode,
    ds_prefix: &'a str,
) -> Result<RecordIter<'a>, DbError> {
    match node {
        PlanNode::Scan { partition, prefix } => execute_scan(txn, partition, prefix, ds_prefix),

        PlanNode::IndexScan {
            partition,
            column,
            value,
        } => execute_index_scan(txn, partition, column, value, ds_prefix),

        PlanNode::Filter { predicate, input } => {
            let source = execute_node(txn, input, ds_prefix)?;
            let reader = TxnCellReader {
                txn,
                partition: partition_from_node(input).to_string(),
                ds_prefix,
            };

            Ok(Box::new(source.filter_map(move |result| {
                match result {
                    Err(e) => Some(Err(e)),
                    Ok(mut record) => {
                        // Lazy evaluation: reads columns on demand, short-circuits on first failure
                        match exec::matches_group_lazy(&mut record, predicate, &reader) {
                            Ok(true) => Some(Ok(record)),
                            Ok(false) => None,
                            Err(e) => Some(Err(e)),
                        }
                    }
                }
            })))
        }

        PlanNode::Sort { sorts, input } => {
            let source = execute_node(txn, input, ds_prefix)?;
            let sort_cols: Vec<String> = sorts.iter().map(|s| s.field.clone()).collect();

            // Materialize: collect all records, batch-read sort columns, sort
            let mut records: Vec<Record> = Vec::new();
            for result in source {
                let mut record = result?;
                let full_id = format!("{ds_prefix}{}", record.id);
                read_cells_batch(
                    txn,
                    partition_from_node(input),
                    &full_id,
                    &mut record,
                    &sort_cols,
                )?;
                records.push(record);
            }

            records.sort_by(|a, b| {
                for sort in sorts {
                    let a_val = a.cells.get(&sort.field).map(|c| &c.value);
                    let b_val = b.cells.get(&sort.field).map(|c| &c.value);
                    let ord = crate::exec::compare_field_values(a_val, b_val);
                    let ord = match sort.direction {
                        slate_query::SortDirection::Asc => ord,
                        slate_query::SortDirection::Desc => ord.reverse(),
                    };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });

            Ok(Box::new(records.into_iter().map(Ok)))
        }

        PlanNode::Limit { skip, take, input } => {
            let source = execute_node(txn, input, ds_prefix)?;
            let iter = source.skip(*skip);
            match take {
                Some(take) => Ok(Box::new(iter.take(*take))),
                None => Ok(Box::new(iter)),
            }
        }

        PlanNode::Projection { columns, input } => {
            let source = execute_node(txn, input, ds_prefix)?;
            let partition = partition_from_node(input).to_string();
            let cols = columns.clone();

            Ok(Box::new(source.map(move |result| {
                let mut record = result?;

                // Batch-read any projected columns not already on the record
                let full_id = format!("{ds_prefix}{}", record.id);
                read_cells_batch(txn, &partition, &full_id, &mut record, &cols)?;

                // Strip columns not in projection
                let proj_set: HashSet<&str> = cols.iter().map(|s| s.as_str()).collect();
                record.cells.retain(|k, _| proj_set.contains(k.as_str()));

                Ok(record)
            })))
        }
    }
}

/// Single-pass scan: iterate all data keys, build records with cells inline.
///
/// Keys are sorted by record_id then column. One key per column (no versioning).
/// We group by record_id, check _id expiry, and yield complete records.
fn execute_scan<'a, T: Transaction + 'a>(
    txn: &'a T,
    partition: &'a str,
    prefix: &'a str,
    ds_prefix: &'a str,
) -> Result<RecordIter<'a>, DbError> {
    let scan_prefix = encoding::data_scan_prefix(prefix);
    let iter = txn.scan_prefix(partition, &scan_prefix)?;

    let mut inner = iter;
    let mut current_id: Option<String> = None;
    let mut current_cells: std::collections::HashMap<String, Cell> =
        std::collections::HashMap::new();
    let mut current_id_cell: Option<CellValue> = None;
    let mut done = false;

    Ok(Box::new(std::iter::from_fn(move || {
        if done {
            return None;
        }

        loop {
            match inner.next() {
                None => {
                    done = true;
                    // Flush last record
                    return flush_record(
                        &mut current_id,
                        &mut current_cells,
                        &mut current_id_cell,
                        ds_prefix,
                    );
                }
                Some(Err(e)) => return Some(Err(DbError::from(e))),
                Some(Ok((key, value))) => {
                    let (full_record_id, column) = match encoding::parse_cell_key(&key) {
                        Some(p) => p,
                        None => continue,
                    };

                    // New record? Flush the previous one
                    if current_id.as_deref() != Some(full_record_id) {
                        let flushed = flush_record(
                            &mut current_id,
                            &mut current_cells,
                            &mut current_id_cell,
                            ds_prefix,
                        );
                        current_id = Some(full_record_id.to_string());
                        current_cells = std::collections::HashMap::new();
                        current_id_cell = None;

                        // Process this key into the new record
                        process_cell(column, &value, &mut current_cells, &mut current_id_cell);

                        if let Some(record) = flushed {
                            return Some(record);
                        }
                    } else {
                        process_cell(column, &value, &mut current_cells, &mut current_id_cell);
                    }
                }
            }
        }
    })))
}

/// Process a single cell key/value into the current record's state.
///
/// For _id cells, we store the full CellValue (need expire_at for expiry check).
/// For data cells, we store a Cell (value + timestamp only).
fn process_cell(
    column: &str,
    value: &[u8],
    cells: &mut std::collections::HashMap<String, Cell>,
    id_cell: &mut Option<CellValue>,
) {
    if let Ok(cv) = bincode::deserialize::<CellValue>(value) {
        if column == ID_COLUMN {
            *id_cell = Some(cv);
        } else {
            cells.insert(
                column.to_string(),
                Cell {
                    value: cv.value,
                    timestamp: cv.timestamp,
                },
            );
        }
    }
}

/// Flush the current record if _id exists and is not expired.
fn flush_record(
    current_id: &mut Option<String>,
    current_cells: &mut std::collections::HashMap<String, Cell>,
    current_id_cell: &mut Option<CellValue>,
    ds_prefix: &str,
) -> Option<Result<Record, DbError>> {
    let full_id = current_id.as_ref()?;
    let id_cv = current_id_cell.as_ref()?;

    if is_expired(id_cv.expire_at) {
        return None;
    }

    let short_id = full_id.strip_prefix(ds_prefix).unwrap_or(full_id);
    Some(Ok(Record {
        id: short_id.to_string(),
        cells: std::mem::take(current_cells),
    }))
}

/// Index scan — look up record IDs from the index, yield lightweight records.
///
/// Only pre-populates the indexed column's cell (we already know the value).
/// Checks _id expiry. Downstream nodes (Filter, Sort, Projection) or the
/// top-level wrapper in `execute()` fill in remaining columns as needed.
fn execute_index_scan<'a, T: Transaction + 'a>(
    txn: &'a T,
    partition: &'a str,
    column: &'a str,
    value: &'a crate::record::Value,
    ds_prefix: &'a str,
) -> Result<RecordIter<'a>, DbError> {
    let prefix = encoding::index_scan_prefix(column, value);
    let iter = txn.scan_prefix(partition, &prefix)?;

    Ok(Box::new(iter.filter_map(move |result| {
        match result {
            Err(e) => Some(Err(DbError::from(e))),
            Ok((key, _)) => {
                let (_, full_record_id) = match encoding::parse_index_key(&key) {
                    Some(parsed) => parsed,
                    None => return None,
                };

                // Check _id exists and isn't expired
                let id_cv = match read_cell_value(txn, partition, full_record_id, ID_COLUMN) {
                    Ok(Some(cv)) => cv,
                    Ok(None) => return None,
                    Err(e) => return Some(Err(e)),
                };
                if is_expired(id_cv.expire_at) {
                    return None;
                }

                let short_id = full_record_id
                    .strip_prefix(ds_prefix)
                    .unwrap_or(full_record_id);

                // Pre-populate the indexed column — we know the value from the index
                let mut cells = std::collections::HashMap::new();
                cells.insert(
                    column.to_string(),
                    Cell {
                        value: value.clone(),
                        timestamp: id_cv.timestamp,
                    },
                );

                Some(Ok(Record {
                    id: short_id.to_string(),
                    cells,
                }))
            }
        }
    })))
}

/// Extract the partition from a plan node (walks down to Scan/IndexScan).
fn partition_from_node(node: &PlanNode) -> &str {
    match node {
        PlanNode::Scan { partition, .. } => partition,
        PlanNode::IndexScan { partition, .. } => partition,
        PlanNode::Filter { input, .. } => partition_from_node(input),
        PlanNode::Sort { input, .. } => partition_from_node(input),
        PlanNode::Limit { input, .. } => partition_from_node(input),
        PlanNode::Projection { input, .. } => partition_from_node(input),
    }
}

/// Read the raw CellValue by exact key lookup.
fn read_cell_value<T: Transaction>(
    txn: &T,
    partition: &str,
    full_record_id: &str,
    column: &str,
) -> Result<Option<CellValue>, DbError> {
    let key = encoding::cell_key(full_record_id, column);
    match txn.get(partition, &key)? {
        None => Ok(None),
        Some(value) => Ok(Some(bincode::deserialize(&value)?)),
    }
}

/// Read a cell by exact key lookup.
fn read_latest_cell<T: Transaction>(
    txn: &T,
    partition: &str,
    full_record_id: &str,
    column: &str,
) -> Result<Option<Cell>, DbError> {
    match read_cell_value(txn, partition, full_record_id, column)? {
        None => Ok(None),
        Some(cv) => Ok(Some(Cell {
            value: cv.value,
            timestamp: cv.timestamp,
        })),
    }
}

/// Check if the plan tree uses IndexScan as its leaf node.
fn uses_index_scan(node: &PlanNode) -> bool {
    match node {
        PlanNode::IndexScan { .. } => true,
        PlanNode::Scan { .. } => false,
        PlanNode::Filter { input, .. } => uses_index_scan(input),
        PlanNode::Sort { input, .. } => uses_index_scan(input),
        PlanNode::Limit { input, .. } => uses_index_scan(input),
        PlanNode::Projection { input, .. } => uses_index_scan(input),
    }
}

/// Check if the plan tree has a Projection node at the top.
fn has_projection(node: &PlanNode) -> bool {
    matches!(node, PlanNode::Projection { .. })
}

/// Read all remaining cells for a record that aren't already loaded.
fn read_remaining_cells<T: Transaction>(
    txn: &T,
    partition: &str,
    full_record_id: &str,
    record: &mut Record,
) -> Result<(), DbError> {
    let prefix = encoding::record_prefix(full_record_id);
    let iter = txn.scan_prefix(partition, &prefix)?;

    for result in iter {
        let (key, value) = result?;
        let (_, column) = match encoding::parse_cell_key(&key) {
            Some(p) => p,
            None => continue,
        };
        if column == ID_COLUMN {
            continue;
        }
        if record.cells.contains_key(column) {
            continue;
        }
        if let Ok(cv) = bincode::deserialize::<CellValue>(&value) {
            record.cells.insert(
                column.to_string(),
                Cell {
                    value: cv.value,
                    timestamp: cv.timestamp,
                },
            );
        }
    }
    Ok(())
}

/// Batch-read specific columns for a record using multi_get.
fn read_cells_batch<T: Transaction>(
    txn: &T,
    partition: &str,
    full_record_id: &str,
    record: &mut Record,
    columns: &[String],
) -> Result<(), DbError> {
    let missing: Vec<&String> = columns
        .iter()
        .filter(|col| !record.cells.contains_key(col.as_str()))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let keys: Vec<Vec<u8>> = missing
        .iter()
        .map(|col| encoding::cell_key(full_record_id, col))
        .collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let values = txn.multi_get(partition, &key_refs)?;
    for (col, value_opt) in missing.iter().zip(values) {
        if let Some(bytes) = value_opt {
            let cv: CellValue = bincode::deserialize(&bytes)?;
            record.cells.insert(
                col.to_string(),
                Cell {
                    value: cv.value,
                    timestamp: cv.timestamp,
                },
            );
        }
    }
    Ok(())
}

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
