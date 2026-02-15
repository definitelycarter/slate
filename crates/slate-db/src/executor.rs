use std::collections::HashMap;
use std::collections::HashSet;

use slate_store::Transaction;

use crate::cell::{Cell, RecordValue};
use crate::database::is_expired;
use crate::encoding;
use crate::error::DbError;
use crate::exec;
use crate::planner::PlanNode;
use crate::record::Record;

type RecordIter<'a> = Box<dyn Iterator<Item = Result<Record, DbError>> + 'a>;

/// Executes a query plan tree, returning a lazy record iterator.
pub fn execute<'a, T: Transaction + 'a>(
    txn: &'a T,
    node: &'a PlanNode,
    ds_prefix: &'a str,
) -> Result<RecordIter<'a>, DbError> {
    execute_node(txn, node, ds_prefix)
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

            Ok(Box::new(source.filter_map(move |result| match result {
                Err(e) => Some(Err(e)),
                Ok(record) => match exec::matches_group(&record, predicate) {
                    Ok(true) => Some(Ok(record)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                },
            })))
        }

        PlanNode::Sort { sorts, input } => {
            let source = execute_node(txn, input, ds_prefix)?;

            // Records already have all columns — just sort
            let mut records: Vec<Record> = source.collect::<Result<Vec<_>, _>>()?;

            records.sort_by(|a, b| {
                for sort in sorts {
                    let a_val = a.cells.get(&sort.field).map(|c| &c.value);
                    let b_val = b.cells.get(&sort.field).map(|c| &c.value);
                    let ord = exec::compare_field_values(a_val, b_val);
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
            let cols = columns.clone();

            Ok(Box::new(source.map(move |result| {
                let mut record = result?;
                let proj_set: HashSet<&str> = cols.iter().map(|s| s.as_str()).collect();
                record.cells.retain(|k, _| proj_set.contains(k.as_str()));
                Ok(record)
            })))
        }
    }
}

/// Scan all records under a prefix. One key = one record.
fn execute_scan<'a, T: Transaction + 'a>(
    txn: &'a T,
    partition: &'a str,
    prefix: &'a str,
    ds_prefix: &'a str,
) -> Result<RecordIter<'a>, DbError> {
    let scan_prefix = encoding::data_scan_prefix(prefix);
    let iter = txn.scan_prefix(partition, &scan_prefix)?;

    Ok(Box::new(iter.filter_map(move |result| match result {
        Err(e) => Some(Err(DbError::from(e))),
        Ok((key, value)) => {
            let full_record_id = match encoding::parse_record_key(&key) {
                Some(id) => id,
                None => return None,
            };

            let record_value: RecordValue = match bincode::deserialize(&value) {
                Ok(rv) => rv,
                Err(_) => return None,
            };

            if is_expired(record_value.expire_at) {
                return None;
            }

            let short_id = full_record_id
                .strip_prefix(ds_prefix)
                .unwrap_or(full_record_id);

            let cells = build_cells(record_value.cells);

            Some(Ok(Record {
                id: short_id.to_string(),
                cells,
            }))
        }
    })))
}

/// Index scan — look up record IDs from the index, multi_get full records.
fn execute_index_scan<'a, T: Transaction + 'a>(
    txn: &'a T,
    partition: &'a str,
    column: &'a str,
    value: &'a crate::record::Value,
    ds_prefix: &'a str,
) -> Result<RecordIter<'a>, DbError> {
    let prefix = encoding::index_scan_prefix(column, value);
    let iter = txn.scan_prefix(partition, &prefix)?;

    // Collect record IDs from index
    let record_ids: Vec<String> = iter
        .filter_map(|result| match result {
            Ok((key, _)) => {
                let (_, full_record_id) = encoding::parse_index_key(&key)?;
                Some(full_record_id.to_string())
            }
            Err(_) => None,
        })
        .collect();

    // Batch read full records
    let keys: Vec<Vec<u8>> = record_ids
        .iter()
        .map(|id| encoding::record_key(id))
        .collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let values = txn.multi_get(partition, &key_refs)?;

    let mut records = Vec::new();
    for (full_id, value_opt) in record_ids.iter().zip(values) {
        if let Some(bytes) = value_opt {
            let record_value: RecordValue = match bincode::deserialize(&bytes) {
                Ok(rv) => rv,
                Err(_) => continue,
            };

            if is_expired(record_value.expire_at) {
                continue;
            }

            let short_id = full_id.strip_prefix(ds_prefix).unwrap_or(full_id);
            let cells = build_cells(record_value.cells);

            records.push(Record {
                id: short_id.to_string(),
                cells,
            });
        }
    }

    Ok(Box::new(records.into_iter().map(Ok)))
}

/// Build a cells HashMap from stored cells, filtering out expired ones.
fn build_cells(stored: HashMap<String, crate::cell::StoredCell>) -> HashMap<String, Cell> {
    let mut cells = HashMap::new();
    for (col, stored_cell) in stored {
        if !is_expired(stored_cell.expire_at) {
            cells.insert(
                col,
                Cell {
                    value: stored_cell.value,
                },
            );
        }
    }
    cells
}
