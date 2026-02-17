use std::collections::HashSet;

use bson::RawDocumentBuf;
use slate_query::LogicalOp;
use slate_store::Transaction;

use crate::encoding;
use crate::error::DbError;
use crate::exec;
use crate::planner::PlanNode;

type DocIter<'a> = Box<dyn Iterator<Item = Result<bson::Document, DbError>> + 'a>;
type RawIter<'a> = Box<dyn Iterator<Item = Result<(String, RawDocumentBuf), DbError>> + 'a>;

/// Executes a query plan tree, returning a lazy document iterator.
pub fn execute<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    node: &'a PlanNode,
) -> Result<DocIter<'a>, DbError> {
    execute_node(txn, node)
}

// ── ID tier ─────────────────────────────────────────────────────
//
// Nodes below ReadRecord produce record IDs without materializing documents.

/// Execute an ID-tier node, returning a list of record IDs.
fn execute_ids<T: Transaction>(txn: &mut T, node: &PlanNode) -> Result<Vec<String>, DbError> {
    match node {
        PlanNode::Scan { partition } => execute_scan_ids(txn, partition),

        PlanNode::IndexScan {
            partition,
            column,
            value,
        } => execute_index_scan_ids(txn, partition, column, value),

        PlanNode::IndexMerge { logical, lhs, rhs } => {
            let left_ids = execute_ids(txn, lhs)?;
            let right_ids = execute_ids(txn, rhs)?;

            match logical {
                LogicalOp::Or => {
                    // Union: combine both sets, deduplicate
                    let mut seen = HashSet::with_capacity(left_ids.len() + right_ids.len());
                    let mut result = Vec::with_capacity(left_ids.len() + right_ids.len());
                    for id in left_ids.into_iter().chain(right_ids) {
                        if seen.insert(id.clone()) {
                            result.push(id);
                        }
                    }
                    Ok(result)
                }
                LogicalOp::And => {
                    // Intersect: keep only IDs present in both sets
                    let right_set: HashSet<String> = right_ids.into_iter().collect();
                    let result = left_ids
                        .into_iter()
                        .filter(|id| right_set.contains(id))
                        .collect();
                    Ok(result)
                }
            }
        }

        _ => Err(DbError::InvalidQuery(
            "unexpected node in ID tier".to_string(),
        )),
    }
}

/// Scan all record keys in a partition, returning just the IDs (no deserialization).
fn execute_scan_ids<T: Transaction>(txn: &mut T, partition: &str) -> Result<Vec<String>, DbError> {
    let scan_prefix = encoding::data_scan_prefix("");
    let iter = txn.scan_prefix(partition, &scan_prefix)?;

    let ids: Vec<String> = iter
        .filter_map(|result| match result {
            Ok((key, _)) => {
                let record_id = encoding::parse_record_key(&key)?;
                Some(record_id.to_string())
            }
            Err(_) => None,
        })
        .collect();

    Ok(ids)
}

/// Scan an index prefix, returning record IDs that match the column=value.
fn execute_index_scan_ids<T: Transaction>(
    txn: &mut T,
    partition: &str,
    column: &str,
    value: &bson::Bson,
) -> Result<Vec<String>, DbError> {
    let prefix = encoding::index_scan_prefix(column, value);
    let iter = txn.scan_prefix(partition, &prefix)?;

    let ids: Vec<String> = iter
        .filter_map(|result| match result {
            Ok((key, _)) => {
                let (_, record_id) = encoding::parse_index_key(&key)?;
                Some(record_id.to_string())
            }
            Err(_) => None,
        })
        .collect();

    Ok(ids)
}

// ── Raw tier ────────────────────────────────────────────────────
//
// Nodes between ReadRecord and Projection operate on raw BSON bytes.
// Filter and Sort access individual fields lazily without full deserialization.

fn execute_raw_node<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    node: &'a PlanNode,
) -> Result<RawIter<'a>, DbError> {
    match node {
        PlanNode::ReadRecord { input } => execute_read_record(txn, input),

        PlanNode::Filter { predicate, input } => {
            let source = execute_raw_node(txn, input)?;

            Ok(Box::new(source.filter_map(move |result| match result {
                Err(e) => Some(Err(e)),
                Ok((id, raw)) => match exec::raw_matches_group(&raw, &id, predicate) {
                    Ok(true) => Some(Ok((id, raw))),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                },
            })))
        }

        PlanNode::Sort { sorts, input } => {
            let source = execute_raw_node(txn, input)?;

            let mut records: Vec<(String, RawDocumentBuf)> =
                source.collect::<Result<Vec<_>, _>>()?;

            records.sort_by(|(a_id, a_raw), (b_id, b_raw)| {
                for sort in sorts {
                    let ord = if sort.field == "_id" {
                        a_id.cmp(b_id)
                    } else {
                        let a_val = exec::raw_get_path(a_raw, &sort.field).ok().flatten();
                        let b_val = exec::raw_get_path(b_raw, &sort.field).ok().flatten();
                        exec::raw_compare_field_values(a_val, b_val)
                    };
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
            let source = execute_raw_node(txn, input)?;
            let iter = source.skip(*skip);
            match take {
                Some(take) => Ok(Box::new(iter.take(*take))),
                None => Ok(Box::new(iter)),
            }
        }

        _ => Err(DbError::InvalidQuery(
            "unexpected node in raw tier".to_string(),
        )),
    }
}

// ── Document tier ───────────────────────────────────────────────
//
// Projection materializes raw records into bson::Document.
// When no Projection node exists, fallback materializes via to_document().

fn execute_node<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    node: &'a PlanNode,
) -> Result<DocIter<'a>, DbError> {
    match node {
        PlanNode::Projection { columns, input } => {
            let source = execute_raw_node(txn, input)?;
            let cols = columns.clone();

            Ok(Box::new(source.map(move |result| {
                let (id, raw) = result?;
                let mut doc = materialize_for_projection(&raw, &cols)?;
                doc.insert("_id", id.as_str());
                exec::apply_projection(&mut doc, &cols);
                Ok(doc)
            })))
        }

        // No Projection node — full materialization
        _ => {
            let source = execute_raw_node(txn, node)?;
            Ok(Box::new(source.map(|result| {
                let (id, raw) = result?;
                let mut doc = raw.to_document()?;
                doc.insert("_id", id.as_str());
                Ok(doc)
            })))
        }
    }
}

/// ReadRecord: the boundary between ID tier and raw tier.
///
/// For `Scan` inputs, collects (id, raw) tuples in a single pass.
/// For `IndexScan`/`IndexMerge` inputs, collects IDs first then batch-fetches via `multi_get`.
fn execute_read_record<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    input: &'a PlanNode,
) -> Result<RawIter<'a>, DbError> {
    // Optimization: when the input is a Scan, iterate keys + values together
    // in a single pass — avoids collecting all IDs then re-reading via multi_get.
    if let PlanNode::Scan { partition } = input {
        return execute_scan_raw(txn, partition);
    }

    // For IndexScan/IndexMerge: collect IDs, then batch-fetch raw bytes
    let partition = extract_partition(input);
    let record_ids = execute_ids(txn, input)?;

    let keys: Vec<Vec<u8>> = record_ids
        .iter()
        .map(|id| encoding::record_key(id))
        .collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let values = txn.multi_get(&partition, &key_refs)?;

    let mut records = Vec::new();
    for (id, value_opt) in record_ids.into_iter().zip(values) {
        if let Some(bytes) = value_opt {
            let raw = match RawDocumentBuf::from_bytes(bytes.into_vec()) {
                Ok(r) => r,
                Err(_) => continue,
            };
            records.push((id, raw));
        }
    }

    Ok(Box::new(records.into_iter().map(Ok)))
}

/// Single-pass scan: lazily iterate data keys, mapping to (id, raw) tuples.
/// No collection — records stream one at a time through the pipeline.
fn execute_scan_raw<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    partition: &str,
) -> Result<RawIter<'a>, DbError> {
    let scan_prefix = encoding::data_scan_prefix("");
    let iter = txn.scan_prefix(partition, &scan_prefix)?;

    Ok(Box::new(iter.filter_map(|result| match result {
        Ok((key, value)) => {
            let record_id = encoding::parse_record_key(&key)?.to_string();
            let raw = RawDocumentBuf::from_bytes(value.into_vec()).ok()?;
            Some(Ok((record_id, raw)))
        }
        Err(e) => Some(Err(DbError::Store(e))),
    })))
}

/// Extract the partition name from an ID-tier node.
fn extract_partition(node: &PlanNode) -> String {
    match node {
        PlanNode::Scan { partition } => partition.clone(),
        PlanNode::IndexScan { partition, .. } => partition.clone(),
        PlanNode::IndexMerge { lhs, .. } => extract_partition(lhs),
        _ => String::new(),
    }
}

/// Selectively materialize a raw document for projection.
/// Only deserializes top-level keys that are in the projection set.
fn materialize_for_projection(
    raw: &RawDocumentBuf,
    columns: &[String],
) -> Result<bson::Document, DbError> {
    let top_keys: HashSet<&str> = columns
        .iter()
        .map(|c| c.split('.').next().unwrap_or(c.as_str()))
        .collect();

    let mut doc = bson::Document::new();
    for result in raw.iter() {
        let (key, raw_val) = result?;
        if top_keys.contains(key) {
            doc.insert(key, bson::Bson::try_from(raw_val)?);
        }
    }
    Ok(doc)
}
