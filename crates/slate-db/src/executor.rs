use std::borrow::Cow;
use std::collections::HashSet;

use bson::RawDocument;
use slate_query::{LogicalOp, SortDirection};
use slate_store::Transaction;

use crate::encoding;
use crate::error::DbError;
use crate::exec;
use crate::planner::PlanNode;

type DocIter<'a> = Box<dyn Iterator<Item = Result<bson::Document, DbError>> + 'a>;
type RawIter<'a> = Box<dyn Iterator<Item = Result<(String, Cow<'a, [u8]>), DbError>> + 'a>;
type IdIter<'a> = Box<dyn Iterator<Item = Result<(String, Option<Cow<'a, [u8]>>), DbError>> + 'a>;

/// Executes a query plan tree, returning a lazy document iterator.
pub fn execute<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    node: &'a PlanNode,
) -> Result<DocIter<'a>, DbError> {
    execute_node(txn, node)
}

// ── ID tier ─────────────────────────────────────────────────────
//
// Nodes below ReadRecord produce record IDs (and optionally raw bytes)
// as a lazy iterator. Scan carries the data it already has; IndexScan
// yields None for bytes since index entries don't contain record data.

/// Execute an ID-tier node, returning a lazy iterator of (id, maybe_bytes).
fn execute_id_node<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    node: &'a PlanNode,
) -> Result<IdIter<'a>, DbError> {
    match node {
        PlanNode::Scan { collection } => execute_scan(txn, collection),

        PlanNode::IndexScan {
            collection,
            column,
            value,
            direction,
            limit,
        } => execute_index_scan(txn, collection, column, value.as_ref(), *direction, *limit),

        PlanNode::IndexMerge { logical, lhs, rhs } => {
            // IndexMerge must collect both sides for dedup — stays eager.
            let left: Vec<String> = execute_id_node(txn, lhs)?
                .filter_map(|r| r.ok().map(|(id, _)| id))
                .collect();
            let right: Vec<String> = execute_id_node(txn, rhs)?
                .filter_map(|r| r.ok().map(|(id, _)| id))
                .collect();

            let ids = match logical {
                LogicalOp::Or => {
                    let mut seen = HashSet::with_capacity(left.len() + right.len());
                    let mut result = Vec::with_capacity(left.len() + right.len());
                    for id in left.into_iter().chain(right) {
                        if seen.insert(id.clone()) {
                            result.push(id);
                        }
                    }
                    result
                }
                LogicalOp::And => {
                    let right_set: HashSet<String> = right.into_iter().collect();
                    left.into_iter()
                        .filter(|id| right_set.contains(id))
                        .collect()
                }
            };

            // After dedup, bytes are unknown — ReadRecord will fetch them.
            Ok(Box::new(ids.into_iter().map(|id| Ok((id, None)))))
        }

        _ => Err(DbError::InvalidQuery(
            "unexpected node in ID tier".to_string(),
        )),
    }
}

/// Scan all records lazily, yielding (id, Some(bytes)) — data is kept, not discarded.
fn execute_scan<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    collection: &str,
) -> Result<IdIter<'a>, DbError> {
    let scan_prefix = encoding::data_scan_prefix("");
    let iter = txn.scan_prefix(collection, &scan_prefix)?;

    Ok(Box::new(iter.filter_map(|result| match result {
        Ok((key, value)) => {
            let record_id = encoding::parse_record_key(&key)?.to_string();
            Some(Ok((record_id, Some(value))))
        }
        Err(e) => Some(Err(DbError::Store(e))),
    })))
}

/// Scan an index prefix lazily, yielding (id, None) — index entries have no record data.
/// `value: Some(v)` scans entries matching a specific value (Eq lookup).
/// `value: None` scans the entire column index (ordered scan).
/// `direction` controls forward (Asc) or reverse (Desc) iteration.
fn execute_index_scan<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    collection: &str,
    column: &str,
    value: Option<&bson::Bson>,
    direction: SortDirection,
    limit: Option<usize>,
) -> Result<IdIter<'a>, DbError> {
    let prefix = match value {
        Some(v) => encoding::index_scan_prefix(column, v),
        None => encoding::index_scan_field_prefix(column),
    };
    let iter = match direction {
        SortDirection::Asc => txn.scan_prefix(collection, &prefix)?,
        SortDirection::Desc => txn.scan_prefix_rev(collection, &prefix)?,
    };

    let mapped = iter.filter_map(|result| match result {
        Ok((key, _)) => {
            let (_, record_id) = encoding::parse_index_key(&key)?;
            Some(Ok((record_id.to_string(), None)))
        }
        Err(e) => Some(Err(DbError::Store(e))),
    });

    // When a limit is pushed down from the planner, cap the index walk early.
    Ok(match limit {
        Some(n) => Box::new(mapped.take(n)),
        None => Box::new(mapped),
    })
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
                Ok((id, cow)) => {
                    let raw = RawDocument::from_bytes(&cow).ok()?;
                    match exec::raw_matches_group(raw, &id, predicate) {
                        Ok(true) => Some(Ok((id, cow))),
                        Ok(false) => None,
                        Err(e) => Some(Err(e)),
                    }
                }
            })))
        }

        PlanNode::Sort { sorts, input } => {
            let source = execute_raw_node(txn, input)?;

            let mut records: Vec<(String, Vec<u8>)> = source
                .map(|r| r.map(|(id, cow)| (id, cow.into_owned())))
                .collect::<Result<Vec<_>, _>>()?;

            records.sort_by(|(a_id, a_bytes), (b_id, b_bytes)| {
                for sort in sorts {
                    let ord = if sort.field == "_id" {
                        a_id.cmp(b_id)
                    } else {
                        let a_raw = RawDocument::from_bytes(a_bytes).ok();
                        let b_raw = RawDocument::from_bytes(b_bytes).ok();
                        let a_val =
                            a_raw.and_then(|r| exec::raw_get_path(r, &sort.field).ok().flatten());
                        let b_val =
                            b_raw.and_then(|r| exec::raw_get_path(r, &sort.field).ok().flatten());
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

            Ok(Box::new(
                records
                    .into_iter()
                    .map(|(id, bytes)| Ok((id, Cow::Owned(bytes)))),
            ))
        }

        PlanNode::IndexedSort {
            sorts,
            needed,
            input,
        } => {
            // Group-buffered sort: input is ordered by sorts[0] from IndexScan.
            // Buffer complete groups, sub-sort within groups by sorts[1..], stop early.
            let source = execute_raw_node(txn, input)?;
            let primary_field = &sorts[0].field;
            let needed = *needed;

            // Collect records into groups by primary sort field value.
            // Each group = all records with the same value for sorts[0].
            let mut groups: Vec<Vec<(String, Vec<u8>)>> = Vec::new();
            let mut current_group: Vec<(String, Vec<u8>)> = Vec::new();
            let mut current_value: Option<bson::Bson> = None;
            let mut total_from_complete_groups: usize = 0;

            for result in source {
                let (id, cow) = result?;
                let bytes = cow.into_owned();

                // Extract primary sort field value for group boundary detection.
                let raw = RawDocument::from_bytes(&bytes)?;
                let val = exec::raw_get_path(raw, primary_field)?;
                let val_owned: bson::Bson = match val {
                    Some(v) => v.try_into().unwrap_or(bson::Bson::Null),
                    None => bson::Bson::Null,
                };

                let same_group = match &current_value {
                    Some(prev) => *prev == val_owned,
                    None => false,
                };

                if same_group {
                    current_group.push((id, bytes));
                } else {
                    // New group boundary.
                    if !current_group.is_empty() {
                        total_from_complete_groups += current_group.len();
                        groups.push(std::mem::take(&mut current_group));

                        // If we already have enough from complete groups, stop.
                        if total_from_complete_groups >= needed {
                            break;
                        }
                    }
                    current_value = Some(val_owned);
                    current_group.push((id, bytes));
                }
            }

            // Don't forget the last group (may be incomplete if we didn't break).
            if !current_group.is_empty() {
                groups.push(current_group);
            }

            // Sub-sort within each group by sorts[1..].
            let sub_sorts = &sorts[1..];
            if !sub_sorts.is_empty() {
                for group in &mut groups {
                    group.sort_by(|(a_id, a_bytes), (b_id, b_bytes)| {
                        for sort in sub_sorts {
                            let ord = if sort.field == "_id" {
                                a_id.cmp(b_id)
                            } else {
                                let a_raw = RawDocument::from_bytes(a_bytes).ok();
                                let b_raw = RawDocument::from_bytes(b_bytes).ok();
                                let a_val = a_raw.and_then(|r| {
                                    exec::raw_get_path(r, &sort.field).ok().flatten()
                                });
                                let b_val = b_raw.and_then(|r| {
                                    exec::raw_get_path(r, &sort.field).ok().flatten()
                                });
                                exec::raw_compare_field_values(a_val, b_val)
                            };
                            let ord = match sort.direction {
                                SortDirection::Asc => ord,
                                SortDirection::Desc => ord.reverse(),
                            };
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
            }

            // Flatten groups into a single iterator.
            let records: Vec<(String, Vec<u8>)> = groups.into_iter().flatten().collect();
            Ok(Box::new(
                records
                    .into_iter()
                    .map(|(id, bytes)| Ok((id, Cow::Owned(bytes)))),
            ))
        }

        PlanNode::Limit { skip, take, input } => {
            let source = execute_raw_node(txn, input)?;
            Ok(apply_limit(source, *skip, *take))
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
                let (id, cow) = result?;
                let raw = RawDocument::from_bytes(&cow)?;
                let mut doc = materialize_for_projection(raw, &cols)?;
                doc.insert("_id", id.as_str());
                exec::apply_projection(&mut doc, &cols);
                Ok(doc)
            })))
        }

        // No Projection node — full materialization
        _ => {
            let source = execute_raw_node(txn, node)?;
            Ok(Box::new(source.map(|result| {
                let (id, cow) = result?;
                let raw = RawDocument::from_bytes(&cow)?;
                let mut doc: bson::Document = raw.try_into()?;
                doc.insert("_id", id.as_str());
                Ok(doc)
            })))
        }
    }
}

/// ReadRecord: the boundary between ID tier and raw tier.
///
/// - Scan: data flows through lazily — bytes are already available from the ID tier.
/// - IndexScan/IndexMerge: IDs are collected eagerly (releasing the `scan_prefix`
///   borrow on `txn`), then each record is fetched lazily via `txn.get()`.
///   The ID collection is cheap (small strings); the expensive record fetch is
///   what Limit cuts short.
fn execute_read_record<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    input: &'a PlanNode,
) -> Result<RawIter<'a>, DbError> {
    if matches!(input, PlanNode::Scan { .. }) {
        // Scan path: ID tier already carries the data — just unwrap the Option.
        let source = execute_id_node(txn, input)?;
        return Ok(Box::new(source.filter_map(|result| match result {
            Ok((id, Some(bytes))) => Some(Ok((id, bytes))),
            Ok((_, None)) => None,
            Err(e) => Some(Err(e)),
        })));
    }

    // IndexScan/IndexMerge path: collect IDs (releases txn borrow), then fetch lazily.
    let collection = extract_collection(input);
    let ids: Vec<String> = execute_id_node(txn, input)?
        .filter_map(|r| r.ok().map(|(id, _)| id))
        .collect();

    Ok(Box::new(ids.into_iter().filter_map(move |id| {
        let key = encoding::record_key(&id);
        match txn.get(&collection, &key) {
            Ok(Some(bytes)) => Some(Ok((id, Cow::Owned(bytes.into_owned())))),
            Ok(None) => None, // dangling index entry
            Err(e) => Some(Err(DbError::Store(e))),
        }
    })))
}

/// Generic skip/take over any boxed iterator.
fn apply_limit<'a, T: 'a>(
    iter: Box<dyn Iterator<Item = T> + 'a>,
    skip: usize,
    take: Option<usize>,
) -> Box<dyn Iterator<Item = T> + 'a> {
    let iter = iter.skip(skip);
    match take {
        Some(n) => Box::new(iter.take(n)),
        None => Box::new(iter),
    }
}

/// Extract the collection name from an ID-tier node.
fn extract_collection(node: &PlanNode) -> String {
    match node {
        PlanNode::Scan { collection } => collection.clone(),
        PlanNode::IndexScan { collection, .. } => collection.clone(),
        PlanNode::IndexMerge { lhs, .. } => extract_collection(lhs),
        _ => String::new(),
    }
}

/// Selectively materialize a raw document for projection.
/// Only deserializes top-level keys that are in the projection set.
fn materialize_for_projection(
    raw: &RawDocument,
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
