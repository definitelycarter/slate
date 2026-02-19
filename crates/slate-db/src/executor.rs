use std::borrow::Cow;
use std::collections::HashSet;

use bson::raw::{RawBson, RawBsonRef};
use bson::{RawDocument, RawDocumentBuf};
use slate_query::{LogicalOp, SortDirection};
use slate_store::Transaction;

use crate::encoding;
use crate::error::DbError;
use crate::exec;
use crate::planner::PlanNode;

// ── RawValue ────────────────────────────────────────────────────
//
// Cow-like enum for raw BSON values. Borrowed holds a zero-copy
// reference into the store snapshot; Owned holds bytes returned by
// RocksDB. Downstream nodes call `.as_document()` to get a uniform
// `&RawDocument` view without redundant `from_bytes` calls.

pub(crate) enum RawValue<'a> {
    Borrowed(RawBsonRef<'a>),
    Owned(RawBson),
}

impl<'a> RawValue<'a> {
    /// Uniform borrowed view regardless of ownership.
    #[allow(dead_code)]
    fn as_ref(&self) -> RawBsonRef<'_> {
        match self {
            Self::Borrowed(r) => *r,
            Self::Owned(b) => b.as_raw_bson_ref(),
        }
    }

    /// Extract `&RawDocument` if this value is a document.
    fn as_document(&self) -> Option<&RawDocument> {
        match self {
            Self::Borrowed(RawBsonRef::Document(d)) => Some(d),
            Self::Owned(RawBson::Document(d)) => Some(d),
            _ => None,
        }
    }

    /// Convert to owned `RawBson`, dropping the lifetime.
    /// Returns `None` for borrowed variants (would require cloning the backing store).
    fn into_owned(self) -> Option<RawBson> {
        match self {
            Self::Owned(b) => Some(b),
            Self::Borrowed(_) => None,
        }
    }

    /// Convert to `bson::Bson` for materialization into a document.
    fn to_bson(&self) -> Result<bson::Bson, DbError> {
        Ok(bson::Bson::try_from(self.as_ref())?)
    }
}

type BsonIter<'a> = Box<dyn Iterator<Item = Result<bson::Bson, DbError>> + 'a>;
type RawIter<'a> = Box<dyn Iterator<Item = Result<(String, Option<RawValue<'a>>), DbError>> + 'a>;

/// Executes a query plan tree, returning a lazy BSON value iterator.
pub fn execute<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    node: &'a PlanNode,
) -> Result<BsonIter<'a>, DbError> {
    execute_node(txn, node)
}

/// Executes a distinct plan tree, returning unique field values.
pub fn execute_distinct<T: Transaction>(
    txn: &mut T,
    node: &PlanNode,
) -> Result<Vec<bson::Bson>, DbError> {
    match node {
        PlanNode::Distinct {
            field,
            direction,
            input,
        } => {
            let source = execute_raw_node(txn, input)?;
            let mut unique: Vec<bson::Bson> = Vec::new();

            for result in source {
                let (_id, opt_val) = result?;
                let val = opt_val.ok_or_else(|| DbError::InvalidQuery("expected value".into()))?;
                let raw = val
                    .as_document()
                    .ok_or_else(|| DbError::InvalidQuery("expected document".into()))?;
                let values = exec::raw_get_path_values(raw, field)?;

                for val in values {
                    let bson_val = bson::Bson::try_from(val)?;
                    if !unique.contains(&bson_val) {
                        unique.push(bson_val);
                    }
                }
            }

            if let Some(dir) = direction {
                unique.sort_by(|a, b| {
                    let ord = exec::compare_bson_values(a, b);
                    match dir {
                        SortDirection::Asc => ord,
                        SortDirection::Desc => ord.reverse(),
                    }
                });
            }

            Ok(unique)
        }
        _ => Err(DbError::InvalidQuery(
            "expected Distinct node at top of plan".to_string(),
        )),
    }
}

// ── ID tier ─────────────────────────────────────────────────────
//
// Nodes below ReadRecord produce record IDs (and optionally raw values)
// as a lazy iterator. Scan carries the data it already has; IndexScan
// yields None since index entries don't contain record data.

/// Execute an ID-tier node, returning a lazy iterator of (id, maybe_value).
fn execute_id_node<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    node: &'a PlanNode,
) -> Result<RawIter<'a>, DbError> {
    match node {
        PlanNode::Scan { collection } => execute_scan(txn, collection),

        PlanNode::IndexScan {
            collection,
            column,
            value,
            direction,
            limit,
            complete_groups,
        } => execute_index_scan(
            txn,
            collection,
            column,
            value.as_ref(),
            *direction,
            *limit,
            *complete_groups,
        ),

        PlanNode::IndexMerge { logical, lhs, rhs } => {
            // IndexMerge must collect both sides for dedup — stays eager.
            // Values are collected as Option<RawBson> (no lifetime) to release
            // the txn borrow between left and right collection.
            let left: Vec<(String, Option<RawBson>)> = execute_id_node(txn, lhs)?
                .map(|r| r.map(|(id, val)| (id, val.and_then(RawValue::into_owned))))
                .collect::<Result<_, _>>()?;
            let right: Vec<(String, Option<RawBson>)> = execute_id_node(txn, rhs)?
                .map(|r| r.map(|(id, val)| (id, val.and_then(RawValue::into_owned))))
                .collect::<Result<_, _>>()?;

            let merged = match logical {
                LogicalOp::Or => {
                    let mut seen = HashSet::with_capacity(left.len() + right.len());
                    let mut result = Vec::with_capacity(left.len() + right.len());
                    for (id, val) in left.into_iter().chain(right) {
                        if seen.insert(id.clone()) {
                            result.push((id, val));
                        }
                    }
                    result
                }
                LogicalOp::And => {
                    let right_set: HashSet<String> =
                        right.iter().map(|(id, _)| id.clone()).collect();
                    left.into_iter()
                        .filter(|(id, _)| right_set.contains(id))
                        .collect()
                }
            };

            Ok(Box::new(
                merged
                    .into_iter()
                    .map(|(id, val)| Ok((id, val.map(RawValue::Owned)))),
            ))
        }

        _ => Err(DbError::InvalidQuery(
            "unexpected node in ID tier".to_string(),
        )),
    }
}

/// Scan all records lazily, yielding (id, Some(RawValue)) — data is kept, not discarded.
fn execute_scan<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    collection: &str,
) -> Result<RawIter<'a>, DbError> {
    let scan_prefix = encoding::data_scan_prefix("");
    let iter = txn.scan_prefix(collection, &scan_prefix)?;

    Ok(Box::new(iter.filter_map(|result| match result {
        Ok((key, value)) => {
            let record_id = encoding::parse_record_key(&key)?.to_string();
            let val = match value {
                Cow::Borrowed(b) => match RawDocument::from_bytes(b) {
                    Ok(raw) => RawValue::Borrowed(RawBsonRef::Document(raw)),
                    Err(e) => return Some(Err(DbError::from(e))),
                },
                Cow::Owned(v) => match RawDocumentBuf::from_bytes(v) {
                    Ok(raw) => RawValue::Owned(RawBson::Document(raw)),
                    Err(e) => return Some(Err(DbError::from(e))),
                },
            };
            Some(Ok((record_id, Some(val))))
        }
        Err(e) => Some(Err(DbError::Store(e))),
    })))
}

/// Scan an index prefix, yielding (id, value).
/// For Eq scans (`value: Some`), returns the known indexed value as `RawValue`.
/// For ordered scans (`value: None`), returns `None` (value decoding deferred
/// until the key format includes a type tag).
/// When `complete_groups` is true and limit is set, reads past the limit
/// to finish the last value group for correct downstream sub-sorting.
fn execute_index_scan<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    collection: &str,
    column: &str,
    value: Option<&bson::Bson>,
    direction: SortDirection,
    limit: Option<usize>,
    complete_groups: bool,
) -> Result<RawIter<'a>, DbError> {
    // Some(v): `i:{column}\x00{value_bytes}\x00` — entries matching exact value (Eq filter)
    // None:    `i:{column}\x00`               — all entries for column (ordered scan)
    let prefix = match value {
        Some(v) => encoding::index_scan_prefix(column, v),
        None => encoding::index_scan_field_prefix(column),
    };

    // For Eq scans, convert the known value to RawBson once — cloned per record.
    let raw_val: Option<RawBson> = match value {
        Some(v) => Some(RawBson::try_from(v.clone())?),
        None => None,
    };

    let mut iter = match direction {
        SortDirection::Asc => txn.scan_prefix(collection, &prefix)?,
        SortDirection::Desc => txn.scan_prefix_rev(collection, &prefix)?,
    };

    let mut count = 0usize;
    let mut boundary_prefix: Option<Vec<u8>> = None;
    let mut done = false;

    Ok(Box::new(std::iter::from_fn(move || {
        if done {
            return None;
        }

        for result in iter.by_ref() {
            match result {
                Ok((key, _)) => {
                    // Past the limit — check if we should stop.
                    if let Some(n) = limit {
                        if count >= n {
                            if complete_groups {
                                let val_prefix = encoding::index_key_value_prefix(&key);
                                let changed = match (&boundary_prefix, val_prefix) {
                                    (Some(prev), Some(cur)) => prev.as_slice() != cur,
                                    _ => false,
                                };
                                if changed {
                                    done = true;
                                    return None;
                                }
                                if boundary_prefix.is_none() {
                                    boundary_prefix = val_prefix.map(|p| p.to_vec());
                                }
                            } else {
                                done = true;
                                return None;
                            }
                        }
                    }

                    match encoding::parse_index_key(&key) {
                        Some((_, record_id)) => {
                            count += 1;
                            let item_val = raw_val.as_ref().map(|v| RawValue::Owned(v.clone()));
                            return Some(Ok((record_id.to_string(), item_val)));
                        }
                        None => continue,
                    }
                }
                Err(e) => {
                    done = true;
                    return Some(Err(DbError::Store(e)));
                }
            }
        }

        done = true;
        None
    })))
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
                Ok((id, Some(val))) => {
                    let raw = match val.as_document() {
                        Some(r) => r,
                        None => {
                            return Some(Err(DbError::InvalidQuery("expected document".into())));
                        }
                    };
                    match exec::raw_matches_group(raw, &id, predicate) {
                        Ok(true) => Some(Ok((id, Some(val)))),
                        Ok(false) => None,
                        Err(e) => Some(Err(e)),
                    }
                }
                Ok((_, None)) => None,
            })))
        }

        PlanNode::Sort { sorts, input } => {
            let source = execute_raw_node(txn, input)?;

            let mut records: Vec<(String, Option<RawValue<'a>>)> =
                source.collect::<Result<Vec<_>, _>>()?;

            records.sort_by(|(a_id, a_opt), (b_id, b_opt)| {
                for sort in sorts {
                    let ord = if sort.field == "_id" {
                        a_id.cmp(b_id)
                    } else {
                        let a_raw = a_opt.as_ref().and_then(|v| v.as_document());
                        let b_raw = b_opt.as_ref().and_then(|v| v.as_document());
                        let a_field =
                            a_raw.and_then(|r| exec::raw_get_path(r, &sort.field).ok().flatten());
                        let b_field =
                            b_raw.and_then(|r| exec::raw_get_path(r, &sort.field).ok().flatten());
                        exec::raw_compare_field_values(a_field, b_field)
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

            Ok(Box::new(records.into_iter().map(|(id, val)| Ok((id, val)))))
        }

        PlanNode::Limit { skip, take, input } => {
            let source = execute_raw_node(txn, input)?;
            Ok(apply_limit(source, *skip, *take))
        }

        // ID-tier nodes can appear directly in the raw tier when ReadRecord is skipped
        // (e.g. index-covered projections). Delegate to the ID-tier executor.
        PlanNode::Scan { .. } | PlanNode::IndexScan { .. } | PlanNode::IndexMerge { .. } => {
            execute_id_node(txn, node)
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
) -> Result<BsonIter<'a>, DbError> {
    match node {
        PlanNode::Projection { columns, input } => {
            let source = execute_raw_node(txn, input)?;
            let cols = columns.clone();

            Ok(Box::new(source.map(move |result| {
                let (id, opt_val) = result?;
                let val = opt_val.ok_or_else(|| DbError::InvalidQuery("expected value".into()))?;

                let doc = if let Some(raw) = val.as_document() {
                    // Full document — selective materialization
                    let mut doc = materialize_for_projection(raw, &cols)?;
                    doc.insert("_id", id.as_str());
                    exec::apply_projection(&mut doc, &cols);
                    doc
                } else {
                    // Scalar value from index — construct document directly
                    let bson_val = val.to_bson()?;
                    let mut doc = bson::Document::new();
                    doc.insert("_id", id.as_str());
                    for col in &cols {
                        doc.insert(col.as_str(), bson_val.clone());
                    }
                    doc
                };
                Ok(bson::Bson::Document(doc))
            })))
        }

        // No Projection node — full materialization
        _ => {
            let source = execute_raw_node(txn, node)?;
            Ok(Box::new(source.map(|result| {
                let (id, opt_val) = result?;
                let val = opt_val.ok_or_else(|| DbError::InvalidQuery("expected value".into()))?;
                let raw = val
                    .as_document()
                    .ok_or_else(|| DbError::InvalidQuery("expected document".into()))?;
                let mut doc: bson::Document = raw.try_into()?;
                doc.insert("_id", id.as_str());
                Ok(bson::Bson::Document(doc))
            })))
        }
    }
}

/// ReadRecord: the boundary between ID tier and raw tier.
///
/// - Scan: data flows through lazily — RawValue is already available from the ID tier.
/// - IndexScan/IndexMerge: IDs are collected eagerly (releasing the `scan_prefix`
///   borrow on `txn`), then each record is fetched lazily via `txn.get()`.
///   The ID collection is cheap (small strings); the expensive record fetch is
///   what Limit cuts short.
fn execute_read_record<'a, T: Transaction + 'a>(
    txn: &'a mut T,
    input: &'a PlanNode,
) -> Result<RawIter<'a>, DbError> {
    if matches!(input, PlanNode::Scan { .. }) {
        // Scan path: ID tier already carries the data — just pass through.
        let source = execute_id_node(txn, input)?;
        return Ok(Box::new(source.filter_map(|result| match result {
            Ok((id, Some(val))) => Some(Ok((id, Some(val)))),
            Ok((_, None)) => None,
            Err(e) => Some(Err(e)),
        })));
    }

    // IndexScan/IndexMerge path: collect IDs (releases txn borrow), then fetch lazily.
    let collection = extract_collection(input);
    let ids: Vec<String> = execute_id_node(txn, input)?
        .map(|r| r.map(|(id, _)| id))
        .collect::<Result<_, _>>()?;

    Ok(Box::new(ids.into_iter().filter_map(move |id| {
        let key = encoding::record_key(&id);
        match txn.get(&collection, &key) {
            Ok(Some(bytes)) => {
                let raw = match RawDocumentBuf::from_bytes(bytes.into_owned()) {
                    Ok(r) => r,
                    Err(e) => return Some(Err(DbError::from(e))),
                };
                Some(Ok((id, Some(RawValue::Owned(RawBson::Document(raw))))))
            }
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
