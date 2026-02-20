use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use bson::raw::{RawArrayBuf, RawBson, RawBsonRef};
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
    pub(crate) fn as_ref(&self) -> RawBsonRef<'_> {
        match self {
            Self::Borrowed(r) => *r,
            Self::Owned(b) => b.as_raw_bson_ref(),
        }
    }

    /// Extract `&RawDocument` if this value is a document.
    pub(crate) fn as_document(&self) -> Option<&RawDocument> {
        match self {
            Self::Borrowed(RawBsonRef::Document(d)) => Some(d),
            Self::Owned(RawBson::Document(d)) => Some(d),
            _ => None,
        }
    }

    /// Convert to owned `RawBson`, dropping the lifetime.
    pub(crate) fn into_raw_bson(self) -> Option<RawBson> {
        match self {
            Self::Owned(b) => Some(b),
            Self::Borrowed(r) => crate::exec::to_raw_bson(r),
        }
    }

    /// Extract as `RawDocumentBuf`, moving owned data without copying.
    pub(crate) fn into_document_buf(self) -> Option<RawDocumentBuf> {
        match self {
            Self::Owned(RawBson::Document(buf)) => Some(buf),
            Self::Borrowed(RawBsonRef::Document(d)) => Some(d.to_raw_document_buf()),
            _ => None,
        }
    }
}

pub(crate) type RawIter<'a> =
    Box<dyn Iterator<Item = Result<(Option<String>, Option<RawValue<'a>>), DbError>> + 'a>;

/// Executes a query plan tree, returning a lazy raw iterator.
///
/// Both `find()` and `distinct()` call this — different plans, same executor.
/// Takes `&T` (not `&mut T`) plus a pre-resolved CF handle for the collection.
pub fn execute<'c, T: Transaction + 'c>(
    txn: &'c T,
    cf: &'c T::Cf,
    node: &'c PlanNode,
) -> Result<RawIter<'c>, DbError> {
    execute_node(txn, cf, node)
}

/// Scan all records lazily, yielding (Some(id), Some(RawValue)).
fn execute_scan<'c, T: Transaction + 'c>(
    txn: &'c T,
    cf: &'c T::Cf,
) -> Result<RawIter<'c>, DbError> {
    let scan_prefix = encoding::data_scan_prefix("");
    let iter = txn.scan_prefix(cf, &scan_prefix)?;

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
            Some(Ok((Some(record_id), Some(val))))
        }
        Err(e) => Some(Err(DbError::Store(e))),
    })))
}

/// Scan an index prefix, yielding (Some(id), maybe_value).
fn execute_index_scan<'c, T: Transaction + 'c>(
    txn: &'c T,
    cf: &'c T::Cf,
    column: &str,
    value: Option<&bson::Bson>,
    direction: SortDirection,
    limit: Option<usize>,
    complete_groups: bool,
) -> Result<RawIter<'c>, DbError> {
    let prefix = match value {
        Some(v) => encoding::index_scan_prefix(column, v),
        None => encoding::index_scan_field_prefix(column),
    };

    // TODO: Store RawBson in PlanNode::IndexScan instead of bson::Bson to avoid this conversion.
    // The value isn't read from the index key — it's the known Eq value from the query,
    // carried through so index-covered projections can emit it without a record fetch.
    let raw_val: Option<RawBson> = match value {
        Some(v) => Some(RawBson::try_from(v.clone())?),
        None => None,
    };

    let mut iter = match direction {
        SortDirection::Asc => txn.scan_prefix(cf, &prefix)?,
        SortDirection::Desc => txn.scan_prefix_rev(cf, &prefix)?,
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
                Ok((key, stored_value)) => {
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
                            let item_val = raw_val.as_ref().map(|v| {
                                RawValue::Owned(encoding::coerce_to_stored_type(v, &stored_value))
                            });
                            return Some(Ok((Some(record_id.to_string()), item_val)));
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

fn execute_node<'c, T: Transaction + 'c>(
    txn: &'c T,
    cf: &'c T::Cf,
    node: &'c PlanNode,
) -> Result<RawIter<'c>, DbError> {
    match node {
        PlanNode::Scan { .. } => execute_scan(txn, cf),

        PlanNode::IndexScan {
            column,
            value,
            direction,
            limit,
            complete_groups,
            ..
        } => execute_index_scan(
            txn,
            cf,
            column,
            value.as_ref(),
            *direction,
            *limit,
            *complete_groups,
        ),

        PlanNode::IndexMerge { logical, lhs, rhs } => {
            let left: Vec<(Option<String>, Option<RawBson>)> = execute_node(txn, cf, lhs)?
                .map(|r| r.map(|(id, val)| (id, val.and_then(RawValue::into_raw_bson))))
                .collect::<Result<_, _>>()?;
            let right: Vec<(Option<String>, Option<RawBson>)> = execute_node(txn, cf, rhs)?
                .map(|r| r.map(|(id, val)| (id, val.and_then(RawValue::into_raw_bson))))
                .collect::<Result<_, _>>()?;

            let merged = match logical {
                LogicalOp::Or => {
                    let mut seen = HashSet::with_capacity(left.len() + right.len());
                    let mut result = Vec::with_capacity(left.len() + right.len());
                    for (id, val) in left.into_iter().chain(right) {
                        if let Some(ref id_str) = id {
                            if !seen.insert(id_str.clone()) {
                                continue;
                            }
                        }
                        result.push((id, val));
                    }
                    result
                }
                LogicalOp::And => {
                    let right_set: HashSet<String> =
                        right.iter().filter_map(|(id, _)| id.clone()).collect();
                    left.into_iter()
                        .filter(|(id, _)| id.as_ref().map_or(false, |s| right_set.contains(s)))
                        .collect()
                }
            };

            Ok(Box::new(
                merged
                    .into_iter()
                    .map(|(id, val)| Ok((id, val.map(RawValue::Owned)))),
            ))
        }

        PlanNode::ReadRecord { input } => execute_read_record(txn, cf, input),

        PlanNode::Filter { predicate, input } => {
            let source = execute_node(txn, cf, input)?;

            Ok(Box::new(source.filter_map(move |result| match result {
                Err(e) => Some(Err(e)),
                Ok((id, Some(val))) => {
                    let raw = match val.as_document() {
                        Some(r) => r,
                        None => {
                            return Some(Err(DbError::InvalidQuery("expected document".into())));
                        }
                    };
                    let id_str = id.as_deref().unwrap_or("");
                    match exec::raw_matches_group(raw, id_str, predicate) {
                        Ok(true) => Some(Ok((id, Some(val)))),
                        Ok(false) => None,
                        Err(e) => Some(Err(e)),
                    }
                }
                Ok((_, None)) => None,
            })))
        }

        PlanNode::Sort { sorts, input } => {
            let mut source = execute_node(txn, cf, input)?;

            // Peek first item — if it's an array (from Distinct), sort its elements
            match source.next() {
                Some(Ok((id, Some(RawValue::Owned(RawBson::Array(arr)))))) => {
                    let sort = match sorts.first() {
                        Some(s) => s,
                        None => {
                            return Ok(Box::new(std::iter::once(Ok((
                                id,
                                Some(RawValue::Owned(RawBson::Array(arr))),
                            )))));
                        }
                    };
                    let mut elements: Vec<RawBson> = arr
                        .into_iter()
                        .filter_map(|r| exec::to_raw_bson(r.ok()?))
                        .collect();

                    elements.sort_by(|a, b| {
                        let a_ref = a.as_raw_bson_ref();
                        let b_ref = b.as_raw_bson_ref();
                        let ord = match (&a_ref, &b_ref) {
                            (RawBsonRef::Document(a_doc), RawBsonRef::Document(b_doc)) => {
                                let a_field = exec::raw_get_path(a_doc, &sort.field).ok().flatten();
                                let b_field = exec::raw_get_path(b_doc, &sort.field).ok().flatten();
                                exec::raw_compare_field_values(a_field, b_field)
                            }
                            _ => exec::raw_compare_field_values(Some(a_ref), Some(b_ref)),
                        };
                        match sort.direction {
                            SortDirection::Asc => ord,
                            SortDirection::Desc => ord.reverse(),
                        }
                    });

                    let mut buf = bson::RawArrayBuf::new();
                    for elem in &elements {
                        exec::push_raw(&mut buf, elem.as_raw_bson_ref());
                    }
                    return Ok(Box::new(std::iter::once(Ok((
                        id,
                        Some(RawValue::Owned(RawBson::Array(buf))),
                    )))));
                }
                Some(first) => {
                    // Not an array — collect all records for sorting
                    let mut records: Vec<(Option<String>, Option<RawValue<'c>>)> =
                        std::iter::once(first)
                            .chain(source)
                            .collect::<Result<Vec<_>, _>>()?;

                    records.sort_by(|(a_id, a_opt), (b_id, b_opt)| {
                        for sort in sorts {
                            let ord = if sort.field == "_id" {
                                let a_str = a_id.as_deref().unwrap_or("");
                                let b_str = b_id.as_deref().unwrap_or("");
                                a_str.cmp(b_str)
                            } else {
                                let a_raw = a_opt.as_ref().and_then(|v| v.as_document());
                                let b_raw = b_opt.as_ref().and_then(|v| v.as_document());
                                let a_field = a_raw.and_then(|r| {
                                    exec::raw_get_path(r, &sort.field).ok().flatten()
                                });
                                let b_field = b_raw.and_then(|r| {
                                    exec::raw_get_path(r, &sort.field).ok().flatten()
                                });
                                exec::raw_compare_field_values(a_field, b_field)
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

                    Ok(Box::new(records.into_iter().map(|(id, val)| Ok((id, val)))))
                }
                None => Ok(Box::new(std::iter::empty())),
            }
        }

        PlanNode::Limit { skip, take, input } => {
            let mut source = execute_node(txn, cf, input)?;

            // Check first item — if it's a single array (from Distinct), slice its elements
            match source.next() {
                Some(Ok((id, Some(RawValue::Owned(RawBson::Array(arr)))))) => {
                    let take_n = take.unwrap_or(usize::MAX);
                    let mut buf = RawArrayBuf::new();
                    for elem in arr.into_iter().skip(*skip).take(take_n) {
                        if let Ok(val) = elem {
                            exec::push_raw(&mut buf, val);
                        }
                    }
                    Ok(Box::new(std::iter::once(Ok((
                        id,
                        Some(RawValue::Owned(RawBson::Array(buf))),
                    )))))
                }
                Some(first) => {
                    // Not an array — chain first item back and apply normal skip/take
                    let full = std::iter::once(first).chain(source);
                    Ok(apply_limit(Box::new(full), *skip, *take))
                }
                None => Ok(Box::new(std::iter::empty())),
            }
        }

        PlanNode::Projection { columns, input } => {
            let source = execute_node(txn, cf, input)?;

            // Build the key_map once, not per record.
            let key_map: Option<HashMap<String, Vec<String>>> = columns.as_ref().map(|cols| {
                let mut map: HashMap<String, Vec<String>> = HashMap::new();
                for col in cols {
                    match col.split_once('.') {
                        None => {
                            map.entry(col.clone()).or_default();
                        }
                        Some((top, rest)) => {
                            map.entry(top.to_string())
                                .or_default()
                                .push(rest.to_string());
                        }
                    }
                }
                map
            });
            // Keep first column name for scalar index-covered path
            let first_col = columns.as_ref().and_then(|c| c.first().cloned());

            Ok(Box::new(source.map(move |result| {
                let (id, opt_val) = result?;
                let val = opt_val.ok_or_else(|| DbError::InvalidQuery("expected value".into()))?;

                // No columns specified: prepend _id, pass through all fields
                let km = match key_map {
                    Some(ref km) => km,
                    None => {
                        if let Some(ref id_str) = id {
                            if let Some(raw) = val.as_document() {
                                let mut buf = raw.to_raw_document_buf();
                                buf.append("_id", id_str.as_str());
                                return Ok((id, Some(RawValue::Owned(RawBson::Document(buf)))));
                            }
                        }
                        return Ok((id, Some(val)));
                    }
                };

                let mut buf = RawDocumentBuf::new();

                // Inject _id from the tuple
                if let Some(ref id_str) = id {
                    buf.append("_id", id_str.as_str());
                }

                if let Some(raw) = val.as_document() {
                    raw_project_document(raw, km, &mut buf)?;
                } else {
                    // Scalar input (index-covered): construct { field: value }
                    if let Some(ref field) = first_col {
                        buf.append_ref(field.as_str(), val.as_ref());
                    }
                }

                Ok((id, Some(RawValue::Owned(RawBson::Document(buf)))))
            })))
        }

        PlanNode::Distinct { field, input, .. } => {
            use std::collections::HashSet;

            let source = execute_node(txn, cf, input)?;
            let mut seen = HashSet::new();
            let mut buf = bson::RawArrayBuf::new();

            for result in source {
                let (_id, opt_val) = result?;
                let val = match opt_val {
                    Some(v) => v,
                    None => continue,
                };
                let raw = val
                    .as_document()
                    .ok_or_else(|| DbError::InvalidQuery("expected document".into()))?;

                exec::raw_walk_path(raw, field, &mut |raw_ref| {
                    if !matches!(raw_ref, RawBsonRef::Null) {
                        exec::try_insert(&mut seen, &mut buf, raw_ref);
                    }
                    Ok(())
                })?;
            }

            Ok(Box::new(std::iter::once(Ok((
                None,
                Some(RawValue::Owned(RawBson::Array(buf))),
            )))))
        }
    }
}

/// ReadRecord: the boundary between ID tier and raw tier.
fn execute_read_record<'c, T: Transaction + 'c>(
    txn: &'c T,
    cf: &'c T::Cf,
    input: &'c PlanNode,
) -> Result<RawIter<'c>, DbError> {
    if matches!(input, PlanNode::Scan { .. }) {
        let source = execute_node(txn, cf, input)?;
        return Ok(Box::new(source.filter_map(|result| match result {
            Ok((id, Some(val))) => Some(Ok((id, Some(val)))),
            Ok((_, None)) => None,
            Err(e) => Some(Err(e)),
        })));
    }

    // IndexScan/IndexMerge path: stream IDs directly into record fetches.
    let source = execute_node(txn, cf, input)?;

    Ok(Box::new(source.filter_map(move |result| {
        let id = match result {
            Ok((Some(id), _)) => id,
            Ok((None, _)) => return None,
            Err(e) => return Some(Err(e)),
        };
        let key = encoding::record_key(&id);
        match txn.get(cf, &key) {
            Ok(Some(bytes)) => {
                let raw = match RawDocumentBuf::from_bytes(bytes.into_owned()) {
                    Ok(r) => r,
                    Err(e) => return Some(Err(DbError::from(e))),
                };
                Some(Ok((
                    Some(id),
                    Some(RawValue::Owned(RawBson::Document(raw))),
                )))
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

// ── Raw projection ──────────────────────────────────────────────
//
// Selectively copies fields from a source `&RawDocument` into a destination
// `RawDocumentBuf`, guided by a list of column paths (e.g. `["name", "address.city"]`).
//
// - Flat columns (`name`): `append_ref` copies the raw bytes directly.
// - Dotted columns (`address.city`): builds a trimmed sub-document containing
//   only the requested nested fields, recursively. Only the selected leaf
//   values are copied — not the entire parent.
// - Arrays of sub-documents (`triggers.type`): each element is walked and
//   trimmed to the requested fields.

/// Project selected columns from `src` into `dest` using a pre-built key map.
///
/// `key_map` maps top-level field names to their sub-paths:
/// - `"name" → []` means copy the whole field
/// - `"address" → ["city", "zip"]` means recurse and copy only those nested fields
fn raw_project_document<S: AsRef<str>>(
    src: &RawDocument,
    key_map: &HashMap<String, Vec<S>>,
    dest: &mut RawDocumentBuf,
) -> Result<(), DbError> {
    for result in src.iter() {
        let (key, raw_val) = result?;
        if let Some(sub_paths) = key_map.get(key) {
            if sub_paths.is_empty() {
                // Flat column: copy raw bytes directly
                dest.append_ref(key, raw_val);
            } else {
                // Dotted column: recurse into sub-document or array
                let sub_map = build_key_map(sub_paths);
                match raw_val {
                    RawBsonRef::Document(sub_doc) => {
                        let mut trimmed = RawDocumentBuf::new();
                        raw_project_document(sub_doc, &sub_map, &mut trimmed)?;
                        dest.append(key, RawBson::Document(trimmed));
                    }
                    RawBsonRef::Array(arr) => {
                        let mut out = RawArrayBuf::new();
                        for elem in arr {
                            let elem = elem?;
                            match elem {
                                RawBsonRef::Document(elem_doc) => {
                                    let mut trimmed = RawDocumentBuf::new();
                                    raw_project_document(elem_doc, &sub_map, &mut trimmed)?;
                                    out.push(RawBson::Document(trimmed));
                                }
                                other => {
                                    // Non-document array element: copy as-is
                                    out.push(other.to_raw_bson());
                                }
                            }
                        }
                        dest.append(key, RawBson::Array(out));
                    }
                    _ => {
                        // Scalar where we expected a sub-document — copy as-is
                        dest.append_ref(key, raw_val);
                    }
                }
            }
        }
    }

    Ok(())
}

/// Build a key_map from a list of paths (e.g. ["city", "address.zip"] → {"city": [], "address": ["zip"]}).
fn build_key_map<S: AsRef<str>>(paths: &[S]) -> HashMap<String, Vec<&str>> {
    let mut map: HashMap<String, Vec<&str>> = HashMap::new();
    for path in paths {
        match path.as_ref().split_once('.') {
            None => {
                map.entry(path.as_ref().to_string()).or_default();
            }
            Some((top, rest)) => {
                map.entry(top.to_string()).or_default().push(rest);
            }
        }
    }
    map
}
