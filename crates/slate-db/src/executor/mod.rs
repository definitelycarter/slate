#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use bson::raw::{RawArrayBuf, RawBson, RawBsonRef};
use bson::{RawDocument, RawDocumentBuf};
use slate_query::{LogicalOp, SortDirection};
use slate_store::Transaction;
pub(crate) mod exec;
use crate::encoding;
use crate::error::DbError;
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
            Self::Borrowed(r) => exec::to_raw_bson(r),
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

/// Typed result from executing a plan.
pub enum ExecutionResult<'a> {
    /// Read query — lazy iterator of (id, doc) tuples.
    Rows(RawIter<'a>),
    /// Delete mutation — count of deleted records.
    Delete { deleted: u64 },
    /// Update/replace mutation — counts of matched and modified records.
    Update { matched: u64, modified: u64 },
    /// Insert mutation — IDs of inserted records.
    Insert { ids: Vec<String> },
}

// ── Executor ────────────────────────────────────────────────────

pub struct Executor<'c, T: Transaction> {
    txn: &'c T,
    cf: &'c T::Cf,
}

impl<'c, T: Transaction + 'c> Executor<'c, T> {
    pub fn new(txn: &'c T, cf: &'c T::Cf) -> Self {
        Self { txn, cf }
    }

    /// Execute a query plan tree, returning a typed result.
    ///
    /// The root node type determines the result variant:
    /// - `Delete` root → drains the pipeline, returns `Delete { deleted }`
    /// - `InsertIndex` root (wrapping Update/Replace) → drains, returns `Update { matched, modified }`
    /// - Everything else → returns `Rows(iter)` for the caller to consume
    pub fn execute(&self, node: &'c PlanNode) -> Result<ExecutionResult<'c>, DbError> {
        match node {
            PlanNode::Delete { .. } => {
                let iter = self.execute_node(node)?;
                let mut deleted = 0u64;
                for result in iter {
                    result?;
                    deleted += 1;
                }
                Ok(ExecutionResult::Delete { deleted })
            }
            PlanNode::InsertIndex { input, .. }
                if matches!(**input, PlanNode::InsertRecord { .. }) =>
            {
                let iter = self.execute_node(node)?;
                let mut ids = Vec::new();
                for result in iter {
                    let (id, _) = result?;
                    if let Some(id_str) = id {
                        ids.push(id_str);
                    }
                }
                Ok(ExecutionResult::Insert { ids })
            }
            PlanNode::InsertIndex { .. } => {
                let iter = self.execute_node(node)?;
                let mut matched = 0u64;
                let mut modified = 0u64;
                for result in iter {
                    let (_id, opt_val) = result?;
                    matched += 1;
                    if opt_val.is_some() {
                        modified += 1;
                    }
                }
                Ok(ExecutionResult::Update { matched, modified })
            }
            _ => {
                let iter = self.execute_node(node)?;
                Ok(ExecutionResult::Rows(iter))
            }
        }
    }

    /// Scan all records lazily, yielding (Some(id), Some(RawValue)).
    fn execute_scan(&self) -> Result<RawIter<'c>, DbError> {
        let scan_prefix = encoding::data_scan_prefix("");
        let iter = self.txn.scan_prefix(self.cf, &scan_prefix)?;

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
    fn execute_index_scan(
        &self,
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
        let raw_val: Option<RawBson> = match value {
            Some(v) => Some(RawBson::try_from(v.clone())?),
            None => None,
        };

        let txn = self.txn;
        let cf = self.cf;
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
                                    RawValue::Owned(encoding::coerce_to_stored_type(
                                        v,
                                        &stored_value,
                                    ))
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

    fn execute_node(&self, node: &'c PlanNode) -> Result<RawIter<'c>, DbError> {
        match node {
            PlanNode::Scan { .. } => self.execute_scan(),

            PlanNode::IndexScan {
                column,
                value,
                direction,
                limit,
                complete_groups,
                ..
            } => self.execute_index_scan(
                column,
                value.as_ref(),
                *direction,
                *limit,
                *complete_groups,
            ),

            PlanNode::IndexMerge { logical, lhs, rhs } => {
                // TODO: And path could stream left lazily (only right needs collecting).
                // Or path could use streaming HashSet dedup. Neither emitted by planner yet.
                let left: Vec<(Option<String>, Option<RawBson>)> = self
                    .execute_node(lhs)?
                    .map(|r| r.map(|(id, val)| (id, val.and_then(RawValue::into_raw_bson))))
                    .collect::<Result<_, _>>()?;
                let right: Vec<(Option<String>, Option<RawBson>)> = self
                    .execute_node(rhs)?
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

            PlanNode::ReadRecord { input } => self.execute_read_record(input),

            PlanNode::Filter { predicate, input } => {
                let source = self.execute_node(input)?;

                Ok(Box::new(source.filter_map(move |result| match result {
                    Err(e) => Some(Err(e)),
                    Ok((id, Some(val))) => {
                        let raw = match val.as_document() {
                            Some(r) => r,
                            None => {
                                return Some(Err(DbError::InvalidQuery(
                                    "expected document".into(),
                                )));
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
                let mut source = self.execute_node(input)?;

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
                                    let a_field =
                                        exec::raw_get_path(a_doc, &sort.field).ok().flatten();
                                    let b_field =
                                        exec::raw_get_path(b_doc, &sort.field).ok().flatten();
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
                let mut source = self.execute_node(input)?;

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
                let source = self.execute_node(input)?;

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
                    let val =
                        opt_val.ok_or_else(|| DbError::InvalidQuery("expected value".into()))?;

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
                let source = self.execute_node(input)?;
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

            // ── Mutation nodes ──────────────────────────────────────
            PlanNode::DeleteIndex {
                indexed_fields,
                input,
            } => {
                let source = self.execute_node(input)?;
                let txn = self.txn;
                let cf = self.cf;

                Ok(Box::new(source.map(move |result| {
                    let (id, opt_val) = result?;
                    if let (Some(id_str), Some(val)) = (&id, &opt_val) {
                        if let Some(raw) = val.as_document() {
                            for field in indexed_fields {
                                for value in exec::raw_get_path_values(raw, field)? {
                                    let idx_key = encoding::raw_index_key(field, value, id_str);
                                    txn.delete(cf, &idx_key)?;
                                }
                            }
                            // TTL index
                            if let Some(bson::raw::RawBsonRef::DateTime(dt)) = raw.get("ttl")? {
                                let idx_key = encoding::raw_index_key(
                                    "ttl",
                                    bson::raw::RawBsonRef::DateTime(dt),
                                    id_str,
                                );
                                txn.delete(cf, &idx_key)?;
                            }
                        }
                    }
                    Ok((id, opt_val))
                })))
            }

            PlanNode::Delete { input } => {
                let source = self.execute_node(input)?;
                let txn = self.txn;
                let cf = self.cf;

                Ok(Box::new(source.map(move |result| {
                    let (id, _opt_val) = result?;
                    if let Some(ref id_str) = id {
                        let key = encoding::record_key(id_str);
                        txn.delete(cf, &key)?;
                    }
                    Ok((id, None))
                })))
            }

            PlanNode::Update { update, input } => {
                let source = self.execute_node(input)?;
                let txn = self.txn;
                let cf = self.cf;

                Ok(Box::new(source.map(move |result| {
                    let (id, opt_val) = result?;
                    let id_str = match &id {
                        Some(s) => s,
                        None => return Ok((id, None)),
                    };
                    let old_raw = match &opt_val {
                        Some(val) => match val.as_document() {
                            Some(r) => r,
                            None => return Ok((id, None)),
                        },
                        None => return Ok((id, None)),
                    };

                    let (merged, changed) = exec::raw_merge_doc(old_raw, update)?;
                    if changed {
                        let key = encoding::record_key(id_str);
                        txn.put(cf, &key, merged.as_bytes())?;
                        Ok((id, Some(RawValue::Owned(RawBson::Document(merged)))))
                    } else {
                        Ok((id, None))
                    }
                })))
            }

            PlanNode::Replace { replacement, input } => {
                let source = self.execute_node(input)?;
                let txn = self.txn;
                let cf = self.cf;
                // Pre-serialize the replacement once
                let replacement_bytes =
                    bson::to_vec(replacement).map_err(|e| DbError::Serialization(e.to_string()))?;

                Ok(Box::new(source.map(move |result| {
                    let (id, _opt_val) = result?;
                    if let Some(ref id_str) = id {
                        let key = encoding::record_key(id_str);
                        txn.put(cf, &key, &replacement_bytes)?;
                    }
                    let raw = RawDocumentBuf::from_bytes(replacement_bytes.clone())
                        .map_err(|e| DbError::from(e))?;
                    Ok((id, Some(RawValue::Owned(RawBson::Document(raw)))))
                })))
            }

            PlanNode::InsertIndex {
                indexed_fields,
                input,
            } => {
                let source = self.execute_node(input)?;
                let txn = self.txn;
                let cf = self.cf;

                Ok(Box::new(source.map(move |result| {
                    let (id, opt_val) = result?;
                    if let (Some(id_str), Some(val)) = (&id, &opt_val) {
                        if let Some(raw) = val.as_document() {
                            for field in indexed_fields {
                                for value in exec::raw_get_path_values(raw, field)? {
                                    let idx_key = encoding::raw_index_key(field, value, id_str);
                                    let type_byte = encoding::raw_bson_ref_type_byte(value);
                                    txn.put(cf, &idx_key, &type_byte)?;
                                }
                            }
                            // TTL index
                            if let Some(bson::raw::RawBsonRef::DateTime(dt)) = raw.get("ttl")? {
                                let idx_key = encoding::raw_index_key(
                                    "ttl",
                                    bson::raw::RawBsonRef::DateTime(dt),
                                    id_str,
                                );
                                let type_byte = encoding::raw_bson_ref_type_byte(
                                    bson::raw::RawBsonRef::DateTime(dt),
                                );
                                txn.put(cf, &idx_key, &type_byte)?;
                            }
                        }
                    }
                    Ok((id, opt_val))
                })))
            }

            PlanNode::InsertRecord { input } => {
                let source = self.execute_node(input)?;
                let txn = self.txn;
                let cf = self.cf;

                Ok(Box::new(source.map(move |result| {
                    let (_id, opt_val) = result?;
                    let val = match &opt_val {
                        Some(v) => v,
                        None => {
                            return Err(DbError::InvalidQuery(
                                "InsertRecord requires document".into(),
                            ));
                        }
                    };

                    let raw = val
                        .as_document()
                        .ok_or_else(|| DbError::InvalidQuery("expected document".into()))?;

                    // Extract _id from doc if present, otherwise generate one
                    let id_str = match raw.get("_id")? {
                        Some(RawBsonRef::String(s)) => s.to_string(),
                        Some(other) => format!("{:?}", other),
                        None => uuid::Uuid::new_v4().to_string(),
                    };

                    // Duplicate key check
                    let key = encoding::record_key(&id_str);
                    if txn.get(cf, &key)?.is_some() {
                        return Err(DbError::DuplicateKey(id_str));
                    }

                    // Write doc bytes as-is
                    txn.put(cf, &key, raw.as_bytes())?;

                    Ok((Some(id_str), opt_val))
                })))
            }

            PlanNode::Values { docs } => Ok(Box::new(docs.iter().map(|raw| {
                let id = raw.get("_id").ok().flatten().and_then(|v| match v {
                    RawBsonRef::String(s) => Some(s.to_string()),
                    _ => None,
                });
                Ok((id, Some(RawValue::Borrowed(RawBsonRef::Document(raw)))))
            }))),
        }
    }

    /// ReadRecord: the boundary between ID tier and raw tier.
    fn execute_read_record(&self, input: &'c PlanNode) -> Result<RawIter<'c>, DbError> {
        if matches!(input, PlanNode::Scan { .. }) {
            let source = self.execute_node(input)?;
            return Ok(Box::new(source.filter_map(|result| match result {
                Ok((id, Some(val))) => Some(Ok((id, Some(val)))),
                Ok((_, None)) => None,
                Err(e) => Some(Err(e)),
            })));
        }

        // IndexScan/IndexMerge path: stream IDs directly into record fetches.
        let source = self.execute_node(input)?;
        let txn = self.txn;
        let cf = self.cf;

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

/// Project selected columns from `src` into `dest` using a pre-built key map.
fn raw_project_document<S: AsRef<str>>(
    src: &RawDocument,
    key_map: &HashMap<String, Vec<S>>,
    dest: &mut RawDocumentBuf,
) -> Result<(), DbError> {
    for result in src.iter() {
        let (key, raw_val) = result?;
        if let Some(sub_paths) = key_map.get(key) {
            if sub_paths.is_empty() {
                dest.append_ref(key, raw_val);
            } else {
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
                                    out.push(other.to_raw_bson());
                                }
                            }
                        }
                        dest.append(key, RawBson::Array(out));
                    }
                    _ => {
                        dest.append_ref(key, raw_val);
                    }
                }
            }
        }
    }

    Ok(())
}

/// Build a key_map from a list of paths.
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
