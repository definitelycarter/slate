use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use bson::RawBson;
use bson::raw::RawBsonRef;
use bson::{Bson, RawDocument};
use slate_query::{Filter, FilterGroup, FilterNode, LogicalOp, Operator};
use slate_store::Transaction;

use crate::error::DbError;

// ── TTL helpers ────────────────────────────────────────────────

/// Fast O(1) TTL check on an enveloped record value.
/// Delegates to `encoding::is_record_expired` which reads the fixed-offset
/// prefix — no BSON field walking needed.
#[inline]
pub(crate) fn is_ttl_expired(data: &[u8], now_millis: i64) -> bool {
    crate::encoding::is_record_expired(data, now_millis)
}

/// Fetch a record by key and check TTL. Returns `None` if missing or expired.
pub(crate) fn get_record_if_alive<'a, T: Transaction>(
    txn: &'a T,
    cf: &'a T::Cf,
    key: &[u8],
    now_millis: i64,
) -> Result<Option<Cow<'a, [u8]>>, DbError> {
    match txn.get(cf, key)? {
        Some(bytes) if is_ttl_expired(&bytes, now_millis) => Ok(None),
        other => Ok(other),
    }
}

// ── Multi-key path resolution (for indexing) ───────────────────

/// Resolve an index path that may contain `[]` array traversal segments.
/// Returns all scalar Bson values reachable through the path.
///
/// - `"status"`        → vec![&String("active")]
/// - `"address.city"`  → vec![&String("Austin")]
/// - `"items.[].sku"`  → vec![&String("A1"), &String("B2")]
/// - `"tags.[]"`       → vec![&String("rust"), &String("db")]
pub(crate) fn get_path_values<'a>(doc: &'a bson::Document, path: &str) -> Vec<&'a Bson> {
    let segments: Vec<&str> = path.split('.').collect();
    let mut results = Vec::new();
    collect_from_doc(doc, &segments, 0, &mut results);
    results
}

fn collect_from_doc<'a>(
    doc: &'a bson::Document,
    segments: &[&str],
    idx: usize,
    out: &mut Vec<&'a Bson>,
) {
    if idx >= segments.len() {
        return;
    }

    let seg = segments[idx];
    if seg == "[]" {
        return;
    }

    if let Some(value) = doc.get(seg) {
        collect_from_value(value, segments, idx + 1, out);
    }
}

fn collect_from_value<'a>(value: &'a Bson, segments: &[&str], idx: usize, out: &mut Vec<&'a Bson>) {
    if idx >= segments.len() {
        match value {
            Bson::Array(arr) => {
                for elem in arr {
                    if is_indexable_scalar(elem) {
                        out.push(elem);
                    }
                }
            }
            _ if is_indexable_scalar(value) => {
                out.push(value);
            }
            _ => {}
        }
        return;
    }

    let seg = segments[idx];

    if seg == "[]" {
        if let Bson::Array(arr) = value {
            for elem in arr {
                collect_from_value(elem, segments, idx + 1, out);
            }
        }
    } else if let Bson::Document(d) = value {
        collect_from_doc(d, segments, idx, out);
    }
}

fn is_indexable_scalar(value: &Bson) -> bool {
    matches!(
        value,
        Bson::String(_)
            | Bson::Int32(_)
            | Bson::Int64(_)
            | Bson::Double(_)
            | Bson::Boolean(_)
            | Bson::DateTime(_)
    )
}

// ── Projection ──────────────────────────────────────────────────

/// Apply projection to a document, supporting dot-notation paths.
/// Keeps `_id` always. For dotted paths like "address.city", outputs
/// `{ "address": { "city": <value> } }` — only the requested sub-path.
pub(crate) fn apply_projection(doc: &mut bson::Document, columns: &[String]) {
    use std::collections::{HashMap, HashSet};

    // Separate flat keys from dotted paths grouped by top-level key
    let mut flat_keys: HashSet<&str> = HashSet::new();
    // top_key → vec of remaining sub-paths
    let mut nested: HashMap<&str, Vec<&str>> = HashMap::new();

    for col in columns {
        if let Some(dot_pos) = col.find('.') {
            let top = &col[..dot_pos];
            let rest = &col[dot_pos + 1..];
            nested.entry(top).or_default().push(rest);
        } else {
            flat_keys.insert(col.as_str());
        }
    }

    // Remove top-level keys that aren't needed
    let keys_to_remove: Vec<String> = doc
        .keys()
        .filter(|k| {
            *k != "_id" && !flat_keys.contains(k.as_str()) && !nested.contains_key(k.as_str())
        })
        .cloned()
        .collect();
    for key in keys_to_remove {
        doc.remove(&key);
    }

    // Trim nested documents to only requested sub-paths
    for (top_key, sub_paths) in &nested {
        if let Some(Bson::Document(sub_doc)) = doc.get(*top_key) {
            let sub_columns: Vec<String> = sub_paths.iter().map(|s| s.to_string()).collect();
            let mut trimmed = sub_doc.clone();
            apply_projection(&mut trimmed, &sub_columns);
            trimmed.remove("_id");
            doc.insert(top_key.to_string(), Bson::Document(trimmed));
        }
    }
}

// ── Raw BSON _id extraction ─────────────────────────────────────

/// Extract `_id` from a raw document as a zero-copy `&str` borrow.
/// Returns `Ok(None)` if `_id` is missing or not a string.
pub(crate) fn raw_extract_id(raw: &RawDocument) -> Result<Option<&str>, DbError> {
    let bytes = raw.as_bytes();
    match super::raw_bson::find_field(bytes, "_id") {
        Some(loc) if loc.type_byte == 0x02 => {
            let len = i32::from_le_bytes(
                bytes[loc.value_start..loc.value_start + 4]
                    .try_into()
                    .map_err(|_| DbError::Serialization("truncated _id".into()))?,
            ) as usize;
            std::str::from_utf8(&bytes[loc.value_start + 4..loc.value_start + 4 + len - 1])
                .map(Some)
                .map_err(|_| DbError::Serialization("invalid _id utf8".into()))
        }
        _ => Ok(None),
    }
}

// ── Raw BSON filter matching ────────────────────────────────────
//
// Filter matching operates on raw byte scanning to avoid the overhead of
// `RawDocument::get()` (which constructs RawElement objects and wraps every
// step in Result). Field values are materialized to `RawBsonRef` only when
// needed for comparison.

/// Walk a dot-separated path through raw BSON bytes.
/// Returns None if the path is missing or Null.
pub(crate) fn raw_get_path<'a>(
    raw: &'a RawDocument,
    path: &str,
) -> Result<Option<RawBsonRef<'a>>, DbError> {
    let bytes = raw.as_bytes();
    let loc = match super::raw_bson::find_field_path(bytes, path) {
        Some(loc) => loc,
        None => return Ok(None),
    };
    if loc.type_byte == 0x0A {
        return Ok(None); // Null → None (existing behavior)
    }
    Ok(super::raw_bson::field_to_raw_bson_ref(bytes, &loc))
}

pub(crate) fn raw_matches_group(raw: &RawDocument, group: &FilterGroup) -> Result<bool, DbError> {
    match group.logical {
        LogicalOp::And => {
            for child in &group.children {
                if !raw_matches_node(raw, child)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        LogicalOp::Or => {
            for child in &group.children {
                if raw_matches_node(raw, child)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn raw_matches_node(raw: &RawDocument, node: &FilterNode) -> Result<bool, DbError> {
    match node {
        FilterNode::Condition(filter) => raw_matches_filter(raw, filter),
        FilterNode::Group(group) => raw_matches_group(raw, group),
    }
}

/// Zero-allocation case-insensitive substring search (ASCII only).
fn ascii_icontains(haystack: &str, needle: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    if n.len() > h.len() {
        return false;
    }
    h.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}

fn raw_matches_filter(raw: &RawDocument, filter: &Filter) -> Result<bool, DbError> {
    let field_value = raw_get_path(raw, &filter.field)?;

    match filter.operator {
        Operator::IsNull => match &filter.value {
            Bson::Boolean(true) => Ok(field_value.is_none()),
            Bson::Boolean(false) => Ok(field_value.is_some()),
            _ => Ok(field_value.is_none()),
        },
        Operator::Eq => match field_value {
            Some(RawBsonRef::Array(arr)) => {
                for elem in arr.into_iter().flatten() {
                    if raw_values_eq(&elem, &filter.value) {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Some(v) => Ok(raw_values_eq(&v, &filter.value)),
            None => Ok(false),
        },
        Operator::IContains => match (field_value, &filter.value) {
            (Some(RawBsonRef::String(haystack)), Bson::String(needle)) => {
                Ok(ascii_icontains(haystack, needle))
            }
            _ => Ok(false),
        },
        Operator::IStartsWith => match (field_value, &filter.value) {
            (Some(RawBsonRef::String(haystack)), Bson::String(needle)) => Ok(haystack.len()
                >= needle.len()
                && haystack.as_bytes()[..needle.len()].eq_ignore_ascii_case(needle.as_bytes())),
            _ => Ok(false),
        },
        Operator::IEndsWith => match (field_value, &filter.value) {
            (Some(RawBsonRef::String(haystack)), Bson::String(needle)) => Ok(haystack.len()
                >= needle.len()
                && haystack.as_bytes()[haystack.len() - needle.len()..]
                    .eq_ignore_ascii_case(needle.as_bytes())),
            _ => Ok(false),
        },
        Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte => {
            let predicate: fn(Ordering) -> bool = match filter.operator {
                Operator::Gt => |o| o == Ordering::Greater,
                Operator::Gte => |o| o != Ordering::Less,
                Operator::Lt => |o| o == Ordering::Less,
                Operator::Lte => |o| o != Ordering::Greater,
                _ => unreachable!(),
            };
            match field_value {
                Some(RawBsonRef::Array(arr)) => {
                    for elem in arr.into_iter().flatten() {
                        if raw_compare_values(Some(&elem), &filter.value, predicate)? {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                _ => raw_compare_values(field_value.as_ref(), &filter.value, predicate),
            }
        }
    }
}

pub(crate) fn raw_values_eq(store_val: &RawBsonRef, query_val: &Bson) -> bool {
    match (store_val, query_val) {
        // ── Direct type matches ─────────────────────────────────
        (RawBsonRef::String(a), Bson::String(b)) => *a == b.as_str(),
        (RawBsonRef::Int32(a), Bson::Int32(b)) => *a == *b,
        (RawBsonRef::Int32(a), Bson::Int64(b)) => (*a as i64) == *b,
        (RawBsonRef::Int64(a), Bson::Int64(b)) => *a == *b,
        (RawBsonRef::Int64(a), Bson::Int32(b)) => *a == (*b as i64),
        (RawBsonRef::Double(a), Bson::Double(b)) => *a == *b,
        (RawBsonRef::Double(a), Bson::Int64(b)) => *a == (*b as f64),
        (RawBsonRef::Double(a), Bson::Int32(b)) => *a == (*b as f64),
        (RawBsonRef::Int64(a), Bson::Double(b)) => (*a as f64) == *b,
        (RawBsonRef::Int32(a), Bson::Double(b)) => (*a as f64) == *b,
        (RawBsonRef::Boolean(a), Bson::Boolean(b)) => *a == *b,
        (RawBsonRef::DateTime(a), Bson::DateTime(b)) => {
            a.timestamp_millis() == b.timestamp_millis()
        }

        // ── Cross-type coercion: Bson::String → stored type ─────
        (RawBsonRef::Int32(a), Bson::String(s)) => s.parse::<i64>().is_ok_and(|b| (*a as i64) == b),
        (RawBsonRef::Int64(a), Bson::String(s)) => s.parse::<i64>().is_ok_and(|b| *a == b),
        (RawBsonRef::Double(a), Bson::String(s)) => s.parse::<f64>().is_ok_and(|b| *a == b),
        (RawBsonRef::Boolean(a), Bson::String(s)) => match s.as_str() {
            "true" => *a,
            "false" => !*a,
            _ => false,
        },
        (RawBsonRef::DateTime(a), Bson::String(s)) => bson::DateTime::parse_rfc3339_str(s)
            .is_ok_and(|dt| a.timestamp_millis() == dt.timestamp_millis()),

        // ── Cross-type coercion: Bson::Int → DateTime (epoch seconds) ─
        (RawBsonRef::DateTime(a), Bson::Int64(b)) => a.timestamp_millis() == (*b * 1000),
        (RawBsonRef::DateTime(a), Bson::Int32(b)) => a.timestamp_millis() == (*b as i64 * 1000),

        // ── Incompatible types: silent exclusion ────────────────
        _ => false,
    }
}

/// Apply a pre-parsed Mutation to a raw document.
///
/// Fast path: the raw byte-level mutation engine handles flat-field operators
/// (`$set`, `$unset`, `$inc`, `$push`, `$pop`) by splicing/overwriting bytes
/// directly — no deserialization. Falls back to full `bson::Document` round-trip
/// for dot-paths, `$rename`, `$lpush`, and Document/Array `$set` values.
pub(crate) fn apply_mutation(
    old_raw: &RawDocument,
    mutation: &slate_query::Mutation,
) -> Result<Option<bson::RawDocumentBuf>, DbError> {
    // Fast path: raw byte-level mutation engine
    match super::raw_mutation::raw_apply_mutation(old_raw, mutation)? {
        super::raw_mutation::RawMutationResult::Applied(buf) => return Ok(Some(buf)),
        super::raw_mutation::RawMutationResult::Unchanged => return Ok(None),
        super::raw_mutation::RawMutationResult::Fallback => { /* continue to slow path */ }
    }

    // Slow path: full deserialization
    let mut doc: bson::Document = bson::from_slice(old_raw.as_bytes())?;
    let mut changed = false;

    for fm in &mutation.ops {
        // Determine whether this op should create intermediate sub-documents
        let creates = matches!(
            fm.op,
            slate_query::MutationOp::Set(_)
                | slate_query::MutationOp::Inc(_)
                | slate_query::MutationOp::Push(_)
                | slate_query::MutationOp::LPush(_)
        );

        match &fm.op {
            slate_query::MutationOp::Set(val) => {
                if let Some((parent, leaf)) =
                    super::mutation_ops::resolve_parent_mut(&mut doc, &fm.field, creates)?
                {
                    changed |= super::mutation_ops::op_set(parent, leaf, val)?;
                }
            }
            slate_query::MutationOp::Unset => {
                if let Some((parent, leaf)) =
                    super::mutation_ops::resolve_parent_mut(&mut doc, &fm.field, false)?
                {
                    changed |= super::mutation_ops::op_unset(parent, leaf)?;
                }
            }
            slate_query::MutationOp::Inc(amount) => {
                if let Some((parent, leaf)) =
                    super::mutation_ops::resolve_parent_mut(&mut doc, &fm.field, creates)?
                {
                    changed |= super::mutation_ops::op_inc(parent, leaf, amount)?;
                }
            }
            slate_query::MutationOp::Rename(new_name) => {
                if let Some((parent, leaf)) =
                    super::mutation_ops::resolve_parent_mut(&mut doc, &fm.field, false)?
                {
                    changed |= super::mutation_ops::op_rename(parent, leaf, new_name)?;
                }
            }
            slate_query::MutationOp::Push(val) => {
                if let Some((parent, leaf)) =
                    super::mutation_ops::resolve_parent_mut(&mut doc, &fm.field, creates)?
                {
                    changed |= super::mutation_ops::op_push(parent, leaf, val)?;
                }
            }
            slate_query::MutationOp::LPush(val) => {
                if let Some((parent, leaf)) =
                    super::mutation_ops::resolve_parent_mut(&mut doc, &fm.field, creates)?
                {
                    changed |= super::mutation_ops::op_lpush(parent, leaf, val)?;
                }
            }
            slate_query::MutationOp::Pop => {
                if let Some((parent, leaf)) =
                    super::mutation_ops::resolve_parent_mut(&mut doc, &fm.field, false)?
                {
                    changed |= super::mutation_ops::op_pop(parent, leaf)?;
                }
            }
        }
    }

    if !changed {
        return Ok(None);
    }

    let raw = bson::RawDocumentBuf::from_document(&doc)?;
    Ok(Some(raw))
}

pub(crate) fn raw_compare_values(
    field_value: Option<&RawBsonRef>,
    query_val: &Bson,
    predicate: fn(Ordering) -> bool,
) -> Result<bool, DbError> {
    match field_value {
        Some(store_val) => Ok(match (store_val, query_val) {
            // ── Direct type matches ─────────────────────────────
            (RawBsonRef::Int32(a), Bson::Int32(b)) => predicate(a.cmp(b)),
            (RawBsonRef::Int32(a), Bson::Int64(b)) => predicate((*a as i64).cmp(b)),
            (RawBsonRef::Int64(a), Bson::Int64(b)) => predicate(a.cmp(b)),
            (RawBsonRef::Int64(a), Bson::Int32(b)) => predicate(a.cmp(&(*b as i64))),
            (RawBsonRef::Double(a), Bson::Double(b)) => {
                predicate(a.partial_cmp(b).unwrap_or(Ordering::Equal))
            }
            (RawBsonRef::Double(a), Bson::Int64(b)) => {
                predicate(a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal))
            }
            (RawBsonRef::Double(a), Bson::Int32(b)) => {
                predicate(a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal))
            }
            (RawBsonRef::Int64(a), Bson::Double(b)) => {
                predicate((*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal))
            }
            (RawBsonRef::Int32(a), Bson::Double(b)) => {
                predicate((*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal))
            }
            (RawBsonRef::DateTime(a), Bson::DateTime(b)) => {
                predicate(a.timestamp_millis().cmp(&b.timestamp_millis()))
            }
            (RawBsonRef::String(a), Bson::String(b)) => predicate((*a).cmp(b.as_str())),

            // ── Cross-type coercion: Bson::String → stored type ─
            (RawBsonRef::Int32(a), Bson::String(s)) => s
                .parse::<i64>()
                .is_ok_and(|b| predicate((*a as i64).cmp(&b))),
            (RawBsonRef::Int64(a), Bson::String(s)) => {
                s.parse::<i64>().is_ok_and(|b| predicate(a.cmp(&b)))
            }
            (RawBsonRef::Double(a), Bson::String(s)) => s
                .parse::<f64>()
                .is_ok_and(|b| predicate(a.partial_cmp(&b).unwrap_or(Ordering::Equal))),
            (RawBsonRef::DateTime(a), Bson::String(s)) => bson::DateTime::parse_rfc3339_str(s)
                .is_ok_and(|dt| predicate(a.timestamp_millis().cmp(&dt.timestamp_millis()))),

            // ── Cross-type coercion: Bson::Int → DateTime (epoch seconds) ─
            (RawBsonRef::DateTime(a), Bson::Int64(b)) => {
                predicate(a.timestamp_millis().cmp(&(*b * 1000)))
            }
            (RawBsonRef::DateTime(a), Bson::Int32(b)) => {
                predicate(a.timestamp_millis().cmp(&(*b as i64 * 1000)))
            }

            // ── Incompatible types: silent exclusion ────────────
            _ => false,
        }),
        None => Ok(false),
    }
}

// ── Raw BSON value comparison for sorting ───────────────────────

pub(crate) fn raw_compare_field_values(a: Option<RawBsonRef>, b: Option<RawBsonRef>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (Some(RawBsonRef::Null), None)
        | (None, Some(RawBsonRef::Null))
        | (Some(RawBsonRef::Null), Some(RawBsonRef::Null)) => Ordering::Equal,
        (None, Some(_)) | (Some(RawBsonRef::Null), Some(_)) => Ordering::Less,
        (Some(_), None) | (Some(_), Some(RawBsonRef::Null)) => Ordering::Greater,
        (Some(a), Some(b)) => raw_compare_two_values(&a, &b),
    }
}

fn raw_compare_two_values(a: &RawBsonRef, b: &RawBsonRef) -> Ordering {
    match (a, b) {
        (RawBsonRef::String(a), RawBsonRef::String(b)) => a.cmp(b),
        (RawBsonRef::Int32(a), RawBsonRef::Int32(b)) => a.cmp(b),
        (RawBsonRef::Int64(a), RawBsonRef::Int64(b)) => a.cmp(b),
        (RawBsonRef::Int32(a), RawBsonRef::Int64(b)) => (*a as i64).cmp(b),
        (RawBsonRef::Int64(a), RawBsonRef::Int32(b)) => a.cmp(&(*b as i64)),
        (RawBsonRef::Double(a), RawBsonRef::Double(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (RawBsonRef::Double(a), RawBsonRef::Int64(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        (RawBsonRef::Double(a), RawBsonRef::Int32(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        (RawBsonRef::Int64(a), RawBsonRef::Double(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (RawBsonRef::Int32(a), RawBsonRef::Double(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (RawBsonRef::Boolean(a), RawBsonRef::Boolean(b)) => a.cmp(b),
        (RawBsonRef::DateTime(a), RawBsonRef::DateTime(b)) => {
            a.timestamp_millis().cmp(&b.timestamp_millis())
        }
        _ => Ordering::Equal,
    }
}

// ── Distinct helpers ────────────────────────────────────────────

pub(crate) fn hash_raw(raw_ref: RawBsonRef<'_>) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    match raw_ref {
        RawBsonRef::String(s) => {
            0u8.hash(&mut hasher);
            s.hash(&mut hasher);
        }
        RawBsonRef::Int32(i) => {
            1u8.hash(&mut hasher);
            i.hash(&mut hasher);
        }
        RawBsonRef::Int64(i) => {
            2u8.hash(&mut hasher);
            i.hash(&mut hasher);
        }
        RawBsonRef::Double(f) => {
            3u8.hash(&mut hasher);
            f.to_bits().hash(&mut hasher);
        }
        RawBsonRef::Boolean(b) => {
            4u8.hash(&mut hasher);
            b.hash(&mut hasher);
        }
        RawBsonRef::DateTime(dt) => {
            5u8.hash(&mut hasher);
            dt.timestamp_millis().hash(&mut hasher);
        }
        RawBsonRef::ObjectId(oid) => {
            6u8.hash(&mut hasher);
            oid.bytes().hash(&mut hasher);
        }
        RawBsonRef::Document(d) => {
            7u8.hash(&mut hasher);
            d.as_bytes().hash(&mut hasher);
        }
        RawBsonRef::Array(a) => {
            8u8.hash(&mut hasher);
            a.as_bytes().hash(&mut hasher);
        }
        _ => {
            255u8.hash(&mut hasher);
        }
    }
    hasher.finish()
}

pub(crate) fn push_raw(buf: &mut bson::RawArrayBuf, raw_ref: RawBsonRef<'_>) {
    match raw_ref {
        RawBsonRef::String(s) => buf.push(s),
        RawBsonRef::Int32(i) => buf.push(i),
        RawBsonRef::Int64(i) => buf.push(i),
        RawBsonRef::Double(f) => buf.push(f),
        RawBsonRef::Boolean(b) => buf.push(b),
        RawBsonRef::DateTime(dt) => buf.push(dt),
        RawBsonRef::ObjectId(oid) => buf.push(oid),
        RawBsonRef::Document(d) => buf.push(d.to_raw_document_buf()),
        RawBsonRef::Array(a) => buf.push(a.to_raw_array_buf()),
        _ => {}
    }
}

pub(crate) fn try_insert(
    seen: &mut HashSet<u64>,
    buf: &mut bson::RawArrayBuf,
    raw_ref: RawBsonRef<'_>,
) {
    let h = hash_raw(raw_ref);
    if seen.insert(h) {
        push_raw(buf, raw_ref);
    }
}

pub(crate) fn to_raw_bson(raw_ref: RawBsonRef<'_>) -> Option<RawBson> {
    Some(match raw_ref {
        RawBsonRef::String(s) => RawBson::String(s.to_string()),
        RawBsonRef::Int32(i) => RawBson::Int32(i),
        RawBsonRef::Int64(i) => RawBson::Int64(i),
        RawBsonRef::Double(f) => RawBson::Double(f),
        RawBsonRef::Boolean(b) => RawBson::Boolean(b),
        RawBsonRef::DateTime(dt) => RawBson::DateTime(dt),
        RawBsonRef::ObjectId(oid) => RawBson::ObjectId(oid),
        RawBsonRef::Document(d) => RawBson::Document(d.to_raw_document_buf()),
        RawBsonRef::Array(a) => RawBson::Array(a.to_raw_array_buf()),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{RawDocumentBuf, doc};

    #[test]
    fn get_path_values_scalar() {
        let doc = doc! { "status": "active" };
        let vals = get_path_values(&doc, "status");
        assert_eq!(vals, vec![&Bson::String("active".into())]);
    }

    #[test]
    fn get_path_values_nested_scalar() {
        let doc = doc! { "address": { "city": "Austin" } };
        let vals = get_path_values(&doc, "address.city");
        assert_eq!(vals, vec![&Bson::String("Austin".into())]);
    }

    #[test]
    fn get_path_values_array_of_scalars() {
        let doc = doc! { "tags": ["rust", "db", "fast"] };
        let vals = get_path_values(&doc, "tags.[]");
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0], &Bson::String("rust".into()));
        assert_eq!(vals[1], &Bson::String("db".into()));
        assert_eq!(vals[2], &Bson::String("fast".into()));
    }

    #[test]
    fn get_path_values_array_of_objects() {
        let doc = doc! { "items": [{ "sku": "A1", "qty": 2 }, { "sku": "B2", "qty": 1 }] };
        let vals = get_path_values(&doc, "items.[].sku");
        assert_eq!(vals.len(), 2);
        assert_eq!(vals[0], &Bson::String("A1".into()));
        assert_eq!(vals[1], &Bson::String("B2".into()));
    }

    #[test]
    fn get_path_values_missing_field() {
        let doc = doc! { "name": "test" };
        let vals = get_path_values(&doc, "missing");
        assert!(vals.is_empty());
    }

    #[test]
    fn get_path_values_missing_nested() {
        let doc = doc! { "name": "test" };
        let vals = get_path_values(&doc, "missing.[].sku");
        assert!(vals.is_empty());
    }

    #[test]
    fn get_path_values_not_array() {
        let doc = doc! { "name": "test" };
        let vals = get_path_values(&doc, "name.[]");
        assert!(vals.is_empty());
    }

    #[test]
    fn get_path_values_nested_array() {
        let doc = doc! {
            "order": {
                "items": [{ "sku": "X1" }, { "sku": "X2" }]
            }
        };
        let vals = get_path_values(&doc, "order.items.[].sku");
        assert_eq!(vals.len(), 2);
        assert_eq!(vals[0], &Bson::String("X1".into()));
        assert_eq!(vals[1], &Bson::String("X2".into()));
    }

    #[test]
    fn get_path_values_skips_non_scalar() {
        let doc = doc! { "items": [{ "meta": { "a": 1 } }] };
        let vals = get_path_values(&doc, "items.[].meta");
        assert!(vals.is_empty());
    }

    #[test]
    fn get_path_values_null_skipped() {
        let doc = doc! { "status": bson::Bson::Null };
        let vals = get_path_values(&doc, "status");
        assert!(vals.is_empty());
    }

    #[test]
    fn get_path_values_array_scalars_direct() {
        let doc = doc! { "tags": ["rust", "db", "fast"] };
        let vals = get_path_values(&doc, "tags");
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0], &Bson::String("rust".into()));
        assert_eq!(vals[1], &Bson::String("db".into()));
        assert_eq!(vals[2], &Bson::String("fast".into()));
    }

    #[test]
    fn get_path_values_array_mixed_types() {
        let doc = doc! { "vals": [1_i64, "hello", true, bson::Bson::Null] };
        let vals = get_path_values(&doc, "vals");
        // Null is not indexable, so only 3 values
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0], &Bson::Int64(1));
        assert_eq!(vals[1], &Bson::String("hello".into()));
        assert_eq!(vals[2], &Bson::Boolean(true));
    }

    #[test]
    fn get_path_values_empty_array() {
        let doc = doc! { "tags": bson::Bson::Array(vec![]) };
        let vals = get_path_values(&doc, "tags");
        assert!(vals.is_empty());
    }

    // ── Raw BSON tests ──────────────────────────────────────────

    fn make_raw(doc: &bson::Document) -> RawDocumentBuf {
        let bytes = bson::to_vec(doc).unwrap();
        RawDocumentBuf::from_bytes(bytes).unwrap()
    }

    #[test]
    fn raw_get_path_simple() {
        let raw = make_raw(&doc! { "status": "active", "count": 42 });
        let val = raw_get_path(&raw, "status").unwrap();
        assert_eq!(val, Some(RawBsonRef::String("active")));
    }

    #[test]
    fn raw_get_path_dotted() {
        let raw = make_raw(&doc! { "address": { "city": "Austin" } });
        let val = raw_get_path(&raw, "address.city").unwrap();
        assert_eq!(val, Some(RawBsonRef::String("Austin")));
    }

    #[test]
    fn raw_get_path_missing() {
        let raw = make_raw(&doc! { "name": "test" });
        let val = raw_get_path(&raw, "missing").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn raw_get_path_null() {
        let raw = make_raw(&doc! { "status": bson::Bson::Null });
        let val = raw_get_path(&raw, "status").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn raw_get_path_id_returns_value() {
        let raw = make_raw(&doc! { "_id": "abc123", "name": "test" });
        let val = raw_get_path(&raw, "_id").unwrap();
        assert_eq!(val, Some(RawBsonRef::String("abc123")));
    }

    #[test]
    fn raw_matches_group_and() {
        let raw = make_raw(&doc! { "status": "active", "score": 80 });
        let group = FilterGroup {
            logical: LogicalOp::And,
            children: vec![
                FilterNode::Condition(Filter {
                    field: "status".into(),
                    operator: Operator::Eq,
                    value: Bson::String("active".into()),
                }),
                FilterNode::Condition(Filter {
                    field: "score".into(),
                    operator: Operator::Gt,
                    value: Bson::Int64(50),
                }),
            ],
        };
        assert!(raw_matches_group(&raw, &group).unwrap());
    }

    #[test]
    fn raw_matches_group_and_short_circuit() {
        let raw = make_raw(&doc! { "status": "rejected", "score": 80 });
        let group = FilterGroup {
            logical: LogicalOp::And,
            children: vec![
                FilterNode::Condition(Filter {
                    field: "status".into(),
                    operator: Operator::Eq,
                    value: Bson::String("active".into()),
                }),
                FilterNode::Condition(Filter {
                    field: "score".into(),
                    operator: Operator::Gt,
                    value: Bson::Int64(50),
                }),
            ],
        };
        assert!(!raw_matches_group(&raw, &group).unwrap());
    }

    #[test]
    fn raw_matches_group_or() {
        let raw = make_raw(&doc! { "status": "rejected", "score": 80 });
        let group = FilterGroup {
            logical: LogicalOp::Or,
            children: vec![
                FilterNode::Condition(Filter {
                    field: "status".into(),
                    operator: Operator::Eq,
                    value: Bson::String("active".into()),
                }),
                FilterNode::Condition(Filter {
                    field: "score".into(),
                    operator: Operator::Gt,
                    value: Bson::Int64(50),
                }),
            ],
        };
        assert!(raw_matches_group(&raw, &group).unwrap());
    }

    #[test]
    fn raw_matches_filter_id_eq() {
        let raw = make_raw(&doc! { "_id": "abc123", "name": "test" });
        let filter = Filter {
            field: "_id".into(),
            operator: Operator::Eq,
            value: Bson::String("abc123".into()),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());

        let filter_miss = Filter {
            field: "_id".into(),
            operator: Operator::Eq,
            value: Bson::String("xyz".into()),
        };
        assert!(!raw_matches_filter(&raw, &filter_miss).unwrap());
    }

    #[test]
    fn raw_matches_filter_is_null() {
        let raw = make_raw(&doc! { "name": "test" });
        let filter = Filter {
            field: "missing".into(),
            operator: Operator::IsNull,
            value: Bson::Boolean(true),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());

        let filter_not_null = Filter {
            field: "name".into(),
            operator: Operator::IsNull,
            value: Bson::Boolean(true),
        };
        assert!(!raw_matches_filter(&raw, &filter_not_null).unwrap());
    }

    #[test]
    fn raw_matches_filter_icontains() {
        let raw = make_raw(&doc! { "name": "Hello World" });
        let filter = Filter {
            field: "name".into(),
            operator: Operator::IContains,
            value: Bson::String("hello".into()),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn raw_compare_field_values_basic() {
        use std::cmp::Ordering;
        assert_eq!(
            raw_compare_field_values(Some(RawBsonRef::Int32(10)), Some(RawBsonRef::Int32(20))),
            Ordering::Less
        );
        assert_eq!(
            raw_compare_field_values(Some(RawBsonRef::String("b")), Some(RawBsonRef::String("a"))),
            Ordering::Greater
        );
        assert_eq!(raw_compare_field_values(None, None), Ordering::Equal);
        assert_eq!(
            raw_compare_field_values(None, Some(RawBsonRef::Int32(1))),
            Ordering::Less
        );
    }

    #[test]
    fn raw_compare_field_values_cross_int() {
        use std::cmp::Ordering;
        assert_eq!(
            raw_compare_field_values(Some(RawBsonRef::Int32(10)), Some(RawBsonRef::Int64(10))),
            Ordering::Equal
        );
    }

    // ── Cross-type: Double ↔ Int field comparison ─────────────────

    #[test]
    fn raw_compare_field_values_double_vs_int64() {
        use std::cmp::Ordering;
        assert_eq!(
            raw_compare_field_values(Some(RawBsonRef::Double(10.0)), Some(RawBsonRef::Int64(10))),
            Ordering::Equal
        );
        assert_eq!(
            raw_compare_field_values(Some(RawBsonRef::Double(10.5)), Some(RawBsonRef::Int64(10))),
            Ordering::Greater
        );
        assert_eq!(
            raw_compare_field_values(Some(RawBsonRef::Int64(10)), Some(RawBsonRef::Double(10.5))),
            Ordering::Less
        );
    }

    #[test]
    fn raw_compare_field_values_double_vs_int32() {
        use std::cmp::Ordering;
        assert_eq!(
            raw_compare_field_values(Some(RawBsonRef::Double(5.0)), Some(RawBsonRef::Int32(5))),
            Ordering::Equal
        );
        assert_eq!(
            raw_compare_field_values(Some(RawBsonRef::Int32(5)), Some(RawBsonRef::Double(4.9))),
            Ordering::Greater
        );
    }

    // ── Cross-type coercion: Double ↔ Int filter eq ─────────────

    #[test]
    fn coerce_double_to_int64_eq() {
        let raw = make_raw(&doc! { "score": 50.0_f64 });
        let filter = Filter {
            field: "score".into(),
            operator: Operator::Eq,
            value: Bson::Int64(50),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn coerce_int64_to_double_eq() {
        let raw = make_raw(&doc! { "score": 50_i64 });
        let filter = Filter {
            field: "score".into(),
            operator: Operator::Eq,
            value: Bson::Double(50.0),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn coerce_double_to_int32_eq() {
        let raw = make_raw(&doc! { "price": 10.0_f64 });
        let filter = Filter {
            field: "price".into(),
            operator: Operator::Eq,
            value: Bson::Int32(10),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn coerce_int32_to_double_eq() {
        let raw = make_raw(&doc! { "price": 10_i32 });
        let filter = Filter {
            field: "price".into(),
            operator: Operator::Eq,
            value: Bson::Double(10.0),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    // ── Cross-type coercion: Double ↔ Int filter comparison ─────

    #[test]
    fn coerce_double_to_int64_gt() {
        let raw = make_raw(&doc! { "score": 50.5_f64 });
        let filter = Filter {
            field: "score".into(),
            operator: Operator::Gt,
            value: Bson::Int64(50),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn coerce_int32_to_double_lt() {
        let raw = make_raw(&doc! { "score": 9_i32 });
        let filter = Filter {
            field: "score".into(),
            operator: Operator::Lt,
            value: Bson::Double(9.5),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    // ── Cross-type coercion: eq ─────────────────────────────────

    #[test]
    fn coerce_string_to_int32_eq() {
        let raw = make_raw(&doc! { "score": 50_i32 });
        let filter = Filter {
            field: "score".into(),
            operator: Operator::Eq,
            value: Bson::String("50".into()),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn coerce_string_to_int64_eq() {
        let raw = make_raw(&doc! { "score": 50_i64 });
        let filter = Filter {
            field: "score".into(),
            operator: Operator::Eq,
            value: Bson::String("50".into()),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn coerce_string_to_double_eq() {
        let raw = make_raw(&doc! { "price": 19.99 });
        let filter = Filter {
            field: "price".into(),
            operator: Operator::Eq,
            value: Bson::String("19.99".into()),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn coerce_string_to_bool_eq() {
        let raw = make_raw(&doc! { "active": true });
        let filter = Filter {
            field: "active".into(),
            operator: Operator::Eq,
            value: Bson::String("true".into()),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());

        let filter_false = Filter {
            field: "active".into(),
            operator: Operator::Eq,
            value: Bson::String("false".into()),
        };
        assert!(!raw_matches_filter(&raw, &filter_false).unwrap());
    }

    #[test]
    fn coerce_string_to_datetime_eq() {
        let dt = bson::DateTime::parse_rfc3339_str("2024-01-15T00:00:00Z").unwrap();
        let raw = make_raw(&doc! { "created_at": dt });
        let filter = Filter {
            field: "created_at".into(),
            operator: Operator::Eq,
            value: Bson::String("2024-01-15T00:00:00Z".into()),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn coerce_int_to_datetime_eq() {
        let dt = bson::DateTime::from_millis(1705276800 * 1000);
        let raw = make_raw(&doc! { "created_at": dt });
        let filter = Filter {
            field: "created_at".into(),
            operator: Operator::Eq,
            value: Bson::Int64(1705276800),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    // ── Cross-type coercion: comparisons ────────────────────────

    #[test]
    fn coerce_string_to_int_gt() {
        let raw = make_raw(&doc! { "score": 80_i64 });
        let filter = Filter {
            field: "score".into(),
            operator: Operator::Gt,
            value: Bson::String("50".into()),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());

        let filter_not = Filter {
            field: "score".into(),
            operator: Operator::Gt,
            value: Bson::String("100".into()),
        };
        assert!(!raw_matches_filter(&raw, &filter_not).unwrap());
    }

    #[test]
    fn coerce_string_to_double_lte() {
        let raw = make_raw(&doc! { "price": 19.99 });
        let filter = Filter {
            field: "price".into(),
            operator: Operator::Lte,
            value: Bson::String("19.99".into()),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());

        let filter_not = Filter {
            field: "price".into(),
            operator: Operator::Lte,
            value: Bson::String("19.98".into()),
        };
        assert!(!raw_matches_filter(&raw, &filter_not).unwrap());
    }

    #[test]
    fn coerce_string_to_datetime_gte() {
        let dt = bson::DateTime::parse_rfc3339_str("2024-06-15T12:00:00Z").unwrap();
        let raw = make_raw(&doc! { "updated_at": dt });
        let filter = Filter {
            field: "updated_at".into(),
            operator: Operator::Gte,
            value: Bson::String("2024-01-01T00:00:00Z".into()),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());

        let filter_not = Filter {
            field: "updated_at".into(),
            operator: Operator::Gte,
            value: Bson::String("2025-01-01T00:00:00Z".into()),
        };
        assert!(!raw_matches_filter(&raw, &filter_not).unwrap());
    }

    #[test]
    fn coerce_int_to_datetime_lt() {
        let dt = bson::DateTime::from_millis(1705276800 * 1000);
        let raw = make_raw(&doc! { "created_at": dt });
        // epoch seconds after the stored date
        let filter = Filter {
            field: "created_at".into(),
            operator: Operator::Lt,
            value: Bson::Int64(1705276800 + 86400),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    // ── Cross-type coercion: full operator coverage ────────────

    #[test]
    fn coerce_string_to_int_gte() {
        let raw = make_raw(&doc! { "score": 80_i64 });
        let f = |op| Filter {
            field: "score".into(),
            operator: op,
            value: Bson::String("80".into()),
        };
        assert!(raw_matches_filter(&raw, &f(Operator::Gte)).unwrap());
        let f_above = Filter {
            field: "score".into(),
            operator: Operator::Gte,
            value: Bson::String("81".into()),
        };
        assert!(!raw_matches_filter(&raw, &f_above).unwrap());
    }

    #[test]
    fn coerce_string_to_int_lt() {
        let raw = make_raw(&doc! { "score": 80_i64 });
        let f = Filter {
            field: "score".into(),
            operator: Operator::Lt,
            value: Bson::String("100".into()),
        };
        assert!(raw_matches_filter(&raw, &f).unwrap());
        let f_not = Filter {
            field: "score".into(),
            operator: Operator::Lt,
            value: Bson::String("80".into()),
        };
        assert!(!raw_matches_filter(&raw, &f_not).unwrap());
    }

    #[test]
    fn coerce_string_to_int_lte() {
        let raw = make_raw(&doc! { "score": 80_i64 });
        let f = Filter {
            field: "score".into(),
            operator: Operator::Lte,
            value: Bson::String("80".into()),
        };
        assert!(raw_matches_filter(&raw, &f).unwrap());
        let f_not = Filter {
            field: "score".into(),
            operator: Operator::Lte,
            value: Bson::String("79".into()),
        };
        assert!(!raw_matches_filter(&raw, &f_not).unwrap());
    }

    #[test]
    fn coerce_string_to_double_gt() {
        let raw = make_raw(&doc! { "price": 19.99 });
        let f = Filter {
            field: "price".into(),
            operator: Operator::Gt,
            value: Bson::String("19.98".into()),
        };
        assert!(raw_matches_filter(&raw, &f).unwrap());
        let f_not = Filter {
            field: "price".into(),
            operator: Operator::Gt,
            value: Bson::String("19.99".into()),
        };
        assert!(!raw_matches_filter(&raw, &f_not).unwrap());
    }

    #[test]
    fn coerce_string_to_double_gte() {
        let raw = make_raw(&doc! { "price": 19.99 });
        let f = Filter {
            field: "price".into(),
            operator: Operator::Gte,
            value: Bson::String("19.99".into()),
        };
        assert!(raw_matches_filter(&raw, &f).unwrap());
        let f_not = Filter {
            field: "price".into(),
            operator: Operator::Gte,
            value: Bson::String("20.00".into()),
        };
        assert!(!raw_matches_filter(&raw, &f_not).unwrap());
    }

    #[test]
    fn coerce_string_to_double_lt() {
        let raw = make_raw(&doc! { "price": 19.99 });
        let f = Filter {
            field: "price".into(),
            operator: Operator::Lt,
            value: Bson::String("20.00".into()),
        };
        assert!(raw_matches_filter(&raw, &f).unwrap());
        let f_not = Filter {
            field: "price".into(),
            operator: Operator::Lt,
            value: Bson::String("19.99".into()),
        };
        assert!(!raw_matches_filter(&raw, &f_not).unwrap());
    }

    #[test]
    fn coerce_string_to_datetime_gt() {
        let dt = bson::DateTime::parse_rfc3339_str("2024-06-15T12:00:00Z").unwrap();
        let raw = make_raw(&doc! { "ts": dt });
        let f = Filter {
            field: "ts".into(),
            operator: Operator::Gt,
            value: Bson::String("2024-06-15T11:00:00Z".into()),
        };
        assert!(raw_matches_filter(&raw, &f).unwrap());
        let f_not = Filter {
            field: "ts".into(),
            operator: Operator::Gt,
            value: Bson::String("2024-06-15T12:00:00Z".into()),
        };
        assert!(!raw_matches_filter(&raw, &f_not).unwrap());
    }

    #[test]
    fn coerce_string_to_datetime_lt() {
        let dt = bson::DateTime::parse_rfc3339_str("2024-06-15T12:00:00Z").unwrap();
        let raw = make_raw(&doc! { "ts": dt });
        let f = Filter {
            field: "ts".into(),
            operator: Operator::Lt,
            value: Bson::String("2024-06-15T13:00:00Z".into()),
        };
        assert!(raw_matches_filter(&raw, &f).unwrap());
        let f_not = Filter {
            field: "ts".into(),
            operator: Operator::Lt,
            value: Bson::String("2024-06-15T12:00:00Z".into()),
        };
        assert!(!raw_matches_filter(&raw, &f_not).unwrap());
    }

    #[test]
    fn coerce_string_to_datetime_lte() {
        let dt = bson::DateTime::parse_rfc3339_str("2024-06-15T12:00:00Z").unwrap();
        let raw = make_raw(&doc! { "ts": dt });
        let f = Filter {
            field: "ts".into(),
            operator: Operator::Lte,
            value: Bson::String("2024-06-15T12:00:00Z".into()),
        };
        assert!(raw_matches_filter(&raw, &f).unwrap());
        let f_not = Filter {
            field: "ts".into(),
            operator: Operator::Lte,
            value: Bson::String("2024-06-15T11:00:00Z".into()),
        };
        assert!(!raw_matches_filter(&raw, &f_not).unwrap());
    }

    #[test]
    fn coerce_int_to_datetime_gt() {
        let dt = bson::DateTime::from_millis(1705276800 * 1000);
        let raw = make_raw(&doc! { "ts": dt });
        let f = Filter {
            field: "ts".into(),
            operator: Operator::Gt,
            value: Bson::Int64(1705276800 - 1),
        };
        assert!(raw_matches_filter(&raw, &f).unwrap());
        let f_not = Filter {
            field: "ts".into(),
            operator: Operator::Gt,
            value: Bson::Int64(1705276800),
        };
        assert!(!raw_matches_filter(&raw, &f_not).unwrap());
    }

    #[test]
    fn coerce_int_to_datetime_gte() {
        let dt = bson::DateTime::from_millis(1705276800 * 1000);
        let raw = make_raw(&doc! { "ts": dt });
        let f = Filter {
            field: "ts".into(),
            operator: Operator::Gte,
            value: Bson::Int64(1705276800),
        };
        assert!(raw_matches_filter(&raw, &f).unwrap());
        let f_not = Filter {
            field: "ts".into(),
            operator: Operator::Gte,
            value: Bson::Int64(1705276800 + 1),
        };
        assert!(!raw_matches_filter(&raw, &f_not).unwrap());
    }

    #[test]
    fn coerce_int_to_datetime_lte() {
        let dt = bson::DateTime::from_millis(1705276800 * 1000);
        let raw = make_raw(&doc! { "ts": dt });
        let f = Filter {
            field: "ts".into(),
            operator: Operator::Lte,
            value: Bson::Int64(1705276800),
        };
        assert!(raw_matches_filter(&raw, &f).unwrap());
        let f_not = Filter {
            field: "ts".into(),
            operator: Operator::Lte,
            value: Bson::Int64(1705276800 - 1),
        };
        assert!(!raw_matches_filter(&raw, &f_not).unwrap());
    }

    // ── Silent exclusion on failed coercion ─────────────────────

    #[test]
    fn coerce_invalid_string_to_int_excludes() {
        let raw = make_raw(&doc! { "score": 50_i32 });
        let filter = Filter {
            field: "score".into(),
            operator: Operator::Eq,
            value: Bson::String("not_a_number".into()),
        };
        assert!(!raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn coerce_invalid_string_to_double_excludes() {
        let raw = make_raw(&doc! { "price": 19.99 });
        let filter = Filter {
            field: "price".into(),
            operator: Operator::Gt,
            value: Bson::String("abc".into()),
        };
        assert!(!raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn coerce_invalid_string_to_bool_excludes() {
        let raw = make_raw(&doc! { "active": true });
        let filter = Filter {
            field: "active".into(),
            operator: Operator::Eq,
            value: Bson::String("yes".into()),
        };
        assert!(!raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn coerce_invalid_string_to_datetime_excludes() {
        let dt = bson::DateTime::parse_rfc3339_str("2024-01-15T00:00:00Z").unwrap();
        let raw = make_raw(&doc! { "created_at": dt });
        let filter = Filter {
            field: "created_at".into(),
            operator: Operator::Eq,
            value: Bson::String("not-a-date".into()),
        };
        assert!(!raw_matches_filter(&raw, &filter).unwrap());
    }

    // ── Incompatible types: silent exclusion ────────────────────

    #[test]
    fn incompatible_bool_vs_int_excludes() {
        let raw = make_raw(&doc! { "flag": true });
        let filter = Filter {
            field: "flag".into(),
            operator: Operator::Eq,
            value: Bson::Int64(1),
        };
        assert!(!raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn incompatible_string_vs_float_excludes() {
        let raw = make_raw(&doc! { "name": "hello" });
        let filter = Filter {
            field: "name".into(),
            operator: Operator::Eq,
            value: Bson::Double(1.0),
        };
        assert!(!raw_matches_filter(&raw, &filter).unwrap());
    }

    // ── Array element matching ──────────────────────────────────

    #[test]
    fn array_element_eq() {
        let raw = make_raw(&doc! { "tags": ["rust", "db", "bson"] });
        let filter = Filter {
            field: "tags".into(),
            operator: Operator::Eq,
            value: Bson::String("db".into()),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn array_element_eq_no_match() {
        let raw = make_raw(&doc! { "tags": ["rust", "db"] });
        let filter = Filter {
            field: "tags".into(),
            operator: Operator::Eq,
            value: Bson::String("python".into()),
        };
        assert!(!raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn array_element_eq_numeric() {
        let raw = make_raw(&doc! { "scores": [10, 20, 30] });
        let filter = Filter {
            field: "scores".into(),
            operator: Operator::Eq,
            value: Bson::Int32(20),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn array_element_gt() {
        let raw = make_raw(&doc! { "scores": [10, 20, 30] });
        let filter = Filter {
            field: "scores".into(),
            operator: Operator::Gt,
            value: Bson::Int32(25),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn array_element_gt_no_match() {
        let raw = make_raw(&doc! { "scores": [10, 20, 30] });
        let filter = Filter {
            field: "scores".into(),
            operator: Operator::Gt,
            value: Bson::Int32(30),
        };
        assert!(!raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn array_element_lte() {
        let raw = make_raw(&doc! { "scores": [10, 20, 30] });
        let filter = Filter {
            field: "scores".into(),
            operator: Operator::Lte,
            value: Bson::Int32(15),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn array_element_cross_type() {
        let raw = make_raw(&doc! { "values": [1.5, 2.5, 3.5] });
        let filter = Filter {
            field: "values".into(),
            operator: Operator::Eq,
            value: Bson::String("2.5".into()),
        };
        assert!(raw_matches_filter(&raw, &filter).unwrap());
    }

    #[test]
    fn array_empty_no_match() {
        let raw = make_raw(&doc! { "tags": bson::Bson::Array(vec![]) });
        let filter = Filter {
            field: "tags".into(),
            operator: Operator::Eq,
            value: Bson::String("anything".into()),
        };
        assert!(!raw_matches_filter(&raw, &filter).unwrap());
    }

    // ── is_ttl_expired ──────────────────────────────────────────

    #[test]
    fn ttl_expired_returns_true() {
        let past = bson::DateTime::from_millis(1_000);
        let bson_bytes = bson::to_vec(&doc! { "_id": "1", "ttl": past }).unwrap();
        let envelope = crate::encoding::encode_record(&bson_bytes);
        let now = bson::DateTime::now().timestamp_millis();
        assert!(is_ttl_expired(&envelope, now));
    }

    #[test]
    fn ttl_future_returns_false() {
        let future = bson::DateTime::from_millis(i64::MAX - 1);
        let bson_bytes = bson::to_vec(&doc! { "_id": "1", "ttl": future }).unwrap();
        let envelope = crate::encoding::encode_record(&bson_bytes);
        let now = bson::DateTime::now().timestamp_millis();
        assert!(!is_ttl_expired(&envelope, now));
    }

    #[test]
    fn ttl_missing_returns_false() {
        let bson_bytes = bson::to_vec(&doc! { "_id": "1", "name": "Alice" }).unwrap();
        let envelope = crate::encoding::encode_record(&bson_bytes);
        let now = bson::DateTime::now().timestamp_millis();
        assert!(!is_ttl_expired(&envelope, now));
    }

    #[test]
    fn ttl_non_datetime_returns_false() {
        let bson_bytes = bson::to_vec(&doc! { "_id": "1", "ttl": "not-a-date" }).unwrap();
        let envelope = crate::encoding::encode_record(&bson_bytes);
        let now = bson::DateTime::now().timestamp_millis();
        assert!(!is_ttl_expired(&envelope, now));
    }

    #[test]
    fn ttl_equal_to_now_returns_false() {
        let now_millis = 1_700_000_000_000_i64;
        let dt = bson::DateTime::from_millis(now_millis);
        let bson_bytes = bson::to_vec(&doc! { "_id": "1", "ttl": dt }).unwrap();
        let envelope = crate::encoding::encode_record(&bson_bytes);
        // ttl == now → not expired (< is strict)
        assert!(!is_ttl_expired(&envelope, now_millis));
    }

    // ── raw_extract_id ──────────────────────────────────────────

    #[test]
    fn raw_extract_id_string() {
        let raw = make_raw(&doc! { "_id": "doc123", "name": "test" });
        let id = raw_extract_id(&raw).unwrap();
        assert_eq!(id, Some("doc123"));
    }

    #[test]
    fn raw_extract_id_missing() {
        let raw = make_raw(&doc! { "name": "test" });
        let id = raw_extract_id(&raw).unwrap();
        assert_eq!(id, None);
    }

    #[test]
    fn raw_extract_id_non_string() {
        // ObjectId _id should return None (we only extract string _ids)
        let raw = make_raw(&doc! { "_id": bson::oid::ObjectId::new(), "name": "test" });
        let id = raw_extract_id(&raw).unwrap();
        assert_eq!(id, None);
    }

    #[test]
    fn raw_extract_id_int_returns_none() {
        let raw = make_raw(&doc! { "_id": 42_i32 });
        let id = raw_extract_id(&raw).unwrap();
        assert_eq!(id, None);
    }

    #[test]
    fn raw_extract_id_empty_string() {
        let raw = make_raw(&doc! { "_id": "", "name": "test" });
        let id = raw_extract_id(&raw).unwrap();
        assert_eq!(id, Some(""));
    }

    #[test]
    fn raw_extract_id_unicode() {
        let raw = make_raw(&doc! { "_id": "日本語キー", "name": "test" });
        let id = raw_extract_id(&raw).unwrap();
        assert_eq!(id, Some("日本語キー"));
    }
}
