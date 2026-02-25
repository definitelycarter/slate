use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use crate::expression::Expression;
use bson::RawBson;
use bson::raw::RawBsonRef;
use bson::{Bson, RawDocument};

use crate::error::DbError;

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

// ── Expression-based filter matching ─────────────────────────────
//
// The Expression carries owned `Bson` values (from the query), while the
// document fields are read as `RawBsonRef` (zero-copy from raw bytes).
// The `raw_value_eq_bson` / `raw_compare_bson` helpers bridge this gap.

pub(crate) fn raw_matches_expr(raw: &RawDocument, expr: &Expression) -> Result<bool, DbError> {
    match expr {
        Expression::And(children) => {
            for child in children {
                if !raw_matches_expr(raw, child)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Expression::Or(children) => {
            for child in children {
                if raw_matches_expr(raw, child)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expression::Eq(field, val) => {
            // $eq: null matches both missing fields and explicit null values
            if matches!(val, Bson::Null) {
                return Ok(raw_get_path(raw, field)?.is_none());
            }
            match raw_get_path(raw, field)? {
                Some(RawBsonRef::Array(arr)) => {
                    for elem in arr.into_iter().flatten() {
                        if raw_value_eq_bson(&elem, val) {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                Some(v) => Ok(raw_value_eq_bson(&v, val)),
                None => Ok(false),
            }
        }
        Expression::Gt(field, val)
        | Expression::Gte(field, val)
        | Expression::Lt(field, val)
        | Expression::Lte(field, val) => {
            let predicate: fn(Ordering) -> bool = match expr {
                Expression::Gt(..) => |o| o == Ordering::Greater,
                Expression::Gte(..) => |o| o != Ordering::Less,
                Expression::Lt(..) => |o| o == Ordering::Less,
                Expression::Lte(..) => |o| o != Ordering::Greater,
                _ => unreachable!(),
            };
            let field_value = raw_get_path(raw, field)?;
            match field_value {
                Some(RawBsonRef::Array(arr)) => {
                    for elem in arr.into_iter().flatten() {
                        if raw_compare_bson(Some(&elem), val, predicate)? {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                _ => raw_compare_bson(field_value.as_ref(), val, predicate),
            }
        }
        Expression::Regex(field, re) => match raw_get_path(raw, field)? {
            Some(RawBsonRef::String(s)) => Ok(re.is_match(s)),
            _ => Ok(false),
        },
        Expression::Exists(field, expected) => {
            // $exists checks physical presence — even a null value counts as "exists"
            let bytes = raw.as_bytes();
            let present = super::raw_bson::find_field_path(bytes, field).is_some();
            Ok(*expected == present)
        }
    }
}

/// Equality: stored `RawBsonRef` (from document) vs query `Bson` (from Expression).
fn raw_value_eq_bson(store_val: &RawBsonRef, query_val: &Bson) -> bool {
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

        // ── Cross-type coercion: Bson::String → stored type ─
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

        // ── Cross-type coercion: Int → DateTime (epoch seconds) ─
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

/// Comparison: stored `RawBsonRef` (from document) vs query `Bson` (from Expression).
fn raw_compare_bson(
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

            // ── Cross-type coercion: Int → DateTime (epoch seconds) ─
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
