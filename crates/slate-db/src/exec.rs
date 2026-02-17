use std::cmp::Ordering;

use bson::raw::RawBsonRef;
use bson::{Bson, RawDocument, RawDocumentBuf};
use slate_query::{Filter, FilterGroup, FilterNode, LogicalOp, Operator, QueryValue};

use crate::error::DbError;

// ── Filter matching ─────────────────────────────────────────────

pub(crate) fn matches_group(doc: &bson::Document, group: &FilterGroup) -> Result<bool, DbError> {
    match group.logical {
        LogicalOp::And => {
            for child in &group.children {
                if !matches_node(doc, child)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        LogicalOp::Or => {
            for child in &group.children {
                if matches_node(doc, child)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn matches_node(doc: &bson::Document, node: &FilterNode) -> Result<bool, DbError> {
    match node {
        FilterNode::Condition(filter) => matches_filter(doc, filter),
        FilterNode::Group(group) => matches_group(doc, group),
    }
}

/// Walk a dot-separated path through nested documents.
/// "foo.bar.baz" → doc.get("foo") → subdoc.get("bar") → subdoc.get("baz")
/// Returns None if any intermediate step is missing, not a Document, or Null.
pub(crate) fn get_path<'a>(doc: &'a bson::Document, path: &str) -> Option<&'a Bson> {
    if !path.contains('.') {
        return doc.get(path).filter(|v| !matches!(v, Bson::Null));
    }
    let mut parts = path.split('.');
    let first = parts.next()?;
    let mut current = doc.get(first)?;
    for part in parts {
        match current {
            Bson::Document(d) => current = d.get(part)?,
            _ => return None,
        }
    }
    if matches!(current, Bson::Null) {
        None
    } else {
        Some(current)
    }
}

fn matches_filter(doc: &bson::Document, filter: &Filter) -> Result<bool, DbError> {
    let field_value = get_path(doc, &filter.field);

    match filter.operator {
        Operator::IsNull => match &filter.value {
            QueryValue::Bool(true) => Ok(field_value.is_none()),
            QueryValue::Bool(false) => Ok(field_value.is_some()),
            _ => Ok(field_value.is_none()),
        },
        Operator::Eq => match field_value {
            Some(v) => Ok(values_eq(v, &filter.value)),
            None => Ok(false),
        },
        Operator::IContains => match (field_value, &filter.value) {
            (Some(Bson::String(haystack)), QueryValue::String(needle)) => {
                Ok(haystack.to_lowercase().contains(&needle.to_lowercase()))
            }
            _ => Ok(false),
        },
        Operator::IStartsWith => match (field_value, &filter.value) {
            (Some(Bson::String(haystack)), QueryValue::String(needle)) => {
                Ok(haystack.to_lowercase().starts_with(&needle.to_lowercase()))
            }
            _ => Ok(false),
        },
        Operator::IEndsWith => match (field_value, &filter.value) {
            (Some(Bson::String(haystack)), QueryValue::String(needle)) => {
                Ok(haystack.to_lowercase().ends_with(&needle.to_lowercase()))
            }
            _ => Ok(false),
        },
        Operator::Gt => compare_values(field_value, &filter.value, |ord| ord == Ordering::Greater),
        Operator::Gte => compare_values(field_value, &filter.value, |ord| ord != Ordering::Less),
        Operator::Lt => compare_values(field_value, &filter.value, |ord| ord == Ordering::Less),
        Operator::Lte => compare_values(field_value, &filter.value, |ord| ord != Ordering::Greater),
    }
}

fn values_eq(store_val: &Bson, query_val: &QueryValue) -> bool {
    match (store_val, query_val) {
        (Bson::String(a), QueryValue::String(b)) => a == b,
        (Bson::Int32(a), QueryValue::Int(b)) => (*a as i64) == *b,
        (Bson::Int64(a), QueryValue::Int(b)) => a == b,
        (Bson::Double(a), QueryValue::Float(b)) => a == b,
        (Bson::Boolean(a), QueryValue::Bool(b)) => a == b,
        (Bson::DateTime(a), QueryValue::Date(b)) => a.timestamp_millis() == (*b * 1000),
        _ => false,
    }
}

fn compare_values(
    field_value: Option<&Bson>,
    query_val: &QueryValue,
    predicate: fn(Ordering) -> bool,
) -> Result<bool, DbError> {
    match field_value {
        Some(store_val) => match (store_val, query_val) {
            (Bson::Int32(a), QueryValue::Int(b)) => Ok(predicate((*a as i64).cmp(b))),
            (Bson::Int64(a), QueryValue::Int(b)) => Ok(predicate(a.cmp(b))),
            (Bson::Double(a), QueryValue::Float(b)) => {
                Ok(predicate(a.partial_cmp(b).unwrap_or(Ordering::Equal)))
            }
            (Bson::DateTime(a), QueryValue::Date(b)) => {
                Ok(predicate(a.timestamp_millis().cmp(&(*b * 1000))))
            }
            (Bson::String(a), QueryValue::String(b)) => Ok(predicate(a.as_str().cmp(b.as_str()))),
            _ => Ok(false),
        },
        None => Ok(false),
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
        if is_indexable_scalar(value) {
            out.push(value);
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

// ── Value comparison for sorting ─────────────────────────────────

pub(crate) fn compare_field_values(a: Option<&Bson>, b: Option<&Bson>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (Some(Bson::Null), None)
        | (None, Some(Bson::Null))
        | (Some(Bson::Null), Some(Bson::Null)) => Ordering::Equal,
        (None, Some(_)) | (Some(Bson::Null), Some(_)) => Ordering::Less,
        (Some(_), None) | (Some(_), Some(Bson::Null)) => Ordering::Greater,
        (Some(a), Some(b)) => compare_two_values(a, b),
    }
}

fn compare_two_values(a: &Bson, b: &Bson) -> Ordering {
    match (a, b) {
        (Bson::String(a), Bson::String(b)) => a.cmp(b),
        (Bson::Int32(a), Bson::Int32(b)) => a.cmp(b),
        (Bson::Int64(a), Bson::Int64(b)) => a.cmp(b),
        (Bson::Int32(a), Bson::Int64(b)) => (*a as i64).cmp(b),
        (Bson::Int64(a), Bson::Int32(b)) => a.cmp(&(*b as i64)),
        (Bson::Double(a), Bson::Double(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Bson::Boolean(a), Bson::Boolean(b)) => a.cmp(b),
        (Bson::DateTime(a), Bson::DateTime(b)) => a.timestamp_millis().cmp(&b.timestamp_millis()),
        _ => Ordering::Equal,
    }
}

// ── Raw BSON filter matching ────────────────────────────────────
//
// These functions mirror the bson::Document-based filter matching above
// but operate on RawDocumentBuf / RawBsonRef to avoid full deserialization.

/// Walk a dot-separated path through raw BSON bytes.
/// Returns None if the path is missing, Null, or if `path` is `"_id"` (handled by caller).
pub(crate) fn raw_get_path<'a>(
    raw: &'a RawDocumentBuf,
    path: &str,
) -> Result<Option<RawBsonRef<'a>>, DbError> {
    if path == "_id" {
        return Ok(None);
    }

    if !path.contains('.') {
        return match raw.get(path)? {
            Some(RawBsonRef::Null) | None => Ok(None),
            some => Ok(some),
        };
    }

    let (first, rest) = path.split_once('.').unwrap();
    match raw.get(first)? {
        Some(RawBsonRef::Document(sub)) => raw_get_path_in_doc(sub, rest),
        _ => Ok(None),
    }
}

fn raw_get_path_in_doc<'a>(
    doc: &'a RawDocument,
    path: &str,
) -> Result<Option<RawBsonRef<'a>>, DbError> {
    if !path.contains('.') {
        return match doc.get(path)? {
            Some(RawBsonRef::Null) | None => Ok(None),
            some => Ok(some),
        };
    }

    let (first, rest) = path.split_once('.').unwrap();
    match doc.get(first)? {
        Some(RawBsonRef::Document(sub)) => raw_get_path_in_doc(sub, rest),
        _ => Ok(None),
    }
}

pub(crate) fn raw_matches_group(
    raw: &RawDocumentBuf,
    id: &str,
    group: &FilterGroup,
) -> Result<bool, DbError> {
    match group.logical {
        LogicalOp::And => {
            for child in &group.children {
                if !raw_matches_node(raw, id, child)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        LogicalOp::Or => {
            for child in &group.children {
                if raw_matches_node(raw, id, child)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn raw_matches_node(raw: &RawDocumentBuf, id: &str, node: &FilterNode) -> Result<bool, DbError> {
    match node {
        FilterNode::Condition(filter) => raw_matches_filter(raw, id, filter),
        FilterNode::Group(group) => raw_matches_group(raw, id, group),
    }
}

fn raw_matches_filter(raw: &RawDocumentBuf, id: &str, filter: &Filter) -> Result<bool, DbError> {
    // _id is stored externally, not in raw bytes
    if filter.field == "_id" {
        return match filter.operator {
            Operator::Eq => match &filter.value {
                QueryValue::String(s) => Ok(id == s.as_str()),
                _ => Ok(false),
            },
            Operator::IsNull => match &filter.value {
                QueryValue::Bool(true) => Ok(false),
                QueryValue::Bool(false) => Ok(true),
                _ => Ok(false),
            },
            _ => Ok(false),
        };
    }

    let field_value = raw_get_path(raw, &filter.field)?;

    match filter.operator {
        Operator::IsNull => match &filter.value {
            QueryValue::Bool(true) => Ok(field_value.is_none()),
            QueryValue::Bool(false) => Ok(field_value.is_some()),
            _ => Ok(field_value.is_none()),
        },
        Operator::Eq => match field_value {
            Some(v) => Ok(raw_values_eq(&v, &filter.value)),
            None => Ok(false),
        },
        Operator::IContains => match (field_value, &filter.value) {
            (Some(RawBsonRef::String(haystack)), QueryValue::String(needle)) => {
                Ok(haystack.to_lowercase().contains(&needle.to_lowercase()))
            }
            _ => Ok(false),
        },
        Operator::IStartsWith => match (field_value, &filter.value) {
            (Some(RawBsonRef::String(haystack)), QueryValue::String(needle)) => {
                Ok(haystack.to_lowercase().starts_with(&needle.to_lowercase()))
            }
            _ => Ok(false),
        },
        Operator::IEndsWith => match (field_value, &filter.value) {
            (Some(RawBsonRef::String(haystack)), QueryValue::String(needle)) => {
                Ok(haystack.to_lowercase().ends_with(&needle.to_lowercase()))
            }
            _ => Ok(false),
        },
        Operator::Gt => raw_compare_values(field_value.as_ref(), &filter.value, |o| {
            o == Ordering::Greater
        }),
        Operator::Gte => {
            raw_compare_values(field_value.as_ref(), &filter.value, |o| o != Ordering::Less)
        }
        Operator::Lt => {
            raw_compare_values(field_value.as_ref(), &filter.value, |o| o == Ordering::Less)
        }
        Operator::Lte => raw_compare_values(field_value.as_ref(), &filter.value, |o| {
            o != Ordering::Greater
        }),
    }
}

fn raw_values_eq(store_val: &RawBsonRef, query_val: &QueryValue) -> bool {
    match (store_val, query_val) {
        (RawBsonRef::String(a), QueryValue::String(b)) => *a == b.as_str(),
        (RawBsonRef::Int32(a), QueryValue::Int(b)) => (*a as i64) == *b,
        (RawBsonRef::Int64(a), QueryValue::Int(b)) => *a == *b,
        (RawBsonRef::Double(a), QueryValue::Float(b)) => *a == *b,
        (RawBsonRef::Boolean(a), QueryValue::Bool(b)) => *a == *b,
        (RawBsonRef::DateTime(a), QueryValue::Date(b)) => a.timestamp_millis() == (*b * 1000),
        _ => false,
    }
}

fn raw_compare_values(
    field_value: Option<&RawBsonRef>,
    query_val: &QueryValue,
    predicate: fn(Ordering) -> bool,
) -> Result<bool, DbError> {
    match field_value {
        Some(store_val) => match (store_val, query_val) {
            (RawBsonRef::Int32(a), QueryValue::Int(b)) => Ok(predicate((*a as i64).cmp(b))),
            (RawBsonRef::Int64(a), QueryValue::Int(b)) => Ok(predicate(a.cmp(b))),
            (RawBsonRef::Double(a), QueryValue::Float(b)) => {
                Ok(predicate(a.partial_cmp(b).unwrap_or(Ordering::Equal)))
            }
            (RawBsonRef::DateTime(a), QueryValue::Date(b)) => {
                Ok(predicate(a.timestamp_millis().cmp(&(*b * 1000))))
            }
            (RawBsonRef::String(a), QueryValue::String(b)) => Ok(predicate((*a).cmp(b.as_str()))),
            _ => Ok(false),
        },
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
        (RawBsonRef::Boolean(a), RawBsonRef::Boolean(b)) => a.cmp(b),
        (RawBsonRef::DateTime(a), RawBsonRef::DateTime(b)) => {
            a.timestamp_millis().cmp(&b.timestamp_millis())
        }
        _ => Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

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
    fn raw_get_path_id_returns_none() {
        let raw = make_raw(&doc! { "name": "test" });
        let val = raw_get_path(&raw, "_id").unwrap();
        assert_eq!(val, None);
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
                    value: QueryValue::String("active".into()),
                }),
                FilterNode::Condition(Filter {
                    field: "score".into(),
                    operator: Operator::Gt,
                    value: QueryValue::Int(50),
                }),
            ],
        };
        assert!(raw_matches_group(&raw, "id1", &group).unwrap());
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
                    value: QueryValue::String("active".into()),
                }),
                FilterNode::Condition(Filter {
                    field: "score".into(),
                    operator: Operator::Gt,
                    value: QueryValue::Int(50),
                }),
            ],
        };
        assert!(!raw_matches_group(&raw, "id1", &group).unwrap());
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
                    value: QueryValue::String("active".into()),
                }),
                FilterNode::Condition(Filter {
                    field: "score".into(),
                    operator: Operator::Gt,
                    value: QueryValue::Int(50),
                }),
            ],
        };
        assert!(raw_matches_group(&raw, "id1", &group).unwrap());
    }

    #[test]
    fn raw_matches_filter_id_eq() {
        let raw = make_raw(&doc! { "name": "test" });
        let filter = Filter {
            field: "_id".into(),
            operator: Operator::Eq,
            value: QueryValue::String("abc123".into()),
        };
        assert!(raw_matches_filter(&raw, "abc123", &filter).unwrap());
        assert!(!raw_matches_filter(&raw, "xyz", &filter).unwrap());
    }

    #[test]
    fn raw_matches_filter_is_null() {
        let raw = make_raw(&doc! { "name": "test" });
        let filter = Filter {
            field: "missing".into(),
            operator: Operator::IsNull,
            value: QueryValue::Bool(true),
        };
        assert!(raw_matches_filter(&raw, "id1", &filter).unwrap());

        let filter_not_null = Filter {
            field: "name".into(),
            operator: Operator::IsNull,
            value: QueryValue::Bool(true),
        };
        assert!(!raw_matches_filter(&raw, "id1", &filter_not_null).unwrap());
    }

    #[test]
    fn raw_matches_filter_icontains() {
        let raw = make_raw(&doc! { "name": "Hello World" });
        let filter = Filter {
            field: "name".into(),
            operator: Operator::IContains,
            value: QueryValue::String("hello".into()),
        };
        assert!(raw_matches_filter(&raw, "id1", &filter).unwrap());
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
}
