use std::cmp::Ordering;

use bson::raw::RawBsonRef;
use bson::{Bson, RawDocument};
use slate_query::{Filter, FilterGroup, FilterNode, LogicalOp, Operator};

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

// ── Raw BSON filter matching ────────────────────────────────────
//
// Filter matching operates on RawDocument / RawBsonRef to avoid full deserialization.

/// Walk a dot-separated path through raw BSON bytes.
/// Returns None if the path is missing, Null, or if `path` is `"_id"` (handled by caller).
pub(crate) fn raw_get_path<'a>(
    raw: &'a RawDocument,
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

/// Walk a dot-separated path through raw BSON, collecting all reachable values.
/// Unlike `raw_get_path` (which returns a single value), this flattens arrays:
/// - Scalar field → `vec![value]`
/// - Array of scalars → each element individually
/// - Path through array of sub-documents → traverse each element, collect leaves
/// - Missing/null → empty vec
pub(crate) fn raw_get_path_values<'a>(
    raw: &'a RawDocument,
    path: &str,
) -> Result<Vec<RawBsonRef<'a>>, DbError> {
    if path == "_id" {
        return Ok(vec![]);
    }

    let mut results = Vec::new();
    raw_collect_path_values(raw, path, &mut results)?;
    Ok(results)
}

fn raw_collect_path_values<'a>(
    doc: &'a RawDocument,
    path: &str,
    out: &mut Vec<RawBsonRef<'a>>,
) -> Result<(), DbError> {
    let (first, rest) = match path.split_once('.') {
        Some((f, r)) => (f, Some(r)),
        None => (path, None),
    };

    match doc.get(first)? {
        Some(RawBsonRef::Null) | None => {}
        Some(RawBsonRef::Array(arr)) => {
            for elem in arr.into_iter() {
                let val = elem?;
                match (val, rest) {
                    // Array element is a sub-document and there's more path to walk
                    (RawBsonRef::Document(sub), Some(rest)) => {
                        raw_collect_path_values(sub, rest, out)?;
                    }
                    // Array element is a leaf (no more path segments)
                    (val, None) if !matches!(val, RawBsonRef::Null) => {
                        out.push(val);
                    }
                    _ => {}
                }
            }
        }
        Some(RawBsonRef::Document(sub)) => {
            if let Some(rest) = rest {
                raw_collect_path_values(sub, rest, out)?;
            } else {
                out.push(RawBsonRef::Document(sub));
            }
        }
        Some(val) => {
            if rest.is_none() {
                out.push(val);
            }
        }
    }

    Ok(())
}

pub(crate) fn raw_matches_group(
    raw: &RawDocument,
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

fn raw_matches_node(raw: &RawDocument, id: &str, node: &FilterNode) -> Result<bool, DbError> {
    match node {
        FilterNode::Condition(filter) => raw_matches_filter(raw, id, filter),
        FilterNode::Group(group) => raw_matches_group(raw, id, group),
    }
}

fn raw_matches_filter(raw: &RawDocument, id: &str, filter: &Filter) -> Result<bool, DbError> {
    // _id is stored externally, not in raw bytes
    if filter.field == "_id" {
        return match filter.operator {
            Operator::Eq => match &filter.value {
                Bson::String(s) => Ok(id == s.as_str()),
                _ => Ok(false),
            },
            Operator::IsNull => match &filter.value {
                Bson::Boolean(true) => Ok(false),
                Bson::Boolean(false) => Ok(true),
                _ => Ok(false),
            },
            _ => Ok(false),
        };
    }

    let field_value = raw_get_path(raw, &filter.field)?;

    match filter.operator {
        Operator::IsNull => match &filter.value {
            Bson::Boolean(true) => Ok(field_value.is_none()),
            Bson::Boolean(false) => Ok(field_value.is_some()),
            _ => Ok(field_value.is_none()),
        },
        Operator::Eq => match field_value {
            Some(v) => Ok(raw_values_eq(&v, &filter.value)),
            None => Ok(false),
        },
        Operator::IContains => match (field_value, &filter.value) {
            (Some(RawBsonRef::String(haystack)), Bson::String(needle)) => {
                Ok(haystack.to_lowercase().contains(&needle.to_lowercase()))
            }
            _ => Ok(false),
        },
        Operator::IStartsWith => match (field_value, &filter.value) {
            (Some(RawBsonRef::String(haystack)), Bson::String(needle)) => {
                Ok(haystack.to_lowercase().starts_with(&needle.to_lowercase()))
            }
            _ => Ok(false),
        },
        Operator::IEndsWith => match (field_value, &filter.value) {
            (Some(RawBsonRef::String(haystack)), Bson::String(needle)) => {
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

fn raw_values_eq(store_val: &RawBsonRef, query_val: &Bson) -> bool {
    match (store_val, query_val) {
        // ── Direct type matches ─────────────────────────────────
        (RawBsonRef::String(a), Bson::String(b)) => *a == b.as_str(),
        (RawBsonRef::Int32(a), Bson::Int32(b)) => *a == *b,
        (RawBsonRef::Int32(a), Bson::Int64(b)) => (*a as i64) == *b,
        (RawBsonRef::Int64(a), Bson::Int64(b)) => *a == *b,
        (RawBsonRef::Int64(a), Bson::Int32(b)) => *a == (*b as i64),
        (RawBsonRef::Double(a), Bson::Double(b)) => *a == *b,
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

fn raw_compare_values(
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
                    value: Bson::String("active".into()),
                }),
                FilterNode::Condition(Filter {
                    field: "score".into(),
                    operator: Operator::Gt,
                    value: Bson::Int64(50),
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
                    value: Bson::String("active".into()),
                }),
                FilterNode::Condition(Filter {
                    field: "score".into(),
                    operator: Operator::Gt,
                    value: Bson::Int64(50),
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
                    value: Bson::String("active".into()),
                }),
                FilterNode::Condition(Filter {
                    field: "score".into(),
                    operator: Operator::Gt,
                    value: Bson::Int64(50),
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
            value: Bson::String("abc123".into()),
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
            value: Bson::Boolean(true),
        };
        assert!(raw_matches_filter(&raw, "id1", &filter).unwrap());

        let filter_not_null = Filter {
            field: "name".into(),
            operator: Operator::IsNull,
            value: Bson::Boolean(true),
        };
        assert!(!raw_matches_filter(&raw, "id1", &filter_not_null).unwrap());
    }

    #[test]
    fn raw_matches_filter_icontains() {
        let raw = make_raw(&doc! { "name": "Hello World" });
        let filter = Filter {
            field: "name".into(),
            operator: Operator::IContains,
            value: Bson::String("hello".into()),
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

    // ── Cross-type coercion: eq ─────────────────────────────────

    #[test]
    fn coerce_string_to_int32_eq() {
        let raw = make_raw(&doc! { "score": 50_i32 });
        let filter = Filter {
            field: "score".into(),
            operator: Operator::Eq,
            value: Bson::String("50".into()),
        };
        assert!(raw_matches_filter(&raw, "id1", &filter).unwrap());
    }

    #[test]
    fn coerce_string_to_int64_eq() {
        let raw = make_raw(&doc! { "score": 50_i64 });
        let filter = Filter {
            field: "score".into(),
            operator: Operator::Eq,
            value: Bson::String("50".into()),
        };
        assert!(raw_matches_filter(&raw, "id1", &filter).unwrap());
    }

    #[test]
    fn coerce_string_to_double_eq() {
        let raw = make_raw(&doc! { "price": 19.99 });
        let filter = Filter {
            field: "price".into(),
            operator: Operator::Eq,
            value: Bson::String("19.99".into()),
        };
        assert!(raw_matches_filter(&raw, "id1", &filter).unwrap());
    }

    #[test]
    fn coerce_string_to_bool_eq() {
        let raw = make_raw(&doc! { "active": true });
        let filter = Filter {
            field: "active".into(),
            operator: Operator::Eq,
            value: Bson::String("true".into()),
        };
        assert!(raw_matches_filter(&raw, "id1", &filter).unwrap());

        let filter_false = Filter {
            field: "active".into(),
            operator: Operator::Eq,
            value: Bson::String("false".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &filter_false).unwrap());
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
        assert!(raw_matches_filter(&raw, "id1", &filter).unwrap());
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
        assert!(raw_matches_filter(&raw, "id1", &filter).unwrap());
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
        assert!(raw_matches_filter(&raw, "id1", &filter).unwrap());

        let filter_not = Filter {
            field: "score".into(),
            operator: Operator::Gt,
            value: Bson::String("100".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &filter_not).unwrap());
    }

    #[test]
    fn coerce_string_to_double_lte() {
        let raw = make_raw(&doc! { "price": 19.99 });
        let filter = Filter {
            field: "price".into(),
            operator: Operator::Lte,
            value: Bson::String("19.99".into()),
        };
        assert!(raw_matches_filter(&raw, "id1", &filter).unwrap());

        let filter_not = Filter {
            field: "price".into(),
            operator: Operator::Lte,
            value: Bson::String("19.98".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &filter_not).unwrap());
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
        assert!(raw_matches_filter(&raw, "id1", &filter).unwrap());

        let filter_not = Filter {
            field: "updated_at".into(),
            operator: Operator::Gte,
            value: Bson::String("2025-01-01T00:00:00Z".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &filter_not).unwrap());
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
        assert!(raw_matches_filter(&raw, "id1", &filter).unwrap());
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
        assert!(raw_matches_filter(&raw, "id1", &f(Operator::Gte)).unwrap());
        let f_above = Filter {
            field: "score".into(),
            operator: Operator::Gte,
            value: Bson::String("81".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &f_above).unwrap());
    }

    #[test]
    fn coerce_string_to_int_lt() {
        let raw = make_raw(&doc! { "score": 80_i64 });
        let f = Filter {
            field: "score".into(),
            operator: Operator::Lt,
            value: Bson::String("100".into()),
        };
        assert!(raw_matches_filter(&raw, "id1", &f).unwrap());
        let f_not = Filter {
            field: "score".into(),
            operator: Operator::Lt,
            value: Bson::String("80".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &f_not).unwrap());
    }

    #[test]
    fn coerce_string_to_int_lte() {
        let raw = make_raw(&doc! { "score": 80_i64 });
        let f = Filter {
            field: "score".into(),
            operator: Operator::Lte,
            value: Bson::String("80".into()),
        };
        assert!(raw_matches_filter(&raw, "id1", &f).unwrap());
        let f_not = Filter {
            field: "score".into(),
            operator: Operator::Lte,
            value: Bson::String("79".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &f_not).unwrap());
    }

    #[test]
    fn coerce_string_to_double_gt() {
        let raw = make_raw(&doc! { "price": 19.99 });
        let f = Filter {
            field: "price".into(),
            operator: Operator::Gt,
            value: Bson::String("19.98".into()),
        };
        assert!(raw_matches_filter(&raw, "id1", &f).unwrap());
        let f_not = Filter {
            field: "price".into(),
            operator: Operator::Gt,
            value: Bson::String("19.99".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &f_not).unwrap());
    }

    #[test]
    fn coerce_string_to_double_gte() {
        let raw = make_raw(&doc! { "price": 19.99 });
        let f = Filter {
            field: "price".into(),
            operator: Operator::Gte,
            value: Bson::String("19.99".into()),
        };
        assert!(raw_matches_filter(&raw, "id1", &f).unwrap());
        let f_not = Filter {
            field: "price".into(),
            operator: Operator::Gte,
            value: Bson::String("20.00".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &f_not).unwrap());
    }

    #[test]
    fn coerce_string_to_double_lt() {
        let raw = make_raw(&doc! { "price": 19.99 });
        let f = Filter {
            field: "price".into(),
            operator: Operator::Lt,
            value: Bson::String("20.00".into()),
        };
        assert!(raw_matches_filter(&raw, "id1", &f).unwrap());
        let f_not = Filter {
            field: "price".into(),
            operator: Operator::Lt,
            value: Bson::String("19.99".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &f_not).unwrap());
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
        assert!(raw_matches_filter(&raw, "id1", &f).unwrap());
        let f_not = Filter {
            field: "ts".into(),
            operator: Operator::Gt,
            value: Bson::String("2024-06-15T12:00:00Z".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &f_not).unwrap());
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
        assert!(raw_matches_filter(&raw, "id1", &f).unwrap());
        let f_not = Filter {
            field: "ts".into(),
            operator: Operator::Lt,
            value: Bson::String("2024-06-15T12:00:00Z".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &f_not).unwrap());
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
        assert!(raw_matches_filter(&raw, "id1", &f).unwrap());
        let f_not = Filter {
            field: "ts".into(),
            operator: Operator::Lte,
            value: Bson::String("2024-06-15T11:00:00Z".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &f_not).unwrap());
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
        assert!(raw_matches_filter(&raw, "id1", &f).unwrap());
        let f_not = Filter {
            field: "ts".into(),
            operator: Operator::Gt,
            value: Bson::Int64(1705276800),
        };
        assert!(!raw_matches_filter(&raw, "id1", &f_not).unwrap());
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
        assert!(raw_matches_filter(&raw, "id1", &f).unwrap());
        let f_not = Filter {
            field: "ts".into(),
            operator: Operator::Gte,
            value: Bson::Int64(1705276800 + 1),
        };
        assert!(!raw_matches_filter(&raw, "id1", &f_not).unwrap());
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
        assert!(raw_matches_filter(&raw, "id1", &f).unwrap());
        let f_not = Filter {
            field: "ts".into(),
            operator: Operator::Lte,
            value: Bson::Int64(1705276800 - 1),
        };
        assert!(!raw_matches_filter(&raw, "id1", &f_not).unwrap());
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
        assert!(!raw_matches_filter(&raw, "id1", &filter).unwrap());
    }

    #[test]
    fn coerce_invalid_string_to_double_excludes() {
        let raw = make_raw(&doc! { "price": 19.99 });
        let filter = Filter {
            field: "price".into(),
            operator: Operator::Gt,
            value: Bson::String("abc".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &filter).unwrap());
    }

    #[test]
    fn coerce_invalid_string_to_bool_excludes() {
        let raw = make_raw(&doc! { "active": true });
        let filter = Filter {
            field: "active".into(),
            operator: Operator::Eq,
            value: Bson::String("yes".into()),
        };
        assert!(!raw_matches_filter(&raw, "id1", &filter).unwrap());
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
        assert!(!raw_matches_filter(&raw, "id1", &filter).unwrap());
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
        assert!(!raw_matches_filter(&raw, "id1", &filter).unwrap());
    }

    #[test]
    fn incompatible_string_vs_float_excludes() {
        let raw = make_raw(&doc! { "name": "hello" });
        let filter = Filter {
            field: "name".into(),
            operator: Operator::Eq,
            value: Bson::Double(1.0),
        };
        assert!(!raw_matches_filter(&raw, "id1", &filter).unwrap());
    }
}
