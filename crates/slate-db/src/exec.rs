use std::cmp::Ordering;

use bson::Bson;
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
