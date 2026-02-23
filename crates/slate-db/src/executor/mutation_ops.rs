use bson::{Bson, Document};

use crate::error::DbError;

/// Resolve a dot-path to its parent document and leaf field name.
///
/// For `"address.city"`, walks into `doc["address"]` and returns `(&mut sub_doc, "city")`.
/// If `create` is true, missing intermediate sub-documents are created.
/// If `create` is false, returns `None` when an intermediate is missing.
pub(crate) fn resolve_parent_mut<'a>(
    doc: &'a mut Document,
    path: &'a str,
    create: bool,
) -> Result<Option<(&'a mut Document, &'a str)>, DbError> {
    let segments: Vec<&str> = path.split('.').collect();
    if segments.is_empty() {
        return Err(DbError::InvalidQuery("empty field path".into()));
    }
    if segments.len() == 1 {
        // Return a self-referential borrow — caller gets (doc, leaf).
        // We use a raw pointer to work around the borrow checker here,
        // since we need to return both &mut doc and a &str that lives
        // as long as the input path (which it does, via the slice).
        let leaf = segments[0];
        // SAFETY: we're returning the same doc we received, just repackaged.
        let doc_ptr = doc as *mut Document;
        return Ok(Some((unsafe { &mut *doc_ptr }, leaf)));
    }

    // Walk through intermediate segments, stopping before the last one (the leaf).
    let (intermediates, leaf) = segments.split_at(segments.len() - 1);
    let leaf = leaf[0];

    let mut current = doc as *mut Document;

    for &segment in intermediates {
        let current_ref = unsafe { &mut *current };

        if !current_ref.contains_key(segment) {
            if create {
                current_ref.insert(segment.to_string(), Bson::Document(Document::new()));
            } else {
                return Ok(None);
            }
        }

        match current_ref.get_mut(segment) {
            Some(Bson::Document(sub)) => {
                current = sub as *mut Document;
            }
            Some(_) => {
                return Err(DbError::InvalidQuery(format!(
                    "field path '{path}': intermediate '{segment}' is not a document"
                )));
            }
            None => return Ok(None),
        }
    }

    Ok(Some((unsafe { &mut *current }, leaf)))
}

/// `$set` — Set field to value. Creates the field if it doesn't exist.
pub(crate) fn op_set(doc: &mut Document, field: &str, value: &Bson) -> Result<bool, DbError> {
    let existing = doc.get(field);
    if existing == Some(value) {
        return Ok(false);
    }
    doc.insert(field.to_string(), value.clone());
    Ok(true)
}

/// `$unset` — Remove field from document.
pub(crate) fn op_unset(doc: &mut Document, field: &str) -> Result<bool, DbError> {
    Ok(doc.remove(field).is_some())
}

/// `$inc` — Increment a numeric field.
///
/// Type promotion rules:
/// - i32 + i32 → i32 (unless overflow, then i64)
/// - i32 + i64 → i64
/// - i64 + i64 → i64
/// - any + f64 → f64
/// - missing field treated as 0 with the same type as the increment value
pub(crate) fn op_inc(doc: &mut Document, field: &str, amount: &Bson) -> Result<bool, DbError> {
    let current = doc.get(field).cloned().unwrap_or_else(|| match amount {
        Bson::Int32(_) => Bson::Int32(0),
        Bson::Int64(_) => Bson::Int64(0),
        Bson::Double(_) => Bson::Double(0.0),
        _ => Bson::Int32(0),
    });

    let result = match (&current, amount) {
        (Bson::Int32(a), Bson::Int32(b)) => match a.checked_add(*b) {
            Some(sum) => Bson::Int32(sum),
            None => Bson::Int64(*a as i64 + *b as i64),
        },
        (Bson::Int32(a), Bson::Int64(b)) => Bson::Int64(*a as i64 + b),
        (Bson::Int64(a), Bson::Int32(b)) => Bson::Int64(a + *b as i64),
        (Bson::Int64(a), Bson::Int64(b)) => Bson::Int64(a + b),
        (Bson::Double(a), Bson::Double(b)) => Bson::Double(a + b),
        (Bson::Int32(a), Bson::Double(b)) => Bson::Double(*a as f64 + b),
        (Bson::Int64(a), Bson::Double(b)) => Bson::Double(*a as f64 + b),
        (Bson::Double(a), Bson::Int32(b)) => Bson::Double(a + *b as f64),
        (Bson::Double(a), Bson::Int64(b)) => Bson::Double(a + *b as f64),
        _ => {
            return Err(DbError::InvalidQuery(format!(
                "$inc: field '{field}' is not numeric"
            )));
        }
    };

    doc.insert(field.to_string(), result);
    Ok(true)
}

/// `$rename` — Rename a field within the same parent document.
pub(crate) fn op_rename(doc: &mut Document, field: &str, new_name: &str) -> Result<bool, DbError> {
    match doc.remove(field) {
        Some(val) => {
            doc.insert(new_name.to_string(), val);
            Ok(true)
        }
        None => Ok(false),
    }
}

/// `$push` — Append a value to the end of an array field.
pub(crate) fn op_push(doc: &mut Document, field: &str, value: &Bson) -> Result<bool, DbError> {
    match doc.get_mut(field) {
        Some(Bson::Array(arr)) => {
            arr.push(value.clone());
            Ok(true)
        }
        Some(_) => Err(DbError::InvalidQuery(format!(
            "$push: field '{field}' is not an array"
        ))),
        None => {
            doc.insert(field.to_string(), Bson::Array(vec![value.clone()]));
            Ok(true)
        }
    }
}

/// `$lpush` — Prepend a value to the beginning of an array field.
pub(crate) fn op_lpush(doc: &mut Document, field: &str, value: &Bson) -> Result<bool, DbError> {
    match doc.get_mut(field) {
        Some(Bson::Array(arr)) => {
            arr.insert(0, value.clone());
            Ok(true)
        }
        Some(_) => Err(DbError::InvalidQuery(format!(
            "$lpush: field '{field}' is not an array"
        ))),
        None => {
            doc.insert(field.to_string(), Bson::Array(vec![value.clone()]));
            Ok(true)
        }
    }
}

/// `$pop` — Remove the last element of an array field.
pub(crate) fn op_pop(doc: &mut Document, field: &str) -> Result<bool, DbError> {
    match doc.get_mut(field) {
        Some(Bson::Array(arr)) => {
            if arr.is_empty() {
                Ok(false)
            } else {
                arr.pop();
                Ok(true)
            }
        }
        Some(_) => Err(DbError::InvalidQuery(format!(
            "$pop: field '{field}' is not an array"
        ))),
        None => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    // ── resolve_parent_mut ──────────────────────────────────────

    #[test]
    fn resolve_flat_field() {
        let mut doc = doc! { "a": 1 };
        let (parent, leaf) = resolve_parent_mut(&mut doc, "a", false).unwrap().unwrap();
        assert_eq!(leaf, "a");
        assert_eq!(parent.get_i32("a").unwrap(), 1);
    }

    #[test]
    fn resolve_nested_field() {
        let mut doc = doc! { "address": { "city": "Austin" } };
        let (parent, leaf) = resolve_parent_mut(&mut doc, "address.city", false)
            .unwrap()
            .unwrap();
        assert_eq!(leaf, "city");
        assert_eq!(parent.get_str("city").unwrap(), "Austin");
    }

    #[test]
    fn resolve_missing_intermediate_no_create() {
        let mut doc = doc! { "a": 1 };
        let result = resolve_parent_mut(&mut doc, "missing.field", false).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn resolve_missing_intermediate_with_create() {
        let mut doc = doc! {};
        let (parent, leaf) = resolve_parent_mut(&mut doc, "a.b.c", true)
            .unwrap()
            .unwrap();
        assert_eq!(leaf, "c");
        // Intermediate sub-documents should have been created
        assert!(parent.is_empty());
    }

    #[test]
    fn resolve_non_document_intermediate() {
        let mut doc = doc! { "a": 42 };
        let result = resolve_parent_mut(&mut doc, "a.b", false);
        assert!(result.is_err());
    }

    // ── op_set ──────────────────────────────────────────────────

    #[test]
    fn set_new_field() {
        let mut doc = doc! { "a": 1 };
        assert!(op_set(&mut doc, "b", &Bson::Int32(2)).unwrap());
        assert_eq!(doc.get_i32("b").unwrap(), 2);
    }

    #[test]
    fn set_existing_field_changes() {
        let mut doc = doc! { "a": 1 };
        assert!(op_set(&mut doc, "a", &Bson::Int32(2)).unwrap());
        assert_eq!(doc.get_i32("a").unwrap(), 2);
    }

    #[test]
    fn set_same_value_no_change() {
        let mut doc = doc! { "a": 1 };
        assert!(!op_set(&mut doc, "a", &Bson::Int32(1)).unwrap());
    }

    // ── op_unset ────────────────────────────────────────────────

    #[test]
    fn unset_existing() {
        let mut doc = doc! { "a": 1, "b": 2 };
        assert!(op_unset(&mut doc, "a").unwrap());
        assert!(!doc.contains_key("a"));
    }

    #[test]
    fn unset_missing_no_change() {
        let mut doc = doc! { "a": 1 };
        assert!(!op_unset(&mut doc, "b").unwrap());
    }

    // ── op_inc ──────────────────────────────────────────────────

    #[test]
    fn inc_existing_i32() {
        let mut doc = doc! { "score": 10 };
        assert!(op_inc(&mut doc, "score", &Bson::Int32(5)).unwrap());
        assert_eq!(doc.get_i32("score").unwrap(), 15);
    }

    #[test]
    fn inc_missing_field_creates() {
        let mut doc = doc! {};
        assert!(op_inc(&mut doc, "score", &Bson::Int32(5)).unwrap());
        assert_eq!(doc.get_i32("score").unwrap(), 5);
    }

    #[test]
    fn inc_i32_overflow_promotes_to_i64() {
        let mut doc = doc! { "n": i32::MAX };
        assert!(op_inc(&mut doc, "n", &Bson::Int32(1)).unwrap());
        assert_eq!(doc.get_i64("n").unwrap(), i32::MAX as i64 + 1);
    }

    #[test]
    fn inc_i32_by_i64() {
        let mut doc = doc! { "n": 10_i32 };
        assert!(op_inc(&mut doc, "n", &Bson::Int64(100)).unwrap());
        assert_eq!(doc.get_i64("n").unwrap(), 110);
    }

    #[test]
    fn inc_by_double() {
        let mut doc = doc! { "n": 10_i32 };
        assert!(op_inc(&mut doc, "n", &Bson::Double(0.5)).unwrap());
        assert_eq!(doc.get_f64("n").unwrap(), 10.5);
    }

    #[test]
    fn inc_non_numeric_errors() {
        let mut doc = doc! { "name": "alice" };
        assert!(op_inc(&mut doc, "name", &Bson::Int32(1)).is_err());
    }

    #[test]
    fn inc_negative_decrements() {
        let mut doc = doc! { "score": 10 };
        assert!(op_inc(&mut doc, "score", &Bson::Int32(-3)).unwrap());
        assert_eq!(doc.get_i32("score").unwrap(), 7);
    }

    // ── op_rename ───────────────────────────────────────────────

    #[test]
    fn rename_existing() {
        let mut doc = doc! { "old": "value" };
        assert!(op_rename(&mut doc, "old", "new").unwrap());
        assert!(!doc.contains_key("old"));
        assert_eq!(doc.get_str("new").unwrap(), "value");
    }

    #[test]
    fn rename_missing_no_change() {
        let mut doc = doc! { "a": 1 };
        assert!(!op_rename(&mut doc, "missing", "new").unwrap());
    }

    // ── op_push ─────────────────────────────────────────────────

    #[test]
    fn push_to_existing_array() {
        let mut doc = doc! { "tags": ["a", "b"] };
        assert!(op_push(&mut doc, "tags", &Bson::String("c".into())).unwrap());
        let arr = doc.get_array("tags").unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[2], Bson::String("c".into()));
    }

    #[test]
    fn push_creates_array() {
        let mut doc = doc! {};
        assert!(op_push(&mut doc, "tags", &Bson::String("a".into())).unwrap());
        let arr = doc.get_array("tags").unwrap();
        assert_eq!(arr.len(), 1);
    }

    #[test]
    fn push_on_non_array_errors() {
        let mut doc = doc! { "tags": "not_array" };
        assert!(op_push(&mut doc, "tags", &Bson::String("a".into())).is_err());
    }

    // ── op_lpush ────────────────────────────────────────────────

    #[test]
    fn lpush_to_existing_array() {
        let mut doc = doc! { "q": ["b", "c"] };
        assert!(op_lpush(&mut doc, "q", &Bson::String("a".into())).unwrap());
        let arr = doc.get_array("q").unwrap();
        assert_eq!(arr[0], Bson::String("a".into()));
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn lpush_creates_array() {
        let mut doc = doc! {};
        assert!(op_lpush(&mut doc, "q", &Bson::String("first".into())).unwrap());
        let arr = doc.get_array("q").unwrap();
        assert_eq!(arr.len(), 1);
    }

    // ── op_pop ──────────────────────────────────────────────────

    #[test]
    fn pop_from_array() {
        let mut doc = doc! { "arr": [1, 2, 3] };
        assert!(op_pop(&mut doc, "arr").unwrap());
        let arr = doc.get_array("arr").unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn pop_empty_array_no_change() {
        let mut doc = doc! { "arr": bson::Bson::Array(vec![]) };
        assert!(!op_pop(&mut doc, "arr").unwrap());
    }

    #[test]
    fn pop_missing_field_no_change() {
        let mut doc = doc! {};
        assert!(!op_pop(&mut doc, "arr").unwrap());
    }

    #[test]
    fn pop_non_array_errors() {
        let mut doc = doc! { "arr": 42 };
        assert!(op_pop(&mut doc, "arr").is_err());
    }
}
