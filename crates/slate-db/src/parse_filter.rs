use regex::Regex;

use crate::executor::raw_bson::{self, Element, ElementIter, FieldLocation};
use crate::expression::Expression;

/// Parse error for filter documents.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterParseError(pub String);

impl std::fmt::Display for FilterParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "filter parse error: {}", self.0)
    }
}

impl std::error::Error for FilterParseError {}

/// Parse a BSON filter document into an Expression tree.
///
/// Zero-alloc for common cases — borrows field names and values directly from
/// the raw BSON bytes. Only allocates `Vec`s for And/Or children and compiles
/// `Regex` for `$regex` patterns.
///
/// Uses raw byte scanning (no `RawDocument::iter()`) for maximum throughput.
pub fn parse_filter(bytes: &[u8]) -> Result<Expression<'_>, FilterParseError> {
    let iter = ElementIter::new(bytes, 0)
        .ok_or_else(|| FilterParseError("truncated BSON document".into()))?;

    let mut children = Vec::new();

    for elem in iter {
        match elem.key {
            "$and" => children.push(parse_logical_array(bytes, &elem, Expression::And)?),
            "$or" => children.push(parse_logical_array(bytes, &elem, Expression::Or)?),
            k if k.starts_with('$') => {
                return Err(FilterParseError(format!("unknown top-level operator: {k}")));
            }
            _ => children.push(parse_field_condition(bytes, &elem)?),
        }
    }

    if children.is_empty() {
        return Err(FilterParseError("empty filter document".into()));
    }

    if children.len() == 1 {
        Ok(children.pop().unwrap())
    } else {
        Ok(Expression::And(children))
    }
}

/// Parse a `$and` or `$or` array value into a logical expression.
fn parse_logical_array<'a>(
    bytes: &'a [u8],
    elem: &Element<'_>,
    make: fn(Vec<Expression<'a>>) -> Expression<'a>,
) -> Result<Expression<'a>, FilterParseError> {
    if elem.type_byte != 0x04 {
        return Err(FilterParseError("$and/$or value must be an array".into()));
    }

    // Array elements are documents with numeric keys ("0", "1", ...)
    let arr_iter = ElementIter::new(bytes, elem.value_start)
        .ok_or_else(|| FilterParseError("truncated $and/$or array".into()))?;

    let mut children = Vec::new();
    for arr_elem in arr_iter {
        if arr_elem.type_byte != 0x03 {
            return Err(FilterParseError(
                "$and/$or array elements must be documents".into(),
            ));
        }
        // Recurse into the sub-document
        children.push(parse_filter_at(bytes, arr_elem.value_start)?);
    }

    if children.is_empty() {
        return Err(FilterParseError("$and/$or array must not be empty".into()));
    }

    Ok(make(children))
}

/// Parse a filter document at a given byte offset (for sub-documents in arrays).
fn parse_filter_at(bytes: &[u8], base: usize) -> Result<Expression<'_>, FilterParseError> {
    let iter = ElementIter::new(bytes, base)
        .ok_or_else(|| FilterParseError("truncated BSON sub-document".into()))?;

    let mut children = Vec::new();

    for elem in iter {
        match elem.key {
            "$and" => children.push(parse_logical_array(bytes, &elem, Expression::And)?),
            "$or" => children.push(parse_logical_array(bytes, &elem, Expression::Or)?),
            k if k.starts_with('$') => {
                return Err(FilterParseError(format!("unknown top-level operator: {k}")));
            }
            _ => children.push(parse_field_condition(bytes, &elem)?),
        }
    }

    if children.is_empty() {
        return Err(FilterParseError("empty filter document".into()));
    }

    if children.len() == 1 {
        Ok(children.pop().unwrap())
    } else {
        Ok(Expression::And(children))
    }
}

/// Parse a field condition: either implicit $eq or an operator sub-document.
fn parse_field_condition<'a>(
    bytes: &'a [u8],
    elem: &Element<'a>,
) -> Result<Expression<'a>, FilterParseError> {
    // If value is a document (0x03) whose first key starts with $, it's an operator doc
    if elem.type_byte == 0x03 {
        if let Some(mut sub_iter) = ElementIter::new(bytes, elem.value_start) {
            if let Some(first) = sub_iter.next() {
                if first.key.starts_with('$') {
                    return parse_operator_doc(bytes, elem.key, elem.value_start);
                }
            }
        }
    }

    // Otherwise: implicit $eq — zero alloc, borrows field + value directly
    let loc = FieldLocation {
        element_start: 0, // unused for value extraction
        type_byte: elem.type_byte,
        value_start: elem.value_start,
        element_end: elem.value_end,
    };
    let val = raw_bson::field_to_raw_bson_ref(bytes, &loc)
        .ok_or_else(|| FilterParseError("unsupported BSON type in filter value".into()))?;
    Ok(Expression::Eq(elem.key, val))
}

/// Parse an operator sub-document like `{ "$gt": 21, "$lte": 100 }`.
fn parse_operator_doc<'a>(
    bytes: &'a [u8],
    field: &'a str,
    doc_base: usize,
) -> Result<Expression<'a>, FilterParseError> {
    let iter = ElementIter::new(bytes, doc_base)
        .ok_or_else(|| FilterParseError("truncated operator sub-document".into()))?;

    // Check for $regex first (needs special handling with $options sibling)
    let peek_iter = ElementIter::new(bytes, doc_base).unwrap();
    for e in peek_iter {
        if e.key == "$regex" {
            return parse_regex(bytes, field, doc_base);
        }
    }

    let mut conditions: Vec<Expression<'a>> = Vec::new();

    for elem in iter {
        let expr = match elem.key {
            "$eq" | "$gt" | "$gte" | "$lt" | "$lte" => {
                let loc = FieldLocation {
                    element_start: 0,
                    type_byte: elem.type_byte,
                    value_start: elem.value_start,
                    element_end: elem.value_end,
                };
                let val = raw_bson::field_to_raw_bson_ref(bytes, &loc).ok_or_else(|| {
                    FilterParseError("unsupported BSON type in operator value".into())
                })?;
                match elem.key {
                    "$eq" => Expression::Eq(field, val),
                    "$gt" => Expression::Gt(field, val),
                    "$gte" => Expression::Gte(field, val),
                    "$lt" => Expression::Lt(field, val),
                    "$lte" => Expression::Lte(field, val),
                    _ => unreachable!(),
                }
            }
            "$exists" => {
                if elem.type_byte != 0x08 {
                    return Err(FilterParseError("$exists value must be a boolean".into()));
                }
                let b = bytes[elem.value_start] != 0;
                Expression::Exists(field, b)
            }
            "$options" => {
                return Err(FilterParseError("$options without $regex".into()));
            }
            k => return Err(FilterParseError(format!("unknown field operator: {k}"))),
        };
        conditions.push(expr);
    }

    match conditions.len() {
        0 => Err(FilterParseError("empty operator document".into())),
        1 => Ok(conditions.pop().unwrap()),
        _ => Ok(Expression::And(conditions)),
    }
}

/// Parse a `$regex` + optional `$options` sub-document.
fn parse_regex<'a>(
    bytes: &'a [u8],
    field: &'a str,
    doc_base: usize,
) -> Result<Expression<'a>, FilterParseError> {
    let iter = ElementIter::new(bytes, doc_base)
        .ok_or_else(|| FilterParseError("truncated $regex sub-document".into()))?;

    let mut pattern: Option<&str> = None;
    let mut options: Option<&str> = None;

    for elem in iter {
        match elem.key {
            "$regex" => {
                if elem.type_byte != 0x02 {
                    return Err(FilterParseError("$regex value must be a string".into()));
                }
                // Read string directly from bytes: i32(len) + utf8 + nul
                let len = i32::from_le_bytes(
                    bytes[elem.value_start..elem.value_start + 4]
                        .try_into()
                        .map_err(|_| FilterParseError("truncated string".into()))?,
                ) as usize;
                let s = std::str::from_utf8(
                    &bytes[elem.value_start + 4..elem.value_start + 4 + len - 1],
                )
                .map_err(|_| FilterParseError("invalid utf8 in $regex".into()))?;
                pattern = Some(s);
            }
            "$options" => {
                if elem.type_byte != 0x02 {
                    return Err(FilterParseError("$options value must be a string".into()));
                }
                let len = i32::from_le_bytes(
                    bytes[elem.value_start..elem.value_start + 4]
                        .try_into()
                        .map_err(|_| FilterParseError("truncated string".into()))?,
                ) as usize;
                let s = std::str::from_utf8(
                    &bytes[elem.value_start + 4..elem.value_start + 4 + len - 1],
                )
                .map_err(|_| FilterParseError("invalid utf8 in $options".into()))?;
                options = Some(s);
            }
            k => {
                return Err(FilterParseError(format!(
                    "unexpected key alongside $regex: {k}"
                )));
            }
        }
    }

    let pat = pattern.ok_or_else(|| FilterParseError("missing $regex pattern".into()))?;

    // Build final pattern with flags
    let full_pattern = if let Some(opts) = options {
        let mut prefix = String::with_capacity(4 + opts.len() + pat.len());
        prefix.push_str("(?");
        for ch in opts.chars() {
            match ch {
                'i' | 's' | 'm' | 'x' => prefix.push(ch),
                c => return Err(FilterParseError(format!("unknown regex option: {c}"))),
            }
        }
        prefix.push(')');
        prefix.push_str(pat);
        prefix
    } else {
        pat.to_string()
    };

    let re = Regex::new(&full_pattern)
        .map_err(|e| FilterParseError(format!("invalid regex pattern: {e}")))?;

    Ok(Expression::Regex(field, re))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::raw::RawBsonRef;
    use bson::rawdoc;

    #[test]
    fn bare_field_implicit_eq() {
        let doc = rawdoc! { "status": "active" };
        let expr = parse_filter(doc.as_bytes()).unwrap();
        match expr {
            Expression::Eq(f, v) => {
                assert_eq!(f, "status");
                assert_eq!(v, RawBsonRef::String("active"));
            }
            _ => panic!("expected Eq, got {:?}", expr),
        }
    }

    #[test]
    fn multiple_bare_fields_become_and() {
        let doc = rawdoc! { "status": "active", "age": 30_i32 };
        let expr = parse_filter(doc.as_bytes()).unwrap();
        match expr {
            Expression::And(children) => {
                assert_eq!(children.len(), 2);
                assert!(matches!(&children[0], Expression::Eq(f, _) if *f == "status"));
                assert!(matches!(&children[1], Expression::Eq(f, _) if *f == "age"));
            }
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn operator_doc_gte() {
        let doc = rawdoc! { "age": { "$gte": 21_i32 } };
        let expr = parse_filter(doc.as_bytes()).unwrap();
        match expr {
            Expression::Gte(f, v) => {
                assert_eq!(f, "age");
                assert_eq!(v, RawBsonRef::Int32(21));
            }
            _ => panic!("expected Gte, got {:?}", expr),
        }
    }

    #[test]
    fn multiple_operators_same_field() {
        let doc = rawdoc! { "score": { "$gt": 50_i32, "$lte": 100_i32 } };
        let expr = parse_filter(doc.as_bytes()).unwrap();
        match expr {
            Expression::And(children) => {
                assert_eq!(children.len(), 2);
                assert!(matches!(&children[0], Expression::Gt(..)));
                assert!(matches!(&children[1], Expression::Lte(..)));
            }
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn explicit_or() {
        let doc = rawdoc! { "$or": [{ "status": "active" }, { "status": "pending" }] };
        let expr = parse_filter(doc.as_bytes()).unwrap();
        match expr {
            Expression::Or(children) => assert_eq!(children.len(), 2),
            _ => panic!("expected Or"),
        }
    }

    #[test]
    fn explicit_and() {
        let doc = rawdoc! { "$and": [{ "a": 1_i32 }, { "b": 2_i32 }] };
        let expr = parse_filter(doc.as_bytes()).unwrap();
        match expr {
            Expression::And(children) => assert_eq!(children.len(), 2),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn nested_or_containing_and() {
        let doc = rawdoc! {
            "$or": [
                { "status": "active" },
                { "$and": [{ "score": { "$gt": 90_i32 } }, { "verified": true }] }
            ]
        };
        let expr = parse_filter(doc.as_bytes()).unwrap();
        match expr {
            Expression::Or(children) => {
                assert_eq!(children.len(), 2);
                assert!(matches!(&children[0], Expression::Eq(..)));
                assert!(matches!(&children[1], Expression::And(..)));
            }
            _ => panic!("expected Or"),
        }
    }

    #[test]
    fn regex_simple() {
        let doc = rawdoc! { "email": { "$regex": "^admin@" } };
        let expr = parse_filter(doc.as_bytes()).unwrap();
        match expr {
            Expression::Regex(f, re) => {
                assert_eq!(f, "email");
                assert_eq!(re.as_str(), "^admin@");
            }
            _ => panic!("expected Regex, got {:?}", expr),
        }
    }

    #[test]
    fn regex_with_options() {
        let doc = rawdoc! { "name": { "$regex": "^john", "$options": "i" } };
        let expr = parse_filter(doc.as_bytes()).unwrap();
        match expr {
            Expression::Regex(f, re) => {
                assert_eq!(f, "name");
                assert_eq!(re.as_str(), "(?i)^john");
            }
            _ => panic!("expected Regex"),
        }
    }

    #[test]
    fn exists_true() {
        let doc = rawdoc! { "email": { "$exists": true } };
        let expr = parse_filter(doc.as_bytes()).unwrap();
        assert!(matches!(expr, Expression::Exists(f, true) if f == "email"));
    }

    #[test]
    fn exists_false() {
        let doc = rawdoc! { "deleted_at": { "$exists": false } };
        let expr = parse_filter(doc.as_bytes()).unwrap();
        assert!(matches!(expr, Expression::Exists(f, false) if f == "deleted_at"));
    }

    #[test]
    fn unknown_top_level_operator_errors() {
        let doc = rawdoc! { "$nor": [{ "a": 1_i32 }] };
        let err = parse_filter(doc.as_bytes()).unwrap_err();
        assert!(err.0.contains("unknown top-level operator"), "{}", err.0);
    }

    #[test]
    fn unknown_field_operator_errors() {
        let doc = rawdoc! { "age": { "$between": 10_i32 } };
        let err = parse_filter(doc.as_bytes()).unwrap_err();
        assert!(err.0.contains("unknown field operator"), "{}", err.0);
    }

    #[test]
    fn empty_doc_errors() {
        let doc = rawdoc! {};
        let err = parse_filter(doc.as_bytes()).unwrap_err();
        assert!(err.0.contains("empty"), "{}", err.0);
    }

    #[test]
    fn explicit_eq_operator() {
        let doc = rawdoc! { "status": { "$eq": "active" } };
        let expr = parse_filter(doc.as_bytes()).unwrap();
        match expr {
            Expression::Eq(f, v) => {
                assert_eq!(f, "status");
                assert_eq!(v, RawBsonRef::String("active"));
            }
            _ => panic!("expected Eq"),
        }
    }

    #[test]
    fn embedded_doc_as_eq_value() {
        // A sub-document that doesn't start with $ is an implicit $eq value
        let doc = rawdoc! { "address": { "city": "Austin", "state": "TX" } };
        let expr = parse_filter(doc.as_bytes()).unwrap();
        assert!(matches!(expr, Expression::Eq(f, _) if f == "address"));
    }

    #[test]
    fn regex_invalid_pattern_errors() {
        let doc = rawdoc! { "name": { "$regex": "[invalid" } };
        let err = parse_filter(doc.as_bytes()).unwrap_err();
        assert!(err.0.contains("invalid regex"), "{}", err.0);
    }

    #[test]
    fn regex_unexpected_sibling_key_errors() {
        let doc = rawdoc! { "name": { "$regex": "foo", "$gt": 1_i32 } };
        let err = parse_filter(doc.as_bytes()).unwrap_err();
        assert!(
            err.0.contains("unexpected key alongside $regex"),
            "{}",
            err.0
        );
    }
}
