use bson::RawDocument;
use bson::raw::RawBsonRef;
use regex::Regex;

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
/// Follows MongoDB query semantics:
/// - Top-level document is an implicit AND of all entries
/// - `{ "field": value }` is implicit `$eq`
/// - `{ "field": { "$gt": v } }` uses operator sub-documents
/// - `{ "$or": [...] }` / `{ "$and": [...] }` for explicit logical ops
/// - `{ "field": { "$regex": "pattern", "$options": "i" } }` for regex
/// - `{ "field": { "$exists": true } }` for field existence checks
pub fn parse_filter(doc: &RawDocument) -> Result<Expression<'_>, FilterParseError> {
    let mut children = Vec::new();

    for result in doc.iter() {
        let (key, value) = result.map_err(|e| FilterParseError(format!("malformed BSON: {e}")))?;

        match key {
            "$and" => children.push(parse_logical_array(value, Expression::And)?),
            "$or" => children.push(parse_logical_array(value, Expression::Or)?),
            k if k.starts_with('$') => {
                return Err(FilterParseError(format!("unknown top-level operator: {k}")));
            }
            _ => children.push(parse_field_condition(key, value)?),
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
    value: RawBsonRef<'a>,
    make: fn(Vec<Expression<'a>>) -> Expression<'a>,
) -> Result<Expression<'a>, FilterParseError> {
    let arr = match value {
        RawBsonRef::Array(a) => a,
        _ => return Err(FilterParseError("$and/$or value must be an array".into())),
    };

    let mut children = Vec::new();
    for elem in arr {
        let elem = elem.map_err(|e| FilterParseError(format!("malformed BSON array: {e}")))?;
        match elem {
            RawBsonRef::Document(sub_doc) => {
                children.push(parse_filter(sub_doc)?);
            }
            _ => {
                return Err(FilterParseError(
                    "$and/$or array elements must be documents".into(),
                ));
            }
        }
    }

    if children.is_empty() {
        return Err(FilterParseError("$and/$or array must not be empty".into()));
    }

    Ok(make(children))
}

/// Parse a field condition: either implicit $eq or an operator sub-document.
fn parse_field_condition<'a>(
    field: &'a str,
    value: RawBsonRef<'a>,
) -> Result<Expression<'a>, FilterParseError> {
    // If value is a document whose first key starts with $, it's an operator doc
    if let RawBsonRef::Document(sub_doc) = value {
        let mut iter = sub_doc.iter();
        if let Some(Ok((first_key, _))) = iter.next() {
            if first_key.starts_with('$') {
                return parse_operator_doc(field, sub_doc);
            }
        }
    }

    // Otherwise: implicit $eq — zero alloc, borrows field + value directly
    Ok(Expression::Eq(field, value))
}

/// Parse an operator sub-document like `{ "$gt": 21, "$lte": 100 }`.
fn parse_operator_doc<'a>(
    field: &'a str,
    doc: &'a RawDocument,
) -> Result<Expression<'a>, FilterParseError> {
    let mut conditions: Vec<Expression<'a>> = Vec::new();

    for result in doc.iter() {
        let (op_key, op_value) =
            result.map_err(|e| FilterParseError(format!("malformed BSON: {e}")))?;

        // $regex needs special handling (consumes $options sibling)
        if op_key == "$regex" {
            return parse_regex(field, doc);
        }

        let expr = match op_key {
            "$eq" => Expression::Eq(field, op_value),
            "$gt" => Expression::Gt(field, op_value),
            "$gte" => Expression::Gte(field, op_value),
            "$lt" => Expression::Lt(field, op_value),
            "$lte" => Expression::Lte(field, op_value),
            "$exists" => match op_value {
                RawBsonRef::Boolean(b) => Expression::Exists(field, b),
                _ => return Err(FilterParseError("$exists value must be a boolean".into())),
            },
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
    field: &'a str,
    doc: &'a RawDocument,
) -> Result<Expression<'a>, FilterParseError> {
    let mut pattern: Option<&str> = None;
    let mut options: Option<&str> = None;

    for result in doc.iter() {
        let (key, value) = result.map_err(|e| FilterParseError(format!("malformed BSON: {e}")))?;
        match key {
            "$regex" => match value {
                RawBsonRef::String(s) => pattern = Some(s),
                _ => return Err(FilterParseError("$regex value must be a string".into())),
            },
            "$options" => match value {
                RawBsonRef::String(s) => options = Some(s),
                _ => return Err(FilterParseError("$options value must be a string".into())),
            },
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
    use bson::rawdoc;

    #[test]
    fn bare_field_implicit_eq() {
        let doc = rawdoc! { "status": "active" };
        let expr = parse_filter(&doc).unwrap();
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
        let expr = parse_filter(&doc).unwrap();
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
        let expr = parse_filter(&doc).unwrap();
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
        let expr = parse_filter(&doc).unwrap();
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
        let expr = parse_filter(&doc).unwrap();
        match expr {
            Expression::Or(children) => assert_eq!(children.len(), 2),
            _ => panic!("expected Or"),
        }
    }

    #[test]
    fn explicit_and() {
        let doc = rawdoc! { "$and": [{ "a": 1_i32 }, { "b": 2_i32 }] };
        let expr = parse_filter(&doc).unwrap();
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
        let expr = parse_filter(&doc).unwrap();
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
        let expr = parse_filter(&doc).unwrap();
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
        let expr = parse_filter(&doc).unwrap();
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
        let expr = parse_filter(&doc).unwrap();
        assert!(matches!(expr, Expression::Exists(f, true) if f == "email"));
    }

    #[test]
    fn exists_false() {
        let doc = rawdoc! { "deleted_at": { "$exists": false } };
        let expr = parse_filter(&doc).unwrap();
        assert!(matches!(expr, Expression::Exists(f, false) if f == "deleted_at"));
    }

    #[test]
    fn unknown_top_level_operator_errors() {
        let doc = rawdoc! { "$nor": [{ "a": 1_i32 }] };
        let err = parse_filter(&doc).unwrap_err();
        assert!(err.0.contains("unknown top-level operator"), "{}", err.0);
    }

    #[test]
    fn unknown_field_operator_errors() {
        let doc = rawdoc! { "age": { "$between": 10_i32 } };
        let err = parse_filter(&doc).unwrap_err();
        assert!(err.0.contains("unknown field operator"), "{}", err.0);
    }

    #[test]
    fn empty_doc_errors() {
        let doc = rawdoc! {};
        let err = parse_filter(&doc).unwrap_err();
        assert!(err.0.contains("empty"), "{}", err.0);
    }

    #[test]
    fn explicit_eq_operator() {
        let doc = rawdoc! { "status": { "$eq": "active" } };
        let expr = parse_filter(&doc).unwrap();
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
        let expr = parse_filter(&doc).unwrap();
        assert!(matches!(expr, Expression::Eq(f, _) if f == "address"));
    }

    #[test]
    fn regex_invalid_pattern_errors() {
        let doc = rawdoc! { "name": { "$regex": "[invalid" } };
        let err = parse_filter(&doc).unwrap_err();
        assert!(err.0.contains("invalid regex"), "{}", err.0);
    }

    #[test]
    fn regex_unexpected_sibling_key_errors() {
        let doc = rawdoc! { "name": { "$regex": "foo", "$gt": 1_i32 } };
        let err = parse_filter(&doc).unwrap_err();
        assert!(
            err.0.contains("unexpected key alongside $regex"),
            "{}",
            err.0
        );
    }
}
