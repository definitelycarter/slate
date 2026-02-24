use bson::raw::RawBsonRef;
use bson::{Bson, RawDocumentBuf};
use regex::Regex;

use super::expression::Expression;

/// Parse error for filter documents.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterParseError(pub String);

impl std::fmt::Display for FilterParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "filter parse error: {}", self.0)
    }
}

impl std::error::Error for FilterParseError {}

/// Convert a RawBsonRef directly to an owned Bson value.
fn to_bson(raw: RawBsonRef<'_>) -> Result<Bson, FilterParseError> {
    match raw {
        RawBsonRef::Double(f) => Ok(Bson::Double(f)),
        RawBsonRef::String(s) => Ok(Bson::String(s.to_string())),
        RawBsonRef::Boolean(b) => Ok(Bson::Boolean(b)),
        RawBsonRef::Int32(i) => Ok(Bson::Int32(i)),
        RawBsonRef::Int64(i) => Ok(Bson::Int64(i)),
        RawBsonRef::Null => Ok(Bson::Null),
        RawBsonRef::DateTime(dt) => Ok(Bson::DateTime(dt)),
        RawBsonRef::ObjectId(oid) => Ok(Bson::ObjectId(oid)),
        RawBsonRef::Document(d) => Ok(Bson::Document(
            d.to_raw_document_buf()
                .to_document()
                .map_err(|e| FilterParseError(format!("invalid embedded document: {e}")))?,
        )),
        RawBsonRef::Array(a) => {
            let raw_buf = a.to_raw_array_buf();
            let mut items = Vec::new();
            for result in raw_buf.into_iter() {
                let elem = result
                    .map_err(|e| FilterParseError(format!("invalid array element: {e}")))?;
                items.push(to_bson(elem)?);
            }
            Ok(Bson::Array(items))
        }
        other => Err(FilterParseError(format!(
            "unsupported BSON type in filter: {:?}",
            other.element_type()
        ))),
    }
}

/// Parse a RawDocumentBuf filter into an owned Expression tree.
///
/// Iterates the raw bytes directly — field keys are read as `&str` (zero-copy)
/// and converted to owned `String` only when building Expression leaves.
/// Values are converted from `RawBsonRef` to owned `Bson` once per leaf.
pub fn parse_filter(doc: &RawDocumentBuf) -> Result<Expression, FilterParseError> {
    let mut children = Vec::new();

    for result in doc.iter() {
        let (key, value) = result
            .map_err(|e| FilterParseError(format!("malformed BSON: {e}")))?;

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
fn parse_logical_array(
    value: RawBsonRef<'_>,
    make: fn(Vec<Expression>) -> Expression,
) -> Result<Expression, FilterParseError> {
    let arr = match value {
        RawBsonRef::Array(arr) => arr,
        _ => return Err(FilterParseError("$and/$or value must be an array".into())),
    };

    let mut children = Vec::new();
    for result in arr.into_iter() {
        let elem = result.map_err(|e| FilterParseError(format!("malformed BSON: {e}")))?;
        match elem {
            RawBsonRef::Document(sub_doc) => {
                let buf = sub_doc.to_raw_document_buf();
                children.push(parse_filter(&buf)?);
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
fn parse_field_condition(
    field: &str,
    value: RawBsonRef<'_>,
) -> Result<Expression, FilterParseError> {
    // If value is a document whose first key starts with $, it's an operator doc
    if let RawBsonRef::Document(sub_doc) = value {
        if let Some(Ok((first_key, _))) = sub_doc.iter().next() {
            if first_key.starts_with('$') {
                return parse_operator_doc(field, sub_doc);
            }
        }
    }

    // Otherwise: implicit $eq
    Ok(Expression::Eq(field.to_string(), to_bson(value)?))
}

/// Parse an operator sub-document like `{ "$gt": 21, "$lte": 100 }`.
fn parse_operator_doc(
    field: &str,
    doc: &bson::RawDocument,
) -> Result<Expression, FilterParseError> {
    // Check for $regex first (needs special handling with $options sibling)
    for result in doc.iter() {
        let (key, _) = result.map_err(|e| FilterParseError(format!("malformed BSON: {e}")))?;
        if key == "$regex" {
            return parse_regex(field, doc);
        }
    }

    let mut conditions: Vec<Expression> = Vec::new();

    for result in doc.iter() {
        let (key, value) =
            result.map_err(|e| FilterParseError(format!("malformed BSON: {e}")))?;
        let expr = match key {
            "$eq" => Expression::Eq(field.to_string(), to_bson(value)?),
            "$gt" => Expression::Gt(field.to_string(), to_bson(value)?),
            "$gte" => Expression::Gte(field.to_string(), to_bson(value)?),
            "$lt" => Expression::Lt(field.to_string(), to_bson(value)?),
            "$lte" => Expression::Lte(field.to_string(), to_bson(value)?),
            "$exists" => match value {
                RawBsonRef::Boolean(b) => Expression::Exists(field.to_string(), b),
                _ => {
                    return Err(FilterParseError("$exists value must be a boolean".into()));
                }
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
fn parse_regex(field: &str, doc: &bson::RawDocument) -> Result<Expression, FilterParseError> {
    let mut pattern: Option<String> = None;
    let mut options: Option<String> = None;

    for result in doc.iter() {
        let (key, value) =
            result.map_err(|e| FilterParseError(format!("malformed BSON: {e}")))?;
        match key {
            "$regex" => match value {
                RawBsonRef::String(s) => pattern = Some(s.to_string()),
                _ => {
                    return Err(FilterParseError("$regex value must be a string".into()));
                }
            },
            "$options" => match value {
                RawBsonRef::String(s) => options = Some(s.to_string()),
                _ => {
                    return Err(FilterParseError("$options value must be a string".into()));
                }
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
        prefix.push_str(&pat);
        prefix
    } else {
        pat
    };

    let re = Regex::new(&full_pattern)
        .map_err(|e| FilterParseError(format!("invalid regex pattern: {e}")))?;

    Ok(Expression::Regex(field.to_string(), re))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    /// Helper: create a RawDocumentBuf from a doc! macro.
    fn raw(doc: bson::Document) -> RawDocumentBuf {
        RawDocumentBuf::from_document(&doc).unwrap()
    }

    #[test]
    fn bare_field_implicit_eq() {
        let doc = raw(doc! { "status": "active" });
        let expr = parse_filter(&doc).unwrap();
        match expr {
            Expression::Eq(f, v) => {
                assert_eq!(f, "status");
                assert_eq!(v, Bson::String("active".into()));
            }
            _ => panic!("expected Eq, got {:?}", expr),
        }
    }

    #[test]
    fn multiple_bare_fields_become_and() {
        let doc = raw(doc! { "status": "active", "age": 30_i32 });
        let expr = parse_filter(&doc).unwrap();
        match expr {
            Expression::And(children) => {
                assert_eq!(children.len(), 2);
                assert!(matches!(&children[0], Expression::Eq(f, _) if f == "status"));
                assert!(matches!(&children[1], Expression::Eq(f, _) if f == "age"));
            }
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn operator_doc_gte() {
        let doc = raw(doc! { "age": { "$gte": 21_i32 } });
        let expr = parse_filter(&doc).unwrap();
        match expr {
            Expression::Gte(f, v) => {
                assert_eq!(f, "age");
                assert_eq!(v, Bson::Int32(21));
            }
            _ => panic!("expected Gte, got {:?}", expr),
        }
    }

    #[test]
    fn multiple_operators_same_field() {
        let doc = raw(doc! { "score": { "$gt": 50_i32, "$lte": 100_i32 } });
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
        let doc = raw(doc! { "$or": [{ "status": "active" }, { "status": "pending" }] });
        let expr = parse_filter(&doc).unwrap();
        match expr {
            Expression::Or(children) => assert_eq!(children.len(), 2),
            _ => panic!("expected Or"),
        }
    }

    #[test]
    fn explicit_and() {
        let doc = raw(doc! { "$and": [{ "a": 1_i32 }, { "b": 2_i32 }] });
        let expr = parse_filter(&doc).unwrap();
        match expr {
            Expression::And(children) => assert_eq!(children.len(), 2),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn nested_or_containing_and() {
        let doc = raw(doc! {
            "$or": [
                { "status": "active" },
                { "$and": [{ "score": { "$gt": 90_i32 } }, { "verified": true }] }
            ]
        });
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
        let doc = raw(doc! { "email": { "$regex": "^admin@" } });
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
        let doc = raw(doc! { "name": { "$regex": "^john", "$options": "i" } });
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
        let doc = raw(doc! { "email": { "$exists": true } });
        let expr = parse_filter(&doc).unwrap();
        assert!(matches!(expr, Expression::Exists(ref f, true) if f == "email"));
    }

    #[test]
    fn exists_false() {
        let doc = raw(doc! { "deleted_at": { "$exists": false } });
        let expr = parse_filter(&doc).unwrap();
        assert!(matches!(expr, Expression::Exists(ref f, false) if f == "deleted_at"));
    }

    #[test]
    fn unknown_top_level_operator_errors() {
        let doc = raw(doc! { "$nor": [{ "a": 1_i32 }] });
        let err = parse_filter(&doc).unwrap_err();
        assert!(err.0.contains("unknown top-level operator"), "{}", err.0);
    }

    #[test]
    fn unknown_field_operator_errors() {
        let doc = raw(doc! { "age": { "$between": 10_i32 } });
        let err = parse_filter(&doc).unwrap_err();
        assert!(err.0.contains("unknown field operator"), "{}", err.0);
    }

    #[test]
    fn empty_doc_errors() {
        let doc = raw(doc! {});
        let err = parse_filter(&doc).unwrap_err();
        assert!(err.0.contains("empty"), "{}", err.0);
    }

    #[test]
    fn explicit_eq_operator() {
        let doc = raw(doc! { "status": { "$eq": "active" } });
        let expr = parse_filter(&doc).unwrap();
        match expr {
            Expression::Eq(f, v) => {
                assert_eq!(f, "status");
                assert_eq!(v, Bson::String("active".into()));
            }
            _ => panic!("expected Eq"),
        }
    }

    #[test]
    fn embedded_doc_as_eq_value() {
        let doc = raw(doc! { "address": { "city": "Austin", "state": "TX" } });
        let expr = parse_filter(&doc).unwrap();
        assert!(matches!(expr, Expression::Eq(ref f, _) if f == "address"));
    }

    #[test]
    fn regex_invalid_pattern_errors() {
        let doc = raw(doc! { "name": { "$regex": "[invalid" } });
        let err = parse_filter(&doc).unwrap_err();
        assert!(err.0.contains("invalid regex"), "{}", err.0);
    }

    #[test]
    fn regex_unexpected_sibling_key_errors() {
        let doc = raw(doc! { "name": { "$regex": "foo", "$gt": 1_i32 } });
        let err = parse_filter(&doc).unwrap_err();
        assert!(
            err.0.contains("unexpected key alongside $regex"),
            "{}",
            err.0
        );
    }
}
