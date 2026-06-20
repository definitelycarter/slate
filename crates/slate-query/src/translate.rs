//! Mongo-style find → [`slate_ast::Query`] translation.
//!
//! This is the second query *surface* (the first being SQL text in
//! `slate-sql`): it turns a `(filter, FindOptions)` request — a document of
//! `$`-operators plus sort/skip/take/projection — into the shared query AST,
//! which `slate-planner` then lowers exactly as it lowers a parsed SQL query.
//! There is no separate planner for find; the filter becomes a `WHERE`
//! predicate and goes through the one sargability pass.
//!
//! The synthetic alias the request binds to is always [`ALIAS`] (`c`), so a
//! field path `"a.b"` becomes `c.a.b`.

use bson::raw::{RawBsonRef, RawDocument};
use slate_ast::{
    BinOp, FromClause, FromSource, Literal, OrderByItem, Query, ScalarExpr, SelectClause,
    SortDirection, UnaryOp,
};

use crate::error::{Result, TranslateError};
use crate::{FindOptions, SortDirection as FindSort};

/// The alias a find request binds its documents to.
pub const ALIAS: &str = "c";

/// Translate a full find request into a query AST.
///
/// The same `filter` translation is reused by the write APIs (which select
/// their documents with a find predicate), so this is the single definition of
/// what a Mongo filter *means*.
pub fn find_to_query(filter: &RawDocument, options: &FindOptions) -> Result<Query> {
    let filter = translate_filter(filter)?;

    let order_by = options
        .sort
        .iter()
        .map(|s| OrderByItem {
            expr: path(&s.field),
            direction: match s.direction {
                FindSort::Asc => SortDirection::Asc,
                FindSort::Desc => SortDirection::Desc,
            },
        })
        .collect();

    Ok(Query {
        select: projection(options.columns.as_deref()),
        from: FromClause {
            source: FromSource::ImplicitContainer {
                alias: ALIAS.into(),
            },
            joins: Vec::new(),
        },
        filter,
        order_by,
        offset: options.skip.map(|n| n as u64),
        limit: options.take.map(|n| n as u64),
    })
}

/// Translate a find filter document into a `WHERE` predicate.
///
/// `Ok(None)` is the match-all (empty) filter; `Ok(Some(_))` a predicate;
/// `Err` an unsupported or malformed filter. Exposed for the write paths.
pub fn translate_filter(doc: &RawDocument) -> Result<Option<ScalarExpr>> {
    let mut conjuncts = Vec::new();
    for entry in doc.iter() {
        let (key, value) = entry.map_err(malformed)?;
        match key.as_str() {
            "$and" => extend(&mut conjuncts, translate_logical(value, BinOp::And)?),
            "$or" => extend(&mut conjuncts, translate_logical(value, BinOp::Or)?),
            k if k.starts_with('$') => {
                return Err(TranslateError::Unsupported(format!(
                    "top-level operator {k}"
                )));
            }
            field => conjuncts.push(translate_field(field, value)?),
        }
    }
    Ok(fold(conjuncts, BinOp::And))
}

/// `SELECT VALUE` clause: identity (whole document) when no projection,
/// otherwise a nested object built from the pk plus the selected columns. A
/// dotted column nests — `"address.city"` projects `{address: {city: ...}}`,
/// and columns sharing a prefix merge under it — matching v1/Mongo, not a flat
/// `{"address.city": ...}` key.
fn projection(columns: Option<&[String]>) -> SelectClause {
    let Some(cols) = columns else {
        return SelectClause::Value(ScalarExpr::Identifier(ALIAS.into()));
    };
    // The pk is always included, mirroring v1.
    let mut tree = PathTree::default();
    for col in std::iter::once("_id").chain(cols.iter().map(|s| s.as_str())) {
        tree.insert(&col.split('.').collect::<Vec<_>>());
    }
    SelectClause::Value(tree.to_object(""))
}

/// An ordered tree of projected field paths, built by splitting columns on `.`.
#[derive(Default)]
struct PathTree {
    order: Vec<String>,
    children: std::collections::HashMap<String, PathTree>,
}

impl PathTree {
    fn insert(&mut self, segments: &[&str]) {
        let Some((head, rest)) = segments.split_first() else {
            return;
        };
        if !self.children.contains_key(*head) {
            self.order.push((*head).to_string());
            self.children
                .insert((*head).to_string(), PathTree::default());
        }
        if let Some(child) = self.children.get_mut(*head) {
            child.insert(rest);
        }
    }

    /// Convert to an object expression. `prefix` is the accumulated dotted path
    /// from the root; a leaf (no children) projects `c.<prefix>`, a branch
    /// projects a nested object over its children.
    fn to_object(&self, prefix: &str) -> ScalarExpr {
        if self.children.is_empty() {
            return path(prefix);
        }
        let fields = self
            .order
            .iter()
            .filter_map(|seg| {
                let child = self.children.get(seg)?;
                let child_prefix = if prefix.is_empty() {
                    seg.clone()
                } else {
                    format!("{prefix}.{seg}")
                };
                Some((seg.clone(), child.to_object(&child_prefix)))
            })
            .collect();
        ScalarExpr::Object(fields)
    }
}

/// `$and` / `$or`: fold a list of sub-filters. Empty list contributes nothing.
fn translate_logical(value: RawBsonRef, op: BinOp) -> Result<Option<ScalarExpr>> {
    let RawBsonRef::Array(arr) = value else {
        return Err(TranslateError::Malformed(
            "$and/$or expects an array".into(),
        ));
    };
    let mut parts = Vec::new();
    for elem in arr.into_iter() {
        let RawBsonRef::Document(sub) = elem.map_err(malformed)? else {
            return Err(TranslateError::Malformed(
                "$and/$or operands must be documents".into(),
            ));
        };
        if let Some(p) = translate_filter(sub)? {
            parts.push(p);
        }
    }
    Ok(fold(parts, op))
}

/// A `{field: ...}` clause — either an operator sub-document or implicit
/// equality.
fn translate_field(field: &str, value: RawBsonRef) -> Result<ScalarExpr> {
    if let RawBsonRef::Document(sub) = value
        && let Some(first) = sub.iter().next()
        && first.map_err(malformed)?.0.as_str().starts_with('$')
    {
        return translate_operators(field, sub);
    }
    eq_or_contains(field, value)
}

/// Mongo `{field: value}` equality, which matches a scalar field *or* an array
/// field containing the value — `c.field = value OR ARRAY_CONTAINS(c.field, value)`.
///
/// The `null` value is special: Mongo's `{field: null}` matches an explicit
/// null, an array containing null, **and a missing field**, so it also tests
/// `NOT IS_DEFINED(c.field)`.
fn eq_or_contains(field: &str, value: RawBsonRef) -> Result<ScalarExpr> {
    // An explicit multikey path (`tags.[]`, `items.[].sku`) is array-membership,
    // tested via MULTIKEY_EQ (which the planner can match to a `.[]` index).
    if field.contains("[]") {
        return Ok(multikey_eq(field, value)?);
    }
    // `literal` is built twice rather than cloned (both are cheap leaf nodes).
    let eq = binary(BinOp::Eq, path(field), literal(value)?);
    let contains = ScalarExpr::Function {
        name: "ARRAY_CONTAINS".into(),
        args: vec![path(field), literal(value)?],
    };
    let matches = binary(BinOp::Or, eq, contains);

    if matches!(value, RawBsonRef::Null) {
        let missing = ScalarExpr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(ScalarExpr::Function {
                name: "IS_DEFINED".into(),
                args: vec![path(field)],
            }),
        };
        Ok(binary(BinOp::Or, missing, matches))
    } else {
        Ok(matches)
    }
}

/// Explicit multikey equality `{field.[]: value}` → a [`ScalarExpr::MultikeyEq`]
/// carrying the verbatim `.[]` path (so the planner can match it to a `.[]`
/// index by name).
fn multikey_eq(field: &str, value: RawBsonRef) -> Result<ScalarExpr> {
    Ok(ScalarExpr::MultikeyEq {
        base: Box::new(ScalarExpr::Identifier(ALIAS.into())),
        index_path: field.to_string(),
        value: Box::new(literal(value)?),
    })
}

/// A `{field: {$op: v, ...}}` operator sub-document.
fn translate_operators(field: &str, doc: &RawDocument) -> Result<ScalarExpr> {
    // `$regex` (with optional `$options`) is special: it maps to REGEXMATCH and
    // doesn't compose with other operators in the same sub-document.
    if let Some(expr) = translate_regex(field, doc)? {
        return Ok(expr);
    }

    let mut conds = Vec::new();
    for entry in doc.iter() {
        let (op, value) = entry.map_err(malformed)?;
        // Only equality is supported on an explicit multikey path; range/exists
        // over `.[]` falls back to v1.
        let multikey = field.contains("[]");
        let cond = match op.as_str() {
            "$eq" => eq_or_contains(field, value)?,
            "$gt" if !multikey => binary(BinOp::Gt, path(field), literal(value)?),
            "$gte" if !multikey => binary(BinOp::Gte, path(field), literal(value)?),
            "$lt" if !multikey => binary(BinOp::Lt, path(field), literal(value)?),
            "$lte" if !multikey => binary(BinOp::Lte, path(field), literal(value)?),
            "$exists" if !multikey => translate_exists(field, value)?,
            other => {
                return Err(TranslateError::Unsupported(format!(
                    "operator {other} on field '{field}'"
                )));
            }
        };
        conds.push(cond);
    }
    fold(conds, BinOp::And)
        .ok_or_else(|| TranslateError::Malformed(format!("empty operator document for '{field}'")))
}

/// `$regex`/`$options` → `REGEXMATCH(c.field, "(?<opts>)<pat>")`, or `None` if
/// this sub-document has no `$regex`.
fn translate_regex(field: &str, doc: &RawDocument) -> Result<Option<ScalarExpr>> {
    let mut pattern: Option<String> = None;
    let mut options: Option<String> = None;
    let mut has_other = false;
    for entry in doc.iter() {
        let (op, value) = entry.map_err(malformed)?;
        match op.as_str() {
            "$regex" => match value {
                RawBsonRef::String(s) => pattern = Some(s.to_string()),
                _ => return Err(TranslateError::Malformed("$regex expects a string".into())),
            },
            "$options" => match value {
                RawBsonRef::String(s) => options = Some(s.to_string()),
                _ => {
                    return Err(TranslateError::Malformed(
                        "$options expects a string".into(),
                    ));
                }
            },
            _ => has_other = true,
        }
    }
    let Some(pat) = pattern else {
        return Ok(None);
    };
    if has_other {
        return Err(TranslateError::Unsupported(
            "$regex combined with other operators".into(),
        ));
    }
    let full = match options {
        Some(opts) => format!("(?{opts}){pat}"),
        None => pat,
    };
    Ok(Some(ScalarExpr::Function {
        name: "REGEXMATCH".into(),
        args: vec![path(field), ScalarExpr::Literal(Literal::Str(full))],
    }))
}

/// `$exists: bool` → `IS_DEFINED(c.field)` (negated when false).
fn translate_exists(field: &str, value: RawBsonRef) -> Result<ScalarExpr> {
    let RawBsonRef::Boolean(b) = value else {
        return Err(TranslateError::Malformed(
            "$exists expects a boolean".into(),
        ));
    };
    let is_def = ScalarExpr::Function {
        name: "IS_DEFINED".into(),
        args: vec![path(field)],
    };
    Ok(if b {
        is_def
    } else {
        ScalarExpr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(is_def),
        }
    })
}

/// Build the member-access path `c.a.b.c` for a dotted field.
fn path(field: &str) -> ScalarExpr {
    let mut expr = ScalarExpr::Identifier(ALIAS.into());
    for part in field.split('.') {
        expr = ScalarExpr::Member {
            base: Box::new(expr),
            field: part.into(),
        };
    }
    expr
}

/// A filter value → a materialized AST value, preserving its exact BSON type
/// (so `Int32` stays `Int32`, which an index bound depends on, and `DateTime`/
/// `ObjectId` are expressible). Documents/arrays as comparison operands are
/// rejected — they aren't a scalar a predicate can compare against.
fn literal(value: RawBsonRef) -> Result<ScalarExpr> {
    if matches!(value, RawBsonRef::Document(_) | RawBsonRef::Array(_)) {
        return Err(TranslateError::Unsupported(format!(
            "non-scalar literal of type {:?}",
            value.element_type()
        )));
    }
    let b = bson::Bson::try_from(value)
        .map_err(|e| TranslateError::Malformed(format!("could not read filter value: {e}")))?;
    Ok(ScalarExpr::Value(b))
}

fn binary(op: BinOp, lhs: ScalarExpr, rhs: ScalarExpr) -> ScalarExpr {
    ScalarExpr::Binary {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
    }
}

/// Combine predicates with `op`, or `None` if there are none.
fn fold(parts: Vec<ScalarExpr>, op: BinOp) -> Option<ScalarExpr> {
    let mut it = parts.into_iter();
    let first = it.next()?;
    Some(it.fold(first, |acc, e| binary(op, acc, e)))
}

fn extend(conjuncts: &mut Vec<ScalarExpr>, group: Option<ScalarExpr>) {
    if let Some(p) = group {
        conjuncts.push(p);
    }
}

fn malformed(e: bson::error::Error) -> TranslateError {
    TranslateError::Malformed(format!("could not read filter: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Sort;
    use bson::{Bson, Document, RawDocumentBuf, doc};

    fn tf(d: Document) -> Result<Option<ScalarExpr>> {
        let raw = RawDocumentBuf::try_from(&d).unwrap();
        translate_filter(&raw)
    }

    /// `doc!` stores small integers as `Int32`, and the translator preserves
    /// that exact type via `ScalarExpr::Value`.
    fn lit_i(n: i32) -> ScalarExpr {
        ScalarExpr::Value(Bson::Int32(n))
    }

    #[test]
    fn empty_filter_is_match_all() {
        assert_eq!(tf(doc! {}).unwrap(), None);
    }

    #[test]
    fn implicit_equality_is_eq_or_array_contains() {
        // {age: 30}  →  c.age = 30 OR ARRAY_CONTAINS(c.age, 30)
        let expr = tf(doc! { "age": 30 }).unwrap().unwrap();
        let expected = binary(
            BinOp::Or,
            binary(BinOp::Eq, path("age"), lit_i(30)),
            ScalarExpr::Function {
                name: "ARRAY_CONTAINS".into(),
                args: vec![path("age"), lit_i(30)],
            },
        );
        assert_eq!(expr, expected);
    }

    #[test]
    fn comparison_operators() {
        assert_eq!(
            tf(doc! { "age": { "$gt": 30 } }).unwrap().unwrap(),
            binary(BinOp::Gt, path("age"), lit_i(30))
        );
        assert_eq!(
            tf(doc! { "age": { "$lte": 5 } }).unwrap().unwrap(),
            binary(BinOp::Lte, path("age"), lit_i(5))
        );
    }

    #[test]
    fn dotted_path() {
        let expr = tf(doc! { "address.city": { "$gt": 1 } }).unwrap().unwrap();
        // path("address.city") == c.address.city
        assert_eq!(expr, binary(BinOp::Gt, path("address.city"), lit_i(1)));
    }

    #[test]
    fn and_of_two_fields() {
        let expr = tf(doc! { "a": { "$gt": 1 }, "b": { "$lt": 2 } })
            .unwrap()
            .unwrap();
        let expected = binary(
            BinOp::And,
            binary(BinOp::Gt, path("a"), lit_i(1)),
            binary(BinOp::Lt, path("b"), lit_i(2)),
        );
        assert_eq!(expr, expected);
    }

    #[test]
    fn or_operator() {
        let expr = tf(doc! { "$or": [ { "a": { "$gt": 1 } }, { "b": { "$lt": 2 } } ] })
            .unwrap()
            .unwrap();
        let expected = binary(
            BinOp::Or,
            binary(BinOp::Gt, path("a"), lit_i(1)),
            binary(BinOp::Lt, path("b"), lit_i(2)),
        );
        assert_eq!(expr, expected);
    }

    #[test]
    fn exists_true_and_false() {
        let is_def = ScalarExpr::Function {
            name: "IS_DEFINED".into(),
            args: vec![path("f")],
        };
        assert_eq!(
            tf(doc! { "f": { "$exists": true } }).unwrap().unwrap(),
            is_def.clone()
        );
        assert_eq!(
            tf(doc! { "f": { "$exists": false } }).unwrap().unwrap(),
            ScalarExpr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(is_def)
            }
        );
    }

    #[test]
    fn regex_with_options() {
        let expr = tf(doc! { "name": { "$regex": "^ad", "$options": "i" } })
            .unwrap()
            .unwrap();
        assert_eq!(
            expr,
            ScalarExpr::Function {
                name: "REGEXMATCH".into(),
                args: vec![
                    path("name"),
                    ScalarExpr::Literal(Literal::Str("(?i)^ad".into()))
                ],
            }
        );
    }

    #[test]
    fn unsupported_operator_errors() {
        let err = tf(doc! { "age": { "$in": [1, 2] } }).unwrap_err();
        assert!(matches!(err, TranslateError::Unsupported(_)), "{err:?}");
    }

    #[test]
    fn datetime_literal_is_preserved() {
        // Non-textual scalars (DateTime, ObjectId) round-trip via `Value`.
        let dt = bson::DateTime::from_millis(1_000);
        let expr = tf(doc! { "when": { "$gt": dt } }).unwrap().unwrap();
        assert_eq!(
            expr,
            binary(
                BinOp::Gt,
                path("when"),
                ScalarExpr::Value(Bson::DateTime(dt))
            )
        );
    }

    #[test]
    fn document_operand_is_unsupported() {
        // A document as a comparison operand isn't a scalar to compare against.
        let err = tf(doc! { "f": { "$gt": { "nested": 1 } } }).unwrap_err();
        assert!(matches!(err, TranslateError::Unsupported(_)), "{err:?}");
    }

    #[test]
    fn malformed_logical_errors() {
        let err = tf(doc! { "$or": "not-an-array" }).unwrap_err();
        assert!(matches!(err, TranslateError::Malformed(_)), "{err:?}");
    }

    #[test]
    fn find_to_query_maps_options() {
        let raw = RawDocumentBuf::try_from(&doc! { "age": { "$gt": 18 } }).unwrap();
        let options = FindOptions {
            sort: vec![Sort {
                field: "age".into(),
                direction: FindSort::Desc,
            }],
            skip: Some(2),
            take: Some(5),
            columns: Some(vec!["name".into()]),
        };
        let q = find_to_query(&raw, &options).unwrap();

        assert!(q.filter.is_some());
        assert_eq!(q.offset, Some(2));
        assert_eq!(q.limit, Some(5));
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.order_by[0].direction, SortDirection::Desc);
        // projection includes the pk plus selected columns as an object
        let SelectClause::Value(ScalarExpr::Object(fields)) = &q.select else {
            panic!("expected object projection, got {:?}", q.select);
        };
        let keys: Vec<&str> = fields.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["_id", "name"]);
    }

    #[test]
    fn no_columns_is_identity_projection() {
        let raw = RawDocumentBuf::try_from(&doc! {}).unwrap();
        let q = find_to_query(&raw, &FindOptions::default()).unwrap();
        assert_eq!(
            q.select,
            SelectClause::Value(ScalarExpr::Identifier("c".into()))
        );
        assert_eq!(q.filter, None);
    }
}
