use bson::raw::RawBsonRef;
use regex::Regex;

/// Logical operator for index merge operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalOp {
    And,
    Or,
}

/// A recursive filter expression tree.
///
/// Borrows field names and values directly from the raw BSON filter document,
/// avoiding heap allocations during parsing. The lifetime `'a` is tied to
/// the `RawDocument` (or `RawDocumentBuf`) that was parsed.
#[derive(Debug, Clone)]
pub enum Expression<'a> {
    // Logical
    And(Vec<Expression<'a>>),
    Or(Vec<Expression<'a>>),
    // Comparison — field name + value borrowed from raw BSON bytes
    Eq(&'a str, RawBsonRef<'a>),
    Gt(&'a str, RawBsonRef<'a>),
    Gte(&'a str, RawBsonRef<'a>),
    Lt(&'a str, RawBsonRef<'a>),
    Lte(&'a str, RawBsonRef<'a>),
    // Pattern — regex is compiled, not borrowed
    Regex(&'a str, Regex),
    // Existence
    Exists(&'a str, bool),
}

impl PartialEq for Expression<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Expression::And(a), Expression::And(b)) => a == b,
            (Expression::Or(a), Expression::Or(b)) => a == b,
            (Expression::Eq(f1, v1), Expression::Eq(f2, v2)) => f1 == f2 && v1 == v2,
            (Expression::Gt(f1, v1), Expression::Gt(f2, v2)) => f1 == f2 && v1 == v2,
            (Expression::Gte(f1, v1), Expression::Gte(f2, v2)) => f1 == f2 && v1 == v2,
            (Expression::Lt(f1, v1), Expression::Lt(f2, v2)) => f1 == f2 && v1 == v2,
            (Expression::Lte(f1, v1), Expression::Lte(f2, v2)) => f1 == f2 && v1 == v2,
            (Expression::Regex(f1, r1), Expression::Regex(f2, r2)) => {
                f1 == f2 && r1.as_str() == r2.as_str()
            }
            (Expression::Exists(f1, b1), Expression::Exists(f2, b2)) => f1 == f2 && b1 == b2,
            _ => false,
        }
    }
}
