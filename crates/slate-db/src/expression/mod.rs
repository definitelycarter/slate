mod eval;

use bson::Bson;
use bson::RawDocument;
use regex::Regex;

use crate::error::DbError;

/// Logical operator for index merge operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum LogicalOp {
    And,
    Or,
}

/// A recursive filter expression tree.
///
/// Owns field names and values so the expression can outlive the document
/// it was parsed from. Scalars (i32, i64, f64, bool, etc.) are copied by
/// value; only strings and nested documents allocate.
#[derive(Debug, Clone)]
pub enum Expression {
    // Logical
    And(Vec<Expression>),
    Or(Vec<Expression>),
    // Comparison — field name + value, owned
    Eq(String, Bson),
    Gt(String, Bson),
    Gte(String, Bson),
    Lt(String, Bson),
    Lte(String, Bson),
    // Pattern — regex is compiled, not borrowed
    Regex(String, Regex),
    // Existence
    Exists(String, bool),
}

impl Expression {
    /// Returns `true` if the raw document satisfies this expression.
    pub(crate) fn matches(&self, raw: &RawDocument) -> Result<bool, DbError> {
        eval::matches(raw, self)
    }
}
