use bson::Bson;
use regex::Regex;

/// Logical operator for index merge operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
