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

impl PartialEq for Expression {
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
