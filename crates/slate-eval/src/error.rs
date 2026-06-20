//! Error type for expression evaluation.

use std::fmt;

/// An error raised while evaluating an expression — an unknown function, wrong
/// arity, or a malformed raw value encountered mid-walk.
///
/// Note: *type* mismatches generally produce [`crate::value::Value::Undefined`]
/// (matching Cosmos semantics) rather than an error, so this is comparatively
/// rare.
#[derive(Debug, Clone, PartialEq)]
pub struct EvalError {
    pub message: String,
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "eval error: {}", self.message)
    }
}

impl std::error::Error for EvalError {}

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, EvalError>;
