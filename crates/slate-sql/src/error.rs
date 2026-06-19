//! Error types for the SQL frontend.

use std::fmt;

/// An error raised while lexing, parsing, or evaluating a query.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlError {
    /// Lexical error — unexpected character, unterminated string, bad number.
    /// `at` is the character offset into the source where the problem began.
    Lex { message: String, at: usize },
    /// Syntactic error — unexpected token or missing clause.
    Parse { message: String },
    /// Evaluation error — unknown function, wrong arity, etc.
    ///
    /// Note: *type* mismatches generally produce [`slate_eval::Value::Undefined`]
    /// (matching Cosmos semantics) rather than an error.
    Eval { message: String },
}

impl From<slate_eval::EvalError> for SqlError {
    fn from(e: slate_eval::EvalError) -> Self {
        SqlError::Eval { message: e.message }
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlError::Lex { message, at } => write!(f, "lex error at {at}: {message}"),
            SqlError::Parse { message } => write!(f, "parse error: {message}"),
            SqlError::Eval { message } => write!(f, "eval error: {message}"),
        }
    }
}

impl std::error::Error for SqlError {}

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, SqlError>;
