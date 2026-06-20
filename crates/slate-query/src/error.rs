//! Error type for translating a Mongo-style find request into the query AST.

use std::fmt;

/// A find request that can't be turned into a [`slate_ast::Query`].
#[derive(Debug, Clone, PartialEq)]
pub enum TranslateError {
    /// A filter operator or literal shape the AST can't express yet
    /// (e.g. `$in`, a non-scalar literal). The string names what was hit.
    Unsupported(String),
    /// A structurally invalid filter — malformed BSON, or an operator with the
    /// wrong operand type (e.g. `$and` not given an array).
    Malformed(String),
}

impl fmt::Display for TranslateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TranslateError::Unsupported(what) => write!(f, "unsupported find filter: {what}"),
            TranslateError::Malformed(what) => write!(f, "malformed find filter: {what}"),
        }
    }
}

impl std::error::Error for TranslateError {}

pub type Result<T> = std::result::Result<T, TranslateError>;
