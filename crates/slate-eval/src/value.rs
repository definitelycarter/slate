//! The evaluation value domain.

use bson::Bson;

/// The result of evaluating a [`slate_ast::ScalarExpr`].
///
/// CosmosDB SQL distinguishes **undefined** (a missing path, an out-of-range
/// index, a type-incompatible operation) from an explicit `null`. Undefined
/// propagates through most operators and, crucially, causes a `SELECT VALUE`
/// row — or an object field / array element — to be *omitted* entirely rather
/// than emitted as null.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Undefined,
    Defined(Bson),
}

impl Value {
    /// Convenience constructor for a defined value.
    pub fn defined(b: impl Into<Bson>) -> Self {
        Value::Defined(b.into())
    }

    pub fn is_undefined(&self) -> bool {
        matches!(self, Value::Undefined)
    }

    /// Borrow the inner `Bson`, or `None` if undefined.
    pub fn as_bson(&self) -> Option<&Bson> {
        match self {
            Value::Defined(b) => Some(b),
            Value::Undefined => None,
        }
    }

    /// Consume into the inner `Bson`, or `None` if undefined.
    pub fn into_bson(self) -> Option<Bson> {
        match self {
            Value::Defined(b) => Some(b),
            Value::Undefined => None,
        }
    }
}
