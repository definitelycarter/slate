//! The evaluation value domain.
//!
//! [`Value`] lives in this standalone crate — rather than inside `slate-eval` —
//! so the function crates that speak it (`slate-udf` today, `slate-trigger` /
//! `slate-validator` later) can depend on the value type without pulling in the
//! whole evaluator (and its `regex` / `serde_json` / `chrono` dependencies).
//! `slate-eval` re-exports it, so `slate_eval::Value` is unchanged.
//!
//! Only the owned [`Value`] lives here for now; the borrow-or-owned `RawValue`
//! of the zero-copy evaluator stays in `slate-eval` until a consumer needs it
//! across the crate boundary.

use bson::Bson;

/// The result of evaluating a [`slate_ast::Expression`].
///
/// CosmosDB SQL distinguishes **undefined** (a missing path, an out-of-range
/// index, a type-incompatible operation) from an explicit `null`. Undefined
/// propagates through most operators and, crucially, causes a `SELECT VALUE`
/// row — or an object field / array element — to be *omitted* entirely rather
/// than emitted as null.
///
/// It is also the currency of a UDF: a function receives its arguments as
/// `&[Value]` and returns a freshly-computed `Value`.
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
