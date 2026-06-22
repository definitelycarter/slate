//! Helpers for the `Subquery` (correlated apply) node.
//!
//! The per-outer-row loop lives in the dispatcher (it re-runs the subplan and so
//! needs the executor). These two helpers are the pure parts: reduce a subplan's
//! row stream to the slot value, and attach that value to the outer row.

use bson::RawBson;
use bson::raw::{CStr, RawArrayBuf};
use slate_ast::SubqueryKind;
use slate_eval::EvalError;

use crate::{ExecError, ValueIter};

/// Reduce a subplan's output rows to the subquery's value:
/// - `Scalar` → the first produced value, or `None` (undefined) if empty.
/// - `Exists` → whether any row was produced.
/// - `Array` → all produced values collected into an array.
pub(crate) fn reduce(iter: ValueIter, kind: SubqueryKind) -> Result<Option<RawBson>, ExecError> {
    match kind {
        SubqueryKind::Scalar => {
            for item in iter {
                if let Some(value) = item? {
                    return Ok(Some(value));
                }
            }
            Ok(None)
        }
        SubqueryKind::Exists => {
            for item in iter {
                if item?.is_some() {
                    return Ok(Some(RawBson::Boolean(true)));
                }
            }
            Ok(Some(RawBson::Boolean(false)))
        }
        SubqueryKind::Array => {
            let mut array = RawArrayBuf::new();
            for item in iter {
                if let Some(value) = item? {
                    array.push(value);
                }
            }
            Ok(Some(RawBson::Array(array)))
        }
    }
}

/// Attach the subquery result to the outer environment row under `key` (the slot
/// name, validated once by the caller). An undefined result (`None`) is omitted,
/// matching how undefined is dropped from documents elsewhere.
pub(crate) fn augment(
    row: RawBson,
    key: &CStr,
    value: Option<RawBson>,
) -> Result<RawBson, ExecError> {
    let RawBson::Document(mut doc) = row else {
        return Err(EvalError {
            message: "subquery applied to a non-environment row".into(),
        }
        .into());
    };
    if let Some(value) = value {
        doc.append(key, value);
    }
    Ok(RawBson::Document(doc))
}
