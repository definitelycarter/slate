//! Row-environment helpers shared by the binding-aware nodes.
//!
//! A row flowing into `Filter`/`Project`/`Sort`/`Unwind` is an *environment
//! document*: a `RawBson::Document` whose top-level fields are the bound aliases
//! (`{c: <doc>, t: <elem>}`). These helpers read such a row **without decoding
//! it into owned BSON** — the bindings borrow straight out of the raw bytes —
//! and evaluate expressions against them via the shared `slate-eval` raw
//! evaluator.

use bson::RawBson;
use bson::raw::RawBsonRef;
use slate_eval::EvalError;
use slate_eval::raweval::RawEnv;
use slate_planner::RowBinding;

use crate::ExecError;

/// Borrow the top-level `(alias, value)` bindings out of an environment row.
///
/// Zero-copy: each value is a `RawBsonRef` into the row's bytes, so this never
/// materializes the bound documents.
pub(crate) fn bindings_of(row: &RawBson) -> Result<Vec<(&str, RawBsonRef<'_>)>, ExecError> {
    let doc = match row.as_raw_bson_ref() {
        RawBsonRef::Document(d) => d,
        other => {
            return Err(EvalError {
                message: format!(
                    "expected an environment row, got {:?}",
                    other.element_type()
                ),
            }
            .into());
        }
    };

    let mut binds = Vec::new();
    for entry in doc.iter() {
        let (k, v) = entry.map_err(|e| EvalError {
            message: format!("could not read row bindings: {e}"),
        })?;
        binds.push((k.as_str(), v));
    }
    Ok(binds)
}

/// Build a raw evaluation environment over already-extracted bindings.
pub(crate) fn raw_env<'a>(bindings: &'a [(&'a str, RawBsonRef<'a>)]) -> RawEnv<'a> {
    RawEnv::new(bindings, None)
}

/// The sole `FROM` alias when the node reads bare rows ([`RowBinding::Alias`]),
/// for compiling the single-binding fast path; `None` for the multi-binding
/// environment shape. Mirrors the binding used by [`with_env`].
pub(crate) fn sole_alias(binding: &RowBinding) -> Option<&str> {
    match binding {
        RowBinding::Alias(alias) => Some(alias.as_str()),
        RowBinding::Env => None,
    }
}

/// Run `f` with a raw evaluation environment for `row` under `binding`.
///
/// In [`RowBinding::Alias`] mode the whole row is bound to one alias with **no
/// allocation** (a one-element stack array); in [`RowBinding::Env`] mode the
/// row's top-level fields are the bindings. The closure returns an owned value
/// (it must not borrow the environment, which is dropped on return).
pub(crate) fn with_env<R>(
    row: &RawBson,
    binding: &RowBinding,
    f: impl FnOnce(&RawEnv) -> Result<R, ExecError>,
) -> Result<R, ExecError> {
    match binding {
        RowBinding::Alias(alias) => {
            let binds = [(alias.as_str(), row.as_raw_bson_ref())];
            f(&RawEnv::new(&binds, None))
        }
        RowBinding::Env => {
            let binds = bindings_of(row)?;
            f(&RawEnv::new(&binds, None))
        }
    }
}
