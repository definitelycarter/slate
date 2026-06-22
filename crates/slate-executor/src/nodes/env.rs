//! Row-environment helpers shared by the binding-aware nodes.
//!
//! A row flowing into `Filter`/`Project`/`Sort`/`Unwind` is an *environment
//! document*: a `RawBson::Document` whose top-level fields are the bound aliases
//! (`{c: <doc>, t: <elem>}`). These helpers read such a row **without decoding
//! it into owned BSON** — the bindings borrow straight out of the raw bytes —
//! and evaluate expressions against them via the shared `slate-eval` raw
//! evaluator.

use std::rc::Rc;

use bson::RawBson;
use bson::raw::{RawBsonRef, RawDocument, RawDocumentBuf};
use slate_eval::EvalError;
use slate_eval::raweval::RawEnv;
use slate_planner::RowBinding;

use crate::ExecError;

/// The query's `@`-parameter values, shared (by `Rc`) across the evaluating
/// nodes of one pipeline. `None` means the query had no parameters.
pub(crate) type Params = Option<Rc<RawDocumentBuf>>;

/// The injected random source backing `RAND()`, shared (by `Rc`) across the
/// evaluating nodes of one pipeline so it outlives this executor. `None` means
/// no source was injected, so `RAND()` is undefined. The closure owns its
/// mutable PRNG state, so cloning the `Rc` into each node keeps the *same*
/// stream — a fresh draw per call across the whole pipeline.
pub(crate) type Rand = Option<Rc<dyn Fn() -> f64>>;

/// Borrow the parameter document for passing to the evaluator.
pub(crate) fn params_doc(params: &Params) -> Option<&RawDocument> {
    params.as_deref().map(|b| &**b)
}

/// Borrow the random source as a plain `&dyn Fn` for passing to the evaluator.
pub(crate) fn rand_fn(rand: &Rand) -> Option<&dyn Fn() -> f64> {
    rand.as_deref()
}

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

/// Build a raw evaluation environment over already-extracted bindings, with the
/// query's `@`-parameters and random source (if any) visible to the expression.
pub(crate) fn raw_env<'a>(
    bindings: &'a [(&'a str, RawBsonRef<'a>)],
    params: Option<&'a RawDocument>,
    rand: Option<&'a dyn Fn() -> f64>,
) -> RawEnv<'a> {
    RawEnv::new(bindings, params).with_rng(rand)
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
    params: Option<&RawDocument>,
    rand: Option<&dyn Fn() -> f64>,
    f: impl FnOnce(&RawEnv) -> Result<R, ExecError>,
) -> Result<R, ExecError> {
    match binding {
        RowBinding::Alias(alias) => {
            let binds = [(alias.as_str(), row.as_raw_bson_ref())];
            f(&RawEnv::new(&binds, params).with_rng(rand))
        }
        RowBinding::Env => {
            let binds = bindings_of(row)?;
            f(&RawEnv::new(&binds, params).with_rng(rand))
        }
    }
}
