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
use slate_eval::raweval::RowEnv;
use slate_planner::RowBinding;

use crate::{ExecEnv, ExecError};

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
fn params_doc(params: &Params) -> Option<&RawDocument> {
    params.as_deref().map(|b| &**b)
}

/// Borrow the random source as a plain `&dyn Fn` for passing to the evaluator.
fn rand_fn(rand: &Rand) -> Option<&dyn Fn() -> f64> {
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

/// Build a row environment over already-extracted bindings, sourcing the
/// query's `@`-parameters, random source, and clock reading from the execution
/// context ([`ExecEnv`]). The node holds an owned `ExecEnv`; this borrows the
/// capability handles out of it for one row's evaluation.
pub(crate) fn row_env<'a>(
    bindings: &'a [(&'a str, RawBsonRef<'a>)],
    env: &'a ExecEnv<'_>,
) -> RowEnv<'a> {
    RowEnv::new(bindings, params_doc(&env.params))
        .with_rng(rand_fn(&env.rand))
        .with_clock(env.clock)
}

/// The sole `FROM` alias when the node reads bare rows ([`RowBinding::Alias`]),
/// for compiling the single-binding fast path; `None` for the multi-binding
/// environment shape. Mirrors the binding used by [`with_row_env`].
pub(crate) fn sole_alias(binding: &RowBinding) -> Option<&str> {
    match binding {
        RowBinding::Alias(alias) => Some(alias.as_str()),
        RowBinding::Env => None,
    }
}

/// Run `f` with a row environment for `row` under `binding`, sourcing the
/// capabilities from the execution context ([`ExecEnv`]).
///
/// In [`RowBinding::Alias`] mode the whole row is bound to one alias with **no
/// allocation** (a one-element stack array); in [`RowBinding::Env`] mode the
/// row's top-level fields are the bindings. The closure returns an owned value
/// (it must not borrow the environment, which is dropped on return).
pub(crate) fn with_row_env<R>(
    row: &RawBson,
    binding: &RowBinding,
    env: &ExecEnv<'_>,
    f: impl FnOnce(&RowEnv) -> Result<R, ExecError>,
) -> Result<R, ExecError> {
    let params = params_doc(&env.params);
    let rand = rand_fn(&env.rand);
    let clock = env.clock;
    match binding {
        RowBinding::Alias(alias) => {
            let binds = [(alias.as_str(), row.as_raw_bson_ref())];
            f(&RowEnv::new(&binds, params).with_rng(rand).with_clock(clock))
        }
        RowBinding::Env => {
            let binds = bindings_of(row)?;
            f(&RowEnv::new(&binds, params).with_rng(rand).with_clock(clock))
        }
    }
}
