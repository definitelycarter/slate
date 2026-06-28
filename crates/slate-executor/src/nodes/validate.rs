//! The `Validate` node — run native validators on each candidate document.
//!
//! Each entry is a binding `(validator_name, native_function_name)`. The native
//! names are resolved against the database-scoped [`ValidatorBag`] **once, up
//! front** — not per row: the executor is single-threaded per query and the bag
//! is read live, so one lookup per validator per query suffices (this mirrors how
//! UDFs bake their resolved handle once at compile). A [`Verdict::Reject`] aborts
//! the write with an error; otherwise the document passes through. With no bag
//! attached, validation is skipped.
//!
//! The body is user code, so it runs behind a `catch_unwind`: a panic aborts the
//! write (fail-safe), as does a validator returning a
//! [`ValidatorError`](slate_validator::ValidatorError). A dangling binding (the
//! bound function is not registered) aborts the write up front, before any
//! document is processed — fail-safe *and* fail-fast.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use bson::RawBson;
use slate_validator::{Validator, ValidatorBag, ValidatorCtx, Verdict};

use crate::{ExecError, ValueIter};

pub(crate) fn execute<'a>(
    bag: Option<&'a ValidatorBag>,
    validators: Vec<(String, String)>,
    source: ValueIter<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    let Some(bag) = bag else {
        return Ok(source);
    };

    // Resolve every binding against the bag once. A dangling binding (bound but
    // never registered) aborts here, before any document is processed.
    let resolved = validators
        .into_iter()
        .map(|(name, func)| match bag.get(&func) {
            Some(validator) => Ok((name, validator)),
            None => Err(ExecError::Validation(format!(
                "validator '{name}' is bound to native function '{func}', which is not registered"
            ))),
        })
        .collect::<Result<Vec<(String, Arc<dyn Validator>)>, ExecError>>()?;

    if resolved.is_empty() {
        return Ok(source);
    }

    Ok(Box::new(source.map(move |result| {
        let opt = result?;
        if let Some(RawBson::Document(ref d)) = opt {
            run_validators(&resolved, d)?;
        }
        Ok(opt)
    })))
}

fn run_validators(
    validators: &[(String, Arc<dyn Validator>)],
    doc: &bson::RawDocument,
) -> Result<(), ExecError> {
    let ctx = ValidatorCtx::new(doc);

    for (name, validator) in validators {
        // The body is the application's code; a panic must abort the write, not
        // unwind through the engine. `AssertUnwindSafe` is sound here — `ctx` and
        // the borrowed `validator` are dropped on a panic.
        match std::panic::catch_unwind(AssertUnwindSafe(|| validator.check(&ctx))) {
            Ok(Ok(Verdict::Accept)) => {}
            Ok(Ok(Verdict::Reject(reason))) => {
                return Err(ExecError::Validation(format!("{name}: {reason}")));
            }
            Ok(Err(err)) => {
                return Err(ExecError::Validation(format!(
                    "validator '{name}' failed: {err}"
                )));
            }
            Err(_) => {
                return Err(ExecError::Validation(format!(
                    "validator '{name}' panicked"
                )));
            }
        }
    }
    Ok(())
}
