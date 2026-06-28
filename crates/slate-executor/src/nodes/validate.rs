//! The `Validate` node — run native validators on each candidate document.
//!
//! Each entry is a binding `(validator_name, native_function_name)`; the native
//! name is resolved against the database-scoped [`ValidatorBag`] at fire time, so
//! a freshly registered validator takes effect immediately (the bag is read
//! live). A [`Verdict::Reject`] aborts the write with an error; otherwise the
//! document passes through. With no bag attached, validation is skipped.
//!
//! The body is user code, so it runs behind a `catch_unwind`: a panic aborts the
//! write (fail-safe), as does a dangling binding (the bound function is not
//! registered) or a validator returning a [`ValidatorError`](slate_validator::ValidatorError).

use std::panic::AssertUnwindSafe;

use bson::RawBson;
use slate_validator::{ValidatorBag, ValidatorCtx, Verdict};

use crate::{ExecError, ValueIter};

pub(crate) fn execute<'a>(
    bag: Option<&'a ValidatorBag>,
    validators: Vec<(String, String)>,
    source: ValueIter<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    let Some(bag) = bag else {
        return Ok(source);
    };

    Ok(Box::new(source.map(move |result| {
        let opt = result?;
        if let Some(RawBson::Document(ref d)) = opt {
            run_validators(bag, &validators, d)?;
        }
        Ok(opt)
    })))
}

fn run_validators(
    bag: &ValidatorBag,
    validators: &[(String, String)],
    doc: &bson::RawDocument,
) -> Result<(), ExecError> {
    let ctx = ValidatorCtx::new(doc);

    for (name, func) in validators {
        // Resolve the binding's target against the live bag. A dangling binding
        // (bound but never registered) aborts the write — fail-safe.
        let Some(validator) = bag.get(func) else {
            return Err(ExecError::Validation(format!(
                "validator '{name}' is bound to native function '{func}', which is not registered"
            )));
        };

        // The body is the application's code; a panic must abort the write, not
        // unwind through the engine. `AssertUnwindSafe` is sound here — `ctx` and
        // `validator` are owned by this frame and dropped on a panic.
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
