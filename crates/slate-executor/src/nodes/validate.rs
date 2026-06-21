//! The `Validate` node — run validator scripts on each document.
//!
//! Each validator is a script returning `{ ok: bool, reason? }`. A `false`
//! result rejects the document with an error; otherwise the document passes
//! through. With no scripting pool, validation is skipped.

use bson::RawBson;
use bson::rawdoc;
use slate_vm::pool::VmPool;
use slate_vm::{ResolvedHook, ScriptCapabilities, runtime_kind};

use crate::{ExecError, ValueIter};

pub(crate) fn execute<'a>(
    pool: Option<&'a VmPool>,
    validators: Vec<ResolvedHook>,
    source: ValueIter<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    let Some(pool) = pool else {
        return Ok(source);
    };

    Ok(Box::new(source.map(move |result| {
        let opt = result?;
        if let Some(RawBson::Document(ref d)) = opt {
            run_validators(pool, &validators, d)?;
        }
        Ok(opt)
    })))
}

// In a build with no script runtime compiled in (e.g. wasm32), `RuntimeKind`
// is uninhabited, so `runtime_kind` and the dispatch below are unreachable and
// the locals feeding them are unused. Mirrors the same allow on `VmPool`'s impl
// in `slate-vm`.
#[allow(unreachable_code, unused_variables)]
fn run_validators(
    pool: &VmPool,
    validators: &[ResolvedHook],
    doc: &bson::RawDocument,
) -> Result<(), ExecError> {
    let caps = ScriptCapabilities::Pure;
    let input = rawdoc! { "doc": doc.to_owned() };

    for validator in validators {
        let runtime = runtime_kind(validator.runtime);
        let handle = pool.get_or_load(
            runtime,
            &validator.name,
            validator.source_hash,
            &validator.source,
        )?;
        let result = handle.call(&input, &caps)?;

        if let Ok(Some(bson::raw::RawBsonRef::Boolean(false))) = result.get("ok") {
            let reason = result.get_str("reason").unwrap_or("validation failed");
            return Err(ExecError::Validation(format!(
                "{}: {reason}",
                validator.name
            )));
        }
    }
    Ok(())
}
