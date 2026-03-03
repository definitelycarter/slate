use bson::RawBson;
use bson::rawdoc;
use slate_vm::pool::VmPool;
use slate_vm::ScriptCapabilities;

use crate::error::DbError;
use crate::executor::RawIter;
use crate::hooks::ResolvedHook;

pub(crate) fn execute<'a>(
    pool: Option<&'a VmPool>,
    validators: Vec<ResolvedHook>,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
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

fn run_validators(
    pool: &VmPool,
    validators: &[ResolvedHook],
    doc: &bson::RawDocument,
) -> Result<(), DbError> {
    let caps = ScriptCapabilities::Pure;
    let input = rawdoc! { "doc": doc.to_owned() };

    for validator in validators {
        let runtime = crate::hooks::runtime_kind(validator.runtime);
        let handle = pool.get_or_load(
            runtime,
            &validator.name,
            validator.source_hash,
            &validator.source,
        )?;
        let result = handle.call(&input, &caps)?;

        // Check if validator returned { ok: false, reason: "..." }
        // The result is a RawDocumentBuf — parse it to check.
        if let Ok(Some(ok_val)) = result.get("ok") {
            if let bson::raw::RawBsonRef::Boolean(false) = ok_val {
                let reason = result
                    .get_str("reason")
                    .unwrap_or("validation failed");
                return Err(DbError::InvalidQuery(format!(
                    "validation failed ({}): {}",
                    validator.name, reason
                )));
            }
        }
    }
    Ok(())
}
