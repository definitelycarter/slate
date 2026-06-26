//! The `Upsert` write node — insert-or-update, with runtime-conditional triggers.
//!
//! For each source document: ensure a primary key, look up the existing doc. If
//! present, fire `updating`, write (`Replace` overwrite or `Merge` field-merge),
//! fire `updated`. If absent, fire `inserting`, `put_nx`, fire `inserted`. The
//! trigger *action* depends on the per-document runtime branch, so the hooks
//! stay inside this node rather than a static `Trigger` wrapper.

use std::rc::Rc;

use bson::raw::{CString, RawDocumentBuf};
use bson::{RawBson, RawDocument};
use slate_engine::{Catalog, CollectionHandle, EngineTransaction};
use slate_eval::EvalError;
use slate_planner::UpsertMode;
use slate_rawbson::raw_merge;
use slate_vm::{ResolvedHook, pool::VmPool};

use super::trigger::fire_hooks;
use crate::watch::WatchSink;
use crate::{ExecError, ValueIter};

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    pool: Option<&'a VmPool>,
    hooks: Vec<ResolvedHook>,
    handle: CollectionHandle<T::Cf>,
    mode: UpsertMode,
    source: ValueIter<'a>,
    watch: Option<Rc<WatchSink>>,
) -> Result<ValueIter<'a>, ExecError> {
    let cf = handle.cf_name().to_string();
    let pk_key = CString::try_from(handle.pk_path()).map_err(|e| EvalError {
        message: format!("invalid pk path: {e}"),
    })?;

    Ok(Box::new(source.map(move |result| {
        let mut new_doc = match result? {
            Some(RawBson::Document(d)) => d,
            _ => {
                return Err(ExecError::Eval(EvalError {
                    message: "upsert requires a document".into(),
                }));
            }
        };

        // Ensure a primary key, generating one if missing.
        let pk = handle.pk_path();
        let has_id = new_doc.get(pk).map_err(decode_err)?.is_some();
        if !has_id {
            new_doc.append(&pk_key, bson::oid::ObjectId::new());
        }

        let raw_id = new_doc.get(pk).map_err(decode_err)?.ok_or_else(|| {
            ExecError::Eval(EvalError {
                message: "primary key missing after ensure".into(),
            })
        })?;

        match txn.get(&handle, &raw_id)? {
            Some(old) => {
                fire_hooks(txn, pool, &cf, &hooks, "updating", &old)?;
                let written = match build_doc(pk, &pk_key, mode, &new_doc, &old)? {
                    Some(doc) => doc,
                    // Merge no-op: nothing written, so nothing for a watch to
                    // observe — return the existing doc unchanged.
                    None => return Ok(Some(RawBson::Document(old.clone()))),
                };
                txn.put(&handle, &written)?;
                fire_hooks(txn, pool, &cf, &hooks, "updated", &written)?;
                // Existing doc overwritten: both old and new states in hand.
                if let Some(sink) = &watch {
                    sink.capture(
                        handle.cf_name(),
                        handle.name(),
                        handle.pk_path(),
                        Some(&old),
                        Some(&written),
                    )?;
                }
                Ok(Some(RawBson::Document(written)))
            }
            None => {
                fire_hooks(txn, pool, &cf, &hooks, "inserting", &new_doc)?;
                txn.put_nx(&handle, &new_doc)?;
                fire_hooks(txn, pool, &cf, &hooks, "inserted", &new_doc)?;
                // Absent before: a fresh insert (only the new state).
                if let Some(sink) = &watch {
                    sink.capture(
                        handle.cf_name(),
                        handle.name(),
                        handle.pk_path(),
                        None,
                        Some(&new_doc),
                    )?;
                }
                Ok(Some(RawBson::Document(new_doc)))
            }
        }
    })))
}

/// Build the document to write. `None` means a merge no-op (nothing changed).
fn build_doc(
    pk_path: &str,
    pk_key: &CString,
    mode: UpsertMode,
    new_raw: &RawDocument,
    old_raw: &RawDocument,
) -> Result<Option<RawDocumentBuf>, ExecError> {
    match mode {
        UpsertMode::Replace => {
            // Original pk (preserves type), then the new doc's other fields.
            let mut buf = RawDocumentBuf::new();
            if let Ok(Some(id_ref)) = old_raw.get(pk_path) {
                buf.append(pk_key, id_ref);
            }
            for entry in new_raw.iter() {
                let (k, v) = entry.map_err(decode_err)?;
                if k != pk_path {
                    buf.append(k, v);
                }
            }
            Ok(Some(buf))
        }
        UpsertMode::Merge => Ok(raw_merge(old_raw, new_raw, pk_path)?),
    }
}

fn decode_err(e: bson::error::Error) -> ExecError {
    ExecError::Eval(EvalError {
        message: format!("malformed document: {e}"),
    })
}
