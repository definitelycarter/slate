//! The `Trigger` node — fire native triggers on each document as a side effect.
//!
//! Used both before a mutation (a tap that passes documents through) and after
//! (wrapping a mutation plan's output). Each trigger receives the firing action,
//! the candidate document, and a [`TriggerCtx`] exposing `get`/`put`/`delete`/
//! `merge` over the transaction — confined to the firing column family by [`CfScopedTxn`]
//! (the trigger names a collection, never a cf). With no trigger bag or no
//! bindings, this is a passthrough.
//!
//! Bindings resolve **once per query** (not per row): a dangling binding (bound
//! but never registered) aborts up front, before any document is processed —
//! fail-safe, mirroring the validator path. A trigger body that errors or panics
//! likewise aborts the write (the panic is caught at this seam).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use bson::raw::{RawBsonRef, RawDocumentBuf};
use bson::{RawBson, RawDocument};
use slate_engine::{Catalog, EngineTransaction};
use slate_trigger::{Trigger, TriggerBag, TriggerCtx, TriggerError, TriggerTxn};

use crate::{ExecError, ValueIter};

/// A resolved trigger binding: the trigger's name (kept for error messages)
/// paired with its live implementation, looked up from the bag once per query.
type ResolvedTrigger = (String, Arc<dyn Trigger>);

pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    bag: Option<&'a TriggerBag>,
    cf: String,
    action: String,
    triggers: Vec<(String, String)>,
    source: ValueIter<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    let resolved = resolve(bag, triggers)?;
    if resolved.is_empty() {
        return Ok(source);
    }

    Ok(Box::new(source.map(move |result| {
        let opt = result?;
        if let Some(RawBson::Document(ref d)) = opt {
            fire(txn, &cf, &resolved, &action, d)?;
        }
        Ok(opt)
    })))
}

/// Resolve `(trigger_name, native_func)` bindings against the live bag, once per
/// query. A dangling binding (bound but unregistered) is an error — caught here,
/// before any document is processed, so it aborts the whole write (fail-safe).
/// With no bag, nothing resolves (triggers are skipped), exactly as a database
/// that never registers a trigger fires none.
///
/// Exposed so the upsert node (which fires conditional per-document actions)
/// shares the same resolve-once + fail-fast behavior.
pub(crate) fn resolve(
    bag: Option<&TriggerBag>,
    triggers: Vec<(String, String)>,
) -> Result<Vec<ResolvedTrigger>, ExecError> {
    let Some(bag) = bag else {
        return Ok(Vec::new());
    };
    triggers
        .into_iter()
        .map(|(name, func)| match bag.get(&func) {
            Some(trigger) => Ok((name, trigger)),
            None => Err(ExecError::Trigger(format!(
                "trigger '{name}' is bound to native function '{func}', which is not registered"
            ))),
        })
        .collect()
}

/// Fire each resolved trigger for `action` on `doc`, under `catch_unwind`. A
/// returned error or a panic aborts the write. Exposed so the upsert node reuses
/// it for its conditional per-document actions.
pub(crate) fn fire<T: EngineTransaction + Catalog>(
    txn: &T,
    cf: &str,
    triggers: &[ResolvedTrigger],
    action: &str,
    doc: &RawDocument,
) -> Result<(), ExecError> {
    let scoped = CfScopedTxn { txn, cf };
    let ctx = TriggerCtx::new(action, doc, &scoped);
    for (name, trigger) in triggers {
        match catch_unwind(AssertUnwindSafe(|| trigger.fire(&ctx))) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                return Err(ExecError::Trigger(format!(
                    "trigger '{name}' failed: {err}"
                )));
            }
            Err(_) => {
                return Err(ExecError::Trigger(format!("trigger '{name}' panicked")));
            }
        }
    }
    Ok(())
}

/// The executor-side [`TriggerTxn`]: the column-family-confined read-write
/// surface a trigger acts through. `cf` is fixed at construction (the firing
/// collection's column family); the trait exposes only a *collection* argument,
/// so a trigger structurally cannot reach across column families. A failing
/// capability operation surfaces as [`TriggerError::Txn`], which aborts the write.
struct CfScopedTxn<'a, T> {
    txn: &'a T,
    cf: &'a str,
}

impl<T: EngineTransaction + Catalog> TriggerTxn for CfScopedTxn<'_, T> {
    fn get(
        &self,
        collection: &str,
        id: RawBsonRef<'_>,
    ) -> Result<Option<RawDocumentBuf>, TriggerError> {
        let handle = self.txn.collection(self.cf, collection).map_err(txn_err)?;
        self.txn.get(&handle, &id).map_err(txn_err)
    }

    fn put(&self, collection: &str, doc: &RawDocument) -> Result<(), TriggerError> {
        let handle = self.txn.collection(self.cf, collection).map_err(txn_err)?;
        self.txn.put(&handle, doc).map_err(txn_err)
    }

    fn merge(&self, collection: &str, doc: &RawDocument) -> Result<(), TriggerError> {
        let handle = self.txn.collection(self.cf, collection).map_err(txn_err)?;
        let pk_path = handle.pk_path();
        // Locate the existing row by `doc`'s own pk value.
        let id = doc
            .get(pk_path)
            .map_err(txn_err)?
            .ok_or_else(|| TriggerError::Txn(format!("merge doc missing pk field '{pk_path}'")))?;
        match self.txn.get(&handle, &id).map_err(txn_err)? {
            // Existing row → overlay `doc`'s fields onto it (pk preserved).
            // `raw_merge` returns `None` when nothing changed — a no-op write.
            Some(old) => match slate_rawbson::raw_merge(&old, doc, pk_path).map_err(txn_err)? {
                Some(merged) => self.txn.put(&handle, &merged).map_err(txn_err),
                None => Ok(()),
            },
            // No row → insert `doc` as-is (the upsert leg).
            None => self.txn.put(&handle, doc).map_err(txn_err),
        }
    }

    fn delete(&self, collection: &str, id: RawBsonRef<'_>) -> Result<(), TriggerError> {
        let handle = self.txn.collection(self.cf, collection).map_err(txn_err)?;
        self.txn.delete(&handle, &id).map_err(txn_err)
    }
}

fn txn_err(e: impl std::fmt::Display) -> TriggerError {
    TriggerError::Txn(e.to_string())
}
