//! The `Trigger` node — fire trigger scripts on each document as a side effect.
//!
//! Used both before a mutation (a tap that passes documents through) and after
//! (wrapping a mutation plan's output). Each hook receives `{ action, doc }`
//! and a read-write `ctx` exposing `get`/`put`/`delete` over the transaction.
//! With no scripting pool or no hooks, this is a passthrough.

use bson::RawBson;
use bson::rawdoc;
use slate_engine::{Catalog, EngineTransaction};
use slate_vm::pool::VmPool;
use slate_vm::{ResolvedHook, ScopedMethod, ScriptCapabilities, runtime_kind};

use crate::{ExecError, ValueIter};

pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    pool: Option<&'a VmPool>,
    cf: String,
    action: String,
    hooks: Vec<ResolvedHook>,
    source: ValueIter<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    if pool.is_none() || hooks.is_empty() {
        return Ok(source);
    }

    Ok(Box::new(source.map(move |result| {
        let opt = result?;
        if let Some(RawBson::Document(ref d)) = opt {
            fire_hooks(txn, pool, &cf, &hooks, &action, d)?;
        }
        Ok(opt)
    })))
}

/// Fire `hooks` for `action` on `doc`. Exposed so mutation nodes that fire
/// conditional triggers (upsert) can reuse it.
// In a build with no script runtime compiled in (e.g. wasm32), `RuntimeKind`
// is uninhabited, so `runtime_kind` and the dispatch below are unreachable and
// the locals feeding them are unused. Mirrors the same allow on `VmPool`'s impl
// in `slate-vm`.
#[allow(unreachable_code, unused_variables)]
pub(crate) fn fire_hooks<T: EngineTransaction + Catalog>(
    txn: &T,
    pool: Option<&VmPool>,
    cf: &str,
    hooks: &[ResolvedHook],
    action: &str,
    doc: &bson::RawDocument,
) -> Result<(), ExecError> {
    let Some(pool) = pool else {
        return Ok(());
    };
    if hooks.is_empty() {
        return Ok(());
    }

    let get_cb = |args: Vec<bson::Bson>| -> Result<bson::Bson, slate_vm::VmError> {
        let coll_name = args.first().and_then(|b| b.as_str()).ok_or_else(|| {
            slate_vm::VmError::InvalidReturn("ctx.get: first argument must be a collection".into())
        })?;
        let doc_id = args.get(1).ok_or_else(|| {
            slate_vm::VmError::InvalidReturn("ctx.get: second argument (id) required".into())
        })?;
        let handle = txn
            .collection(cf, coll_name)
            .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?;
        let wrapper = bson::raw::RawDocumentBuf::try_from(bson::doc! { "v": doc_id.clone() })
            .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?;
        let raw_ref = wrapper
            .get("v")
            .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?
            .ok_or_else(|| {
                slate_vm::VmError::InvalidReturn("ctx.get: failed to encode id".into())
            })?;
        match txn.get(&handle, &raw_ref) {
            Ok(Some(doc)) => {
                let document: bson::Document = bson::deserialize_from_slice(doc.as_bytes())
                    .map_err(slate_vm::VmError::Bson)?;
                Ok(bson::Bson::Document(document))
            }
            Ok(None) => Ok(bson::Bson::Null),
            Err(e) => Err(slate_vm::VmError::InvalidReturn(e.to_string())),
        }
    };

    let put_cb = |args: Vec<bson::Bson>| -> Result<bson::Bson, slate_vm::VmError> {
        let coll_name = args.first().and_then(|b| b.as_str()).ok_or_else(|| {
            slate_vm::VmError::InvalidReturn("ctx.put: first argument must be a collection".into())
        })?;
        let doc_bson = args.get(1).ok_or_else(|| {
            slate_vm::VmError::InvalidReturn("ctx.put: second argument (doc) required".into())
        })?;
        let doc = match doc_bson {
            bson::Bson::Document(d) => d,
            _ => {
                return Err(slate_vm::VmError::InvalidReturn(
                    "ctx.put: second argument must be a document".into(),
                ));
            }
        };
        let handle = txn
            .collection(cf, coll_name)
            .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?;
        let raw = bson::raw::RawDocumentBuf::try_from(doc).map_err(slate_vm::VmError::Bson)?;
        txn.put(&handle, &raw)
            .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?;
        Ok(bson::Bson::Null)
    };

    let delete_cb = |args: Vec<bson::Bson>| -> Result<bson::Bson, slate_vm::VmError> {
        let coll_name = args.first().and_then(|b| b.as_str()).ok_or_else(|| {
            slate_vm::VmError::InvalidReturn(
                "ctx.delete: first argument must be a collection".into(),
            )
        })?;
        let doc_id = args.get(1).ok_or_else(|| {
            slate_vm::VmError::InvalidReturn("ctx.delete: second argument (id) required".into())
        })?;
        let handle = txn
            .collection(cf, coll_name)
            .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?;
        let wrapper = bson::raw::RawDocumentBuf::try_from(bson::doc! { "v": doc_id.clone() })
            .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?;
        let raw_ref = wrapper
            .get("v")
            .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?
            .ok_or_else(|| {
                slate_vm::VmError::InvalidReturn("ctx.delete: failed to encode id".into())
            })?;
        txn.delete(&handle, &raw_ref)
            .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?;
        Ok(bson::Bson::Null)
    };

    let methods = [
        ScopedMethod {
            name: "get",
            callback: &get_cb,
        },
        ScopedMethod {
            name: "put",
            callback: &put_cb,
        },
        ScopedMethod {
            name: "delete",
            callback: &delete_cb,
        },
    ];

    let caps = ScriptCapabilities::ReadWrite { methods: &methods };
    let input = rawdoc! { "action": action, "doc": doc.to_owned() };

    for hook in hooks {
        let runtime = runtime_kind(hook.runtime);
        let handle = pool.get_or_load(runtime, &hook.name, hook.source_hash, &hook.source)?;
        handle.call(&input, &caps)?;
    }
    Ok(())
}
