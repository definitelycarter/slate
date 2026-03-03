use bson::RawBson;
use bson::rawdoc;
use slate_engine::{Catalog, EngineTransaction};
use slate_vm::pool::VmPool;
use slate_vm::{ScriptCapabilities, ScopedMethod};

use crate::error::DbError;
use crate::executor::RawIter;
use crate::hooks::ResolvedHook;

pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    pool: Option<&'a VmPool>,
    cf: String,
    action: String,
    hooks: Vec<ResolvedHook>,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
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

pub(crate) fn fire_hooks<T: EngineTransaction + Catalog>(
    txn: &T,
    pool: Option<&VmPool>,
    cf: &str,
    hooks: &[ResolvedHook],
    action: &str,
    doc: &bson::RawDocument,
) -> Result<(), DbError> {
    let Some(pool) = pool else {
        return Ok(());
    };

    if hooks.is_empty() {
        return Ok(());
    }

    let get_cb = |args: Vec<bson::Bson>| -> Result<bson::Bson, slate_vm::VmError> {
        let coll_name = args.first().and_then(|b| b.as_str()).ok_or_else(|| {
            slate_vm::VmError::InvalidReturn(
                "ctx.get: first argument must be a collection name".into(),
            )
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
                slate_vm::VmError::InvalidReturn("ctx.get: failed to encode doc_id".into())
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
            slate_vm::VmError::InvalidReturn(
                "ctx.put: first argument must be a collection name".into(),
            )
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

        let raw =
            bson::raw::RawDocumentBuf::try_from(doc).map_err(slate_vm::VmError::Bson)?;

        txn.put(&handle, &raw)
            .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?;

        Ok(bson::Bson::Null)
    };

    let delete_cb = |args: Vec<bson::Bson>| -> Result<bson::Bson, slate_vm::VmError> {
        let coll_name = args.first().and_then(|b| b.as_str()).ok_or_else(|| {
            slate_vm::VmError::InvalidReturn(
                "ctx.delete: first argument must be a collection name".into(),
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
                slate_vm::VmError::InvalidReturn("ctx.delete: failed to encode doc_id".into())
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
        let runtime = crate::hooks::runtime_kind(hook.runtime);
        let handle = pool.get_or_load(runtime, &hook.name, hook.source_hash, &hook.source)?;
        handle.call(&input, &caps)?;
    }
    Ok(())
}
