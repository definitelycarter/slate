use bson::raw::RawDocument;
use bson::rawdoc;
use slate_engine::{Catalog, EngineTransaction, FunctionKind};

use crate::database::VmFactory;
use crate::error::DbError;

/// Bundles the resources an [`Executor`] needs: an engine transaction and
/// an optional pre-built VM with all functions for the target collection.
pub struct Context<'a, T: EngineTransaction> {
    pub(crate) txn: &'a T,
    pub(crate) vm: Option<Box<dyn slate_vm::Vm + Send>>,
    /// Trigger function names registered in the VM.
    triggers: Vec<String>,
}

impl<'a, T: EngineTransaction> Context<'a, T> {
    /// Create a bare context with no VM.
    pub fn new(txn: &'a T) -> Self {
        Self {
            txn,
            vm: None,
            triggers: Vec::new(),
        }
    }
}

impl<'a, T: EngineTransaction + Catalog> Context<'a, T> {
    /// Create a context with a VM loaded with all functions for the given collection.
    /// Returns a bare context (no VM) if there is no factory or no functions exist.
    pub(crate) fn with_vm(
        txn: &'a T,
        factory: Option<&VmFactory>,
        cf: &str,
        collection: &str,
    ) -> Result<Self, DbError> {
        let Some(factory) = factory else {
            return Ok(Self::new(txn));
        };

        let triggers = txn.load_functions(cf, collection, FunctionKind::Trigger)?;

        if triggers.is_empty() {
            return Ok(Self::new(txn));
        }

        let mut vm = factory()?;
        let trigger_names: Vec<String> = triggers.iter().map(|f| f.name.clone()).collect();
        for f in &triggers {
            vm.register(&f.name, &f.source)?;
        }

        Ok(Self {
            txn,
            vm: Some(vm),
            triggers: trigger_names,
        })
    }

    /// Fire all trigger functions for the given action and document.
    /// No-op if no VM or no triggers are loaded.
    pub(crate) fn fire_triggers(
        &self,
        cf: &str,
        action: &str,
        doc: &RawDocument,
    ) -> Result<(), DbError> {
        let Some(ref vm) = self.vm else {
            return Ok(());
        };

        if self.triggers.is_empty() {
            return Ok(());
        }

        let txn = self.txn;
        let trigger_cf = cf.to_string();

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
                .collection(&trigger_cf, coll_name)
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
                        .map_err(|e| slate_vm::VmError::Bson(e))?;
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
                .collection(&trigger_cf, coll_name)
                .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?;

            let raw =
                bson::raw::RawDocumentBuf::try_from(doc).map_err(|e| slate_vm::VmError::Bson(e))?;

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
                .collection(&trigger_cf, coll_name)
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
            slate_vm::ScopedMethod {
                name: "get",
                callback: &get_cb,
            },
            slate_vm::ScopedMethod {
                name: "put",
                callback: &put_cb,
            },
            slate_vm::ScopedMethod {
                name: "delete",
                callback: &delete_cb,
            },
        ];

        let input = rawdoc! { "action": action, "doc": doc.to_owned() };
        for name in &self.triggers {
            vm.call_with_scope(name, &input, &methods)?;
        }
        Ok(())
    }
}
