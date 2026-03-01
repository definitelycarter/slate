use std::collections::HashMap;
use std::sync::Mutex;

use bson::raw::RawDocument;
use bson::rawdoc;
use slate_engine::{Catalog, EngineTransaction, FunctionKind};

use crate::database::VmFactory;
use crate::error::DbError;

/// A cached VM instance for a collection, with pre-loaded function names.
pub(crate) struct VmEntry {
    vm: Box<dyn slate_vm::Vm + Send>,
    triggers: Vec<String>,
}

/// Bundles the resources an [`Executor`] needs: an engine transaction and
/// optional VM infrastructure for trigger/validator execution.
pub struct Context<'a, T: EngineTransaction> {
    pub(crate) txn: &'a T,
    pub(crate) vm_registry: Option<&'a Mutex<HashMap<String, VmEntry>>>,
    pub(crate) vm_factory: Option<&'a VmFactory>,
}

impl<'a, T: EngineTransaction> Context<'a, T> {
    pub fn new(txn: &'a T) -> Self {
        Self {
            txn,
            vm_registry: None,
            vm_factory: None,
        }
    }

    pub(crate) fn with_vm(
        mut self,
        registry: &'a Mutex<HashMap<String, VmEntry>>,
        factory: Option<&'a VmFactory>,
    ) -> Self {
        self.vm_registry = Some(registry);
        self.vm_factory = factory;
        self
    }
}

impl<T: EngineTransaction> Clone for Context<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: EngineTransaction> Copy for Context<'_, T> {}

impl<'a, T: EngineTransaction + Catalog> Context<'a, T> {
    /// Ensure a VM exists for `collection`, lazily creating it and
    /// loading+registering all function kinds on first access.
    fn ensure_vm<'g>(
        &self,
        guard: &'g mut HashMap<String, VmEntry>,
        collection: &str,
    ) -> Result<Option<&'g VmEntry>, DbError> {
        let Some(factory) = self.vm_factory else {
            return Ok(None);
        };

        if !guard.contains_key(collection) {
            let mut vm = factory()?;

            let triggers = self.txn.load_functions(collection, FunctionKind::Trigger)?;
            let trigger_names: Vec<String> = triggers.iter().map(|f| f.name.clone()).collect();
            for f in &triggers {
                vm.register(&f.name, &f.source)?;
            }

            guard.insert(
                collection.to_string(),
                VmEntry {
                    vm,
                    triggers: trigger_names,
                },
            );
        }

        Ok(guard.get(collection))
    }

    pub(crate) fn fire_triggers(
        &self,
        collection: &str,
        action: &str,
        doc: &RawDocument,
    ) -> Result<(), DbError> {
        let Some(registry) = self.vm_registry else {
            return Ok(());
        };

        let mut guard = registry.lock().unwrap();
        let Some(entry) = self.ensure_vm(&mut guard, collection)? else {
            return Ok(());
        };

        if entry.triggers.is_empty() {
            return Ok(());
        }

        let txn = self.txn;

        let get_cb = |args: Vec<bson::Bson>| -> Result<bson::Bson, slate_vm::VmError> {
            let coll_name = args
                .first()
                .and_then(|b| b.as_str())
                .ok_or_else(|| {
                    slate_vm::VmError::InvalidReturn(
                        "ctx.get: first argument must be a collection name".into(),
                    )
                })?;
            let doc_id = args.get(1).ok_or_else(|| {
                slate_vm::VmError::InvalidReturn("ctx.get: second argument (id) required".into())
            })?;

            let handle = txn
                .collection(coll_name)
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
                    let document: bson::Document =
                        bson::deserialize_from_slice(doc.as_bytes())
                            .map_err(|e| slate_vm::VmError::Bson(e))?;
                    Ok(bson::Bson::Document(document))
                }
                Ok(None) => Ok(bson::Bson::Null),
                Err(e) => Err(slate_vm::VmError::InvalidReturn(e.to_string())),
            }
        };

        let put_cb = |args: Vec<bson::Bson>| -> Result<bson::Bson, slate_vm::VmError> {
            let coll_name = args
                .first()
                .and_then(|b| b.as_str())
                .ok_or_else(|| {
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
                    ))
                }
            };

            let handle = txn
                .collection(coll_name)
                .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?;

            let raw = bson::raw::RawDocumentBuf::try_from(doc)
                .map_err(|e| slate_vm::VmError::Bson(e))?;

            txn.put(&handle, &raw)
                .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?;

            Ok(bson::Bson::Null)
        };

        let delete_cb = |args: Vec<bson::Bson>| -> Result<bson::Bson, slate_vm::VmError> {
            let coll_name = args
                .first()
                .and_then(|b| b.as_str())
                .ok_or_else(|| {
                    slate_vm::VmError::InvalidReturn(
                        "ctx.delete: first argument must be a collection name".into(),
                    )
                })?;
            let doc_id = args.get(1).ok_or_else(|| {
                slate_vm::VmError::InvalidReturn(
                    "ctx.delete: second argument (id) required".into(),
                )
            })?;

            let handle = txn
                .collection(coll_name)
                .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?;

            let wrapper = bson::raw::RawDocumentBuf::try_from(bson::doc! { "v": doc_id.clone() })
                .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?;
            let raw_ref = wrapper
                .get("v")
                .map_err(|e| slate_vm::VmError::InvalidReturn(e.to_string()))?
                .ok_or_else(|| {
                    slate_vm::VmError::InvalidReturn(
                        "ctx.delete: failed to encode doc_id".into(),
                    )
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
        for name in &entry.triggers {
            entry.vm.call_with_scope(name, &input, &methods)?;
        }
        Ok(())
    }
}
