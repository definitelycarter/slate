use bson::raw::RawDocumentBuf;

use super::bridge;
use crate::error::VmError;
use crate::{RuntimeKind, ScriptCapabilities, ScriptHandle, ScriptRuntime};

// ---------------------------------------------------------------------------
// JsScriptRuntime
// ---------------------------------------------------------------------------

/// Script runtime that delegates to the JS host via `wasm-bindgen`.
///
/// The JS side must provide `slate_vm_load` and `slate_vm_call` functions.
pub struct JsScriptRuntime;

impl JsScriptRuntime {
    pub fn new() -> Self {
        Self
    }
}

impl ScriptRuntime for JsScriptRuntime {
    fn load(&self, name: &str, source: &[u8]) -> Result<Box<dyn ScriptHandle>, VmError> {
        let id = bridge::js_load(name, source);
        Ok(Box::new(JsScriptHandle { id }))
    }

    fn runtime_kind(&self) -> RuntimeKind {
        RuntimeKind::Js
    }
}

// ---------------------------------------------------------------------------
// JsScriptHandle
// ---------------------------------------------------------------------------

/// A compiled script handle identified by an integer ID.
///
/// The JS host maintains a `Map<u32, CompiledFunction>` and looks up
/// the function by this ID when `slate_vm_call` is invoked.
pub struct JsScriptHandle {
    id: u32,
}

// SAFETY: wasm32 is single-threaded — no cross-thread access is possible.
unsafe impl Send for JsScriptHandle {}
unsafe impl Sync for JsScriptHandle {}

impl ScriptHandle for JsScriptHandle {
    fn call(
        &self,
        input: &RawDocumentBuf,
        capabilities: &ScriptCapabilities<'_>,
    ) -> Result<RawDocumentBuf, VmError> {
        let caps_byte = match capabilities {
            ScriptCapabilities::Pure => 0u8,
            ScriptCapabilities::ReadOnly { .. } => 1,
            ScriptCapabilities::ReadWrite { .. } => 2,
        };

        // Install scoped methods so JS can call back via slate_vm_invoke_method.
        let methods = match capabilities {
            ScriptCapabilities::Pure => &[] as &[crate::ScopedMethod<'_>],
            ScriptCapabilities::ReadOnly { methods }
            | ScriptCapabilities::ReadWrite { methods } => methods,
        };

        // SAFETY: we clear the methods before returning, and wasm32 is
        // single-threaded, so the borrowed methods remain valid for the
        // entire duration of js_call.
        unsafe { bridge::install_methods(methods) };

        let result_bytes = bridge::js_call(self.id, input.as_bytes(), caps_byte);

        bridge::clear_methods();

        bridge::parse_result(result_bytes)
    }
}
