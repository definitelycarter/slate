use std::cell::RefCell;

use bson::Bson;
use wasm_bindgen::prelude::*;

use crate::{ScopedMethod, VmError};

// ---------------------------------------------------------------------------
// wasm-bindgen imports — JS must provide these functions
// ---------------------------------------------------------------------------

#[wasm_bindgen]
extern "C" {
    /// Compile a script and return an integer handle ID.
    ///
    /// The JS host stores the compiled function keyed by this ID.
    #[wasm_bindgen(js_name = "slate_vm_load")]
    pub fn js_load(name: &str, source: &[u8]) -> u32;

    /// Execute a previously compiled script.
    ///
    /// - `handle`: ID returned by `slate_vm_load`
    /// - `input`:  BSON document bytes (the script input)
    /// - `caps`:   capability level (0 = Pure, 1 = ReadOnly, 2 = ReadWrite)
    ///
    /// Returns BSON document bytes (the script output).
    /// If the JS side encounters an error, it should return a BSON document
    /// with an `"error"` field: `{ "error": "message" }`.
    #[wasm_bindgen(js_name = "slate_vm_call")]
    pub fn js_call(handle: u32, input: &[u8], caps: u8) -> Vec<u8>;
}

// ---------------------------------------------------------------------------
// Scoped method registry — thread-local, valid only during call()
// ---------------------------------------------------------------------------

/// Raw pointer to the methods slice, with lifetime erased.
///
/// # Safety
///
/// - Only written immediately before `js_call` and cleared immediately after.
/// - wasm32 is single-threaded, so no data races.
/// - The pointer is only dereferenced inside `slate_vm_invoke_method`, which
///   is called synchronously by JS during `js_call`.
struct MethodsPtr(*const [ScopedMethod<'static>]);

// SAFETY: wasm32 is single-threaded.
unsafe impl Send for MethodsPtr {}

thread_local! {
    static METHODS: RefCell<Option<MethodsPtr>> = const { RefCell::new(None) };
}

/// Install scoped methods for the duration of a `js_call`.
///
/// # Safety
///
/// Caller must ensure `clear_methods()` is called before the borrowed
/// methods go out of scope.
pub(crate) unsafe fn install_methods(methods: &[ScopedMethod<'_>]) {
    let ptr = methods as *const [ScopedMethod<'_>] as *const [ScopedMethod<'static>];
    METHODS.with(|m| {
        *m.borrow_mut() = Some(MethodsPtr(ptr));
    });
}

/// Clear the scoped method registry.
pub(crate) fn clear_methods() {
    METHODS.with(|m| {
        *m.borrow_mut() = None;
    });
}

// ---------------------------------------------------------------------------
// Exported callback — JS calls this when a script invokes ctx.get/put/delete
// ---------------------------------------------------------------------------

/// Invoke a scoped method by name.
///
/// Called synchronously by the JS host when a script calls `ctx.get(...)`,
/// `ctx.put(...)`, etc. The `args_bson` is a BSON document with positional
/// keys: `{ "0": arg0, "1": arg1, ... }`.
///
/// Returns a BSON document `{ "v": result }` on success, or
/// `{ "error": "message" }` on failure.
#[wasm_bindgen]
pub fn slate_vm_invoke_method(name: &str, args_bson: &[u8]) -> Vec<u8> {
    match invoke_method_inner(name, args_bson) {
        Ok(bytes) => bytes,
        Err(msg) => {
            let doc = bson::doc! { "error": msg };
            bson::raw::RawDocumentBuf::try_from(doc)
                .expect("error doc serialization")
                .into_bytes()
        }
    }
}

fn invoke_method_inner(name: &str, args_bson: &[u8]) -> Result<Vec<u8>, String> {
    METHODS.with(|m| {
        let guard = m.borrow();
        let ptr = guard
            .as_ref()
            .ok_or_else(|| "no scoped methods registered".to_string())?;

        // SAFETY: pointer is valid — we're inside a synchronous js_call.
        let methods: &[ScopedMethod<'_>] = unsafe { &*ptr.0 };

        let method = methods
            .iter()
            .find(|sm| sm.name == name)
            .ok_or_else(|| format!("unknown method: {name}"))?;

        // Deserialize positional args from BSON document.
        let args = deserialize_args(args_bson)?;

        // Call the Rust closure.
        let result = (method.callback)(args).map_err(|e| e.to_string())?;

        // Serialize result as { "v": result }.
        let doc = bson::doc! { "v": result };
        let raw = bson::raw::RawDocumentBuf::try_from(doc)
            .map_err(|e| e.to_string())?;
        Ok(raw.into_bytes())
    })
}

/// Deserialize `{ "0": v0, "1": v1, ... }` into `Vec<Bson>`.
fn deserialize_args(bson_bytes: &[u8]) -> Result<Vec<Bson>, String> {
    let raw = bson::raw::RawDocumentBuf::from_bytes(bson_bytes.to_vec())
        .map_err(|e| e.to_string())?;
    let doc: bson::Document =
        bson::deserialize_from_slice(raw.as_bytes()).map_err(|e| e.to_string())?;

    let mut args = Vec::new();
    for i in 0.. {
        let key = i.to_string();
        match doc.get(&key) {
            Some(val) => args.push(val.clone()),
            None => break,
        }
    }
    Ok(args)
}

/// Parse a BSON result from JS, checking for error documents.
pub(crate) fn parse_result(bytes: Vec<u8>) -> Result<bson::raw::RawDocumentBuf, VmError> {
    let raw =
        bson::raw::RawDocumentBuf::from_bytes(bytes).map_err(|e| VmError::Js(e.to_string()))?;

    // Check for error sentinel: { "error": "message" }
    if let Ok(Some(err_val)) = raw.get("error") {
        if let bson::raw::RawBsonRef::String(msg) = err_val {
            return Err(VmError::Js(msg.to_string()));
        }
    }

    Ok(raw)
}
