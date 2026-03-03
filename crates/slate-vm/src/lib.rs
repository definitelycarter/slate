mod error;
#[cfg(feature = "lua")]
pub mod lua;
pub mod pool;

pub use error::VmError;
#[cfg(feature = "lua")]
pub use lua::{LuaError, LuaScriptHandle, LuaScriptRuntime};

use bson::Bson;
use bson::raw::RawDocumentBuf;

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

/// Identifies which scripting runtime a script targets.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum RuntimeKind {
    #[cfg(feature = "lua")]
    Lua,
    #[cfg(feature = "wasm")]
    Wasm,
}

/// A borrowed callback for use within a single scoped script call.
///
/// Does not require `Send` or `'static`, enabling capture of borrowed
/// transaction references. Methods are placed on a context table passed
/// as the first argument to the script function.
pub struct ScopedMethod<'a> {
    pub name: &'a str,
    pub callback: &'a dyn Fn(Vec<Bson>) -> Result<Bson, VmError>,
}

/// Describes what external access a script has during execution.
///
/// Capabilities are provided at call time, not compile time, so that
/// cached compiled scripts can be reused across transactions.
pub enum ScriptCapabilities<'a> {
    /// Pure function — no external access.
    Pure,
    /// Read-only access via scoped methods.
    ReadOnly { methods: &'a [ScopedMethod<'a>] },
    /// Full read-write access via scoped methods.
    ReadWrite { methods: &'a [ScopedMethod<'a>] },
}

// ---------------------------------------------------------------------------
// Runtime-agnostic traits
// ---------------------------------------------------------------------------

/// A scripting runtime that can compile source into executable handles.
///
/// Each runtime (Lua, Wasm, etc.) implements this trait once. The pool
/// calls [`ScriptRuntime::load`] to compile source bytes and caches the
/// returned handle.
pub trait ScriptRuntime: Send + Sync {
    /// Compile source bytes into an executable handle.
    fn load(&self, name: &str, source: &[u8]) -> Result<Box<dyn ScriptHandle>, VmError>;

    /// Which runtime kind this is.
    fn runtime_kind(&self) -> RuntimeKind;
}

/// A compiled script ready to execute.
///
/// Handles are cached by the [`pool::VmPool`] and shared via `Arc`.
/// Capabilities are injected at call time so the same compiled handle
/// can be reused across different transactions.
pub trait ScriptHandle: Send + Sync {
    /// Execute the script with the given BSON input and capabilities.
    fn call(
        &self,
        input: &RawDocumentBuf,
        capabilities: &ScriptCapabilities<'_>,
    ) -> Result<RawDocumentBuf, VmError>;
}

