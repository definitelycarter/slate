mod error;
#[cfg(feature = "lua")]
pub mod lua;

pub use error::VmError;
#[cfg(feature = "lua")]
pub use lua::{LuaError, LuaVm};

use std::sync::Arc;

use bson::raw::RawDocumentBuf;
use bson::Bson;

/// A callback function that can be injected into the VM.
///
/// Receives positional arguments as BSON values and returns a BSON value.
pub type VmCallback = Arc<dyn Fn(Vec<Bson>) -> Result<Bson, VmError> + Send + Sync>;

/// A borrowed callback for use within a single scoped VM call.
///
/// Unlike [`VmCallback`], this does not require `Send` or `'static`,
/// enabling capture of borrowed transaction references. Methods are
/// placed on a context table passed as the first argument to the
/// registered function.
pub struct ScopedMethod<'a> {
    pub name: &'a str,
    pub callback: &'a dyn Fn(Vec<Bson>) -> Result<Bson, VmError>,
}

/// Abstraction over a scripting engine that processes BSON documents.
///
/// Functions are compiled once via [`Vm::register`] and called with BSON
/// input/output via [`Vm::call`]. The VM implementation is responsible for
/// sandboxing and resource limits.
pub trait Vm {
    /// Compile and cache a named function from source bytes.
    ///
    /// For Lua, the source should be a UTF-8 chunk that returns a function:
    /// ```lua
    /// return function(ctx, event)
    ///     return { ok = true }
    /// end
    /// ```
    fn register(&mut self, name: &str, source: &[u8]) -> Result<(), VmError>;

    /// Remove a previously registered function.
    fn unregister(&mut self, name: &str) -> Result<(), VmError>;

    /// Call a registered function, passing a BSON document as input
    /// and receiving a BSON document as output.
    fn call(&self, name: &str, input: &RawDocumentBuf) -> Result<RawDocumentBuf, VmError>;

    /// Call a registered function with scoped callbacks available as
    /// methods on a context table passed as the first argument.
    ///
    /// The callbacks exist only for the duration of this call and are
    /// cleaned up before the method returns, enabling triggers to
    /// call back into the database transaction without requiring
    /// `'static` lifetimes.
    ///
    /// The registered function is called as `func(ctx, input)`.
    fn call_with_scope(
        &self,
        name: &str,
        input: &RawDocumentBuf,
        methods: &[ScopedMethod<'_>],
    ) -> Result<RawDocumentBuf, VmError>;

    /// Inject a named callback into a namespace, making it callable from scripts.
    ///
    /// For example, `inject("db", "put", cb)` makes `db.put(args)` available
    /// in Lua. The callback receives a single BSON document and returns one.
    fn inject(
        &self,
        namespace: &str,
        method: &str,
        callback: VmCallback,
    ) -> Result<(), VmError>;

    /// Remove a previously injected namespace and all its methods.
    fn eject(&self, namespace: &str) -> Result<(), VmError>;
}
