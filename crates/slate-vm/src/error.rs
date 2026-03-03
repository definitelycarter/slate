use std::fmt;

#[derive(Debug)]
pub enum VmError {
    /// A JS-side error (only available with the `js` feature).
    #[cfg(feature = "js")]
    Js(String),
    /// A Lua-specific error (only available with the `lua` feature).
    #[cfg(feature = "lua")]
    Lua(crate::lua::LuaError),
    /// BSON serialization/deserialization failure during conversion.
    Bson(bson::error::Error),
    /// The script returned a value that could not be converted to BSON.
    InvalidReturn(String),
    /// Script exceeded the instruction limit.
    InstructionLimit,
    /// No runtime registered for the requested kind.
    UnsupportedRuntime(crate::RuntimeKind),
}

impl fmt::Display for VmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "js")]
            Self::Js(msg) => write!(f, "js runtime error: {msg}"),
            #[cfg(feature = "lua")]
            Self::Lua(e) => write!(f, "{e}"),
            Self::Bson(e) => write!(f, "bson error: {e}"),
            Self::InvalidReturn(msg) => write!(f, "invalid return value: {msg}"),
            Self::InstructionLimit => write!(f, "instruction limit exceeded"),
            Self::UnsupportedRuntime(kind) => write!(f, "unsupported runtime: {kind:?}"),
        }
    }
}

impl std::error::Error for VmError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            #[cfg(feature = "js")]
            Self::Js(_) => None,
            #[cfg(feature = "lua")]
            Self::Lua(e) => Some(e),
            Self::Bson(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(feature = "lua")]
impl From<crate::lua::LuaError> for VmError {
    fn from(e: crate::lua::LuaError) -> Self {
        Self::Lua(e)
    }
}

impl From<bson::error::Error> for VmError {
    fn from(e: bson::error::Error) -> Self {
        Self::Bson(e)
    }
}
