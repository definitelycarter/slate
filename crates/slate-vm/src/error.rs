use std::fmt;

#[derive(Debug)]
pub enum VmError {
    /// A Lua runtime or compilation error.
    Lua(mlua::Error),
    /// BSON serialization/deserialization failure during conversion.
    Bson(bson::error::Error),
    /// The named function was not found in the registry.
    NotFound(String),
    /// The named function is already registered.
    AlreadyRegistered(String),
    /// The Lua function returned a value that could not be converted to BSON.
    InvalidReturn(String),
    /// Script exceeded the instruction limit.
    InstructionLimit,
}

impl fmt::Display for VmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lua(e) => write!(f, "lua error: {e}"),
            Self::Bson(e) => write!(f, "bson error: {e}"),
            Self::NotFound(name) => write!(f, "function not found: {name}"),
            Self::AlreadyRegistered(name) => write!(f, "function already registered: {name}"),
            Self::InvalidReturn(msg) => write!(f, "invalid return value: {msg}"),
            Self::InstructionLimit => write!(f, "instruction limit exceeded"),
        }
    }
}

impl std::error::Error for VmError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Lua(e) => Some(e),
            Self::Bson(e) => Some(e),
            _ => None,
        }
    }
}

impl From<mlua::Error> for VmError {
    fn from(e: mlua::Error) -> Self {
        Self::Lua(e)
    }
}

impl From<bson::error::Error> for VmError {
    fn from(e: bson::error::Error) -> Self {
        Self::Bson(e)
    }
}
