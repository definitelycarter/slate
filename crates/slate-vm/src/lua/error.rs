use std::fmt;

#[derive(Debug)]
pub enum LuaError {
    /// A Lua runtime or compilation error.
    Runtime(mlua::Error),
}

impl fmt::Display for LuaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(e) => write!(f, "lua error: {e}"),
        }
    }
}

impl std::error::Error for LuaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(e) => Some(e),
        }
    }
}

impl From<mlua::Error> for LuaError {
    fn from(e: mlua::Error) -> Self {
        Self::Runtime(e)
    }
}

impl From<mlua::Error> for crate::VmError {
    fn from(e: mlua::Error) -> Self {
        Self::Lua(LuaError::Runtime(e))
    }
}
