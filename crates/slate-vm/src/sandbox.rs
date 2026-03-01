use crate::VmError;

const BLOCKED_GLOBALS: &[&str] = &[
    "os",
    "io",
    "loadfile",
    "dofile",
    "require",
    "debug",
    "package",
    "collectgarbage",
    "rawset",
];

/// Configure a Lua VM with sandboxing: remove dangerous globals and set
/// an instruction limit hook.
pub(crate) fn configure(lua: &mlua::Lua, instruction_limit: u32) -> Result<(), VmError> {
    let globals = lua.globals();
    for name in BLOCKED_GLOBALS {
        globals.set(*name, mlua::Value::Nil)?;
    }
    set_instruction_hook(lua, instruction_limit)?;
    Ok(())
}

/// Install (or reset) the instruction-count hook on the VM.
pub(crate) fn set_instruction_hook(lua: &mlua::Lua, limit: u32) -> Result<(), VmError> {
    lua.set_hook(
        mlua::HookTriggers::new().every_nth_instruction(limit),
        |_, _| Err(mlua::Error::RuntimeError("instruction limit exceeded".into())),
    )?;
    Ok(())
}

/// Create a read-only environment table for a function.
///
/// The table reads from `base` via `__index` but blocks writes via `__newindex`.
/// This prevents user functions from leaking state through globals.
pub(crate) fn read_only_env(lua: &mlua::Lua, base: &mlua::Table) -> Result<mlua::Table, VmError> {
    let env = lua.create_table()?;
    let meta = lua.create_table()?;

    meta.set("__index", base.clone())?;
    meta.set(
        "__newindex",
        lua.create_function(|_, (_t, key, _val): (mlua::Value, mlua::String, mlua::Value)| {
            Err::<(), _>(mlua::Error::RuntimeError(format!(
                "cannot set global '{}'",
                key.to_str()?
            )))
        })?,
    )?;

    env.set_metatable(Some(meta))?;
    Ok(env)
}
