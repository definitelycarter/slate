use mlua::Lua;

use super::convert;
use super::error::LuaError;
use super::sandbox;
use crate::error::VmError;
use crate::{RuntimeKind, ScriptCapabilities, ScriptHandle, ScriptRuntime, ScopedMethod};
use bson::raw::RawDocumentBuf;

const DEFAULT_INSTRUCTION_LIMIT: u32 = 100_000;

// ---------------------------------------------------------------------------
// LuaScriptRuntime
// ---------------------------------------------------------------------------

/// Configuration and factory for Lua script handles.
pub struct LuaScriptRuntime {
    instruction_limit: u32,
}

impl LuaScriptRuntime {
    pub fn new() -> Self {
        Self {
            instruction_limit: DEFAULT_INSTRUCTION_LIMIT,
        }
    }

    pub fn with_instruction_limit(mut self, limit: u32) -> Self {
        self.instruction_limit = limit;
        self
    }
}

impl ScriptRuntime for LuaScriptRuntime {
    fn load(&self, name: &str, source: &[u8]) -> Result<Box<dyn ScriptHandle>, VmError> {
        let lua = Lua::new();
        sandbox::configure(&lua, self.instruction_limit)?;

        let bson_module = convert::create_bson_module(&lua)?;
        lua.globals().set("bson", bson_module)?;

        let source_str = std::str::from_utf8(source)
            .map_err(|e| LuaError::from(mlua::Error::RuntimeError(e.to_string())))?;

        let chunk = lua.load(source_str).set_name(name);
        let value: mlua::Value = chunk.eval()?;

        let func = match value {
            mlua::Value::Function(f) => f,
            _ => {
                return Err(VmError::InvalidReturn(
                    "source must return a function".into(),
                ));
            }
        };

        let env = sandbox::read_only_env(&lua, &lua.globals())?;
        func.set_environment(env)?;

        let func_key = lua.create_registry_value(func)?;

        Ok(Box::new(LuaScriptHandle {
            lua,
            func_key,
            instruction_limit: self.instruction_limit,
        }))
    }

    fn runtime_kind(&self) -> RuntimeKind {
        RuntimeKind::Lua
    }
}

// ---------------------------------------------------------------------------
// LuaScriptHandle
// ---------------------------------------------------------------------------

/// A compiled Lua function with its own sandboxed VM.
pub struct LuaScriptHandle {
    lua: Lua,
    func_key: mlua::RegistryKey,
    instruction_limit: u32,
}

impl ScriptHandle for LuaScriptHandle {
    fn call(
        &self,
        input: &RawDocumentBuf,
        capabilities: &ScriptCapabilities<'_>,
    ) -> Result<RawDocumentBuf, VmError> {
        let func: mlua::Function = self.lua.registry_value(&self.func_key)?;
        sandbox::set_instruction_hook(&self.lua, self.instruction_limit)?;

        let input_table = convert::doc_to_table(&self.lua, input)?;

        match capabilities {
            ScriptCapabilities::Pure => self.call_pure(&func, input_table),
            ScriptCapabilities::ReadOnly { methods }
            | ScriptCapabilities::ReadWrite { methods } => {
                self.call_with_methods(&func, input_table, methods)
            }
        }
    }
}

impl LuaScriptHandle {
    /// Call as `func(input)` — no context table.
    fn call_pure(
        &self,
        func: &mlua::Function,
        input_table: mlua::Table,
    ) -> Result<RawDocumentBuf, VmError> {
        let result: mlua::Value = func.call(input_table).map_err(|e| {
            if is_instruction_limit_error(&e) {
                VmError::InstructionLimit
            } else {
                VmError::from(e)
            }
        })?;

        match result {
            mlua::Value::Table(t) => convert::table_to_doc(&t),
            _ => Err(VmError::InvalidReturn(
                "function must return a table".into(),
            )),
        }
    }

    /// Call as `func(ctx, input)` with scoped methods on the context table.
    fn call_with_methods(
        &self,
        func: &mlua::Function,
        input_table: mlua::Table,
        methods: &[ScopedMethod<'_>],
    ) -> Result<RawDocumentBuf, VmError> {
        self.lua
            .scope(|scope| {
                let ctx = self.lua.create_table()?;

                for sm in methods {
                    let cb = sm.callback;
                    let lua_fn =
                        scope.create_function(move |lua, args: mlua::MultiValue| {
                            let bson_args: Vec<bson::Bson> = args
                                .into_iter()
                                .map(|v| {
                                    convert::lua_value_to_bson(v)
                                        .map_err(|e| mlua::Error::RuntimeError(e.to_string()))
                                })
                                .collect::<Result<_, _>>()?;

                            let result = cb(bson_args)
                                .map_err(|e| mlua::Error::RuntimeError(e.to_string()))?;

                            convert::bson_to_lua(lua, result)
                                .map_err(|e| mlua::Error::RuntimeError(e.to_string()))
                        })?;

                    ctx.set(sm.name, lua_fn)?;
                }

                let result: mlua::Value =
                    func.call((ctx, input_table)).map_err(|e| {
                        if is_instruction_limit_error(&e) {
                            mlua::Error::RuntimeError("instruction limit exceeded".into())
                        } else {
                            e
                        }
                    })?;

                match result {
                    mlua::Value::Table(t) => convert::table_to_doc(&t)
                        .map_err(|e| mlua::Error::RuntimeError(e.to_string())),
                    _ => Err(mlua::Error::RuntimeError(
                        "function must return a table".into(),
                    )),
                }
            })
            .map_err(|e| {
                if is_instruction_limit_error(&e) {
                    VmError::InstructionLimit
                } else {
                    VmError::from(e)
                }
            })
    }
}

fn is_instruction_limit_error(e: &mlua::Error) -> bool {
    match e {
        mlua::Error::RuntimeError(msg) => msg.contains("instruction limit"),
        mlua::Error::CallbackError { cause, .. } => is_instruction_limit_error(cause),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use bson::{Bson, rawdoc};

    fn load(source: &[u8]) -> Box<dyn ScriptHandle> {
        let rt = LuaScriptRuntime::new();
        rt.load("f", source).unwrap()
    }

    // --- Pure calls ---

    #[test]
    fn pure_identity() {
        let handle = load(b"return function(doc) return doc end");
        let input = rawdoc! { "name": "alice", "age": 30 };
        let output = handle.call(&input, &ScriptCapabilities::Pure).unwrap();
        assert_eq!(output.get_str("name").unwrap(), "alice");
        assert_eq!(output.get_i32("age").unwrap(), 30);
    }

    #[test]
    fn pure_validator() {
        let handle = load(
            br#"return function(doc)
                if doc.name ~= nil then
                    return { ok = true }
                else
                    return { ok = false, reason = "name is required" }
                end
            end"#,
        );

        let pass = handle
            .call(&rawdoc! { "name": "alice" }, &ScriptCapabilities::Pure)
            .unwrap();
        assert_eq!(pass.get_bool("ok").unwrap(), true);

        let fail = handle
            .call(&rawdoc! { "status": "active" }, &ScriptCapabilities::Pure)
            .unwrap();
        assert_eq!(fail.get_bool("ok").unwrap(), false);
    }

    #[test]
    fn pure_computed_field() {
        let handle = load(
            br#"return function(doc)
                return { value = doc.first .. " " .. doc.last }
            end"#,
        );
        let input = rawdoc! { "first": "Ada", "last": "Lovelace" };
        let result = handle.call(&input, &ScriptCapabilities::Pure).unwrap();
        assert_eq!(result.get_str("value").unwrap(), "Ada Lovelace");
    }

    #[test]
    fn pure_type_roundtrip() {
        let handle = load(b"return function(doc) return doc end");

        // i32 preserved
        let out = handle
            .call(&rawdoc! { "n": 42_i32 }, &ScriptCapabilities::Pure)
            .unwrap();
        assert_eq!(out.get_i32("n").unwrap(), 42);

        // DateTime preserved
        let ts = bson::DateTime::from_millis(1_700_000_000_000);
        let out = handle
            .call(&rawdoc! { "ts": ts }, &ScriptCapabilities::Pure)
            .unwrap();
        assert_eq!(out.get_datetime("ts").unwrap().timestamp_millis(), 1_700_000_000_000);
    }

    #[test]
    fn pure_roundtrip_i64() {
        let handle = load(b"return function(doc) return doc end");
        let out = handle
            .call(&rawdoc! { "n": 42_i64 }, &ScriptCapabilities::Pure)
            .unwrap();
        assert_eq!(out.get_i64("n").unwrap(), 42);
    }

    #[test]
    fn pure_roundtrip_float() {
        let handle = load(b"return function(doc) return doc end");
        let out = handle
            .call(&rawdoc! { "f": 3.14 }, &ScriptCapabilities::Pure)
            .unwrap();
        let f = out.get_f64("f").unwrap();
        assert!((f - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn pure_roundtrip_bool() {
        let handle = load(b"return function(doc) return doc end");
        let out = handle
            .call(&rawdoc! { "b": true }, &ScriptCapabilities::Pure)
            .unwrap();
        assert_eq!(out.get_bool("b").unwrap(), true);
    }

    #[test]
    fn pure_roundtrip_nested_doc() {
        let handle = load(b"return function(doc) return doc end");
        let input = rawdoc! { "addr": { "city": "Austin", "state": "TX" } };
        let out = handle.call(&input, &ScriptCapabilities::Pure).unwrap();
        let addr = out.get_document("addr").unwrap();
        assert_eq!(addr.get_str("city").unwrap(), "Austin");
        assert_eq!(addr.get_str("state").unwrap(), "TX");
    }

    #[test]
    fn pure_roundtrip_array() {
        let handle = load(b"return function(doc) return doc end");
        let input = rawdoc! { "tags": ["a", "b", "c"] };
        let out = handle.call(&input, &ScriptCapabilities::Pure).unwrap();
        let arr = out.get_array("tags").unwrap();
        let items: Vec<&str> = arr
            .into_iter()
            .map(|r| r.unwrap().as_str().unwrap())
            .collect();
        assert_eq!(items, vec!["a", "b", "c"]);
    }

    #[test]
    fn pure_datetime_constructor() {
        let handle = load(
            b"return function(doc) return { ts = bson.datetime(1700000000000) } end",
        );
        let result = handle.call(&rawdoc! {}, &ScriptCapabilities::Pure).unwrap();
        let dt = result.get_datetime("ts").unwrap();
        assert_eq!(dt.timestamp_millis(), 1_700_000_000_000);
    }

    #[test]
    fn pure_datetime_now() {
        let handle = load(
            b"return function(doc) return { ts = bson.now() } end",
        );
        let before = bson::DateTime::now().timestamp_millis();
        let result = handle.call(&rawdoc! {}, &ScriptCapabilities::Pure).unwrap();
        let after = bson::DateTime::now().timestamp_millis();
        let ts = result.get_datetime("ts").unwrap().timestamp_millis();
        assert!(ts >= before && ts <= after);
    }

    #[test]
    fn pure_objectid_read() {
        let handle = load(
            b"return function(doc) return { hex = doc._id:hex() } end",
        );
        let oid = bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        let input = rawdoc! { "_id": oid };
        let result = handle.call(&input, &ScriptCapabilities::Pure).unwrap();
        assert_eq!(result.get_str("hex").unwrap(), "507f1f77bcf86cd799439011");
    }

    #[test]
    fn pure_objectid_constructor() {
        let handle = load(
            br#"return function(doc)
                return { id = bson.objectid("507f1f77bcf86cd799439011") }
            end"#,
        );
        let result = handle.call(&rawdoc! {}, &ScriptCapabilities::Pure).unwrap();
        let oid = result.get_object_id("id").unwrap();
        assert_eq!(oid.to_hex(), "507f1f77bcf86cd799439011");
    }

    #[test]
    fn pure_sandbox_blocks_globals() {
        let handle = load(
            b"return function(doc) return { has_os = (os ~= nil) } end",
        );
        let result = handle
            .call(&rawdoc! {}, &ScriptCapabilities::Pure)
            .unwrap();
        assert_eq!(result.get_bool("has_os").unwrap(), false);
    }

    #[test]
    fn pure_instruction_limit() {
        let rt = LuaScriptRuntime::new().with_instruction_limit(100);
        let handle = rt
            .load("f", b"return function(doc) while true do end; return {} end")
            .unwrap();
        let result = handle.call(&rawdoc! {}, &ScriptCapabilities::Pure);
        assert!(matches!(result, Err(VmError::InstructionLimit)));
    }

    // --- Calls with capabilities ---

    #[test]
    fn readwrite_scoped_methods() {
        let handle = load(
            br#"return function(ctx, event)
                ctx.put("audit", event.doc.name)
                local found = ctx.get("users", event.doc._id)
                return { echo = found }
            end"#,
        );

        let ops: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let ops_clone = ops.clone();

        let put_cb = move |args: Vec<Bson>| -> Result<Bson, VmError> {
            let coll = args[0].as_str().unwrap().to_string();
            ops_clone.lock().unwrap().push(coll);
            Ok(Bson::Null)
        };

        let get_cb = |args: Vec<Bson>| -> Result<Bson, VmError> {
            let id = args[1].as_str().unwrap();
            Ok(Bson::String(format!("found:{id}")))
        };

        let methods = [
            ScopedMethod {
                name: "put",
                callback: &put_cb,
            },
            ScopedMethod {
                name: "get",
                callback: &get_cb,
            },
        ];

        let input = rawdoc! { "doc": { "_id": "r1", "name": "Alice" } };
        let caps = ScriptCapabilities::ReadWrite { methods: &methods };
        let result = handle.call(&input, &caps).unwrap();

        assert_eq!(result.get_str("echo").unwrap(), "found:r1");
        assert_eq!(ops.lock().unwrap().as_slice(), &["audit"]);
    }

    #[test]
    fn readonly_scoped_methods() {
        let handle = load(
            br#"return function(ctx, event)
                local val = ctx.get("users", event.key)
                return { result = val }
            end"#,
        );

        let get_cb = |_args: Vec<Bson>| -> Result<Bson, VmError> {
            Ok(Bson::String("hello".into()))
        };

        let methods = [ScopedMethod {
            name: "get",
            callback: &get_cb,
        }];

        let input = rawdoc! { "key": "k1" };
        let caps = ScriptCapabilities::ReadOnly { methods: &methods };
        let result = handle.call(&input, &caps).unwrap();
        assert_eq!(result.get_str("result").unwrap(), "hello");
    }

    #[test]
    fn scoped_callback_error_propagates() {
        let handle = load(
            br#"return function(ctx, event)
                ctx.fail()
                return event
            end"#,
        );

        let fail_cb = |_args: Vec<Bson>| -> Result<Bson, VmError> {
            Err(VmError::InvalidReturn("intentional error".into()))
        };

        let methods = [ScopedMethod {
            name: "fail",
            callback: &fail_cb,
        }];

        let caps = ScriptCapabilities::ReadWrite { methods: &methods };
        let result = handle.call(&rawdoc! {}, &caps);
        assert!(result.is_err());
    }

    // --- Pool integration ---

    #[test]
    fn pool_caches_and_calls() {
        use crate::pool::{RuntimeRegistry, VmPool};

        let mut registry = RuntimeRegistry::new();
        registry.register(RuntimeKind::Lua, Arc::new(LuaScriptRuntime::new()));

        let pool = VmPool::new(registry);

        let handle = pool
            .get_or_load(
                RuntimeKind::Lua,
                "add_field",
                1,
                br#"return function(doc) doc.added = true; return doc end"#,
            )
            .unwrap();

        let result = handle
            .call(&rawdoc! { "x": 1 }, &ScriptCapabilities::Pure)
            .unwrap();
        assert_eq!(result.get_bool("added").unwrap(), true);

        // Second get_or_load returns cached handle
        let handle2 = pool
            .get_or_load(
                RuntimeKind::Lua,
                "add_field",
                1,
                br#"return function(doc) doc.added = true; return doc end"#,
            )
            .unwrap();

        let result2 = handle2
            .call(&rawdoc! { "y": 2 }, &ScriptCapabilities::Pure)
            .unwrap();
        assert_eq!(result2.get_bool("added").unwrap(), true);
    }
}
