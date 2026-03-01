use std::collections::HashMap;

use bson::raw::RawDocumentBuf;
use mlua::Lua;

use crate::convert;
use crate::error::VmError;
use crate::sandbox;
use crate::{ScopedMethod, Vm, VmCallback};

const DEFAULT_INSTRUCTION_LIMIT: u32 = 100_000;

pub struct LuaVm {
    lua: Lua,
    registry: HashMap<String, mlua::RegistryKey>,
    instruction_limit: u32,
}

impl LuaVm {
    pub fn new() -> Result<Self, VmError> {
        Self::with_instruction_limit(DEFAULT_INSTRUCTION_LIMIT)
    }

    pub fn with_instruction_limit(limit: u32) -> Result<Self, VmError> {
        let lua = Lua::new();
        sandbox::configure(&lua, limit)?;

        let bson_module = convert::create_bson_module(&lua)?;
        lua.globals().set("bson", bson_module)?;

        Ok(Self {
            lua,
            registry: HashMap::new(),
            instruction_limit: limit,
        })
    }
}

impl Vm for LuaVm {
    fn register(&mut self, name: &str, source: &[u8]) -> Result<(), VmError> {
        if self.registry.contains_key(name) {
            return Err(VmError::AlreadyRegistered(name.to_string()));
        }

        let source_str = std::str::from_utf8(source)
            .map_err(|e| VmError::Lua(mlua::Error::RuntimeError(e.to_string())))?;

        // Evaluate the chunk — it should return a function
        let chunk = self.lua.load(source_str).set_name(name);
        let value: mlua::Value = chunk.eval()?;

        let func = match value {
            mlua::Value::Function(f) => f,
            _ => {
                return Err(VmError::InvalidReturn(
                    "register source must return a function".into(),
                ));
            }
        };

        // Apply read-only environment
        let env = sandbox::read_only_env(&self.lua, &self.lua.globals())?;
        func.set_environment(env)?;

        let key = self.lua.create_registry_value(func)?;
        self.registry.insert(name.to_string(), key);
        Ok(())
    }

    fn unregister(&mut self, name: &str) -> Result<(), VmError> {
        let key = self
            .registry
            .remove(name)
            .ok_or_else(|| VmError::NotFound(name.to_string()))?;
        self.lua.remove_registry_value(key)?;
        Ok(())
    }

    fn inject(
        &self,
        namespace: &str,
        method: &str,
        callback: VmCallback,
    ) -> Result<(), VmError> {
        let globals = self.lua.globals();

        // Get or create the namespace table
        let ns_table: mlua::Table = match globals.get::<mlua::Value>(namespace)? {
            mlua::Value::Table(t) => t,
            mlua::Value::Nil => {
                let t = self.lua.create_table()?;
                globals.raw_set(namespace, t.clone())?;
                t
            }
            _ => {
                return Err(VmError::InvalidReturn(format!(
                    "global '{namespace}' exists and is not a table"
                )));
            }
        };

        // Create a Lua function backed by the Rust callback
        let lua_fn = self.lua.create_function(move |lua, args: mlua::MultiValue| {
            // Convert each Lua arg to a Bson value
            let bson_args: Vec<bson::Bson> = args
                .into_iter()
                .map(|v| {
                    convert::lua_value_to_bson(v)
                        .map_err(|e| mlua::Error::RuntimeError(e.to_string()))
                })
                .collect::<Result<_, _>>()?;

            // Call the Rust callback
            let result = callback(bson_args)
                .map_err(|e| mlua::Error::RuntimeError(e.to_string()))?;

            // Convert result back to Lua
            convert::bson_to_lua(lua, result)
                .map_err(|e| mlua::Error::RuntimeError(e.to_string()))
        })?;

        ns_table.set(method, lua_fn)?;
        Ok(())
    }

    fn eject(&self, namespace: &str) -> Result<(), VmError> {
        self.lua.globals().raw_set(namespace, mlua::Value::Nil)?;
        Ok(())
    }

    fn call(&self, name: &str, input: &RawDocumentBuf) -> Result<RawDocumentBuf, VmError> {
        let key = self
            .registry
            .get(name)
            .ok_or_else(|| VmError::NotFound(name.to_string()))?;
        let func: mlua::Function = self.lua.registry_value(key)?;

        // Reset instruction counter for this call
        sandbox::set_instruction_hook(&self.lua, self.instruction_limit)?;

        // Convert input BSON → Lua table
        let input_table = convert::doc_to_table(&self.lua, input)?;

        // Call the function
        let result: mlua::Value = func.call(input_table).map_err(|e| {
            if is_instruction_limit_error(&e) {
                VmError::InstructionLimit
            } else {
                VmError::Lua(e)
            }
        })?;

        // Convert result Lua table → BSON
        match result {
            mlua::Value::Table(t) => convert::table_to_doc(&t),
            _ => Err(VmError::InvalidReturn(
                "function must return a table".into(),
            )),
        }
    }

    fn call_with_scope(
        &self,
        name: &str,
        input: &RawDocumentBuf,
        methods: &[ScopedMethod<'_>],
    ) -> Result<RawDocumentBuf, VmError> {
        let key = self
            .registry
            .get(name)
            .ok_or_else(|| VmError::NotFound(name.to_string()))?;
        let func: mlua::Function = self.lua.registry_value(key)?;

        sandbox::set_instruction_hook(&self.lua, self.instruction_limit)?;

        let input_table = convert::doc_to_table(&self.lua, input)?;

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
                                    convert::lua_value_to_bson(v).map_err(|e| {
                                        mlua::Error::RuntimeError(e.to_string())
                                    })
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
                    VmError::Lua(e)
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

    fn vm() -> LuaVm {
        LuaVm::new().unwrap()
    }

    // --- Basic register and call ---

    #[test]
    fn register_and_call_identity() {
        let mut vm = vm();
        vm.register(
            "identity",
            b"return function(doc) return doc end",
        )
        .unwrap();

        let input = rawdoc! { "name": "alice", "age": 30 };
        let output = vm.call("identity", &input).unwrap();

        assert_eq!(output.get_str("name").unwrap(), "alice");
        assert_eq!(output.get_i32("age").unwrap(), 30);
    }

    // --- Validator patterns ---

    #[test]
    fn validator_accepts() {
        let mut vm = vm();
        vm.register(
            "validate",
            br#"return function(doc)
                if doc.name ~= nil then
                    return { ok = true }
                else
                    return { ok = false, reason = "name is required" }
                end
            end"#,
        )
        .unwrap();

        let input = rawdoc! { "name": "alice" };
        let result = vm.call("validate", &input).unwrap();
        assert_eq!(result.get_bool("ok").unwrap(), true);
    }

    #[test]
    fn validator_rejects() {
        let mut vm = vm();
        vm.register(
            "validate",
            br#"return function(doc)
                if doc.name ~= nil then
                    return { ok = true }
                else
                    return { ok = false, reason = "name is required" }
                end
            end"#,
        )
        .unwrap();

        let input = rawdoc! { "status": "active" };
        let result = vm.call("validate", &input).unwrap();
        assert_eq!(result.get_bool("ok").unwrap(), false);
        assert_eq!(
            result.get_str("reason").unwrap(),
            "name is required"
        );
    }

    // --- Computed field pattern ---

    #[test]
    fn computed_field() {
        let mut vm = vm();
        vm.register(
            "full_name",
            br#"return function(doc)
                return { value = doc.first .. " " .. doc.last }
            end"#,
        )
        .unwrap();

        let input = rawdoc! { "first": "Ada", "last": "Lovelace" };
        let result = vm.call("full_name", &input).unwrap();
        assert_eq!(result.get_str("value").unwrap(), "Ada Lovelace");
    }

    // --- BSON type round-tripping ---

    #[test]
    fn roundtrip_string() {
        let mut vm = vm();
        vm.register("identity", b"return function(doc) return doc end")
            .unwrap();
        let input = rawdoc! { "s": "hello world" };
        let output = vm.call("identity", &input).unwrap();
        assert_eq!(output.get_str("s").unwrap(), "hello world");
    }

    #[test]
    fn roundtrip_i64() {
        let mut vm = vm();
        vm.register("identity", b"return function(doc) return doc end")
            .unwrap();
        let input = rawdoc! { "n": 42_i64 };
        let output = vm.call("identity", &input).unwrap();
        assert_eq!(output.get_i64("n").unwrap(), 42);
    }

    #[test]
    fn roundtrip_i32_preserves_type() {
        let mut vm = vm();
        vm.register("identity", b"return function(doc) return doc end")
            .unwrap();
        let input = rawdoc! { "n": 42_i32 };
        let output = vm.call("identity", &input).unwrap();
        // Must come back as i32, not widened to i64
        assert_eq!(output.get_i32("n").unwrap(), 42);
    }

    #[test]
    fn i32_arithmetic_promotes_to_i64() {
        let mut vm = vm();
        vm.register(
            "inc",
            b"return function(doc) return { n = doc.n + 1 } end",
        )
        .unwrap();
        let input = rawdoc! { "n": 10_i32 };
        let output = vm.call("inc", &input).unwrap();
        // Arithmetic on i32 produces a Lua integer, which maps to i64
        assert_eq!(output.get_i64("n").unwrap(), 11);
    }

    #[test]
    fn roundtrip_float() {
        let mut vm = vm();
        vm.register("identity", b"return function(doc) return doc end")
            .unwrap();
        let input = rawdoc! { "f": 3.14 };
        let output = vm.call("identity", &input).unwrap();
        let f = output.get_f64("f").unwrap();
        assert!((f - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn roundtrip_boolean() {
        let mut vm = vm();
        vm.register("identity", b"return function(doc) return doc end")
            .unwrap();
        let input = rawdoc! { "b": true };
        let output = vm.call("identity", &input).unwrap();
        assert_eq!(output.get_bool("b").unwrap(), true);
    }

    #[test]
    fn roundtrip_nested_doc() {
        let mut vm = vm();
        vm.register("identity", b"return function(doc) return doc end")
            .unwrap();
        let input = rawdoc! { "addr": { "city": "Austin", "state": "TX" } };
        let output = vm.call("identity", &input).unwrap();
        let addr = output.get_document("addr").unwrap();
        assert_eq!(addr.get_str("city").unwrap(), "Austin");
        assert_eq!(addr.get_str("state").unwrap(), "TX");
    }

    #[test]
    fn roundtrip_array() {
        let mut vm = vm();
        vm.register("identity", b"return function(doc) return doc end")
            .unwrap();
        let input = rawdoc! { "tags": ["a", "b", "c"] };
        let output = vm.call("identity", &input).unwrap();
        let arr = output.get_array("tags").unwrap();
        let items: Vec<&str> = arr
            .into_iter()
            .map(|r| r.unwrap().as_str().unwrap())
            .collect();
        assert_eq!(items, vec!["a", "b", "c"]);
    }

    // --- DateTime userdata ---

    #[test]
    fn datetime_read_millis() {
        let mut vm = vm();
        vm.register(
            "read_dt",
            b"return function(doc) return { millis = doc.ts:millis() } end",
        )
        .unwrap();

        let ts = bson::DateTime::from_millis(1_700_000_000_000);
        let input = rawdoc! { "ts": ts };
        let result = vm.call("read_dt", &input).unwrap();
        assert_eq!(result.get_i64("millis").unwrap(), 1_700_000_000_000);
    }

    #[test]
    fn datetime_constructor() {
        let mut vm = vm();
        vm.register(
            "make_dt",
            b"return function(doc) return { ts = bson.datetime(1700000000000) } end",
        )
        .unwrap();

        let result = vm.call("make_dt", &rawdoc! {}).unwrap();
        let dt = result.get_datetime("ts").unwrap();
        assert_eq!(dt.timestamp_millis(), 1_700_000_000_000);
    }

    // --- ObjectId userdata ---

    #[test]
    fn objectid_read_hex() {
        let mut vm = vm();
        vm.register(
            "read_oid",
            b"return function(doc) return { hex = doc._id:hex() } end",
        )
        .unwrap();

        let oid = bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        let input = rawdoc! { "_id": oid };
        let result = vm.call("read_oid", &input).unwrap();
        assert_eq!(
            result.get_str("hex").unwrap(),
            "507f1f77bcf86cd799439011"
        );
    }

    #[test]
    fn objectid_constructor() {
        let mut vm = vm();
        vm.register(
            "make_oid",
            br#"return function(doc)
                return { id = bson.objectid("507f1f77bcf86cd799439011") }
            end"#,
        )
        .unwrap();

        let result = vm.call("make_oid", &rawdoc! {}).unwrap();
        let oid = result.get_object_id("id").unwrap();
        assert_eq!(oid.to_hex(), "507f1f77bcf86cd799439011");
    }

    // --- Sandbox enforcement ---

    #[test]
    fn sandbox_os_removed() {
        let mut vm = vm();
        vm.register(
            "check_os",
            b"return function(doc) return { has_os = (os ~= nil) } end",
        )
        .unwrap();

        let result = vm.call("check_os", &rawdoc! {}).unwrap();
        assert_eq!(result.get_bool("has_os").unwrap(), false);
    }

    #[test]
    fn sandbox_global_write_blocked() {
        let mut vm = vm();
        vm.register(
            "write_global",
            b"return function(doc) GLOBAL_VAR = 42; return {} end",
        )
        .unwrap();

        let result = vm.call("write_global", &rawdoc! {});
        assert!(result.is_err());
    }

    // --- Instruction limit ---

    #[test]
    fn instruction_limit_enforced() {
        let mut vm = LuaVm::with_instruction_limit(100).unwrap();
        vm.register(
            "infinite",
            b"return function(doc) while true do end; return {} end",
        )
        .unwrap();

        let result = vm.call("infinite", &rawdoc! {});
        assert!(matches!(result, Err(VmError::InstructionLimit)));
    }

    // --- Error cases ---

    #[test]
    fn call_not_found() {
        let vm = vm();
        let result = vm.call("nonexistent", &rawdoc! {});
        assert!(matches!(result, Err(VmError::NotFound(_))));
    }

    #[test]
    fn double_register_fails() {
        let mut vm = vm();
        vm.register("f", b"return function(doc) return {} end")
            .unwrap();
        let result = vm.register("f", b"return function(doc) return {} end");
        assert!(matches!(result, Err(VmError::AlreadyRegistered(_))));
    }

    #[test]
    fn unregister_not_found() {
        let mut vm = vm();
        let result = vm.unregister("nonexistent");
        assert!(matches!(result, Err(VmError::NotFound(_))));
    }

    #[test]
    fn unregister_then_reregister() {
        let mut vm = vm();
        vm.register("f", b"return function(doc) return { x = 1 } end")
            .unwrap();
        vm.unregister("f").unwrap();
        vm.register("f", b"return function(doc) return { x = 2 } end")
            .unwrap();

        let result = vm.call("f", &rawdoc! {}).unwrap();
        assert_eq!(result.get_i64("x").unwrap(), 2);
    }

    // --- Inject / eject ---

    #[test]
    fn inject_and_call() {
        let mut vm = vm();
        vm.register(
            "trigger",
            br#"return function(doc)
                local result = db.put("audit", doc.name)
                return { called = true, echo = result }
            end"#,
        )
        .unwrap();

        let cb: VmCallback = Arc::new(|args| {
            let collection = args[0].as_str().unwrap();
            let name = args[1].as_str().unwrap();
            Ok(Bson::String(format!("{collection}:{name}")))
        });
        vm.inject("db", "put", cb).unwrap();

        let result = vm.call("trigger", &rawdoc! { "name": "alice" }).unwrap();
        assert_eq!(result.get_bool("called").unwrap(), true);
        assert_eq!(result.get_str("echo").unwrap(), "audit:alice");
    }

    #[test]
    fn inject_records_operations() {
        let mut vm = vm();
        vm.register(
            "trigger",
            br#"return function(doc)
                db.put("audit", doc.name)
                db.put("metrics", "count")
                return {}
            end"#,
        )
        .unwrap();

        let ops: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let ops_clone = ops.clone();
        let cb: VmCallback = Arc::new(move |args| {
            let collection = args[0].as_str().unwrap();
            ops_clone.lock().unwrap().push(collection.to_string());
            Ok(Bson::Null)
        });
        vm.inject("db", "put", cb).unwrap();

        vm.call("trigger", &rawdoc! { "name": "alice" }).unwrap();

        let recorded = ops.lock().unwrap();
        assert_eq!(recorded.as_slice(), &["audit", "metrics"]);
    }

    #[test]
    fn inject_multiple_methods() {
        let mut vm = vm();
        vm.register(
            "trigger",
            br#"return function(doc)
                db.put("users", doc.name)
                local got = db.get("users", doc.name)
                return { found = got }
            end"#,
        )
        .unwrap();

        let store: Arc<Mutex<HashMap<String, String>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let store_put = store.clone();
        vm.inject(
            "db",
            "put",
            Arc::new(move |args| {
                let _collection = args[0].as_str().unwrap();
                let key = args[1].as_str().unwrap().to_string();
                store_put.lock().unwrap().insert(key, "stored".into());
                Ok(Bson::Null)
            }),
        )
        .unwrap();

        let store_get = store.clone();
        vm.inject(
            "db",
            "get",
            Arc::new(move |args| {
                let _collection = args[0].as_str().unwrap();
                let key = args[1].as_str().unwrap();
                let val = store_get
                    .lock()
                    .unwrap()
                    .get(key)
                    .cloned()
                    .unwrap_or_default();
                Ok(Bson::String(val))
            }),
        )
        .unwrap();

        let result = vm.call("trigger", &rawdoc! { "name": "alice" }).unwrap();
        assert_eq!(result.get_str("found").unwrap(), "stored");
    }

    #[test]
    fn eject_removes_namespace() {
        let mut vm = vm();
        vm.register(
            "trigger",
            br#"return function(doc)
                if db then
                    return { has_db = true }
                else
                    return { has_db = false }
                end
            end"#,
        )
        .unwrap();

        let cb: VmCallback = Arc::new(|_| Ok(Bson::Null));
        vm.inject("db", "put", cb).unwrap();

        let result = vm.call("trigger", &rawdoc! {}).unwrap();
        assert_eq!(result.get_bool("has_db").unwrap(), true);

        vm.eject("db").unwrap();

        let result = vm.call("trigger", &rawdoc! {}).unwrap();
        assert_eq!(result.get_bool("has_db").unwrap(), false);
    }

    #[test]
    fn callback_error_propagates() {
        let mut vm = vm();
        vm.register(
            "trigger",
            br#"return function(doc)
                db.fail()
                return {}
            end"#,
        )
        .unwrap();

        let cb: VmCallback = Arc::new(|_| {
            Err(VmError::InvalidReturn("intentional error".into()))
        });
        vm.inject("db", "fail", cb).unwrap();

        let result = vm.call("trigger", &rawdoc! {});
        assert!(result.is_err());
    }

    // --- Scoped context ---

    #[test]
    fn call_with_scope_basic() {
        let mut vm = vm();
        vm.register(
            "trigger",
            br#"return function(ctx, event)
                local doc = ctx.get("users", event.doc._id)
                return { found = (doc ~= nil) }
            end"#,
        )
        .unwrap();

        let get_cb = |args: Vec<Bson>| -> Result<Bson, VmError> {
            let id = args[1].as_str().unwrap();
            if id == "alice" {
                Ok(Bson::Document(bson::doc! { "_id": "alice", "name": "Alice" }))
            } else {
                Ok(Bson::Null)
            }
        };

        let methods = [ScopedMethod {
            name: "get",
            callback: &get_cb,
        }];

        let input = rawdoc! { "doc": { "_id": "alice" } };
        let result = vm.call_with_scope("trigger", &input, &methods).unwrap();
        assert_eq!(result.get_bool("found").unwrap(), true);
    }

    #[test]
    fn call_with_scope_put_records_operations() {
        let mut vm = vm();
        vm.register(
            "trigger",
            br#"return function(ctx, event)
                ctx.put("audit", { action = event.action, doc_id = event.doc._id })
                return event
            end"#,
        )
        .unwrap();

        let ops: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let ops_clone = ops.clone();
        let put_cb = move |args: Vec<Bson>| -> Result<Bson, VmError> {
            let coll = args[0].as_str().unwrap().to_string();
            ops_clone.lock().unwrap().push(coll);
            Ok(Bson::Null)
        };

        let methods = [ScopedMethod {
            name: "put",
            callback: &put_cb,
        }];

        let input = rawdoc! { "action": "deleting", "doc": { "_id": "r1" } };
        vm.call_with_scope("trigger", &input, &methods).unwrap();

        assert_eq!(ops.lock().unwrap().as_slice(), &["audit"]);
    }

    #[test]
    fn call_with_scope_callback_error_propagates() {
        let mut vm = vm();
        vm.register(
            "trigger",
            br#"return function(ctx, event)
                ctx.fail()
                return event
            end"#,
        )
        .unwrap();

        let fail_cb = |_args: Vec<Bson>| -> Result<Bson, VmError> {
            Err(VmError::InvalidReturn("intentional error".into()))
        };

        let methods = [ScopedMethod {
            name: "fail",
            callback: &fail_cb,
        }];

        let result = vm.call_with_scope("trigger", &rawdoc! {}, &methods);
        assert!(result.is_err());
    }

    #[test]
    fn call_with_scope_multiple_methods() {
        let mut vm = vm();
        vm.register(
            "trigger",
            br#"return function(ctx, event)
                ctx.put("audit", { name = event.doc.name })
                local found = ctx.get("users", event.doc._id)
                return { name = found }
            end"#,
        )
        .unwrap();

        let store: Arc<Mutex<HashMap<String, String>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let store_put = store.clone();
        let put_cb = move |args: Vec<Bson>| -> Result<Bson, VmError> {
            let coll = args[0].as_str().unwrap().to_string();
            store_put.lock().unwrap().insert(coll, "written".into());
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
        let result = vm.call_with_scope("trigger", &input, &methods).unwrap();
        assert_eq!(result.get_str("name").unwrap(), "found:r1");
        assert_eq!(
            store.lock().unwrap().get("audit").unwrap(),
            "written"
        );
    }

    #[test]
    fn call_with_scope_empty_methods() {
        let mut vm = vm();
        vm.register(
            "trigger",
            br#"return function(ctx, event)
                return { action = event.action }
            end"#,
        )
        .unwrap();

        let input = rawdoc! { "action": "deleting" };
        let result = vm.call_with_scope("trigger", &input, &[]).unwrap();
        assert_eq!(result.get_str("action").unwrap(), "deleting");
    }
}
