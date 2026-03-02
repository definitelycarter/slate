use bson::raw::{RawBsonRef, RawDocument, RawDocumentBuf};
use bson::spec::ElementType;
use bson::{Bson, Document};
use mlua::{IntoLua, MetaMethod, UserData, UserDataMethods, Value};

use crate::error::VmError;

// ---------------------------------------------------------------------------
// Userdata wrappers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct LuaDateTime(pub bson::DateTime);

impl UserData for LuaDateTime {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("millis", |_, this, ()| Ok(this.0.timestamp_millis()));
        methods.add_meta_method(MetaMethod::ToString, |_, this, ()| {
            Ok(format!("DateTime({})", this.0.timestamp_millis()))
        });
        methods.add_meta_method(MetaMethod::Eq, |_, this, other: mlua::AnyUserData| {
            let other = other.borrow::<LuaDateTime>()?;
            Ok(this.0 == other.0)
        });
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LuaObjectId(pub bson::oid::ObjectId);

impl UserData for LuaObjectId {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("hex", |_, this, ()| Ok(this.0.to_hex()));
        methods.add_meta_method(MetaMethod::ToString, |_, this, ()| {
            Ok(format!("ObjectId({})", this.0.to_hex()))
        });
        methods.add_meta_method(MetaMethod::Eq, |_, this, other: mlua::AnyUserData| {
            let other = other.borrow::<LuaObjectId>()?;
            Ok(this.0 == other.0)
        });
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LuaInt32(pub i32);

impl UserData for LuaInt32 {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("value", |_, this, ()| Ok(this.0));
        methods.add_meta_method(MetaMethod::ToString, |_, this, ()| {
            Ok(format!("{}", this.0))
        });
        methods.add_meta_method(MetaMethod::Eq, |_, this, other: mlua::AnyUserData| {
            let other = other.borrow::<LuaInt32>()?;
            Ok(this.0 == other.0)
        });

        // Arithmetic — extract numeric value from either side, return Lua integer
        methods.add_meta_function(MetaMethod::Add, |_, (a, b): (Value, Value)| {
            Ok(Value::Integer(extract_int(a)? + extract_int(b)?))
        });
        methods.add_meta_function(MetaMethod::Sub, |_, (a, b): (Value, Value)| {
            Ok(Value::Integer(extract_int(a)? - extract_int(b)?))
        });
        methods.add_meta_function(MetaMethod::Mul, |_, (a, b): (Value, Value)| {
            Ok(Value::Integer(extract_int(a)? * extract_int(b)?))
        });
        methods.add_meta_function(MetaMethod::Mod, |_, (a, b): (Value, Value)| {
            Ok(Value::Integer(extract_int(a)? % extract_int(b)?))
        });
        methods.add_meta_method(MetaMethod::Unm, |_, this, ()| {
            Ok(Value::Integer(-(this.0 as i64)))
        });

        // Comparison with plain integers
        methods.add_meta_function(MetaMethod::Lt, |_, (a, b): (Value, Value)| {
            Ok(extract_int(a)? < extract_int(b)?)
        });
        methods.add_meta_function(MetaMethod::Le, |_, (a, b): (Value, Value)| {
            Ok(extract_int(a)? <= extract_int(b)?)
        });
    }
}

/// Extract an integer value from a Lua Value (handles LuaInt32 userdata and plain integers).
fn extract_int(val: Value) -> Result<i64, mlua::Error> {
    match val {
        Value::Integer(n) => Ok(n),
        Value::Number(f) => Ok(f as i64),
        Value::UserData(ud) => {
            let i32_val = ud.borrow::<LuaInt32>()?;
            Ok(i32_val.0 as i64)
        }
        _ => Err(mlua::Error::RuntimeError(
            "expected a number".into(),
        )),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LuaBinary(pub Vec<u8>);

impl UserData for LuaBinary {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("len", |_, this, ()| Ok(this.0.len()));
        methods.add_method("bytes", |lua, this, ()| lua.create_string(&this.0));
        methods.add_meta_method(MetaMethod::ToString, |_, this, ()| {
            Ok(format!("Binary({} bytes)", this.0.len()))
        });
    }
}

// ---------------------------------------------------------------------------
// bson.* constructor module
// ---------------------------------------------------------------------------

pub(crate) fn create_bson_module(lua: &mlua::Lua) -> Result<mlua::Table, VmError> {
    let module = lua.create_table()?;

    module.set(
        "datetime",
        lua.create_function(|_, millis: i64| Ok(LuaDateTime(bson::DateTime::from_millis(millis))))?,
    )?;

    module.set(
        "now",
        lua.create_function(|_, ()| Ok(LuaDateTime(bson::DateTime::now())))?,
    )?;

    module.set(
        "objectid",
        lua.create_function(|_, hex: String| {
            let oid = bson::oid::ObjectId::parse_str(&hex)
                .map_err(|e| mlua::Error::RuntimeError(format!("invalid objectid: {e}")))?;
            Ok(LuaObjectId(oid))
        })?,
    )?;

    module.set(
        "binary",
        lua.create_function(|_, bytes: mlua::String| Ok(LuaBinary(bytes.as_bytes().to_vec())))?,
    )?;

    module.set(
        "i32",
        lua.create_function(|_, n: i32| Ok(LuaInt32(n)))?,
    )?;

    module.set(
        "is_datetime",
        lua.create_function(|_, val: Value| match val {
            Value::UserData(ud) => Ok(ud.borrow::<LuaDateTime>().is_ok()),
            _ => Ok(false),
        })?,
    )?;

    module.set(
        "is_objectid",
        lua.create_function(|_, val: Value| match val {
            Value::UserData(ud) => Ok(ud.borrow::<LuaObjectId>().is_ok()),
            _ => Ok(false),
        })?,
    )?;

    Ok(module)
}

// ---------------------------------------------------------------------------
// BSON -> Lua
// ---------------------------------------------------------------------------

pub(crate) fn doc_to_table(lua: &mlua::Lua, doc: &RawDocumentBuf) -> Result<mlua::Table, VmError> {
    raw_doc_to_table(lua, doc.as_ref())
}

fn raw_doc_to_table(lua: &mlua::Lua, doc: &RawDocument) -> Result<mlua::Table, VmError> {
    let table = lua.create_table()?;

    for result in doc.iter() {
        let (key, value) = result?;
        let lua_val = raw_bson_to_lua(lua, value)?;
        if lua_val != Value::Nil {
            let key_str = key.to_string();
            table.set(key_str, lua_val)?;
        }
    }

    Ok(table)
}

fn raw_bson_to_lua(lua: &mlua::Lua, value: RawBsonRef<'_>) -> Result<Value, VmError> {
    match value.element_type() {
        ElementType::String => {
            let s = value
                .as_str()
                .ok_or_else(|| VmError::InvalidReturn("expected string".into()))?;
            Ok(Value::String(lua.create_string(s)?))
        }
        ElementType::Boolean => {
            let b = value
                .as_bool()
                .ok_or_else(|| VmError::InvalidReturn("expected boolean".into()))?;
            Ok(Value::Boolean(b))
        }
        ElementType::Null | ElementType::Undefined => Ok(Value::Nil),
        ElementType::Int32 => {
            let n = value
                .as_i32()
                .ok_or_else(|| VmError::InvalidReturn("expected i32".into()))?;
            LuaInt32(n).into_lua(lua).map_err(VmError::from)
        }
        ElementType::Int64 => {
            let n = value
                .as_i64()
                .ok_or_else(|| VmError::InvalidReturn("expected i64".into()))?;
            Ok(Value::Integer(n))
        }
        ElementType::Double => {
            let f = value
                .as_f64()
                .ok_or_else(|| VmError::InvalidReturn("expected f64".into()))?;
            Ok(Value::Number(f))
        }
        ElementType::EmbeddedDocument => {
            let subdoc = value
                .as_document()
                .ok_or_else(|| VmError::InvalidReturn("expected document".into()))?;
            let t = raw_doc_to_table(lua, subdoc)?;
            Ok(Value::Table(t))
        }
        ElementType::Array => {
            let arr = value
                .as_array()
                .ok_or_else(|| VmError::InvalidReturn("expected array".into()))?;
            let table = lua.create_table()?;
            for (i, item) in arr.into_iter().enumerate() {
                let item = item?;
                let lua_val = raw_bson_to_lua(lua, item)?;
                table.set((i + 1) as i64, lua_val)?; // 1-indexed
            }
            Ok(Value::Table(table))
        }
        ElementType::DateTime => {
            let dt = value
                .as_datetime()
                .ok_or_else(|| VmError::InvalidReturn("expected datetime".into()))?;
            LuaDateTime(dt).into_lua(lua).map_err(VmError::from)
        }
        ElementType::ObjectId => {
            let oid = value
                .as_object_id()
                .ok_or_else(|| VmError::InvalidReturn("expected objectid".into()))?;
            LuaObjectId(oid).into_lua(lua).map_err(VmError::from)
        }
        ElementType::Binary => {
            let bin = value
                .as_binary()
                .ok_or_else(|| VmError::InvalidReturn("expected binary".into()))?;
            LuaBinary(bin.bytes.to_vec())
                .into_lua(lua)
                .map_err(VmError::from)
        }
        other => Err(VmError::InvalidReturn(format!(
            "unsupported BSON type: {other:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Lua -> BSON
// ---------------------------------------------------------------------------

pub(crate) fn table_to_doc(table: &mlua::Table) -> Result<RawDocumentBuf, VmError> {
    let doc = lua_table_to_bson_doc(table)?;
    let raw = RawDocumentBuf::try_from(&doc)?;
    Ok(raw)
}

fn lua_table_to_bson_doc(table: &mlua::Table) -> Result<Document, VmError> {
    let mut doc = Document::new();
    for pair in table.pairs::<Value, Value>() {
        let (key, value) = pair?;
        let key_str = match key {
            Value::String(s) => s.to_str()?.to_string(),
            Value::Integer(n) => n.to_string(),
            _ => continue,
        };
        let bson_val = lua_value_to_bson(value)?;
        doc.insert(key_str, bson_val);
    }
    Ok(doc)
}

pub(crate) fn lua_value_to_bson(value: Value) -> Result<Bson, VmError> {
    match value {
        Value::Nil => Ok(Bson::Null),
        Value::Boolean(b) => Ok(Bson::Boolean(b)),
        Value::Integer(n) => Ok(Bson::Int64(n)),
        Value::Number(f) => Ok(Bson::Double(f)),
        Value::String(s) => Ok(Bson::String(s.to_str()?.to_string())),
        Value::Table(t) => {
            if is_array_table(&t)? {
                let mut arr = Vec::new();
                let len = t.raw_len();
                for i in 1..=len {
                    let val: Value = t.get(i)?;
                    arr.push(lua_value_to_bson(val)?);
                }
                Ok(Bson::Array(arr))
            } else {
                let doc = lua_table_to_bson_doc(&t)?;
                Ok(Bson::Document(doc))
            }
        }
        Value::UserData(ud) => {
            if let Ok(n) = ud.borrow::<LuaInt32>() {
                Ok(Bson::Int32(n.0))
            } else if let Ok(dt) = ud.borrow::<LuaDateTime>() {
                Ok(Bson::DateTime(dt.0))
            } else if let Ok(oid) = ud.borrow::<LuaObjectId>() {
                Ok(Bson::ObjectId(oid.0))
            } else if let Ok(bin) = ud.borrow::<LuaBinary>() {
                Ok(Bson::Binary(bson::Binary {
                    subtype: bson::spec::BinarySubtype::Generic,
                    bytes: bin.0.clone(),
                }))
            } else {
                Err(VmError::InvalidReturn("unsupported userdata type".into()))
            }
        }
        _ => Err(VmError::InvalidReturn(format!(
            "unsupported Lua type: {:?}",
            value.type_name()
        ))),
    }
}

/// Convert an owned [`Bson`] value into a Lua [`Value`].
pub(crate) fn bson_to_lua(lua: &mlua::Lua, value: Bson) -> Result<Value, VmError> {
    match value {
        Bson::Null => Ok(Value::Nil),
        Bson::Boolean(b) => Ok(Value::Boolean(b)),
        Bson::Int32(n) => LuaInt32(n).into_lua(lua).map_err(VmError::from),
        Bson::Int64(n) => Ok(Value::Integer(n)),
        Bson::Double(f) => Ok(Value::Number(f)),
        Bson::String(s) => Ok(Value::String(lua.create_string(&s)?)),
        Bson::Document(doc) => {
            let raw = RawDocumentBuf::try_from(&doc)?;
            let table = doc_to_table(lua, &raw)?;
            Ok(Value::Table(table))
        }
        Bson::Array(arr) => {
            let table = lua.create_table()?;
            for (i, item) in arr.into_iter().enumerate() {
                let lua_val = bson_to_lua(lua, item)?;
                table.set((i + 1) as i64, lua_val)?;
            }
            Ok(Value::Table(table))
        }
        Bson::DateTime(dt) => LuaDateTime(dt).into_lua(lua).map_err(VmError::from),
        Bson::ObjectId(oid) => LuaObjectId(oid).into_lua(lua).map_err(VmError::from),
        Bson::Binary(bin) => LuaBinary(bin.bytes).into_lua(lua).map_err(VmError::from),
        other => Err(VmError::InvalidReturn(format!(
            "unsupported BSON type: {other:?}"
        ))),
    }
}

/// Detect if a Lua table is an array (sequential integer keys 1..=n).
fn is_array_table(table: &mlua::Table) -> Result<bool, mlua::Error> {
    let raw_len = table.raw_len();
    if raw_len == 0 {
        // Empty table or object — check if there are any keys at all
        let has_keys = table.pairs::<Value, Value>().next().is_some();
        return Ok(!has_keys); // empty table with no keys is an empty array
    }

    // Check that every key is an integer in 1..=raw_len
    let mut count = 0usize;
    for pair in table.pairs::<Value, Value>() {
        let (key, _) = pair?;
        match key {
            Value::Integer(n) if n >= 1 && n <= raw_len as i64 => {
                count += 1;
            }
            _ => return Ok(false),
        }
    }
    Ok(count == raw_len as usize)
}
