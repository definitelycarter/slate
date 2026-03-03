use std::sync::Arc;

use slate_db::{CollectionConfig, DEFAULT_CF, DatabaseBuilder, VmPool, RuntimeRegistry};
use slate_store::MemoryStore;
use slate_vm::{LuaScriptRuntime, RuntimeKind};

fn scripting_pool() -> VmPool {
    let mut reg = RuntimeRegistry::new();
    reg.register(RuntimeKind::Lua, Arc::new(LuaScriptRuntime::new()));
    VmPool::new(reg)
}

#[test]
fn database_with_scripting() {
    let db = DatabaseBuilder::new()
        .with_scripting(scripting_pool())
        .open(MemoryStore::new())
        .unwrap();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "test".into(),
        ..Default::default()
    })
    .unwrap();
    txn.register_trigger(DEFAULT_CF, "test", "audit", "return function(ctx, event) return event end")
        .unwrap();
    txn.commit().unwrap();
}

#[test]
fn database_without_scripting() {
    // Database works fine without a pool — functions are stored but not executed.
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "test".into(),
        ..Default::default()
    })
    .unwrap();
    txn.register_trigger(DEFAULT_CF, "test", "audit", "return function(ctx, event) return event end")
        .unwrap();
    txn.commit().unwrap();
}
