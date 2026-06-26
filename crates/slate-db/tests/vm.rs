use std::sync::Arc;

use slate_db::{DatabaseBuilder, RuntimeRegistry, VmPool};
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
    let txn = db.begin(false).unwrap();
    db.collections().create("test").execute(&txn).unwrap();
    db.collection("test")
        .triggers()
        .create("audit", "return function(ctx, event) return event end")
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();
}

#[test]
fn database_without_scripting() {
    // Database works fine without a pool — functions are stored but not executed.
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    db.collections().create("test").execute(&txn).unwrap();
    db.collection("test")
        .triggers()
        .create("audit", "return function(ctx, event) return event end")
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();
}
