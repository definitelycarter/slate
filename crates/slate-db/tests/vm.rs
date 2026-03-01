use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseConfig};
use slate_store::MemoryStore;
use slate_vm::LuaVm;

#[test]
fn database_with_vm_factory() {
    let db = Database::open(MemoryStore::new(), DatabaseConfig::default())
        .with_vm_factory(|| Ok(Box::new(LuaVm::new()?)));
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
fn database_without_vm_factory() {
    // Database works fine without a VM factory — functions are stored but not executed.
    let db = Database::open(MemoryStore::new(), DatabaseConfig::default());
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
