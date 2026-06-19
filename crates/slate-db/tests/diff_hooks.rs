//! Differential tests: validator accept/reject decisions match in v1 and v2.
//!
//! v1 wires validators automatically (via the hook snapshot, `wrap_before`);
//! v2 expresses the same thing as an explicit `Validate` node in the plan. Both
//! run the same Lua validator over the same document and must agree on whether
//! the insert is rejected.

use bson::Document;
use bson::doc;
use slate_db::{CollectionConfig, DEFAULT_CF, DatabaseBuilder};
use slate_engine::{Catalog, Engine, EngineTransaction, KvEngine};
use slate_planner::{CollectionRef, Node, Plan};
use slate_store::MemoryStore;
use slate_vm::pool::{RuntimeRegistry, VmPool};
use slate_vm::{LuaScriptRuntime, ResolvedHook, RuntimeKind};
use std::sync::Arc;

const COLL: &str = "people";
const LUA_TAG: u8 = 0x01;

const VALIDATOR: &str = r#"
    return function(event)
        if event.doc.age < 40 then
            return { ok = false, reason = "too young" }
        else
            return { ok = true }
        end
    end
"#;

fn lua_pool() -> VmPool {
    let mut reg = RuntimeRegistry::new();
    reg.register(RuntimeKind::Lua, Arc::new(LuaScriptRuntime::new()));
    VmPool::new(reg)
}

/// Insert `doc` under the validator on each pipeline; returns `(v1, v2)` —
/// whether each rejected the insert.
fn validator_rejects(doc: Document) -> (bool, bool) {
    let v1 = {
        let db = DatabaseBuilder::new()
            .with_scripting(lua_pool())
            .open(MemoryStore::new())
            .unwrap();
        {
            let txn = db.begin(false).unwrap();
            txn.create_collection(&CollectionConfig {
                name: COLL.into(),
                ..Default::default()
            })
            .unwrap();
            txn.register_validator(DEFAULT_CF, COLL, "v", VALIDATOR)
                .unwrap();
            txn.commit().unwrap();
        }
        let txn = db.begin(false).unwrap();
        txn.insert_one(DEFAULT_CF, COLL, doc.clone())
            .and_then(|c| c.drain())
            .is_err()
    };

    let v2 = {
        let engine = KvEngine::new(MemoryStore::new());
        {
            let txn = engine.begin(false).unwrap();
            txn.create_collection(DEFAULT_CF, COLL, &Default::default())
                .unwrap();
            txn.commit().unwrap();
        }
        let pool = lua_pool();
        let hook = ResolvedHook {
            name: "v".into(),
            runtime: LUA_TAG,
            source: VALIDATOR.as_bytes().to_vec(),
            source_hash: 0,
        };
        let raw = bson::RawBson::Document(bson::RawDocumentBuf::try_from(&doc).unwrap());
        let plan = Plan::Insert {
            collection: CollectionRef {
                cf: DEFAULT_CF.into(),
                collection: COLL.into(),
            },
            source: Node::Validate {
                validators: vec![hook],
                source: Box::new(Node::Values(vec![raw])),
            },
        };
        let txn = engine.begin(false).unwrap();
        slate_executor::Executor::with_pool(&txn, Some(&pool))
            .execute_collect(plan)
            .is_err()
    };

    (v1, v2)
}

#[test]
fn validator_accepts_valid_document() {
    let (v1, v2) = validator_rejects(doc! { "_id": "9", "name": "kay", "age": 50 });
    assert_eq!(v1, v2, "v1/v2 disagree on acceptance");
    assert!(!v1, "expected the document to be accepted");
}

#[test]
fn validator_rejects_invalid_document() {
    let (v1, v2) = validator_rejects(doc! { "_id": "9", "name": "kid", "age": 30 });
    assert_eq!(v1, v2, "v1/v2 disagree on rejection");
    assert!(v1, "expected the document to be rejected");
}
