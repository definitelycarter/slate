use super::*;

use crate::collection::CollectionConfig;
use crate::database::{Database, DatabaseConfig};
use slate_store::MemoryStore;

fn seeded_db() -> Database<MemoryStore> {
    let db = Database::open(MemoryStore::new(), DatabaseConfig::default());
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "test".into(),
        indexes: vec!["status".into(), "score".into()],
    })
    .unwrap();
    txn.insert_many(
        "test",
        vec![
            bson::doc! { "_id": "1", "name": "Alice", "status": "active", "score": 70 },
            bson::doc! { "_id": "2", "name": "Bob", "status": "inactive", "score": 90 },
            bson::doc! { "_id": "3", "name": "Charlie", "status": "active", "score": 80 },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();
    db
}

#[test]
fn upsert_replace_insert_new() {
    // MockTransaction returns None for get() → insert path
    let txn = MockTransaction::new();
    let docs = vec![rawdoc! { "_id": "1", "name": "Alice", "status": "active" }];
    let plan = Plan::Upsert {
        collection: mock_collection(vec!["status".into()]),
        source: Node::Values(docs),
    };
    let exec = Executor::new(&txn);
    let iter = exec.execute(plan).unwrap();
    let rows = collect_docs(iter);
    assert_eq!(rows.len(), 1);

    let puts = txn.puts.borrow();
    // Engine handles encoding and index maintenance internally
    assert_eq!(puts.len(), 1);
    let written = bson::RawDocument::from_bytes(puts[0].doc.as_bytes()).unwrap();
    assert_eq!(written.get_str("name").unwrap(), "Alice");
}

#[test]
fn upsert_merge_insert_new() {
    let txn = MockTransaction::new();
    let docs = vec![rawdoc! { "_id": "1", "name": "Alice" }];
    let plan = Plan::Merge {
        collection: mock_collection(vec![]),
        source: Node::Values(docs),
    };
    let exec = Executor::new(&txn);
    let iter = exec.execute(plan).unwrap();
    let rows = collect_docs(iter);
    assert_eq!(rows.len(), 1);

    let puts = txn.puts.borrow();
    assert_eq!(puts.len(), 1);
}

#[test]
fn upsert_replace_existing() {
    let db = seeded_db();
    let txn = db.begin(false).unwrap();

    let affected = txn
        .upsert_many(
            "test",
            vec![bson::doc! { "_id": "1", "name": "Alicia", "status": "archived", "score": 99 }],
        )
        .unwrap()
        .drain()
        .unwrap();
    assert_eq!(affected, 1);

    let doc = txn
        .find_one("test", rawdoc! { "_id": "1" })
        .unwrap()
        .unwrap();
    assert_eq!(doc.get_str("name").unwrap(), "Alicia");
    assert_eq!(doc.get_str("status").unwrap(), "archived");
    assert_eq!(doc.get_i32("score").unwrap(), 99);
}

#[test]
fn upsert_merge_existing() {
    let db = seeded_db();
    let txn = db.begin(false).unwrap();

    let affected = txn
        .merge_many("test", vec![bson::doc! { "_id": "1", "score": 99 }])
        .unwrap()
        .drain()
        .unwrap();
    assert_eq!(affected, 1);

    let doc = txn
        .find_one("test", rawdoc! { "_id": "1" })
        .unwrap()
        .unwrap();
    assert_eq!(doc.get_str("name").unwrap(), "Alice");
    assert_eq!(doc.get_str("status").unwrap(), "active");
    assert_eq!(doc.get_i32("score").unwrap(), 99);
}

#[test]
fn upsert_mixed_insert_and_update() {
    let db = seeded_db();
    let txn = db.begin(false).unwrap();

    let affected = txn
        .upsert_many(
            "test",
            vec![
                bson::doc! { "_id": "1", "name": "Alicia", "status": "archived", "score": 99 },
                bson::doc! { "_id": "99", "name": "New", "status": "pending", "score": 50 },
            ],
        )
        .unwrap()
        .drain()
        .unwrap();
    assert_eq!(affected, 2);

    let doc1 = txn
        .find_one("test", rawdoc! { "_id": "1" })
        .unwrap()
        .unwrap();
    assert_eq!(doc1.get_str("name").unwrap(), "Alicia");

    let doc99 = txn
        .find_one("test", rawdoc! { "_id": "99" })
        .unwrap()
        .unwrap();
    assert_eq!(doc99.get_str("name").unwrap(), "New");
}

#[test]
fn upsert_replace_cleans_old_indexes() {
    let db = seeded_db();
    let txn = db.begin(false).unwrap();

    // Alice has status="active". Replace with status="archived".
    txn.upsert_many(
        "test",
        vec![bson::doc! { "_id": "1", "name": "Alice", "status": "archived", "score": 70 }],
    )
    .unwrap()
    .drain()
    .unwrap();

    // Query by old index value should not find Alice
    let results: Vec<_> = txn
        .find(
            "test",
            rawdoc! { "status": "active" },
            slate_query::FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let active_ids: Vec<&str> = results
        .iter()
        .map(|r| {
            let doc = bson::RawDocument::from_bytes(r.as_bytes()).unwrap();
            doc.get_str("_id").unwrap()
        })
        .collect();
    // Only Charlie (id=3) should remain active
    assert_eq!(active_ids, vec!["3"]);

    // Query by new index value should find Alice
    let results2: Vec<_> = txn
        .find(
            "test",
            rawdoc! { "status": "archived" },
            slate_query::FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results2.len(), 1);
    let doc = bson::RawDocument::from_bytes(results2[0].as_bytes()).unwrap();
    assert_eq!(doc.get_str("_id").unwrap(), "1");
}
