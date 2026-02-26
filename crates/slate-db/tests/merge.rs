mod common;
use common::*;

use bson::{Bson, doc, rawdoc};
use slate_query::FindOptions;

// ── Merge Many ──────────────────────────────────────────────────

#[test]
fn merge_many_inserts_new() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let txn = db.begin(false).unwrap();

    let docs = vec![
        doc! { "_id": "m1", "name": "Alice", "status": "active" },
        doc! { "_id": "m2", "name": "Bob", "status": "inactive" },
    ];
    let result = txn.merge_many(COLLECTION, docs).unwrap().drain().unwrap();
    assert_eq!(result, 2);

    let found = txn
        .find(COLLECTION, rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(found.len(), 2);
}

#[test]
fn merge_many_merges_existing() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let mut txn = db.begin(false).unwrap();

    txn.insert_one(
        COLLECTION,
        doc! { "_id": "m1", "name": "Alice", "status": "active", "score": 100 },
    )
    .unwrap()
    .drain()
    .unwrap();

    // Merge only updates status — score should remain
    let docs = vec![doc! { "_id": "m1", "status": "inactive" }];
    let result = txn.merge_many(COLLECTION, docs).unwrap().drain().unwrap();
    assert_eq!(result, 1);

    let doc = txn
        .find_one(COLLECTION, rawdoc! { "_id": "m1" })
        .unwrap()
        .unwrap();
    assert_eq!(doc.get_str("name").unwrap(), "Alice");
    assert_eq!(doc.get_str("status").unwrap(), "inactive");
    assert_eq!(doc.get_i32("score").unwrap(), 100);
}

#[test]
fn merge_many_index_maintenance() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let mut txn = db.begin(false).unwrap();
    txn.create_index(COLLECTION, "status").unwrap();

    txn.insert_one(
        COLLECTION,
        doc! { "_id": "m1", "name": "Alice", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();

    // Merge changes status
    txn.merge_many(COLLECTION, vec![doc! { "_id": "m1", "status": "inactive" }])
        .unwrap()
        .drain()
        .unwrap();

    // Old index entry gone
    let active = txn
        .find(
            COLLECTION,
            eq_filter("status", Bson::String("active".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(active.len(), 0);

    // New index entry present
    let inactive = txn
        .find(
            COLLECTION,
            eq_filter("status", Bson::String("inactive".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(inactive.len(), 1);
}

#[test]
fn merge_many_unchanged_noop() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let mut txn = db.begin(false).unwrap();

    txn.insert_one(
        COLLECTION,
        doc! { "_id": "m1", "name": "Alice", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();

    // Merge with same values — updated count should still be 1 (we count the attempt, not actual changes)
    // But internally raw_merge_update returns false for no-op, so updated stays at 1 because merge_many
    // always increments updated when the record exists
    let docs = vec![doc! { "_id": "m1", "status": "active" }];
    let result = txn.merge_many(COLLECTION, docs).unwrap().drain().unwrap();
    assert_eq!(result, 1);
}

#[test]
fn merge_many_adds_new_field() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let mut txn = db.begin(false).unwrap();

    txn.insert_one(COLLECTION, doc! { "_id": "m1", "name": "Alice" })
        .unwrap()
        .drain()
        .unwrap();

    // Merge adds a new field
    txn.merge_many(COLLECTION, vec![doc! { "_id": "m1", "status": "active" }])
        .unwrap()
        .drain()
        .unwrap();

    let doc = txn
        .find_one(COLLECTION, rawdoc! { "_id": "m1" })
        .unwrap()
        .unwrap();
    assert_eq!(doc.get_str("name").unwrap(), "Alice");
    assert_eq!(doc.get_str("status").unwrap(), "active");
}

#[test]
fn merge_many_mixed_insert_and_merge() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let mut txn = db.begin(false).unwrap();

    txn.insert_one(
        COLLECTION,
        doc! { "_id": "m1", "name": "Alice", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();

    let docs = vec![
        doc! { "_id": "m1", "status": "inactive" }, // merge
        doc! { "_id": "m2", "name": "Bob", "status": "active" }, // insert
    ];
    let result = txn.merge_many(COLLECTION, docs).unwrap().drain().unwrap();
    assert_eq!(result, 2);

    let m1 = txn
        .find_one(COLLECTION, rawdoc! { "_id": "m1" })
        .unwrap()
        .unwrap();
    assert_eq!(m1.get_str("name").unwrap(), "Alice"); // preserved
    assert_eq!(m1.get_str("status").unwrap(), "inactive"); // merged

    let m2 = txn
        .find_one(COLLECTION, rawdoc! { "_id": "m2" })
        .unwrap()
        .unwrap();
    assert_eq!(m2.get_str("name").unwrap(), "Bob");
}
