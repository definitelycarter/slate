mod common;
use common::*;

use bson::{Bson, doc, rawdoc};
use slate_db::v2::IndexOptions;

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
    let result = db
        .collection(COLLECTION)
        .merge_many(docs)
        .execute(&txn)
        .unwrap()
        .affected;
    assert_eq!(result, 2);

    let found = db
        .collection(COLLECTION)
        .find(rawdoc! {})
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(found.len(), 2);
}

#[test]
fn merge_many_merges_existing() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let txn = db.begin(false).unwrap();

    db.collection(COLLECTION)
        .insert_one(doc! { "_id": "m1", "name": "Alice", "status": "active", "score": 100 })
        .execute(&txn)
        .unwrap();

    // Merge only updates status — score should remain
    let docs = vec![doc! { "_id": "m1", "status": "inactive" }];
    let result = db
        .collection(COLLECTION)
        .merge_many(docs)
        .execute(&txn)
        .unwrap()
        .affected;
    assert_eq!(result, 1);

    let doc = db
        .collection(COLLECTION)
        .find(rawdoc! { "_id": "m1" })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
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
    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .indexes()
        .create("status", IndexOptions::default())
        .execute(&txn)
        .unwrap();

    db.collection(COLLECTION)
        .insert_one(doc! { "_id": "m1", "name": "Alice", "status": "active" })
        .execute(&txn)
        .unwrap();

    // Merge changes status
    db.collection(COLLECTION)
        .merge_many(vec![doc! { "_id": "m1", "status": "inactive" }])
        .execute(&txn)
        .unwrap();

    // Old index entry gone
    let active = db
        .collection(COLLECTION)
        .find(eq_filter("status", Bson::String("active".into())))
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(active.len(), 0);

    // New index entry present
    let inactive = db
        .collection(COLLECTION)
        .find(eq_filter("status", Bson::String("inactive".into())))
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(inactive.len(), 1);
}

#[test]
fn merge_many_unchanged_noop() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let txn = db.begin(false).unwrap();

    db.collection(COLLECTION)
        .insert_one(doc! { "_id": "m1", "name": "Alice", "status": "active" })
        .execute(&txn)
        .unwrap();

    // Merge with same values — updated count should still be 1 (we count the attempt, not actual changes)
    // But internally raw_merge_update returns false for no-op, so updated stays at 1 because merge_many
    // always increments updated when the record exists
    let docs = vec![doc! { "_id": "m1", "status": "active" }];
    let result = db
        .collection(COLLECTION)
        .merge_many(docs)
        .execute(&txn)
        .unwrap()
        .affected;
    assert_eq!(result, 1);
}

#[test]
fn merge_many_adds_new_field() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let txn = db.begin(false).unwrap();

    db.collection(COLLECTION)
        .insert_one(doc! { "_id": "m1", "name": "Alice" })
        .execute(&txn)
        .unwrap();

    // Merge adds a new field
    db.collection(COLLECTION)
        .merge_many(vec![doc! { "_id": "m1", "status": "active" }])
        .execute(&txn)
        .unwrap();

    let doc = db
        .collection(COLLECTION)
        .find(rawdoc! { "_id": "m1" })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(doc.get_str("name").unwrap(), "Alice");
    assert_eq!(doc.get_str("status").unwrap(), "active");
}

#[test]
fn merge_many_mixed_insert_and_merge() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let txn = db.begin(false).unwrap();

    db.collection(COLLECTION)
        .insert_one(doc! { "_id": "m1", "name": "Alice", "status": "active" })
        .execute(&txn)
        .unwrap();

    let docs = vec![
        doc! { "_id": "m1", "status": "inactive" }, // merge
        doc! { "_id": "m2", "name": "Bob", "status": "active" }, // insert
    ];
    let result = db
        .collection(COLLECTION)
        .merge_many(docs)
        .execute(&txn)
        .unwrap()
        .affected;
    assert_eq!(result, 2);

    let m1 = db
        .collection(COLLECTION)
        .find(rawdoc! { "_id": "m1" })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(m1.get_str("name").unwrap(), "Alice"); // preserved
    assert_eq!(m1.get_str("status").unwrap(), "inactive"); // merged

    let m2 = db
        .collection(COLLECTION)
        .find(rawdoc! { "_id": "m2" })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(m2.get_str("name").unwrap(), "Bob");
}
