mod common;
use common::*;

use bson::{doc, rawdoc};
use slate_query::FindOptions;

// ── _id type roundtrips ─────────────────────────────────────────

#[test]
fn insert_and_find_string_id() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "my-string", "v": 1 })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = txn
        .find_one(COLLECTION, rawdoc! { "_id": "my-string" })
        .unwrap()
        .unwrap();
    assert_eq!(found.get_str("_id").unwrap(), "my-string");
}

#[test]
fn insert_and_find_objectid_id() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let oid = bson::oid::ObjectId::new();
    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": oid, "v": 1 })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = txn
        .find_one(COLLECTION, rawdoc! { "_id": oid })
        .unwrap()
        .unwrap();
    assert_eq!(found.get_object_id("_id").unwrap(), oid);
}

#[test]
fn insert_and_find_i32_id() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": 42_i32, "v": 1 })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = txn
        .find_one(COLLECTION, rawdoc! { "_id": 42_i32 })
        .unwrap()
        .unwrap();
    assert_eq!(found.get_i32("_id").unwrap(), 42);
}

#[test]
fn insert_and_find_i64_id() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": 999_i64, "v": 1 })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = txn
        .find_one(COLLECTION, rawdoc! { "_id": 999_i64 })
        .unwrap()
        .unwrap();
    assert_eq!(found.get_i64("_id").unwrap(), 999);
}

#[test]
fn insert_objectid_duplicate_fails() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let oid = bson::oid::ObjectId::new();
    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": oid, "v": 1 })
        .unwrap()
        .drain()
        .unwrap();
    let err = txn
        .insert_one(COLLECTION, doc! { "_id": oid, "v": 2 })
        .unwrap()
        .drain()
        .unwrap_err();
    assert!(err.to_string().contains("duplicate key"));
}

#[test]
fn insert_i32_duplicate_fails() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": 7_i32, "v": 1 })
        .unwrap()
        .drain()
        .unwrap();
    let err = txn
        .insert_one(COLLECTION, doc! { "_id": 7_i32, "v": 2 })
        .unwrap()
        .drain()
        .unwrap_err();
    assert!(err.to_string().contains("duplicate key"));
}

#[test]
fn delete_by_objectid() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let oid = bson::oid::ObjectId::new();
    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": oid, "v": 1 })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    txn.delete_one(COLLECTION, rawdoc! { "_id": oid })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = txn.find_one(COLLECTION, rawdoc! { "_id": oid }).unwrap();
    assert!(found.is_none());
}

#[test]
fn upsert_with_objectid() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let oid = bson::oid::ObjectId::new();
    let txn = db.begin(false).unwrap();
    txn.upsert_many(COLLECTION, vec![doc! { "_id": oid, "v": 1 }])
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = txn
        .find_one(COLLECTION, rawdoc! { "_id": oid })
        .unwrap()
        .unwrap();
    assert_eq!(found.get_i32("v").unwrap(), 1);
}

#[test]
fn replace_with_i32_id() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": 10_i32, "name": "Alice", "age": 30 },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    txn.replace_one(
        COLLECTION,
        rawdoc! { "_id": 10_i32 },
        doc! { "_id": 10_i32, "name": "Bob", "age": 25 },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = txn
        .find_one(COLLECTION, rawdoc! { "_id": 10_i32 })
        .unwrap()
        .unwrap();
    assert_eq!(found.get_str("name").unwrap(), "Bob");
}

#[test]
fn update_with_objectid() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let oid = bson::oid::ObjectId::new();
    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": oid, "score": 10 })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    txn.update_one(
        COLLECTION,
        rawdoc! { "_id": oid },
        doc! { "$set": { "score": 99 } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = txn
        .find_one(COLLECTION, rawdoc! { "_id": oid })
        .unwrap()
        .unwrap();
    assert_eq!(found.get_i32("score").unwrap(), 99);
}

#[test]
fn mixed_id_types_in_collection() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let oid = bson::oid::ObjectId::new();
    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
        COLLECTION,
        vec![
            doc! { "_id": "str-1", "t": "string" },
            doc! { "_id": oid, "t": "oid" },
            doc! { "_id": 42_i32, "t": "i32" },
            doc! { "_id": 100_i64, "t": "i64" },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 4);
}
