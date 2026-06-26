mod common;
use common::*;

use bson::{doc, rawdoc};

// ── _id type roundtrips ─────────────────────────────────────────

#[test]
fn insert_and_find_string_id() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(doc! { "_id": "my-string", "v": 1 })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = db
        .collection(COLLECTION)
        .find(rawdoc! { "_id": "my-string" })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(found.get_str("_id").unwrap(), "my-string");
}

#[test]
fn insert_and_find_objectid_id() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let oid = bson::oid::ObjectId::new();
    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(doc! { "_id": oid, "v": 1 })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = db
        .collection(COLLECTION)
        .find(rawdoc! { "_id": oid })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(found.get_object_id("_id").unwrap(), oid);
}

#[test]
fn insert_and_find_i32_id() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(doc! { "_id": 42_i32, "v": 1 })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = db
        .collection(COLLECTION)
        .find(rawdoc! { "_id": 42_i32 })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(found.get_i32("_id").unwrap(), 42);
}

#[test]
fn insert_and_find_i64_id() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(doc! { "_id": 999_i64, "v": 1 })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = db
        .collection(COLLECTION)
        .find(rawdoc! { "_id": 999_i64 })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(found.get_i64("_id").unwrap(), 999);
}

#[test]
fn insert_objectid_duplicate_fails() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let oid = bson::oid::ObjectId::new();
    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(doc! { "_id": oid, "v": 1 })
        .execute(&txn)
        .unwrap();
    let err = db
        .collection(COLLECTION)
        .insert_one(doc! { "_id": oid, "v": 2 })
        .execute(&txn)
        .unwrap_err();
    assert!(err.to_string().contains("duplicate key"));
}

#[test]
fn insert_i32_duplicate_fails() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(doc! { "_id": 7_i32, "v": 1 })
        .execute(&txn)
        .unwrap();
    let err = db
        .collection(COLLECTION)
        .insert_one(doc! { "_id": 7_i32, "v": 2 })
        .execute(&txn)
        .unwrap_err();
    assert!(err.to_string().contains("duplicate key"));
}

#[test]
fn delete_by_objectid() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let oid = bson::oid::ObjectId::new();
    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(doc! { "_id": oid, "v": 1 })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .find(rawdoc! { "_id": oid })
        .delete()
        .one()
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = db
        .collection(COLLECTION)
        .find(rawdoc! { "_id": oid })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap();
    assert!(found.is_none());
}

#[test]
fn upsert_with_objectid() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let oid = bson::oid::ObjectId::new();
    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .upsert_many(vec![doc! { "_id": oid, "v": 1 }])
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = db
        .collection(COLLECTION)
        .find(rawdoc! { "_id": oid })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(found.get_i32("v").unwrap(), 1);
}

#[test]
fn replace_with_i32_id() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(doc! { "_id": 10_i32, "name": "Alice", "age": 30 })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .find(rawdoc! { "_id": 10_i32 })
        .replace(doc! { "_id": 10_i32, "name": "Bob", "age": 25 })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = db
        .collection(COLLECTION)
        .find(rawdoc! { "_id": 10_i32 })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(found.get_str("name").unwrap(), "Bob");
}

#[test]
fn update_with_objectid() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let oid = bson::oid::ObjectId::new();
    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(doc! { "_id": oid, "score": 10 })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .find(rawdoc! { "_id": oid })
        .update(doc! { "$set": { "score": 99 } })
        .one()
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let found = db
        .collection(COLLECTION)
        .find(rawdoc! { "_id": oid })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(found.get_i32("score").unwrap(), 99);
}

#[test]
fn mixed_id_types_in_collection() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let oid = bson::oid::ObjectId::new();
    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_many(vec![
            doc! { "_id": "str-1", "t": "string" },
            doc! { "_id": oid, "t": "oid" },
            doc! { "_id": 42_i32, "t": "i32" },
            doc! { "_id": 100_i64, "t": "i64" },
        ])
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = db
        .collection(COLLECTION)
        .find(rawdoc! {})
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 4);
}
