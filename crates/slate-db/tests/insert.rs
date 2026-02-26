mod common;
use common::*;

use bson::{doc, rawdoc};
use slate_query::FindOptions;

// ── Insert tests ────────────────────────────────────────────────

#[test]
fn insert_one_and_find_one() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "acct-1", "name": "Acme", "revenue": 50000.0 },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let record = txn
        .find_one(COLLECTION, rawdoc! { "_id": "acct-1" })
        .unwrap()
        .unwrap();
    assert_eq!(record.get_str("_id").unwrap(), "acct-1");
    assert_eq!(record.get_str("name").unwrap(), "Acme");
    assert_eq!(record.get_f64("revenue").unwrap(), 50000.0);
}

#[test]
fn insert_one_duplicate_id_fails() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "acct-1", "name": "Acme" })
        .unwrap()
        .drain()
        .unwrap();
    let err = txn
        .insert_one(COLLECTION, doc! { "_id": "acct-1", "name": "Duplicate" })
        .unwrap()
        .drain()
        .unwrap_err();
    assert!(err.to_string().contains("duplicate key"));
}

#[test]
fn insert_one_auto_generated_id() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "name": "No ID" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // Verify the auto-generated _id is an ObjectId
    let txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    let oid = results[0].get_object_id("_id").unwrap();
    assert_eq!(oid.to_hex().len(), 24);
}

#[test]
fn insert_many_batch() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    let count = txn
        .insert_many(
            COLLECTION,
            vec![
                doc! { "_id": "acct-1", "name": "Acme" },
                doc! { "_id": "acct-2", "name": "Globex" },
            ],
        )
        .unwrap()
        .drain()
        .unwrap();
    assert_eq!(count, 2);
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let all = txn
        .find(COLLECTION, rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(all.len(), 2);
}
