mod common;
use common::*;

use bson::{doc, rawdoc};

// ── Insert tests ────────────────────────────────────────────────

#[test]
fn insert_one_and_find_one() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(doc! { "_id": "acct-1", "name": "Acme", "revenue": 50000.0 })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let record = db
        .collection(COLLECTION)
        .find(rawdoc! { "_id": "acct-1" })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
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

    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(doc! { "_id": "acct-1", "name": "Acme" })
        .execute(&txn)
        .unwrap();
    let err = db
        .collection(COLLECTION)
        .insert_one(doc! { "_id": "acct-1", "name": "Duplicate" })
        .execute(&txn)
        .unwrap_err();
    assert!(err.to_string().contains("duplicate key"));
}

#[test]
fn insert_one_auto_generated_id() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(doc! { "name": "No ID" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    // Verify the auto-generated _id is an ObjectId
    let txn = db.begin(true).unwrap();
    let results = db
        .collection(COLLECTION)
        .find(rawdoc! {})
        .iter_raw(&txn)
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

    let txn = db.begin(false).unwrap();
    let count = db
        .collection(COLLECTION)
        .insert_many(vec![
            doc! { "_id": "acct-1", "name": "Acme" },
            doc! { "_id": "acct-2", "name": "Globex" },
        ])
        .execute(&txn)
        .unwrap()
        .affected;
    assert_eq!(count, 2);
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let all = db
        .collection(COLLECTION)
        .find(rawdoc! {})
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(all.len(), 2);
}
