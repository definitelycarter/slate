mod common;
use common::*;

use bson::{Bson, doc, rawdoc};
use slate_query::FindOptions;

// ── Delete tests ────────────────────────────────────────────────

#[test]
fn delete_one_removes_record() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "acct-1", "name": "Acme", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("acct-1".into()));
    let result = txn
        .delete_one(COLLECTION, &filter)
        .unwrap()
        .drain()
        .unwrap();
    assert_eq!(result, 1);
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn delete_many_removes_matching() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("status", Bson::String("active".into()));
    let result = txn
        .delete_many(COLLECTION, &filter)
        .unwrap()
        .drain()
        .unwrap();
    assert_eq!(result, 3);
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
}
