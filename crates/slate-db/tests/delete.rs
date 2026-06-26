mod common;
use common::*;

use bson::{Bson, doc, rawdoc};

// ── Delete tests ────────────────────────────────────────────────

#[test]
fn delete_one_removes_record() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(doc! { "_id": "acct-1", "name": "Acme", "status": "active" })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("acct-1".into()));
    let result = db
        .collection(COLLECTION)
        .find(&filter)
        .delete()
        .one()
        .execute(&txn)
        .unwrap()
        .affected;
    assert_eq!(result, 1);
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = db
        .collection(COLLECTION)
        .find(rawdoc! {})
        .iter_raw(&txn)
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
    let result = db
        .collection(COLLECTION)
        .find(&filter)
        .delete()
        .execute(&txn)
        .unwrap()
        .affected;
    assert_eq!(result, 3);
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = db
        .collection(COLLECTION)
        .find(rawdoc! {})
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
}
