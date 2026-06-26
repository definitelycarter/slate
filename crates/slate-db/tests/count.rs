mod common;
use common::*;

use bson::{Bson, rawdoc};

// ── Count tests ─────────────────────────────────────────────────

#[test]
fn count_all() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let count = db
        .collection(COLLECTION)
        .find(rawdoc! {})
        .iter_raw(&txn)
        .unwrap()
        .count();
    assert_eq!(count, 5);
}

#[test]
fn count_with_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let filter = eq_filter("status", Bson::String("active".into()));
    let count = db
        .collection(COLLECTION)
        .find(filter)
        .iter_raw(&txn)
        .unwrap()
        .count();
    assert_eq!(count, 3);
}
