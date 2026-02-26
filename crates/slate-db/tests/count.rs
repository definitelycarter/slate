mod common;
use common::*;

use bson::{Bson, rawdoc};

// ── Count tests ─────────────────────────────────────────────────

#[test]
fn count_all() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let count = txn.count(COLLECTION, rawdoc! {}).unwrap();
    assert_eq!(count, 5);
}

#[test]
fn count_with_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let filter = eq_filter("status", Bson::String("active".into()));
    let count = txn.count(COLLECTION, filter).unwrap();
    assert_eq!(count, 3);
}
