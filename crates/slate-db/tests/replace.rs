mod common;
use common::*;

use bson::{Bson, doc, rawdoc};

// ── Replace tests ───────────────────────────────────────────────

#[test]
fn replace_one_full_replacement() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .insert_one(
            doc! { "_id": "acct-1", "name": "Acme", "status": "active", "revenue": 50000.0 },
        )
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("acct-1".into()));
    let result = db
        .collection(COLLECTION)
        .find(&filter)
        .replace(doc! { "name": "New Corp" })
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
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("name").unwrap(), "New Corp");
    // Old fields should be gone (replaced, not merged)
    assert!(!results[0].get_check("status"));
    assert!(!results[0].get_check("revenue"));
}
