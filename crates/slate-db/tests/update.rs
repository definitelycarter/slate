mod common;
use common::*;

use bson::{Bson, doc, rawdoc};
use slate_db::v2::IndexOptions;

// ── Update tests ────────────────────────────────────────────────

#[test]
fn update_one_merge() {
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
        .update(doc! { "status": "rejected" })
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
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("status").unwrap(), "rejected");
    assert_eq!(results[0].get_str("name").unwrap(), "Acme"); // unchanged
}

#[test]
fn update_one_no_match() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("nonexistent".into()));
    let result = db
        .collection(COLLECTION)
        .find(&filter)
        .update(doc! { "status": "active" })
        .one()
        .execute(&txn)
        .unwrap()
        .affected;
    assert_eq!(result, 0);
}

#[test]
fn upsert_via_upsert_many() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    let result = db
        .collection(COLLECTION)
        .upsert_many(vec![doc! { "_id": "new-doc", "name": "Upserted" }])
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
    assert_eq!(results[0].get_str("name").unwrap(), "Upserted");
}

#[test]
fn update_many_multiple() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("status", Bson::String("active".into()));
    let result = db
        .collection(COLLECTION)
        .find(&filter)
        .update(doc! { "status": "archived" })
        .execute(&txn)
        .unwrap()
        .affected;
    assert_eq!(result, 3);
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = db
        .collection(COLLECTION)
        .find(eq_filter("status", Bson::String("archived".into())))
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 3);
}

// ── Upsert Many ─────────────────────────────────────────────────

#[test]
fn upsert_many_inserts_new() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let txn = db.begin(false).unwrap();

    let docs = vec![
        doc! { "_id": "u1", "name": "Alice", "status": "active" },
        doc! { "_id": "u2", "name": "Bob", "status": "inactive" },
    ];
    let result = db
        .collection(COLLECTION)
        .upsert_many(docs)
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
fn upsert_many_replaces_existing() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let txn = db.begin(false).unwrap();

    // Insert original
    db.collection(COLLECTION)
        .insert_one(doc! { "_id": "u1", "name": "Alice", "status": "active", "score": 100 })
        .execute(&txn)
        .unwrap();

    // Upsert replaces entirely
    let docs = vec![doc! { "_id": "u1", "name": "Alice Updated", "status": "inactive" }];
    let result = db
        .collection(COLLECTION)
        .upsert_many(docs)
        .execute(&txn)
        .unwrap()
        .affected;
    assert_eq!(result, 1);

    let doc = db
        .collection(COLLECTION)
        .find(rawdoc! { "_id": "u1" })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(doc.get_str("_id").unwrap(), "u1");
    assert_eq!(doc.get_str("name").unwrap(), "Alice Updated");
    assert_eq!(doc.get_str("status").unwrap(), "inactive");
    // score should be gone -- full replace
    assert!(!doc.get_check("score"));
}

#[test]
fn upsert_many_mixed() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let txn = db.begin(false).unwrap();

    db.collection(COLLECTION)
        .insert_one(doc! { "_id": "u1", "name": "Alice", "status": "active" })
        .execute(&txn)
        .unwrap();

    let docs = vec![
        doc! { "_id": "u1", "name": "Alice v2", "status": "inactive" },
        doc! { "_id": "u2", "name": "Bob", "status": "active" },
    ];
    let result = db
        .collection(COLLECTION)
        .upsert_many(docs)
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
fn upsert_many_updates_indexes() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let txn = db.begin(false).unwrap();
    db.collection(COLLECTION)
        .indexes()
        .create("status", IndexOptions::default())
        .execute(&txn)
        .unwrap();

    db.collection(COLLECTION)
        .insert_one(doc! { "_id": "u1", "name": "Alice", "status": "active" })
        .execute(&txn)
        .unwrap();

    // Verify index works before upsert
    let active = db
        .collection(COLLECTION)
        .find(eq_filter("status", Bson::String("active".into())))
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(active.len(), 1);

    // Upsert changes status
    db.collection(COLLECTION)
        .upsert_many(vec![
            doc! { "_id": "u1", "name": "Alice", "status": "inactive" },
        ])
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
