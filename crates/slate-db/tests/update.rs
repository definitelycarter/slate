mod common;
use common::*;

use bson::{Bson, doc, rawdoc};
use slate_query::FindOptions;

// ── Update tests ────────────────────────────────────────────────

#[test]
fn update_one_merge() {
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
        .update_one(COLLECTION, &filter, doc! { "status": "rejected" })
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
    let result = txn
        .update_one(COLLECTION, &filter, doc! { "status": "active" })
        .unwrap()
        .drain()
        .unwrap();
    assert_eq!(result, 0);
}

#[test]
fn upsert_via_upsert_many() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    let result = txn
        .upsert_many(
            COLLECTION,
            vec![doc! { "_id": "new-doc", "name": "Upserted" }],
        )
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
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("name").unwrap(), "Upserted");
}

#[test]
fn update_many_multiple() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("status", Bson::String("active".into()));
    let result = txn
        .update_many(COLLECTION, &filter, doc! { "status": "archived" })
        .unwrap()
        .drain()
        .unwrap();
    assert_eq!(result, 3);
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            COLLECTION,
            eq_filter("status", Bson::String("archived".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
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
    let result = txn.upsert_many(COLLECTION, docs).unwrap().drain().unwrap();
    assert_eq!(result, 2);

    let found = txn
        .find(COLLECTION, rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(found.len(), 2);
}

#[test]
fn upsert_many_replaces_existing() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let mut txn = db.begin(false).unwrap();

    // Insert original
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "u1", "name": "Alice", "status": "active", "score": 100 },
    )
    .unwrap()
    .drain()
    .unwrap();

    // Upsert replaces entirely
    let docs = vec![doc! { "_id": "u1", "name": "Alice Updated", "status": "inactive" }];
    let result = txn.upsert_many(COLLECTION, docs).unwrap().drain().unwrap();
    assert_eq!(result, 1);

    let doc = txn
        .find_one(COLLECTION, rawdoc! { "_id": "u1" })
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
    let mut txn = db.begin(false).unwrap();

    txn.insert_one(
        COLLECTION,
        doc! { "_id": "u1", "name": "Alice", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();

    let docs = vec![
        doc! { "_id": "u1", "name": "Alice v2", "status": "inactive" },
        doc! { "_id": "u2", "name": "Bob", "status": "active" },
    ];
    let result = txn.upsert_many(COLLECTION, docs).unwrap().drain().unwrap();
    assert_eq!(result, 2);

    let found = txn
        .find(COLLECTION, rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(found.len(), 2);
}

#[test]
fn upsert_many_updates_indexes() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let mut txn = db.begin(false).unwrap();
    txn.create_index(COLLECTION, "status").unwrap();

    txn.insert_one(
        COLLECTION,
        doc! { "_id": "u1", "name": "Alice", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();

    // Verify index works before upsert
    let active = txn
        .find(
            COLLECTION,
            eq_filter("status", Bson::String("active".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(active.len(), 1);

    // Upsert changes status
    txn.upsert_many(
        COLLECTION,
        vec![doc! { "_id": "u1", "name": "Alice", "status": "inactive" }],
    )
    .unwrap()
    .drain()
    .unwrap();

    // Old index entry gone
    let active = txn
        .find(
            COLLECTION,
            eq_filter("status", Bson::String("active".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(active.len(), 0);

    // New index entry present
    let inactive = txn
        .find(
            COLLECTION,
            eq_filter("status", Bson::String("inactive".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(inactive.len(), 1);
}
