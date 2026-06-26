mod common;
use common::*;

use bson::{Bson, doc, rawdoc};
use slate_db::v2::IndexOptions;

// ── TTL tests ───────────────────────────────────────────────────

fn past_ttl() -> bson::DateTime {
    bson::DateTime::from_millis(
        bson::DateTime::now().timestamp_millis() - 60_000, // 1 minute ago
    )
}

fn future_ttl() -> bson::DateTime {
    bson::DateTime::from_millis(
        bson::DateTime::now().timestamp_millis() + 600_000, // 10 minutes from now
    )
}

#[test]
fn ttl_expired_docs_hidden_before_purge() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_many(vec![
        doc! { "_id": "a", "name": "Expired", "ttl": past_ttl() },
        doc! { "_id": "b", "name": "Fresh", "ttl": future_ttl() },
        doc! { "_id": "c", "name": "Permanent" },
    ])
    .execute(&txn)
    .unwrap();
    txn.commit().unwrap();

    // Expired docs are immediately invisible (TTL read filtering)
    let txn = db.begin(true).unwrap();
    let results = coll
        .find(rawdoc! {})
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);

    let result = coll
        .find(rawdoc! { "_id": "a" })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap();
    assert!(result.is_none());

    let count = coll.find(rawdoc! {}).iter_raw(&txn).unwrap().count();
    assert_eq!(count, 2);
}

#[test]
fn ttl_purge_makes_expired_docs_invisible() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_many(vec![
        doc! { "_id": "a", "name": "Expired", "ttl": past_ttl() },
        doc! { "_id": "b", "name": "Fresh", "ttl": future_ttl() },
        doc! { "_id": "c", "name": "Permanent" },
    ])
    .execute(&txn)
    .unwrap();
    txn.commit().unwrap();

    // Purge removes expired docs
    let txn = db.begin(false).unwrap();
    let deleted = coll.purge(&txn).unwrap();
    assert_eq!(deleted, 1);
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = coll
        .find(rawdoc! {})
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
    let mut names: Vec<_> = results
        .iter()
        .map(|r| r.get_str("name").unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Fresh", "Permanent"]);

    let result = coll
        .find(rawdoc! { "_id": "a" })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap();
    assert!(result.is_none());

    let count = coll.find(rawdoc! {}).iter_raw(&txn).unwrap().count();
    assert_eq!(count, 2);
}

// ── TTL purge tests ─────────────────────────────────────────────

#[test]
fn ttl_purge_deletes_expired_docs() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_many(vec![
        doc! { "_id": "a", "name": "Expired", "ttl": past_ttl() },
        doc! { "_id": "b", "name": "Fresh", "ttl": future_ttl() },
        doc! { "_id": "c", "name": "Permanent" },
    ])
    .execute(&txn)
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let purged = coll.purge(&txn).unwrap();
    assert_eq!(purged, 1);
    txn.commit().unwrap();

    // Expired doc is physically gone
    let txn = db.begin(true).unwrap();
    // Use a direct scan (count bypasses TTL filter, but purge actually deletes)
    let results = coll
        .find(rawdoc! {})
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
    let mut names: Vec<_> = results
        .iter()
        .map(|r| r.get_str("name").unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Fresh", "Permanent"]);
}

#[test]
fn ttl_purge_skips_unexpired_docs() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_many(vec![
        doc! { "_id": "a", "name": "Fresh", "ttl": future_ttl() },
        doc! { "_id": "b", "name": "Permanent" },
    ])
    .execute(&txn)
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let purged = coll.purge(&txn).unwrap();
    assert_eq!(purged, 0);
    txn.commit().unwrap();
}

#[test]
fn ttl_purge_cleans_user_indexes() {
    let (db, _dir) = temp_db();

    let txn = db.begin(false).unwrap();
    db.collections().create("purge_idx").execute(&txn).unwrap();
    db.collection("purge_idx")
        .indexes()
        .create("status", IndexOptions::default())
        .execute(&txn)
        .unwrap();
    db.collection("purge_idx")
        .insert_many(vec![
            doc! { "_id": "a", "status": "active", "ttl": past_ttl() },
            doc! { "_id": "b", "status": "active", "ttl": future_ttl() },
        ])
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let purged = db.collection("purge_idx").purge(&txn).unwrap();
    assert_eq!(purged, 1);
    txn.commit().unwrap();

    // Index should only have one entry for "active" (doc "b")
    let txn = db.begin(true).unwrap();
    let results = db
        .collection("purge_idx")
        .find(eq_filter("status", Bson::String("active".into())))
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "b");
}

#[test]
fn ttl_index_maintained_on_update() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let old_ttl = future_ttl();
    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "_id": "a", "name": "Doc", "ttl": old_ttl })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    // Update ttl to the past
    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("a".into()));
    coll.find(&filter)
        .update(doc! { "ttl": past_ttl() })
        .one()
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    // Purge should now delete the doc
    let txn = db.begin(false).unwrap();
    let purged = coll.purge(&txn).unwrap();
    assert_eq!(purged, 1);
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = coll
        .find(rawdoc! {})
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn ttl_purge_multiple_expired() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_many(vec![
        doc! { "_id": "a", "ttl": bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 120_000) },
        doc! { "_id": "b", "ttl": bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 60_000) },
        doc! { "_id": "c", "ttl": bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 1_000) },
        doc! { "_id": "d", "ttl": future_ttl() },
    ])
    .execute(&txn).unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let purged = coll.purge(&txn).unwrap();
    assert_eq!(purged, 3);
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = coll
        .find(rawdoc! {})
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "d");
}

#[test]
fn ttl_find_by_id_hides_expired() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "_id": "x", "name": "Expired", "ttl": past_ttl() })
        .execute(&txn)
        .unwrap();
    coll.insert_one(doc! { "_id": "y", "name": "Fresh", "ttl": future_ttl() })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    assert!(
        coll.find(rawdoc! { "_id": "x" })
            .iter_raw(&txn)
            .unwrap()
            .next()
            .transpose()
            .unwrap()
            .is_none()
    );
    assert!(
        coll.find(rawdoc! { "_id": "y" })
            .iter_raw(&txn)
            .unwrap()
            .next()
            .transpose()
            .unwrap()
            .is_some()
    );
}

#[test]
fn ttl_update_skips_expired() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "_id": "a", "status": "old", "ttl": past_ttl() })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    // update_one should match 0 — expired doc is invisible
    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("a".into()));
    let result = coll
        .find(&filter)
        .update(doc! { "status": "new" })
        .one()
        .execute(&txn)
        .unwrap()
        .affected;
    assert_eq!(result, 0);
}

#[test]
fn ttl_merge_into_expired_inserts_fresh() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_one(doc! { "_id": "a", "old_field": true, "ttl": past_ttl() })
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    // Merge with same _id — expired doc treated as non-existent, takes insert path
    let txn = db.begin(false).unwrap();
    let result = coll
        .merge_many(vec![
            doc! { "_id": "a", "new_field": true, "ttl": future_ttl() },
        ])
        .execute(&txn)
        .unwrap()
        .affected;
    assert_eq!(result, 1);
    txn.commit().unwrap();

    // The new doc should be visible and should NOT contain old_field
    let txn = db.begin(true).unwrap();
    let doc = coll
        .find(rawdoc! { "_id": "a" })
        .iter_raw(&txn)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert!(doc.get_check("new_field"));
    assert!(!doc.get_check("old_field"));
}

#[test]
fn ttl_no_ttl_field_always_visible() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let coll = db.collection(COLLECTION);

    let txn = db.begin(false).unwrap();
    coll.insert_many(vec![
        doc! { "_id": "a", "name": "Permanent1" },
        doc! { "_id": "b", "name": "Permanent2" },
    ])
    .execute(&txn)
    .unwrap();
    txn.commit().unwrap();

    // Docs without ttl are always visible
    let txn = db.begin(true).unwrap();
    let results = coll
        .find(rawdoc! {})
        .iter_raw(&txn)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);

    // purge_expired should not touch them
    let txn = db.begin(false).unwrap();
    let purged = coll.purge(&txn).unwrap();
    assert_eq!(purged, 0);
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let count = coll.find(rawdoc! {}).iter_raw(&txn).unwrap().count();
    assert_eq!(count, 2);
}
