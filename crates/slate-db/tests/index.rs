mod common;
use common::*;

use bson::{Bson, doc, rawdoc};
use slate_db::{CollectionConfig, DEFAULT_CF};
use slate_query::FindOptions;

#[allow(dead_code)]
fn create_collection_with_indexes(
    db: &slate_db::Database<slate_store::MemoryStore>,
    name: &str,
    indexes: &[&str],
) {
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: name.to_string(),
        ..Default::default()
    })
    .unwrap();
    for field in indexes {
        txn.create_index(DEFAULT_CF, name, field).unwrap();
    }
    txn.commit().unwrap();
}

// ── Index tests ─────────────────────────────────────────────────

#[test]
fn create_and_use_index() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    txn.insert_many(
        DEFAULT_CF,
        COLLECTION,
        vec![
            doc! { "_id": "r1", "name": "Alice", "status": "active" },
            doc! { "_id": "r2", "name": "Bob", "status": "rejected" },
            doc! { "_id": "r3", "name": "Charlie", "status": "active" },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    // Create index after data exists (tests backfill)
    txn.create_index(DEFAULT_CF, COLLECTION, "status").unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            DEFAULT_CF,
            COLLECTION,
            eq_filter("status", Bson::String("active".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
    let mut names: Vec<_> = results
        .iter()
        .map(|r| r.get_str("name").unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Charlie"]);
}

#[test]
fn numeric_index_scans_decode_without_crashing() {
    // Regression: an index on a numeric field used to crash at scan time with
    // "malformed value in index key" — the sortable numeric encoding embeds bytes
    // the value/doc_id boundary scan mis-read. ObjectId `_id`s give varied doc_id
    // byte patterns that exercise the boundary; index-scan results must match the
    // expected counts.
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    fn det_oid(i: u32) -> bson::oid::ObjectId {
        let mut b = [0u8; 12];
        b[0..4].copy_from_slice(&i.to_be_bytes());
        b[4..8].copy_from_slice(&i.wrapping_mul(2_654_435_761).to_be_bytes());
        b[8..12].copy_from_slice(&(i ^ 0x5bd1_e995).to_be_bytes());
        bson::oid::ObjectId::from_bytes(b)
    }

    let txn = db.begin(false).unwrap();
    let docs: Vec<_> = (0u32..300)
        .map(|i| doc! { "_id": det_oid(i), "priority": (i % 3 + 1) as i32 })
        .collect();
    txn.insert_many(DEFAULT_CF, COLLECTION, docs)
        .unwrap()
        .drain()
        .unwrap();
    txn.create_index(DEFAULT_CF, COLLECTION, "priority")
        .unwrap();
    txn.commit().unwrap();

    let count = |filter: bson::RawDocumentBuf| {
        let txn = db.begin(true).unwrap();
        let n = txn
            .find(DEFAULT_CF, COLLECTION, filter, FindOptions::default())
            .unwrap()
            .drain()
            .unwrap();
        txn.rollback().unwrap();
        n
    };

    // 300 docs, priority cycles 1,2,3 -> 100 each.
    assert_eq!(count(rawdoc! { "priority": 3 }), 100); // eq (was a crash)
    assert_eq!(count(rawdoc! { "priority": { "$gt": 1 } }), 200); // range
    assert_eq!(count(rawdoc! { "priority": { "$gte": 2 } }), 200);
}

#[test]
fn drop_index() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    txn.create_index(DEFAULT_CF, COLLECTION, "status").unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let mut indexes = txn.list_indexes(DEFAULT_CF, COLLECTION).unwrap();
    indexes.sort();
    assert_eq!(indexes, vec!["status", "ttl"]);

    let txn = db.begin(false).unwrap();
    txn.drop_index(DEFAULT_CF, COLLECTION, "status").unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let indexes = txn.list_indexes(DEFAULT_CF, COLLECTION).unwrap();
    assert_eq!(indexes, vec!["ttl"]);
}

// ── Index maintenance on writes ─────────────────────────────────

#[test]
fn index_maintained_on_insert() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    // Create index first, then insert
    let txn = db.begin(false).unwrap();
    txn.create_index(DEFAULT_CF, COLLECTION, "status").unwrap();
    txn.insert_one(
        DEFAULT_CF,
        COLLECTION,
        doc! { "_id": "r1", "name": "Alice", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.insert_one(
        DEFAULT_CF,
        COLLECTION,
        doc! { "_id": "r2", "name": "Bob", "status": "rejected" },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    // Index scan should work
    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            DEFAULT_CF,
            COLLECTION,
            eq_filter("status", Bson::String("active".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("name").unwrap(), "Alice");
}

#[test]
fn index_maintained_on_update() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    txn.create_index(DEFAULT_CF, COLLECTION, "status").unwrap();
    txn.insert_one(
        DEFAULT_CF,
        COLLECTION,
        doc! { "_id": "r1", "name": "Alice", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    // Update the indexed field
    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        DEFAULT_CF,
        COLLECTION,
        &filter,
        doc! { "status": "rejected" },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    // Old index value should not match
    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            DEFAULT_CF,
            COLLECTION,
            eq_filter("status", Bson::String("active".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);

    // New index value should match
    let results = txn
        .find(
            DEFAULT_CF,
            COLLECTION,
            eq_filter("status", Bson::String("rejected".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn index_maintained_on_delete() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    txn.create_index(DEFAULT_CF, COLLECTION, "status").unwrap();
    txn.insert_one(
        DEFAULT_CF,
        COLLECTION,
        doc! { "_id": "r1", "name": "Alice", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.delete_one(DEFAULT_CF, COLLECTION, &filter)
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // Index should be empty
    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            DEFAULT_CF,
            COLLECTION,
            eq_filter("status", Bson::String("active".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);
}

// ── Multi-key and nested path index tests ───────────────────────

#[test]
fn index_on_nested_path() {
    let (db, _dir) = temp_db();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "nested_idx".to_string(),
        ..Default::default()
    })
    .unwrap();
    txn.create_index(DEFAULT_CF, "nested_idx", "address.city")
        .unwrap();
    txn.insert_many(
        DEFAULT_CF,
        "nested_idx",
        vec![
            doc! { "_id": "r1", "name": "Alice", "address": { "city": "Austin", "state": "TX" } },
            doc! { "_id": "r2", "name": "Bob", "address": { "city": "Denver", "state": "CO" } },
            doc! { "_id": "r3", "name": "Charlie", "address": { "city": "Austin", "state": "TX" } },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    // Index scan on address.city
    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            DEFAULT_CF,
            "nested_idx",
            eq_filter("address.city", Bson::String("Austin".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
    let mut names: Vec<_> = results
        .iter()
        .map(|r| r.get_str("name").unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Charlie"]);
}

#[test]
fn index_on_array_of_scalars() {
    let (db, _dir) = temp_db();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "tags_idx".to_string(),
        ..Default::default()
    })
    .unwrap();
    txn.create_index(DEFAULT_CF, "tags_idx", "tags.[]").unwrap();
    txn.insert_many(
        DEFAULT_CF,
        "tags_idx",
        vec![
            doc! { "_id": "r1", "name": "Post A", "tags": ["rust", "db"] },
            doc! { "_id": "r2", "name": "Post B", "tags": ["go", "api"] },
            doc! { "_id": "r3", "name": "Post C", "tags": ["rust", "api"] },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    // Query for tag "rust" via index
    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            DEFAULT_CF,
            "tags_idx",
            eq_filter("tags.[]", Bson::String("rust".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
    let mut names: Vec<_> = results
        .iter()
        .map(|r| r.get_str("name").unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Post A", "Post C"]);

    // Query for tag "api" via index
    let results = txn
        .find(
            DEFAULT_CF,
            "tags_idx",
            eq_filter("tags.[]", Bson::String("api".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
    let mut names: Vec<_> = results
        .iter()
        .map(|r| r.get_str("name").unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Post B", "Post C"]);
}

#[test]
fn index_on_array_of_objects() {
    let (db, _dir) = temp_db();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "items_idx".to_string(),
        ..Default::default()
    })
    .unwrap();
    txn.create_index(DEFAULT_CF, "items_idx", "items.[].sku")
        .unwrap();
    txn.insert_many(
        DEFAULT_CF,
        "items_idx",
        vec![
            doc! { "_id": "order-1", "items": [{ "sku": "A1", "qty": 2 }, { "sku": "B2", "qty": 1 }] },
            doc! { "_id": "order-2", "items": [{ "sku": "C3", "qty": 5 }] },
            doc! { "_id": "order-3", "items": [{ "sku": "A1", "qty": 1 }, { "sku": "C3", "qty": 3 }] },
        ],
    )
    .unwrap().drain().unwrap();
    txn.commit().unwrap();

    // Query for sku "A1"
    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            DEFAULT_CF,
            "items_idx",
            eq_filter("items.[].sku", Bson::String("A1".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
    let mut ids: Vec<_> = results
        .iter()
        .map(|r| r.get_str("_id").unwrap().to_string())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["order-1", "order-3"]);

    // Query for sku "C3"
    let results = txn
        .find(
            DEFAULT_CF,
            "items_idx",
            eq_filter("items.[].sku", Bson::String("C3".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
    let mut ids: Vec<_> = results
        .iter()
        .map(|r| r.get_str("_id").unwrap().to_string())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["order-2", "order-3"]);
}

#[test]
fn multikey_index_maintained_on_update() {
    let (db, _dir) = temp_db();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "tags_upd".to_string(),
        ..Default::default()
    })
    .unwrap();
    txn.create_index(DEFAULT_CF, "tags_upd", "tags.[]").unwrap();
    txn.insert_one(
        DEFAULT_CF,
        "tags_upd",
        doc! { "_id": "r1", "tags": ["rust", "db"] },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    // Update tags
    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        DEFAULT_CF,
        "tags_upd",
        &filter,
        doc! { "tags": ["go", "api"] },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    // Old tags should not match
    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            DEFAULT_CF,
            "tags_upd",
            eq_filter("tags.[]", Bson::String("rust".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);

    // New tags should match
    let results = txn
        .find(
            DEFAULT_CF,
            "tags_upd",
            eq_filter("tags.[]", Bson::String("go".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "r1");
}

#[test]
fn multikey_index_maintained_on_delete() {
    let (db, _dir) = temp_db();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "tags_del".to_string(),
        ..Default::default()
    })
    .unwrap();
    txn.create_index(DEFAULT_CF, "tags_del", "tags.[]").unwrap();
    txn.insert_one(
        DEFAULT_CF,
        "tags_del",
        doc! { "_id": "r1", "tags": ["rust", "db"] },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    // Delete
    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.delete_one(DEFAULT_CF, "tags_del", &filter)
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // Index entries should be cleaned up
    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            DEFAULT_CF,
            "tags_del",
            eq_filter("tags.[]", Bson::String("rust".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn multikey_index_backfill() {
    let (db, _dir) = temp_db();
    create_collection(&db, "backfill");

    // Insert data first, then create the index
    let txn = db.begin(false).unwrap();
    txn.insert_many(
        DEFAULT_CF,
        "backfill",
        vec![
            doc! { "_id": "r1", "tags": ["rust", "db"] },
            doc! { "_id": "r2", "tags": ["go", "api"] },
            doc! { "_id": "r3", "tags": ["rust", "api"] },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    // Now create the index — should backfill
    let txn = db.begin(false).unwrap();
    txn.create_index(DEFAULT_CF, "backfill", "tags.[]").unwrap();
    txn.commit().unwrap();

    // Verify backfill worked
    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            DEFAULT_CF,
            "backfill",
            eq_filter("tags.[]", Bson::String("rust".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
    let mut ids: Vec<_> = results
        .iter()
        .map(|r| r.get_str("_id").unwrap().to_string())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["r1", "r3"]);
}

#[test]
fn multikey_index_replace_one() {
    let (db, _dir) = temp_db();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "tags_rep".to_string(),
        ..Default::default()
    })
    .unwrap();
    txn.create_index(DEFAULT_CF, "tags_rep", "tags.[]").unwrap();
    txn.insert_one(
        DEFAULT_CF,
        "tags_rep",
        doc! { "_id": "r1", "tags": ["rust", "db"] },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    // Replace entirely
    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.replace_one(
        DEFAULT_CF,
        "tags_rep",
        &filter,
        doc! { "tags": ["python", "ml"] },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    // Old tags gone
    let txn = db.begin(true).unwrap();
    assert_eq!(
        txn.find(
            DEFAULT_CF,
            "tags_rep",
            eq_filter("tags.[]", Bson::String("rust".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .len(),
        0
    );

    // New tags present
    let results = txn
        .find(
            DEFAULT_CF,
            "tags_rep",
            eq_filter("tags.[]", Bson::String("python".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "r1");
}

#[test]
fn create_index_shows_in_list() {
    let (db, _dir) = temp_db();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "configured".to_string(),
        ..Default::default()
    })
    .unwrap();
    txn.create_index(DEFAULT_CF, "configured", "status")
        .unwrap();
    txn.create_index(DEFAULT_CF, "configured", "tags.[]")
        .unwrap();
    txn.commit().unwrap();

    // Verify indexes were created
    let txn = db.begin(true).unwrap();
    let mut indexes = txn.list_indexes(DEFAULT_CF, "configured").unwrap();
    indexes.sort();
    assert_eq!(indexes, vec!["status", "tags.[]", "ttl"]);
}

#[test]
fn create_collection_idempotent() {
    let (db, _dir) = temp_db();

    // Create once
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "idem".to_string(),
        ..Default::default()
    })
    .unwrap();
    txn.create_index(DEFAULT_CF, "idem", "status").unwrap();
    txn.commit().unwrap();

    // Insert data
    let txn = db.begin(false).unwrap();
    txn.insert_one(DEFAULT_CF, "idem", doc! { "_id": "r1", "status": "active" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // Create again — should be a no-op, data preserved
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "idem".to_string(),
        ..Default::default()
    })
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(DEFAULT_CF, "idem", rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter_raw()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
}

// ── Unique indexes (public API) ─────────────────────────────────

#[test]
fn unique_index_rejects_duplicate_through_db_api() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    txn.create_unique_index(DEFAULT_CF, COLLECTION, "email")
        .unwrap();
    txn.insert_one(
        DEFAULT_CF,
        COLLECTION,
        doc! { "_id": "a", "email": "x@test.com" },
    )
    .unwrap()
    .drain()
    .unwrap();

    let err = txn
        .insert_one(
            DEFAULT_CF,
            COLLECTION,
            doc! { "_id": "b", "email": "x@test.com" },
        )
        .unwrap()
        .drain()
        .unwrap_err();
    assert!(
        matches!(err, slate_db::DbError::UniqueViolation { .. }),
        "expected DbError::UniqueViolation, got {err:?}"
    );
}

// ── Index ⇄ scan parity guard ────────────────────────────────
//
// An indexed field must return exactly what a full collection scan returns:
// same matches, no cross-type leakage, and a sibling collection sharing the
// column family must not be wiped. Regression guard for two bugs found
// together — the memory backend's `create_cf` data-loss (sibling collections)
// and the non-numeric index range cross-type over-return.

fn seed_k(
    db: &slate_db::Database<slate_store::MemoryStore>,
    name: &str,
    docs: Vec<bson::Document>,
    indexed: bool,
) {
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: name.to_string(),
        ..Default::default()
    })
    .unwrap();
    txn.insert_many(DEFAULT_CF, name, docs)
        .unwrap()
        .drain()
        .unwrap();
    if indexed {
        txn.create_index(DEFAULT_CF, name, "k").unwrap();
    }
    txn.commit().unwrap();
}

fn find_ids(
    db: &slate_db::Database<slate_store::MemoryStore>,
    name: &str,
    filter: bson::Document,
) -> Vec<String> {
    let txn = db.begin(true).unwrap();
    let mut ids: Vec<String> = txn
        .find(DEFAULT_CF, name, &filter, FindOptions::default())
        .unwrap()
        .iter_raw()
        .unwrap()
        .map(|r| r.unwrap().get_str("_id").unwrap().to_string())
        .collect();
    ids.sort();
    ids
}

#[test]
fn indexed_field_matches_scan_for_heterogeneous_values() {
    use bson::DateTime;
    let (db, _dir) = temp_db();
    let dt = |m: i64| Bson::DateTime(DateTime::from_millis(m));
    let docs = || {
        vec![
            doc! { "_id": "s_alpha", "k": "alpha" },
            doc! { "_id": "s_omega", "k": "omega" },
            doc! { "_id": "i_100", "k": 100_i32 },
            doc! { "_id": "i_5", "k": 5_i32 },
            doc! { "_id": "date", "k": dt(1_700_000_000_000) },
            doc! { "_id": "flag", "k": true },
            doc! { "_id": "no_k", "other": 1_i32 },
        ]
    };
    // Same data, one indexed on "k", one not — created as siblings in one CF.
    seed_k(&db, "idx", docs(), true);
    seed_k(&db, "noidx", docs(), false);

    let queries = vec![
        doc! { "k": "omega" },        // string eq
        doc! { "k": { "$gt": "m" } }, // string range (cross-type bait)
        doc! { "k": { "$lt": "m" } },
        doc! { "k": 100_i32 },           // numeric eq
        doc! { "k": { "$gte": 5_i32 } }, // numeric range
        doc! { "k": { "$gt": 4_i32, "$lt": 101_i32 } },
        doc! { "k": true }, // bool eq
    ];
    for q in queries {
        assert_eq!(
            find_ids(&db, "idx", q.clone()),
            find_ids(&db, "noidx", q.clone()),
            "index/scan divergence for query {q:?}"
        );
    }
}

#[test]
fn sibling_collection_index_survives_new_collection() {
    // Regression: creating a second collection used to wipe the first's records
    // and index, because both live in one column family separated by key-prefix.
    let (db, _dir) = temp_db();
    seed_k(
        &db,
        "first",
        vec![
            doc! { "_id": "a", "k": "apple" },
            doc! { "_id": "b", "k": "banana" },
        ],
        true,
    );
    assert_eq!(find_ids(&db, "first", doc! { "k": "apple" }), vec!["a"]);

    seed_k(
        &db,
        "second",
        vec![doc! { "_id": "c", "k": "cherry" }],
        true,
    );

    // First collection's index must still resolve after the sibling is created.
    assert_eq!(find_ids(&db, "first", doc! { "k": "apple" }), vec!["a"]);
    assert_eq!(find_ids(&db, "second", doc! { "k": "cherry" }), vec!["c"]);
}

#[test]
fn indexed_eq_is_exact_not_a_prefix_match() {
    // Regression: a non-numeric indexed Eq used to prefix-match, so Eq("om")
    // wrongly returned "omega". The engine now matches the value exactly.
    let (db, _dir) = temp_db();
    seed_k(
        &db,
        "c",
        vec![
            doc! { "_id": "om", "k": "om" },
            doc! { "_id": "omega", "k": "omega" },
            doc! { "_id": "alpha", "k": "alpha" },
        ],
        true,
    );
    assert_eq!(find_ids(&db, "c", doc! { "k": "om" }), vec!["om"]);
    assert_eq!(find_ids(&db, "c", doc! { "k": "omega" }), vec!["omega"]);
    assert!(find_ids(&db, "c", doc! { "k": "al" }).is_empty());
}

#[test]
fn unique_index_shows_in_list_and_allows_distinct() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let txn = db.begin(false).unwrap();
    txn.create_unique_index(DEFAULT_CF, COLLECTION, "email")
        .unwrap();
    txn.insert_many(
        DEFAULT_CF,
        COLLECTION,
        vec![
            doc! { "_id": "a", "email": "a@test.com" },
            doc! { "_id": "b", "email": "b@test.com" },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let indexes = txn.list_indexes(DEFAULT_CF, COLLECTION).unwrap();
    assert!(indexes.contains(&"email".to_string()));
}
