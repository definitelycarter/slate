mod common;
use common::*;

use bson::{Bson, doc, rawdoc};
use slate_db::CollectionConfig;
use slate_query::FindOptions;

// ── Index tests ─────────────────────────────────────────────────

#[test]
fn create_and_use_index() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
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
    txn.create_index(COLLECTION, "status").unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
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
    assert_eq!(results.len(), 2);
    let mut names: Vec<_> = results
        .iter()
        .map(|r| r.get_str("name").unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Charlie"]);
}

#[test]
fn drop_index() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.create_index(COLLECTION, "status").unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let mut indexes = txn.list_indexes(COLLECTION).unwrap();
    indexes.sort();
    assert_eq!(indexes, vec!["status", "ttl"]);

    let mut txn = db.begin(false).unwrap();
    txn.drop_index(COLLECTION, "status").unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let indexes = txn.list_indexes(COLLECTION).unwrap();
    assert_eq!(indexes, vec!["ttl"]);
}

// ── Index maintenance on writes ─────────────────────────────────

#[test]
fn index_maintained_on_insert() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    // Create index first, then insert
    let mut txn = db.begin(false).unwrap();
    txn.create_index(COLLECTION, "status").unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "r1", "name": "Alice", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.insert_one(
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
            COLLECTION,
            eq_filter("status", Bson::String("active".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
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

    let mut txn = db.begin(false).unwrap();
    txn.create_index(COLLECTION, "status").unwrap();
    txn.insert_one(
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
    txn.update_one(COLLECTION, &filter, doc! { "status": "rejected" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // Old index value should not match
    let txn = db.begin(true).unwrap();
    let results = txn
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
    assert_eq!(results.len(), 0);

    // New index value should match
    let results = txn
        .find(
            COLLECTION,
            eq_filter("status", Bson::String("rejected".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn index_maintained_on_delete() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.create_index(COLLECTION, "status").unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "r1", "name": "Alice", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.delete_one(COLLECTION, &filter)
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // Index should be empty
    let txn = db.begin(true).unwrap();
    let results = txn
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
    assert_eq!(results.len(), 0);
}

// ── Multi-key and nested path index tests ───────────────────────

#[test]
fn index_on_nested_path() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "nested_idx".to_string(),
        indexes: vec!["address.city".to_string()],
        ..Default::default()
    })
    .unwrap();
    txn.insert_many(
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
            "nested_idx",
            eq_filter("address.city", Bson::String("Austin".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
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
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "tags_idx".to_string(),
        indexes: vec!["tags.[]".to_string()],
        ..Default::default()
    })
    .unwrap();
    txn.insert_many(
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
            "tags_idx",
            eq_filter("tags.[]", Bson::String("rust".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
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
            "tags_idx",
            eq_filter("tags.[]", Bson::String("api".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
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
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "items_idx".to_string(),
        indexes: vec!["items.[].sku".to_string()],
        ..Default::default()
    })
    .unwrap();
    txn.insert_many(
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
            "items_idx",
            eq_filter("items.[].sku", Bson::String("A1".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
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
            "items_idx",
            eq_filter("items.[].sku", Bson::String("C3".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
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
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "tags_upd".to_string(),
        indexes: vec!["tags.[]".to_string()],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one("tags_upd", doc! { "_id": "r1", "tags": ["rust", "db"] })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // Update tags
    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one("tags_upd", &filter, doc! { "tags": ["go", "api"] })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // Old tags should not match
    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            "tags_upd",
            eq_filter("tags.[]", Bson::String("rust".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);

    // New tags should match
    let results = txn
        .find(
            "tags_upd",
            eq_filter("tags.[]", Bson::String("go".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "r1");
}

#[test]
fn multikey_index_maintained_on_delete() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "tags_del".to_string(),
        indexes: vec!["tags.[]".to_string()],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one("tags_del", doc! { "_id": "r1", "tags": ["rust", "db"] })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // Delete
    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.delete_one("tags_del", &filter)
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // Index entries should be cleaned up
    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            "tags_del",
            eq_filter("tags.[]", Bson::String("rust".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
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
    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
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
    let mut txn = db.begin(false).unwrap();
    txn.create_index("backfill", "tags.[]").unwrap();
    txn.commit().unwrap();

    // Verify backfill worked
    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            "backfill",
            eq_filter("tags.[]", Bson::String("rust".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
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
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "tags_rep".to_string(),
        indexes: vec!["tags.[]".to_string()],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one("tags_rep", doc! { "_id": "r1", "tags": ["rust", "db"] })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // Replace entirely
    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.replace_one("tags_rep", &filter, doc! { "tags": ["python", "ml"] })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // Old tags gone
    let txn = db.begin(true).unwrap();
    assert_eq!(
        txn.find(
            "tags_rep",
            eq_filter("tags.[]", Bson::String("rust".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .len(),
        0
    );

    // New tags present
    let results = txn
        .find(
            "tags_rep",
            eq_filter("tags.[]", Bson::String("python".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "r1");
}

#[test]
fn create_collection_with_indexes() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "configured".to_string(),
        indexes: vec!["status".to_string(), "tags.[]".to_string()],
        ..Default::default()
    })
    .unwrap();
    txn.commit().unwrap();

    // Verify indexes were created
    let txn = db.begin(true).unwrap();
    let mut indexes = txn.list_indexes("configured").unwrap();
    indexes.sort();
    assert_eq!(indexes, vec!["status", "tags.[]", "ttl"]);
}

#[test]
fn create_collection_idempotent() {
    let (db, _dir) = temp_db();

    // Create once
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "idem".to_string(),
        indexes: vec!["status".to_string()],
        ..Default::default()
    })
    .unwrap();
    txn.commit().unwrap();

    // Insert data
    let mut txn = db.begin(false).unwrap();
    txn.insert_one("idem", doc! { "_id": "r1", "status": "active" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // Create again — should be a no-op, data preserved
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "idem".to_string(),
        indexes: vec!["status".to_string()],
        ..Default::default()
    })
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find("idem", rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
}
