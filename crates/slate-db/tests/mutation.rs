mod common;
use common::*;

use bson::{Bson, doc, rawdoc};
use slate_db::CollectionConfig;
use slate_query::FindOptions;

// ── Mutation operator tests ─────────────────────────────────────

fn get_str_array(doc: &bson::RawDocumentBuf, path: &str) -> Vec<String> {
    let parsed: bson::Document = bson::from_slice(doc.as_bytes()).unwrap();
    let segments: Vec<&str> = path.split('.').collect();
    let mut current = &parsed;
    for seg in &segments[..segments.len() - 1] {
        current = current.get_document(seg).unwrap();
    }
    current
        .get_array(segments.last().unwrap())
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect()
}

#[test]
fn mutation_set_explicit() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
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
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$set": { "status": "archived", "score": 100 } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(d.get_str("name").unwrap(), "Alice");
    assert_eq!(d.get_str("status").unwrap(), "archived");
    assert_eq!(d.get_i32("score").unwrap(), 100);
}

#[test]
fn mutation_unset() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "r1", "name": "Alice", "status": "active", "score": 50 },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "$unset": { "score": "" } })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(d.get_str("name").unwrap(), "Alice");
    assert_eq!(d.get_str("status").unwrap(), "active");
    assert!(!d.get_check("score"));
}

#[test]
fn mutation_inc_i32() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "score": 10_i32 })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "$inc": { "score": 5_i32 } })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(d.get_i32("score").unwrap(), 15);
}

#[test]
fn mutation_inc_missing_field() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "name": "Alice" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "$inc": { "score": 7_i32 } })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(d.get_i32("score").unwrap(), 7);
}

#[test]
fn mutation_inc_negative_decrement() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "score": 100_i32 })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "$inc": { "score": -30_i32 } })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(d.get_i32("score").unwrap(), 70);
}

#[test]
fn mutation_inc_f64() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "balance": 100.50_f64 })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$inc": { "balance": 25.25_f64 } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert!((d.get_f64("balance").unwrap() - 125.75).abs() < f64::EPSILON);
}

#[test]
fn mutation_rename() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "r1", "old_name": "Alice", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$rename": { "old_name": "name" } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(d.get_str("name").unwrap(), "Alice");
    assert!(!d.get_check("old_name"));
    assert_eq!(d.get_str("status").unwrap(), "active");
}

#[test]
fn mutation_push() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "tags": ["rust", "db"] })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "$push": { "tags": "perf" } })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(get_str_array(&d, "tags"), vec!["rust", "db", "perf"]);
}

#[test]
fn mutation_push_creates_array() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "name": "Alice" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "$push": { "tags": "new" } })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(get_str_array(&d, "tags"), vec!["new"]);
}

#[test]
fn mutation_lpush() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "r1", "queue": ["second", "third"] },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "$lpush": { "queue": "first" } })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(get_str_array(&d, "queue"), vec!["first", "second", "third"]);
}

#[test]
fn mutation_pop() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "stack": ["a", "b", "c"] })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "$pop": { "stack": 1 } })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(get_str_array(&d, "stack"), vec!["a", "b"]);
}

#[test]
fn mutation_multiple_operators() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "r1", "name": "Alice", "score": 10_i32, "tags": ["a"] },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! {
            "$set": { "name": "Alice Updated" },
            "$inc": { "score": 5_i32 },
            "$push": { "tags": "b" },
        },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(d.get_str("name").unwrap(), "Alice Updated");
    assert_eq!(d.get_i32("score").unwrap(), 15);
    assert_eq!(get_str_array(&d, "tags"), vec!["a", "b"]);
}

#[test]
fn mutation_bare_fields_implicit_set() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
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
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "status": "archived", "score": 99 },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(d.get_str("name").unwrap(), "Alice");
    assert_eq!(d.get_str("status").unwrap(), "archived");
    assert_eq!(d.get_i32("score").unwrap(), 99);
}

#[test]
fn mutation_dot_path_set() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "r1", "address": { "city": "Austin", "state": "TX" } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$set": { "address.city": "Denver" } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    let addr = d.get_document("address").unwrap();
    assert_eq!(addr.get_str("city").unwrap(), "Denver");
    assert_eq!(addr.get_str("state").unwrap(), "TX");
}

#[test]
fn mutation_dot_path_inc() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "r1", "stats": { "views": 100_i32, "likes": 10_i32 } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$inc": { "stats.views": 1_i32 } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    let stats = d.get_document("stats").unwrap();
    assert_eq!(stats.get_i32("views").unwrap(), 101);
    assert_eq!(stats.get_i32("likes").unwrap(), 10);
}

#[test]
fn mutation_dot_path_creates_intermediates() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "name": "Alice" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$set": { "address.city": "Austin" } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(d.get_str("name").unwrap(), "Alice");
    let addr = d.get_document("address").unwrap();
    assert_eq!(addr.get_str("city").unwrap(), "Austin");
}

#[test]
fn mutation_dot_path_unset() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "r1", "address": { "city": "Austin", "state": "TX", "zip": "78701" } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$unset": { "address.zip": "" } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    let addr = d.get_document("address").unwrap();
    assert_eq!(addr.get_str("city").unwrap(), "Austin");
    assert_eq!(addr.get_str("state").unwrap(), "TX");
    assert!(!addr.get_check("zip"));
}

#[test]
fn mutation_dot_path_push() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "data": { "items": ["a"] } })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "$push": { "data.items": "b" } })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(get_str_array(&d, "data.items"), vec!["a", "b"]);
}

#[test]
fn mutation_update_many_with_inc() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("status", Bson::String("active".into()));
    let result = txn
        .update_many(
            COLLECTION,
            &filter,
            doc! { "$inc": { "revenue": 1000.0_f64 } },
        )
        .unwrap()
        .drain()
        .unwrap();
    assert_eq!(result, 3);
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "acct-1" })
        .unwrap()
        .unwrap();
    assert!((d.get_f64("revenue").unwrap() - 51000.0).abs() < f64::EPSILON);

    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "acct-4" })
        .unwrap()
        .unwrap();
    assert!((d.get_f64("revenue").unwrap() - 96000.0).abs() < f64::EPSILON);

    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "acct-5" })
        .unwrap()
        .unwrap();
    assert!((d.get_f64("revenue").unwrap() - 201000.0).abs() < f64::EPSILON);

    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "acct-2" })
        .unwrap()
        .unwrap();
    assert!((d.get_f64("revenue").unwrap() - 80000.0).abs() < f64::EPSILON);
}

#[test]
fn mutation_index_maintained_on_set() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "idx_mut".to_string(),
        indexes: vec!["status".to_string()],
    })
    .unwrap();
    txn.insert_one(
        "idx_mut",
        doc! { "_id": "r1", "name": "Alice", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        "idx_mut",
        &filter,
        doc! { "$set": { "status": "archived" } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            "idx_mut",
            eq_filter("status", Bson::String("active".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);

    let results = txn
        .find(
            "idx_mut",
            eq_filter("status", Bson::String("archived".into())),
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
fn mutation_index_maintained_on_unset() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "idx_unset".to_string(),
        indexes: vec!["status".to_string()],
    })
    .unwrap();
    txn.insert_one(
        "idx_unset",
        doc! { "_id": "r1", "name": "Alice", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one("idx_unset", &filter, doc! { "$unset": { "status": "" } })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            "idx_unset",
            eq_filter("status", Bson::String("active".into())),
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);

    let d = txn
        .find_one("idx_unset", rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(d.get_str("name").unwrap(), "Alice");
    assert!(!d.get_check("status"));
}

#[test]
fn mutation_push_pop_as_stack() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "name": "stack" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    for val in ["a", "b", "c"] {
        let txn = db.begin(false).unwrap();
        let filter = eq_filter("_id", Bson::String("r1".into()));
        txn.update_one(COLLECTION, &filter, doc! { "$push": { "items": val } })
            .unwrap()
            .drain()
            .unwrap();
        txn.commit().unwrap();
    }

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(get_str_array(&d, "items"), vec!["a", "b", "c"]);

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "$pop": { "items": 1 } })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(get_str_array(&d, "items"), vec!["a", "b"]);
}

#[test]
fn mutation_lpush_pop_as_queue() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "name": "queue" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    for val in ["first", "second", "third"] {
        let txn = db.begin(false).unwrap();
        let filter = eq_filter("_id", Bson::String("r1".into()));
        txn.update_one(COLLECTION, &filter, doc! { "$lpush": { "items": val } })
            .unwrap()
            .drain()
            .unwrap();
        txn.commit().unwrap();
    }

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(get_str_array(&d, "items"), vec!["third", "second", "first"]);

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "$pop": { "items": 1 } })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let d = txn
        .find_one(COLLECTION, rawdoc! { "_id": "r1" })
        .unwrap()
        .unwrap();
    assert_eq!(get_str_array(&d, "items"), vec!["third", "second"]);
}

#[test]
fn mutation_unknown_operator_rejected() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "name": "Alice" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    let result = txn.update_one(COLLECTION, &filter, doc! { "$badop": { "name": "Bob" } });
    let err = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error for $badop"),
    };
    assert!(
        err.contains("$badop"),
        "error should mention the bad op: {err}"
    );
}

#[test]
fn mutation_id_rejected() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "name": "Alice" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    let result = txn.update_one(COLLECTION, &filter, doc! { "$set": { "_id": "r2" } });
    assert!(result.is_err());
}
