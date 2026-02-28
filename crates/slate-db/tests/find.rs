mod common;
use common::*;

use bson::raw::RawDocument;
use bson::{Bson, doc, rawdoc};
use slate_db::{CollectionConfig, Database};
use slate_query::{FindOptions, Sort, SortDirection};
use slate_store::MemoryStore;

// ── Query tests ─────────────────────────────────────────────────

#[test]
fn find_no_filters() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn find_eq_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

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
    assert_eq!(results.len(), 3);
}

#[test]
fn find_gt_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            COLLECTION,
            rawdoc! { "revenue": { "$gt": 80000.0 } },
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2); // Umbrella (95k) and Stark (200k)
}

#[test]
fn find_isnull_filter() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "acct-x", "name": "NoStatus" })
        .unwrap()
        .drain()
        .unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "acct-1", "name": "Acme", "status": "active" },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            COLLECTION,
            rawdoc! { "status": null },
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-x");
}

#[test]
fn find_or_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            COLLECTION,
            rawdoc! { "$or": [{ "status": "snoozed" }, { "status": "rejected" }] },
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
}

// ── Sort tests ──────────────────────────────────────────────────

#[test]
fn find_sort_asc() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            COLLECTION,
            rawdoc! {},
            FindOptions {
                sort: vec![Sort {
                    field: "revenue".into(),
                    direction: SortDirection::Asc,
                }],
                ..Default::default()
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-3"); // Initech 12k
    assert_eq!(results[4].get_str("_id").unwrap(), "acct-5"); // Stark 200k
}

#[test]
fn find_sort_desc() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            COLLECTION,
            rawdoc! {},
            FindOptions {
                sort: vec![Sort {
                    field: "revenue".into(),
                    direction: SortDirection::Desc,
                }],
                ..Default::default()
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-5"); // Stark 200k
    assert_eq!(results[4].get_str("_id").unwrap(), "acct-3"); // Initech 12k
}

// ── Pagination ──────────────────────────────────────────────────

#[test]
fn find_skip_and_take() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            COLLECTION,
            rawdoc! {},
            FindOptions {
                sort: vec![Sort {
                    field: "revenue".into(),
                    direction: SortDirection::Asc,
                }],
                skip: Some(1),
                take: Some(2),
                ..Default::default()
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-1"); // Acme 50k (skipped Initech 12k)
    assert_eq!(results[1].get_str("_id").unwrap(), "acct-2"); // Globex 80k
}

#[test]
fn find_filter_sort_paginate() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            COLLECTION,
            eq_filter("status", Bson::String("active".into())),
            FindOptions {
                sort: vec![Sort {
                    field: "revenue".into(),
                    direction: SortDirection::Desc,
                }],
                skip: Some(1),
                take: Some(1),
                ..Default::default()
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-4"); // Umbrella 95k
}

// ── Projection in query ─────────────────────────────────────────

#[test]
fn find_with_projection() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            COLLECTION,
            rawdoc! {},
            FindOptions {
                columns: Some(vec!["name".into(), "status".into()]),
                ..Default::default()
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 5);
    for record in &results {
        assert!(record.get_check("name"));
        assert!(record.get_check("status"));
        assert!(!record.get_check("revenue"));
        assert!(!record.get_check("active"));
    }
}

#[test]
fn find_projection_includes_filter_columns() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            COLLECTION,
            eq_filter("status", Bson::String("active".into())),
            FindOptions {
                columns: Some(vec!["name".into()]),
                ..Default::default()
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 3);
    for record in &results {
        assert!(record.get_check("name"));
        assert!(!record.get_check("status")); // filter col stripped
    }
}

#[test]
fn find_projection_includes_sort_columns() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            COLLECTION,
            rawdoc! {},
            FindOptions {
                sort: vec![Sort {
                    field: "revenue".into(),
                    direction: SortDirection::Desc,
                }],
                take: Some(2),
                columns: Some(vec!["name".into()]),
                ..Default::default()
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-5"); // Stark 200k
    assert_eq!(results[1].get_str("_id").unwrap(), "acct-4"); // Umbrella 95k
    for record in &results {
        assert!(record.get_check("name"));
        assert!(!record.get_check("revenue")); // sort col stripped
    }
}

// ── Nested documents + dot-notation ─────────────────────────────

#[test]
fn nested_doc_write_and_read() {
    let (db, _dir) = temp_db();
    create_collection(&db, "nested");

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        "nested",
        doc! {
            "_id": "r1",
            "name": "Alice",
            "address": {
                "city": "Austin",
                "state": "TX",
                "zip": "78701"
            }
        },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find("nested", rawdoc! {}, FindOptions::default())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    let record = &results[0];
    assert_eq!(record.get_str("name").unwrap(), "Alice");
    let addr = record.get_document("address").unwrap();
    assert_eq!(addr.get_str("city").unwrap(), "Austin");
    assert_eq!(addr.get_str("state").unwrap(), "TX");
    assert_eq!(addr.get_str("zip").unwrap(), "78701");
}

#[test]
fn dot_notation_filter_eq() {
    let (db, _dir) = temp_db();
    create_collection(&db, "nested");

    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
        "nested",
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

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            "nested",
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
fn dot_notation_sort() {
    let (db, _dir) = temp_db();
    create_collection(&db, "nested");

    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
        "nested",
        vec![
            doc! { "_id": "r1", "name": "Alice", "address": { "city": "Zurich" } },
            doc! { "_id": "r2", "name": "Bob", "address": { "city": "Austin" } },
            doc! { "_id": "r3", "name": "Charlie", "address": { "city": "Denver" } },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            "nested",
            rawdoc! {},
            FindOptions {
                sort: vec![Sort {
                    field: "address.city".into(),
                    direction: SortDirection::Asc,
                }],
                ..Default::default()
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get_str("name").unwrap(), "Bob"); // Austin
    assert_eq!(results[1].get_str("name").unwrap(), "Charlie"); // Denver
    assert_eq!(results[2].get_str("name").unwrap(), "Alice"); // Zurich
}

#[test]
fn dot_notation_projection() {
    let (db, _dir) = temp_db();
    create_collection(&db, "nested");

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        "nested",
        doc! {
            "_id": "r1",
            "name": "Alice",
            "address": {
                "city": "Austin",
                "state": "TX",
                "zip": "78701"
            }
        },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            "nested",
            rawdoc! {},
            FindOptions {
                columns: Some(vec!["name".into(), "address.city".into()]),
                ..Default::default()
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    let record = &results[0];
    assert_eq!(record.get_str("name").unwrap(), "Alice");
    let addr = record.get_document("address").unwrap();
    assert_eq!(addr.get_str("city").unwrap(), "Austin");
    assert!(!addr.get_check("state"));
    assert!(!addr.get_check("zip"));
}

#[test]
fn dot_notation_projection_multiple_subfields() {
    let (db, _dir) = temp_db();
    create_collection(&db, "nested");

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        "nested",
        doc! {
            "_id": "r1",
            "name": "Alice",
            "address": {
                "city": "Austin",
                "state": "TX",
                "zip": "78701"
            }
        },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            "nested",
            rawdoc! {},
            FindOptions {
                columns: Some(vec![
                    "name".into(),
                    "address.city".into(),
                    "address.zip".into(),
                ]),
                ..Default::default()
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    let record = &results[0];
    let addr = record.get_document("address").unwrap();
    assert_eq!(addr.get_str("city").unwrap(), "Austin");
    assert_eq!(addr.get_str("zip").unwrap(), "78701");
    assert!(!addr.get_check("state"));
}

#[test]
fn dot_notation_isnull_missing_parent() {
    let (db, _dir) = temp_db();
    create_collection(&db, "nested");

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        "nested",
        doc! { "_id": "r1", "name": "Alice", "address": { "city": "Austin" } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.insert_one("nested", doc! { "_id": "r2", "name": "Bob" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            "nested",
            rawdoc! { "address.city": null },
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "r2");
}

#[test]
fn dot_notation_deep_nesting() {
    let (db, _dir) = temp_db();
    create_collection(&db, "deep");

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        "deep",
        doc! { "_id": "r1", "data": { "level1": { "level2": { "value": "found" } } } },
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            "deep",
            eq_filter("data.level1.level2.value", Bson::String("found".into())),
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
fn projection_only_uses_selective_read() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            COLLECTION,
            rawdoc! {},
            FindOptions {
                columns: Some(vec!["name".into()]),
                ..Default::default()
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 5);
    for record in &results {
        assert!(record.get_check("_id"));
        assert!(record.get_check("name"));
        assert!(!record.get_check("revenue"));
        assert!(!record.get_check("status"));
        assert!(!record.get_check("active"));
    }
}

// ── find_by_id tests ────────────────────────────────────────────

#[test]
fn find_by_id_returns_document() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let doc = txn
        .find_one(COLLECTION, rawdoc! { "_id": "acct-1" })
        .unwrap()
        .unwrap();
    assert_eq!(doc.get_str("_id").unwrap(), "acct-1");
    assert_eq!(doc.get_str("name").unwrap(), "Acme Corp");
    assert_eq!(doc.get_f64("revenue").unwrap(), 50000.0);
}

#[test]
fn find_by_id_not_found() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let result = txn
        .find_one(COLLECTION, rawdoc! { "_id": "nonexistent" })
        .unwrap();
    assert!(result.is_none());
}

#[test]
fn find_by_id_missing_collection() {
    let (db, _dir) = temp_db();

    let txn = db.begin(true).unwrap();
    let result = txn.find_one("no_such_collection", rawdoc! { "_id": "id-1" });
    assert!(matches!(
        result,
        Err(slate_db::DbError::CollectionNotFound(_))
    ));
}

#[test]
fn find_by_id_with_projection() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let doc = txn
        .find(
            COLLECTION,
            rawdoc! { "_id": "acct-1" },
            FindOptions {
                columns: Some(vec!["name".into(), "status".into()]),
                ..Default::default()
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(doc.get_str("_id").unwrap(), "acct-1");
    assert_eq!(doc.get_str("name").unwrap(), "Acme Corp");
    assert_eq!(doc.get_str("status").unwrap(), "active");
    assert!(!doc.get_check("revenue"));
    assert!(!doc.get_check("active"));
}

// ── OR / AND index integration tests ────────────────────────────

/// Create a collection with indexes and seed data for OR/AND tests.
fn seed_or_test_data(db: &Database<MemoryStore>) {
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "orders".to_string(),
        indexes: vec!["user_id".to_string(), "status".to_string()],
        ..Default::default()
    })
    .unwrap();
    txn.insert_many(
        "orders",
        vec![
            doc! { "_id": "o1", "user_id": "abc", "status": "active", "score": 80, "name": "Order 1" },
            doc! { "_id": "o2", "user_id": "xyz", "status": "archived", "score": 30, "name": "Order 2" },
            doc! { "_id": "o3", "user_id": "abc", "status": "archived", "score": 90, "name": "Order 3" },
            doc! { "_id": "o4", "user_id": "xyz", "status": "active", "score": 20, "name": "Order 4" },
            doc! { "_id": "o5", "user_id": "abc", "status": "active", "score": 10, "name": "Order 5" },
            doc! { "_id": "o6", "user_id": "def", "status": "pending", "score": 60, "name": "Order 6" },
        ],
    )
    .unwrap().drain().unwrap();
    txn.commit().unwrap();
}

fn sorted_ids(docs: &[bson::RawDocumentBuf]) -> Vec<String> {
    let mut ids: Vec<String> = docs
        .iter()
        .map(|d| d.get_str("_id").unwrap().to_string())
        .collect();
    ids.sort();
    ids
}

#[test]
fn find_with_or_indexed() {
    let (db, _dir) = temp_db();
    seed_or_test_data(&db);

    let txn = db.begin(true).unwrap();
    // user_id = "abc" OR status = "active"
    let results = txn
        .find(
            "orders",
            rawdoc! { "$or": [{ "user_id": "abc" }, { "status": "active" }] },
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    // abc: o1, o3, o5. active: o1, o4, o5. Union: o1, o3, o4, o5
    assert_eq!(sorted_ids(&results), vec!["o1", "o3", "o4", "o5"]);
}

#[test]
fn find_with_or_same_field() {
    let (db, _dir) = temp_db();
    seed_or_test_data(&db);

    let txn = db.begin(true).unwrap();
    // status = "active" OR status = "archived"
    let results = txn
        .find(
            "orders",
            rawdoc! { "$or": [{ "status": "active" }, { "status": "archived" }] },
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    // active: o1, o4, o5. archived: o2, o3. Union: o1, o2, o3, o4, o5
    assert_eq!(sorted_ids(&results), vec!["o1", "o2", "o3", "o4", "o5"]);
}

#[test]
fn find_with_or_fallback_scan() {
    let (db, _dir) = temp_db();
    seed_or_test_data(&db);

    let txn = db.begin(true).unwrap();
    // user_id = "abc" OR score > 50 (score not indexed -> full scan)
    let results = txn
        .find(
            "orders",
            rawdoc! { "$or": [{ "user_id": "abc" }, { "score": { "$gt": 50_i64 } }] },
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    // abc: o1, o3, o5. score > 50: o1(80), o3(90), o6(60). Union: o1, o3, o5, o6
    assert_eq!(sorted_ids(&results), vec!["o1", "o3", "o5", "o6"]);
}

#[test]
fn find_with_and_priority() {
    let (db, _dir) = temp_db();
    seed_or_test_data(&db);

    let txn = db.begin(true).unwrap();
    // user_id = "abc" AND status = "active"
    // user_id has higher priority -- planner should use it for IndexScan
    let results = txn
        .find(
            "orders",
            rawdoc! { "status": "active", "user_id": "abc" },
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    // abc AND active: o1, o5
    assert_eq!(sorted_ids(&results), vec!["o1", "o5"]);
}

#[test]
fn find_with_nested_and_or() {
    let (db, _dir) = temp_db();
    seed_or_test_data(&db);

    let txn = db.begin(true).unwrap();
    // (user_id = "abc" AND status = "active") OR (user_id = "xyz" AND status = "archived")
    let results = txn
        .find(
            "orders",
            rawdoc! { "$or": [
                { "user_id": "abc", "status": "active" },
                { "user_id": "xyz", "status": "archived" },
            ] },
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    // (abc AND active): o1, o5. (xyz AND archived): o2. Union: o1, o2, o5
    assert_eq!(sorted_ids(&results), vec!["o1", "o2", "o5"]);
}

#[test]
fn find_with_or_three_values() {
    let (db, _dir) = temp_db();
    seed_or_test_data(&db);

    let txn = db.begin(true).unwrap();
    // user_id = "abc" OR user_id = "xyz" OR user_id = "def"
    let results = txn
        .find(
            "orders",
            rawdoc! { "$or": [{ "user_id": "abc" }, { "user_id": "xyz" }, { "user_id": "def" }] },
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    // All users — all 6 records
    assert_eq!(
        sorted_ids(&results),
        vec!["o1", "o2", "o3", "o4", "o5", "o6"]
    );
}

#[test]
fn find_with_or_partial_index_per_branch() {
    let (db, _dir) = temp_db();
    seed_or_test_data(&db);

    let txn = db.begin(true).unwrap();
    // (user_id = "abc" AND score > 50) OR status = "pending"
    // Each OR branch has one indexed Eq -- IndexMerge(Or), full recheck
    let results = txn
        .find(
            "orders",
            rawdoc! { "$or": [
                { "user_id": "abc", "score": { "$gt": 50_i64 } },
                { "status": "pending" },
            ] },
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    // (abc AND score > 50): o1(80), o3(90). pending: o6. Union: o1, o3, o6
    assert_eq!(sorted_ids(&results), vec!["o1", "o3", "o6"]);
}

// ── Index-covered projection type preservation ─────────────────

#[test]
fn index_covered_preserves_int32_type() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec!["score".to_string()],
        ..Default::default()
    })
    .unwrap();
    // Insert with Int32
    txn.insert_one(COLLECTION, doc! { "_id": "rec-1", "score": 100_i32 })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // Query with Int64 -- same encoded bytes, different BSON type
    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            COLLECTION,
            eq_filter("score", Bson::Int64(100)),
            FindOptions {
                columns: Some(vec!["score".into()]),
                ..Default::default()
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    // Should preserve the stored Int32 type, not the query's Int64
    let score = results[0].get("score").unwrap().unwrap();
    assert!(
        matches!(score, bson::raw::RawBsonRef::Int32(100)),
        "expected Int32(100), got {:?}",
        score
    );
}

#[test]
fn index_covered_preserves_string_type() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec!["status".to_string()],
        ..Default::default()
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "rec-1", "status": "active" })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            COLLECTION,
            eq_filter("status", Bson::String("active".into())),
            FindOptions {
                columns: Some(vec!["status".into()]),
                ..Default::default()
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    let status = results[0].get("status").unwrap().unwrap();
    assert!(
        matches!(status, bson::raw::RawBsonRef::String("active")),
        "expected String(\"active\"), got {:?}",
        status
    );
}

// ── Range scan integration tests ────────────────────────────

#[test]
fn find_gt_on_indexed_field() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "scores".into(),
        indexes: vec!["score".to_string()],
        ..Default::default()
    })
    .unwrap();
    txn.insert_many(
        "scores",
        vec![
            doc! { "_id": "1", "name": "Alice", "score": 70 },
            doc! { "_id": "2", "name": "Bob", "score": 90 },
            doc! { "_id": "3", "name": "Charlie", "score": 80 },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let results = txn
        .find(
            "scores",
            rawdoc! { "score": { "$gt": 75 } },
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let names: Vec<&str> = results
        .iter()
        .map(|r| {
            let doc = RawDocument::from_bytes(r.as_bytes()).unwrap();
            doc.get_str("name").unwrap()
        })
        .collect();
    assert_eq!(results.len(), 2);
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Charlie"));
}

#[test]
fn find_gte_lte_on_indexed_field() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "scores".into(),
        indexes: vec!["score".to_string()],
        ..Default::default()
    })
    .unwrap();
    txn.insert_many(
        "scores",
        vec![
            doc! { "_id": "1", "name": "Alice", "score": 70 },
            doc! { "_id": "2", "name": "Bob", "score": 90 },
            doc! { "_id": "3", "name": "Charlie", "score": 80 },
            doc! { "_id": "4", "name": "Diana", "score": 60 },
        ],
    )
    .unwrap()
    .drain()
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    // score >= 70 AND score <= 80
    let results = txn
        .find(
            "scores",
            rawdoc! { "score": { "$gte": 70, "$lte": 80 } },
            FindOptions::default(),
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let names: Vec<&str> = results
        .iter()
        .map(|r| {
            let doc = RawDocument::from_bytes(r.as_bytes()).unwrap();
            doc.get_str("name").unwrap()
        })
        .collect();
    assert_eq!(results.len(), 2);
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Charlie"));
}
