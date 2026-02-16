use bson::doc;
use slate_db::Database;
use slate_query::{
    Filter, FilterGroup, FilterNode, LogicalOp, Operator, Query, QueryValue, Sort, SortDirection,
};
use slate_store::RocksStore;

const COLLECTION: &str = "accounts";

fn temp_db() -> (Database<RocksStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();
    let db = Database::new(store);
    (db, dir)
}

fn no_filter_query() -> Query {
    Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    }
}

fn eq_filter(field: &str, value: QueryValue) -> FilterGroup {
    FilterGroup {
        logical: LogicalOp::And,
        children: vec![FilterNode::Condition(Filter {
            field: field.into(),
            operator: Operator::Eq,
            value,
        })],
    }
}

/// Insert 5 seed records.
fn seed_records(db: &Database<RocksStore>) {
    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
        COLLECTION,
        vec![
            doc! { "_id": "acct-1", "name": "Acme Corp", "revenue": 50000.0, "status": "active", "active": true },
            doc! { "_id": "acct-2", "name": "Globex", "revenue": 80000.0, "status": "snoozed", "active": true },
            doc! { "_id": "acct-3", "name": "Initech", "revenue": 12000.0, "status": "rejected", "active": false },
            doc! { "_id": "acct-4", "name": "Umbrella", "revenue": 95000.0, "status": "active", "active": true },
            doc! { "_id": "acct-5", "name": "Stark Industries", "revenue": 200000.0, "status": "active", "active": false },
        ],
    )
    .unwrap();
    txn.commit().unwrap();
}

// ── Insert tests ────────────────────────────────────────────────

#[test]
fn insert_one_and_find_one() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    let result = txn
        .insert_one(
            COLLECTION,
            doc! { "_id": "acct-1", "name": "Acme", "revenue": 50000.0 },
        )
        .unwrap();
    assert_eq!(result.id, "acct-1");
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("_id", QueryValue::String("acct-1".into()))),
        sort: vec![],
        skip: None,
        take: Some(1),
        columns: None,
    };
    let record = txn.find_one(COLLECTION, &query).unwrap().unwrap();
    assert_eq!(record.get_str("_id").unwrap(), "acct-1");
    assert_eq!(record.get_str("name").unwrap(), "Acme");
    assert_eq!(record.get_f64("revenue").unwrap(), 50000.0);
}

#[test]
fn insert_one_duplicate_id_fails() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "acct-1", "name": "Acme" })
        .unwrap();
    let err = txn
        .insert_one(COLLECTION, doc! { "_id": "acct-1", "name": "Duplicate" })
        .unwrap_err();
    assert!(err.to_string().contains("duplicate key"));
}

#[test]
fn insert_one_auto_generated_id() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    let result = txn
        .insert_one(COLLECTION, doc! { "name": "No ID" })
        .unwrap();
    txn.commit().unwrap();

    // ID should be a UUID
    assert!(!result.id.is_empty());
    assert!(result.id.contains('-')); // UUID format

    let mut txn = db.begin(true).unwrap();
    let results = txn.find(COLLECTION, &no_filter_query()).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), result.id);
}

#[test]
fn insert_many_batch() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    let results = txn
        .insert_many(
            COLLECTION,
            vec![
                doc! { "_id": "acct-1", "name": "Acme" },
                doc! { "_id": "acct-2", "name": "Globex" },
            ],
        )
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, "acct-1");
    assert_eq!(results[1].id, "acct-2");
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let all = txn.find(COLLECTION, &no_filter_query()).unwrap();
    assert_eq!(all.len(), 2);
}

// ── Query tests ─────────────────────────────────────────────────

#[test]
fn find_no_filters() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let results = txn.find(COLLECTION, &no_filter_query()).unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn find_eq_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", QueryValue::String("active".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn find_gt_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "revenue".into(),
                operator: Operator::Gt,
                value: QueryValue::Float(80000.0),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 2); // Umbrella (95k) and Stark (200k)
}

#[test]
fn find_isnull_filter() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "acct-x", "name": "NoStatus" })
        .unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "acct-1", "name": "Acme", "status": "active" },
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "status".into(),
                operator: Operator::IsNull,
                value: QueryValue::Bool(true),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-x");
}

#[test]
fn find_or_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::Or,
            children: vec![
                FilterNode::Condition(Filter {
                    field: "status".into(),
                    operator: Operator::Eq,
                    value: QueryValue::String("snoozed".into()),
                }),
                FilterNode::Condition(Filter {
                    field: "status".into(),
                    operator: Operator::Eq,
                    value: QueryValue::String("rejected".into()),
                }),
            ],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 2);
}

// ── Sort tests ──────────────────────────────────────────────────

#[test]
fn find_sort_asc() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![Sort {
            field: "revenue".into(),
            direction: SortDirection::Asc,
        }],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-3"); // Initech 12k
    assert_eq!(results[4].get_str("_id").unwrap(), "acct-5"); // Stark 200k
}

#[test]
fn find_sort_desc() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![Sort {
            field: "revenue".into(),
            direction: SortDirection::Desc,
        }],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-5"); // Stark 200k
    assert_eq!(results[4].get_str("_id").unwrap(), "acct-3"); // Initech 12k
}

// ── Pagination ──────────────────────────────────────────────────

#[test]
fn find_skip_and_take() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![Sort {
            field: "revenue".into(),
            direction: SortDirection::Asc,
        }],
        skip: Some(1),
        take: Some(2),
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-1"); // Acme 50k (skipped Initech 12k)
    assert_eq!(results[1].get_str("_id").unwrap(), "acct-2"); // Globex 80k
}

#[test]
fn find_filter_sort_paginate() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", QueryValue::String("active".into()))),
        sort: vec![Sort {
            field: "revenue".into(),
            direction: SortDirection::Desc,
        }],
        skip: Some(1),
        take: Some(1),
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-4"); // Umbrella 95k
}

// ── Projection in query ─────────────────────────────────────────

#[test]
fn find_with_projection() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: Some(vec!["name".into(), "status".into()]),
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 5);
    for record in &results {
        assert!(record.contains_key("name"));
        assert!(record.contains_key("status"));
        assert!(!record.contains_key("revenue"));
        assert!(!record.contains_key("active"));
    }
}

#[test]
fn find_projection_includes_filter_columns() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", QueryValue::String("active".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: Some(vec!["name".into()]),
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 3);
    for record in &results {
        assert!(record.contains_key("name"));
        assert!(!record.contains_key("status")); // filter col stripped
    }
}

#[test]
fn find_projection_includes_sort_columns() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        skip: None,
        take: Some(2),
        sort: vec![Sort {
            field: "revenue".into(),
            direction: SortDirection::Desc,
        }],
        columns: Some(vec!["name".into()]),
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-5"); // Stark 200k
    assert_eq!(results[1].get_str("_id").unwrap(), "acct-4"); // Umbrella 95k
    for record in &results {
        assert!(record.contains_key("name"));
        assert!(!record.contains_key("revenue")); // sort col stripped
    }
}

// ── Update tests ────────────────────────────────────────────────

#[test]
fn update_one_merge() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "acct-1", "name": "Acme", "status": "active" },
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", QueryValue::String("acct-1".into()));
    let result = txn
        .update_one(COLLECTION, &filter, doc! { "status": "rejected" }, false)
        .unwrap();
    assert_eq!(result.matched, 1);
    assert_eq!(result.modified, 1);
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let results = txn.find(COLLECTION, &no_filter_query()).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("status").unwrap(), "rejected");
    assert_eq!(results[0].get_str("name").unwrap(), "Acme"); // unchanged
}

#[test]
fn update_one_no_match() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", QueryValue::String("nonexistent".into()));
    let result = txn
        .update_one(COLLECTION, &filter, doc! { "status": "active" }, false)
        .unwrap();
    assert_eq!(result.matched, 0);
    assert_eq!(result.modified, 0);
    assert!(result.upserted_id.is_none());
}

#[test]
fn update_one_upsert() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", QueryValue::String("new-doc".into()));
    let result = txn
        .update_one(
            COLLECTION,
            &filter,
            doc! { "_id": "new-doc", "name": "Upserted" },
            true,
        )
        .unwrap();
    assert_eq!(result.matched, 0);
    assert!(result.upserted_id.is_some());
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let results = txn.find(COLLECTION, &no_filter_query()).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("name").unwrap(), "Upserted");
}

#[test]
fn update_many_multiple() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("status", QueryValue::String("active".into()));
    let result = txn
        .update_many(COLLECTION, &filter, doc! { "status": "archived" })
        .unwrap();
    assert_eq!(result.matched, 3);
    assert_eq!(result.modified, 3);
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", QueryValue::String("archived".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 3);
}

// ── Replace tests ───────────────────────────────────────────────

#[test]
fn replace_one_full_replacement() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "acct-1", "name": "Acme", "status": "active", "revenue": 50000.0 },
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", QueryValue::String("acct-1".into()));
    let result = txn
        .replace_one(COLLECTION, &filter, doc! { "name": "New Corp" })
        .unwrap();
    assert_eq!(result.matched, 1);
    assert_eq!(result.modified, 1);
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let results = txn.find(COLLECTION, &no_filter_query()).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("name").unwrap(), "New Corp");
    // Old fields should be gone (replaced, not merged)
    assert!(!results[0].contains_key("status"));
    assert!(!results[0].contains_key("revenue"));
}

// ── Delete tests ────────────────────────────────────────────────

#[test]
fn delete_one_removes_record() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "acct-1", "name": "Acme", "status": "active" },
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", QueryValue::String("acct-1".into()));
    let result = txn.delete_one(COLLECTION, &filter).unwrap();
    assert_eq!(result.deleted, 1);
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let results = txn.find(COLLECTION, &no_filter_query()).unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn delete_many_removes_matching() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("status", QueryValue::String("active".into()));
    let result = txn.delete_many(COLLECTION, &filter).unwrap();
    assert_eq!(result.deleted, 3);
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let results = txn.find(COLLECTION, &no_filter_query()).unwrap();
    assert_eq!(results.len(), 2);
}

// ── Count tests ─────────────────────────────────────────────────

#[test]
fn count_all() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let count = txn.count(COLLECTION, None).unwrap();
    assert_eq!(count, 5);
}

#[test]
fn count_with_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let filter = eq_filter("status", QueryValue::String("active".into()));
    let count = txn.count(COLLECTION, Some(&filter)).unwrap();
    assert_eq!(count, 3);
}

// ── Index tests ─────────────────────────────────────────────────

#[test]
fn create_and_use_index() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
        COLLECTION,
        vec![
            doc! { "_id": "r1", "name": "Alice", "status": "active" },
            doc! { "_id": "r2", "name": "Bob", "status": "rejected" },
            doc! { "_id": "r3", "name": "Charlie", "status": "active" },
        ],
    )
    .unwrap();
    // Create index after data exists (tests backfill)
    txn.create_index(COLLECTION, "status").unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", QueryValue::String("active".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).unwrap();
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

    let mut txn = db.begin(false).unwrap();
    txn.create_index(COLLECTION, "status").unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let indexes = txn.list_indexes(COLLECTION).unwrap();
    assert_eq!(indexes, vec!["status"]);

    let mut txn = db.begin(false).unwrap();
    txn.drop_index(COLLECTION, "status").unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let indexes = txn.list_indexes(COLLECTION).unwrap();
    assert!(indexes.is_empty());
}

// ── Collection tests ────────────────────────────────────────────

#[test]
fn list_collections() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.insert_one("contacts", doc! { "_id": "c-1", "name": "Alice" })
        .unwrap();
    txn.insert_one("accounts", doc! { "_id": "a-1", "name": "Acme" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let mut collections = txn.list_collections().unwrap();
    collections.sort();
    assert_eq!(collections, vec!["accounts", "contacts"]);
}

#[test]
fn drop_collection() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "a-1", "name": "Acme" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    txn.drop_collection(COLLECTION).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let results = txn.find(COLLECTION, &no_filter_query()).unwrap();
    assert_eq!(results.len(), 0);
    let collections = txn.list_collections().unwrap();
    assert!(!collections.contains(&COLLECTION.to_string()));
}

// ── Collection isolation ────────────────────────────────────────

#[test]
fn collection_isolation() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.insert_one("contacts", doc! { "_id": "c-1", "name": "Alice" })
        .unwrap();
    txn.insert_one("accounts", doc! { "_id": "a-1", "name": "Acme" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let contacts = txn.find("contacts", &no_filter_query()).unwrap();
    assert_eq!(contacts.len(), 1);
    assert_eq!(contacts[0].get_str("name").unwrap(), "Alice");

    let accounts = txn.find("accounts", &no_filter_query()).unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].get_str("name").unwrap(), "Acme");
}

// ── Index maintenance on writes ─────────────────────────────────

#[test]
fn index_maintained_on_insert() {
    let (db, _dir) = temp_db();

    // Create index first, then insert
    let mut txn = db.begin(false).unwrap();
    txn.create_index(COLLECTION, "status").unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "r1", "name": "Alice", "status": "active" },
    )
    .unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "r2", "name": "Bob", "status": "rejected" },
    )
    .unwrap();
    txn.commit().unwrap();

    // Index scan should work
    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", QueryValue::String("active".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("name").unwrap(), "Alice");
}

#[test]
fn index_maintained_on_update() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.create_index(COLLECTION, "status").unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "r1", "name": "Alice", "status": "active" },
    )
    .unwrap();
    txn.commit().unwrap();

    // Update the indexed field
    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", QueryValue::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "status": "rejected" }, false)
        .unwrap();
    txn.commit().unwrap();

    // Old index value should not match
    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", QueryValue::String("active".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 0);

    // New index value should match
    let query = Query {
        filter: Some(eq_filter("status", QueryValue::String("rejected".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn index_maintained_on_delete() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.create_index(COLLECTION, "status").unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "r1", "name": "Alice", "status": "active" },
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", QueryValue::String("r1".into()));
    txn.delete_one(COLLECTION, &filter).unwrap();
    txn.commit().unwrap();

    // Index should be empty
    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", QueryValue::String("active".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 0);
}

// ── Nested documents + dot-notation ─────────────────────────────

#[test]
fn nested_doc_write_and_read() {
    let (db, _dir) = temp_db();

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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let results = txn.find("nested", &no_filter_query()).unwrap();
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

    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
        "nested",
        vec![
            doc! { "_id": "r1", "name": "Alice", "address": { "city": "Austin", "state": "TX" } },
            doc! { "_id": "r2", "name": "Bob", "address": { "city": "Denver", "state": "CO" } },
            doc! { "_id": "r3", "name": "Charlie", "address": { "city": "Austin", "state": "TX" } },
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter(
            "address.city",
            QueryValue::String("Austin".into()),
        )),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find("nested", &query).unwrap();
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

    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
        "nested",
        vec![
            doc! { "_id": "r1", "name": "Alice", "address": { "city": "Zurich" } },
            doc! { "_id": "r2", "name": "Bob", "address": { "city": "Austin" } },
            doc! { "_id": "r3", "name": "Charlie", "address": { "city": "Denver" } },
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![Sort {
            field: "address.city".into(),
            direction: SortDirection::Asc,
        }],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find("nested", &query).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get_str("name").unwrap(), "Bob"); // Austin
    assert_eq!(results[1].get_str("name").unwrap(), "Charlie"); // Denver
    assert_eq!(results[2].get_str("name").unwrap(), "Alice"); // Zurich
}

#[test]
fn dot_notation_projection() {
    let (db, _dir) = temp_db();

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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: Some(vec!["name".into(), "address.city".into()]),
    };
    let results = txn.find("nested", &query).unwrap();
    assert_eq!(results.len(), 1);
    let record = &results[0];
    assert_eq!(record.get_str("name").unwrap(), "Alice");
    let addr = record.get_document("address").unwrap();
    assert_eq!(addr.get_str("city").unwrap(), "Austin");
    assert!(!addr.contains_key("state"));
    assert!(!addr.contains_key("zip"));
}

#[test]
fn dot_notation_projection_multiple_subfields() {
    let (db, _dir) = temp_db();

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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: Some(vec![
            "name".into(),
            "address.city".into(),
            "address.zip".into(),
        ]),
    };
    let results = txn.find("nested", &query).unwrap();
    assert_eq!(results.len(), 1);
    let record = &results[0];
    let addr = record.get_document("address").unwrap();
    assert_eq!(addr.get_str("city").unwrap(), "Austin");
    assert_eq!(addr.get_str("zip").unwrap(), "78701");
    assert!(!addr.contains_key("state"));
}

#[test]
fn dot_notation_isnull_missing_parent() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        "nested",
        doc! { "_id": "r1", "name": "Alice", "address": { "city": "Austin" } },
    )
    .unwrap();
    txn.insert_one("nested", doc! { "_id": "r2", "name": "Bob" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "address.city".into(),
                operator: Operator::IsNull,
                value: QueryValue::Bool(true),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find("nested", &query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "r2");
}

#[test]
fn dot_notation_deep_nesting() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        "deep",
        doc! { "_id": "r1", "data": { "level1": { "level2": { "value": "found" } } } },
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter(
            "data.level1.level2.value",
            QueryValue::String("found".into()),
        )),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find("deep", &query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "r1");
}

#[test]
fn projection_only_uses_selective_read() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: Some(vec!["name".into()]),
    };
    let results = txn.find(COLLECTION, &query).unwrap();
    assert_eq!(results.len(), 5);
    for record in &results {
        assert!(record.contains_key("_id"));
        assert!(record.contains_key("name"));
        assert!(!record.contains_key("revenue"));
        assert!(!record.contains_key("status"));
        assert!(!record.contains_key("active"));
    }
}

// ── find_by_id tests ────────────────────────────────────────────

#[test]
fn find_by_id_returns_document() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let doc = txn.find_by_id(COLLECTION, "acct-1", None).unwrap().unwrap();
    assert_eq!(doc.get_str("_id").unwrap(), "acct-1");
    assert_eq!(doc.get_str("name").unwrap(), "Acme Corp");
    assert_eq!(doc.get_f64("revenue").unwrap(), 50000.0);
}

#[test]
fn find_by_id_not_found() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let result = txn.find_by_id(COLLECTION, "nonexistent", None).unwrap();
    assert!(result.is_none());
}

#[test]
fn find_by_id_missing_collection() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(true).unwrap();
    let result = txn.find_by_id("no_such_collection", "id-1", None).unwrap();
    assert!(result.is_none());
}

#[test]
fn find_by_id_with_projection() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let doc = txn
        .find_by_id(COLLECTION, "acct-1", Some(&["name", "status"]))
        .unwrap()
        .unwrap();
    assert_eq!(doc.get_str("_id").unwrap(), "acct-1");
    assert_eq!(doc.get_str("name").unwrap(), "Acme Corp");
    assert_eq!(doc.get_str("status").unwrap(), "active");
    assert!(!doc.contains_key("revenue"));
    assert!(!doc.contains_key("active"));
}
