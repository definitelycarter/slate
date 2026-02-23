use bson::raw::RawDocument;
use bson::{Bson, RawBson, doc};
use slate_db::{CollectionConfig, Database, DatabaseConfig};
use slate_query::{
    DistinctQuery, Filter, FilterGroup, FilterNode, LogicalOp, Operator, Query, Sort, SortDirection,
};
use slate_store::MemoryStore;

trait HasKey {
    fn get_check(&self, key: &str) -> bool;
}

impl HasKey for RawDocument {
    fn get_check(&self, key: &str) -> bool {
        self.get(key).ok().flatten().is_some()
    }
}

impl HasKey for bson::RawDocumentBuf {
    fn get_check(&self, key: &str) -> bool {
        self.get(key).ok().flatten().is_some()
    }
}

fn to_bson_vec(raw: RawBson) -> Vec<Bson> {
    match raw {
        RawBson::Array(arr) => arr
            .into_iter()
            .map(|r| Bson::try_from(r.unwrap()).unwrap())
            .collect(),
        _ => panic!("expected RawBson::Array"),
    }
}

const COLLECTION: &str = "accounts";

fn temp_db() -> (Database<MemoryStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = MemoryStore::new();
    let db = Database::open(store, DatabaseConfig::default());
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

fn eq_filter(field: &str, value: Bson) -> FilterGroup {
    FilterGroup {
        logical: LogicalOp::And,
        children: vec![FilterNode::Condition(Filter {
            field: field.into(),
            operator: Operator::Eq,
            value,
        })],
    }
}

fn create_collection(db: &Database<MemoryStore>, name: &str) {
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: name.to_string(),
        indexes: vec![],
    })
    .unwrap();
    txn.commit().unwrap();
}

/// Insert 5 seed records.
fn seed_records(db: &Database<MemoryStore>) {
    create_collection(db, COLLECTION);
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
    create_collection(&db, COLLECTION);

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
        filter: Some(eq_filter("_id", Bson::String("acct-1".into()))),
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
    create_collection(&db, COLLECTION);

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
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    let result = txn
        .insert_one(COLLECTION, doc! { "name": "No ID" })
        .unwrap();
    txn.commit().unwrap();

    // ID should be a UUID
    assert!(!result.id.is_empty());
    assert!(result.id.contains('-')); // UUID format

    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, &no_filter_query())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), result.id);
}

#[test]
fn insert_many_batch() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

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
    let all = txn
        .find(COLLECTION, &no_filter_query())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(all.len(), 2);
}

// ── Query tests ─────────────────────────────────────────────────

#[test]
fn find_no_filters() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, &no_filter_query())
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

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", Bson::String("active".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find(COLLECTION, &query)
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

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "revenue".into(),
                operator: Operator::Gt,
                value: Bson::Double(80000.0),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find(COLLECTION, &query)
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
                value: Bson::Boolean(true),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find(COLLECTION, &query)
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

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::Or,
            children: vec![
                FilterNode::Condition(Filter {
                    field: "status".into(),
                    operator: Operator::Eq,
                    value: Bson::String("snoozed".into()),
                }),
                FilterNode::Condition(Filter {
                    field: "status".into(),
                    operator: Operator::Eq,
                    value: Bson::String("rejected".into()),
                }),
            ],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find(COLLECTION, &query)
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
    let results = txn
        .find(COLLECTION, &query)
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
    let results = txn
        .find(COLLECTION, &query)
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
    let results = txn
        .find(COLLECTION, &query)
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

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", Bson::String("active".into()))),
        sort: vec![Sort {
            field: "revenue".into(),
            direction: SortDirection::Desc,
        }],
        skip: Some(1),
        take: Some(1),
        columns: None,
    };
    let results = txn
        .find(COLLECTION, &query)
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

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: Some(vec!["name".into(), "status".into()]),
    };
    let results = txn
        .find(COLLECTION, &query)
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

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", Bson::String("active".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: Some(vec!["name".into()]),
    };
    let results = txn
        .find(COLLECTION, &query)
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
    let results = txn
        .find(COLLECTION, &query)
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("acct-1".into()));
    let result = txn
        .update_one(COLLECTION, &filter, doc! { "status": "rejected" }, false)
        .unwrap();
    assert_eq!(result.matched, 1);
    assert_eq!(result.modified, 1);
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, &no_filter_query())
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

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("nonexistent".into()));
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
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("new-doc".into()));
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
    let results = txn
        .find(COLLECTION, &no_filter_query())
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

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("status", Bson::String("active".into()));
    let result = txn
        .update_many(COLLECTION, &filter, doc! { "status": "archived" })
        .unwrap();
    assert_eq!(result.matched, 3);
    assert_eq!(result.modified, 3);
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", Bson::String("archived".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find(COLLECTION, &query)
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 3);
}

// ── Replace tests ───────────────────────────────────────────────

#[test]
fn replace_one_full_replacement() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "acct-1", "name": "Acme", "status": "active", "revenue": 50000.0 },
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("acct-1".into()));
    let result = txn
        .replace_one(COLLECTION, &filter, doc! { "name": "New Corp" })
        .unwrap();
    assert_eq!(result.matched, 1);
    assert_eq!(result.modified, 1);
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, &no_filter_query())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("name").unwrap(), "New Corp");
    // Old fields should be gone (replaced, not merged)
    assert!(!results[0].get_check("status"));
    assert!(!results[0].get_check("revenue"));
}

// ── Delete tests ────────────────────────────────────────────────

#[test]
fn delete_one_removes_record() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "acct-1", "name": "Acme", "status": "active" },
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("acct-1".into()));
    let result = txn.delete_one(COLLECTION, &filter).unwrap();
    assert_eq!(result.deleted, 1);
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, &no_filter_query())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn delete_many_removes_matching() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("status", Bson::String("active".into()));
    let result = txn.delete_many(COLLECTION, &filter).unwrap();
    assert_eq!(result.deleted, 3);
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, &no_filter_query())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
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
    let filter = eq_filter("status", Bson::String("active".into()));
    let count = txn.count(COLLECTION, Some(&filter)).unwrap();
    assert_eq!(count, 3);
}

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
    .unwrap();
    // Create index after data exists (tests backfill)
    txn.create_index(COLLECTION, "status").unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", Bson::String("active".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find(COLLECTION, &query)
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

    let mut txn = db.begin(true).unwrap();
    let mut indexes = txn.list_indexes(COLLECTION).unwrap();
    indexes.sort();
    assert_eq!(indexes, vec!["status", "ttl"]);

    let mut txn = db.begin(false).unwrap();
    txn.drop_index(COLLECTION, "status").unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let indexes = txn.list_indexes(COLLECTION).unwrap();
    assert_eq!(indexes, vec!["ttl"]);
}

// ── Collection tests ────────────────────────────────────────────

#[test]
fn list_collections() {
    let (db, _dir) = temp_db();
    create_collection(&db, "contacts");
    create_collection(&db, "accounts");

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
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "a-1", "name": "Acme" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    txn.drop_collection(COLLECTION).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let result = txn
        .find(COLLECTION, &no_filter_query())
        .and_then(|c| c.iter()?.collect::<Result<Vec<_>, _>>());
    assert!(matches!(
        result,
        Err(slate_db::DbError::CollectionNotFound(_))
    ));
    let collections = txn.list_collections().unwrap();
    assert!(!collections.contains(&COLLECTION.to_string()));
}

// ── Collection isolation ────────────────────────────────────────

#[test]
fn collection_isolation() {
    let (db, _dir) = temp_db();
    create_collection(&db, "contacts");
    create_collection(&db, "accounts");

    let mut txn = db.begin(false).unwrap();
    txn.insert_one("contacts", doc! { "_id": "c-1", "name": "Alice" })
        .unwrap();
    txn.insert_one("accounts", doc! { "_id": "a-1", "name": "Acme" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let contacts = txn
        .find("contacts", &no_filter_query())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(contacts.len(), 1);
    assert_eq!(contacts[0].get_str("name").unwrap(), "Alice");

    let accounts = txn
        .find("accounts", &no_filter_query())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].get_str("name").unwrap(), "Acme");
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
        filter: Some(eq_filter("status", Bson::String("active".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find(COLLECTION, &query)
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
    .unwrap();
    txn.commit().unwrap();

    // Update the indexed field
    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "status": "rejected" }, false)
        .unwrap();
    txn.commit().unwrap();

    // Old index value should not match
    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", Bson::String("active".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find(COLLECTION, &query)
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);

    // New index value should match
    let query = Query {
        filter: Some(eq_filter("status", Bson::String("rejected".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find(COLLECTION, &query)
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.delete_one(COLLECTION, &filter).unwrap();
    txn.commit().unwrap();

    // Index should be empty
    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", Bson::String("active".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find(COLLECTION, &query)
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find("nested", &no_filter_query())
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("address.city", Bson::String("Austin".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("nested", &query)
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
    let results = txn
        .find("nested", &query)
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
    let results = txn
        .find("nested", &query)
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
    let results = txn
        .find("nested", &query)
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
                value: Bson::Boolean(true),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("nested", &query)
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter(
            "data.level1.level2.value",
            Bson::String("found".into()),
        )),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("deep", &query)
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

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: Some(vec!["name".into()]),
    };
    let results = txn
        .find(COLLECTION, &query)
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
    let result = txn.find_by_id("no_such_collection", "id-1", None);
    assert!(matches!(
        result,
        Err(slate_db::DbError::CollectionNotFound(_))
    ));
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

// ── Multi-key and nested path index tests ───────────────────────

#[test]
fn index_on_nested_path() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "nested_idx".to_string(),
        indexes: vec!["address.city".to_string()],
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
    .unwrap();
    txn.commit().unwrap();

    // Index scan on address.city
    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("address.city", Bson::String("Austin".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("nested_idx", &query)
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
    .unwrap();
    txn.commit().unwrap();

    // Query for tag "rust" via index
    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("tags.[]", Bson::String("rust".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("tags_idx", &query)
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
    let query = Query {
        filter: Some(eq_filter("tags.[]", Bson::String("api".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("tags_idx", &query)
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
    .unwrap();
    txn.commit().unwrap();

    // Query for sku "A1"
    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("items.[].sku", Bson::String("A1".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("items_idx", &query)
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
    let query = Query {
        filter: Some(eq_filter("items.[].sku", Bson::String("C3".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("items_idx", &query)
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
    })
    .unwrap();
    txn.insert_one("tags_upd", doc! { "_id": "r1", "tags": ["rust", "db"] })
        .unwrap();
    txn.commit().unwrap();

    // Update tags
    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one("tags_upd", &filter, doc! { "tags": ["go", "api"] }, false)
        .unwrap();
    txn.commit().unwrap();

    // Old tags should not match
    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("tags.[]", Bson::String("rust".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("tags_upd", &query)
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);

    // New tags should match
    let query = Query {
        filter: Some(eq_filter("tags.[]", Bson::String("go".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("tags_upd", &query)
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
    })
    .unwrap();
    txn.insert_one("tags_del", doc! { "_id": "r1", "tags": ["rust", "db"] })
        .unwrap();
    txn.commit().unwrap();

    // Delete
    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.delete_one("tags_del", &filter).unwrap();
    txn.commit().unwrap();

    // Index entries should be cleaned up
    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("tags.[]", Bson::String("rust".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("tags_del", &query)
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
    .unwrap();
    txn.commit().unwrap();

    // Now create the index — should backfill
    let mut txn = db.begin(false).unwrap();
    txn.create_index("backfill", "tags.[]").unwrap();
    txn.commit().unwrap();

    // Verify backfill worked
    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("tags.[]", Bson::String("rust".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("backfill", &query)
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
    })
    .unwrap();
    txn.insert_one("tags_rep", doc! { "_id": "r1", "tags": ["rust", "db"] })
        .unwrap();
    txn.commit().unwrap();

    // Replace entirely
    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.replace_one("tags_rep", &filter, doc! { "tags": ["python", "ml"] })
        .unwrap();
    txn.commit().unwrap();

    // Old tags gone
    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("tags.[]", Bson::String("rust".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    assert_eq!(
        txn.find("tags_rep", &query)
            .unwrap()
            .iter()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .len(),
        0
    );

    // New tags present
    let query = Query {
        filter: Some(eq_filter("tags.[]", Bson::String("python".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("tags_rep", &query)
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
    })
    .unwrap();
    txn.commit().unwrap();

    // Verify indexes were created
    let mut txn = db.begin(true).unwrap();
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
    })
    .unwrap();
    txn.commit().unwrap();

    // Insert data
    let mut txn = db.begin(false).unwrap();
    txn.insert_one("idem", doc! { "_id": "r1", "status": "active" })
        .unwrap();
    txn.commit().unwrap();

    // Create again — should be a no-op, data preserved
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "idem".to_string(),
        indexes: vec!["status".to_string()],
    })
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find("idem", &no_filter_query())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
}

// ── OR / AND index integration tests ────────────────────────────

fn eq_condition(field: &str, value: Bson) -> FilterNode {
    FilterNode::Condition(Filter {
        field: field.into(),
        operator: Operator::Eq,
        value,
    })
}

fn gt_condition(field: &str, value: Bson) -> FilterNode {
    FilterNode::Condition(Filter {
        field: field.into(),
        operator: Operator::Gt,
        value,
    })
}

/// Create a collection with indexes and seed data for OR/AND tests.
fn seed_or_test_data(db: &Database<MemoryStore>) {
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "orders".to_string(),
        indexes: vec!["user_id".to_string(), "status".to_string()],
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
    .unwrap();
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

    // user_id = "abc" OR status = "active"
    let q = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::Or,
            children: vec![
                eq_condition("user_id", Bson::String("abc".into())),
                eq_condition("status", Bson::String("active".into())),
            ],
        }),
        ..no_filter_query()
    };
    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find("orders", &q)
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

    // status = "active" OR status = "archived"
    let q = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::Or,
            children: vec![
                eq_condition("status", Bson::String("active".into())),
                eq_condition("status", Bson::String("archived".into())),
            ],
        }),
        ..no_filter_query()
    };
    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find("orders", &q)
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

    // user_id = "abc" OR score > 50 (score not indexed → full scan)
    let q = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::Or,
            children: vec![
                eq_condition("user_id", Bson::String("abc".into())),
                gt_condition("score", Bson::Int64(50)),
            ],
        }),
        ..no_filter_query()
    };
    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find("orders", &q)
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

    // user_id = "abc" AND status = "active"
    // user_id has higher priority — planner should use it for IndexScan
    let q = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![
                eq_condition("status", Bson::String("active".into())),
                eq_condition("user_id", Bson::String("abc".into())),
            ],
        }),
        ..no_filter_query()
    };
    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find("orders", &q)
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

    // (user_id = "abc" AND status = "active") OR (user_id = "xyz" AND status = "archived")
    let q = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::Or,
            children: vec![
                FilterNode::Group(FilterGroup {
                    logical: LogicalOp::And,
                    children: vec![
                        eq_condition("user_id", Bson::String("abc".into())),
                        eq_condition("status", Bson::String("active".into())),
                    ],
                }),
                FilterNode::Group(FilterGroup {
                    logical: LogicalOp::And,
                    children: vec![
                        eq_condition("user_id", Bson::String("xyz".into())),
                        eq_condition("status", Bson::String("archived".into())),
                    ],
                }),
            ],
        }),
        ..no_filter_query()
    };
    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find("orders", &q)
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

    // user_id = "abc" OR user_id = "xyz" OR user_id = "def"
    let q = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::Or,
            children: vec![
                eq_condition("user_id", Bson::String("abc".into())),
                eq_condition("user_id", Bson::String("xyz".into())),
                eq_condition("user_id", Bson::String("def".into())),
            ],
        }),
        ..no_filter_query()
    };
    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find("orders", &q)
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

    // (user_id = "abc" AND score > 50) OR status = "pending"
    // Each OR branch has one indexed Eq — IndexMerge(Or), full recheck
    let q = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::Or,
            children: vec![
                FilterNode::Group(FilterGroup {
                    logical: LogicalOp::And,
                    children: vec![
                        eq_condition("user_id", Bson::String("abc".into())),
                        gt_condition("score", Bson::Int64(50)),
                    ],
                }),
                eq_condition("status", Bson::String("pending".into())),
            ],
        }),
        ..no_filter_query()
    };
    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find("orders", &q)
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    // (abc AND score > 50): o1(80), o3(90). pending: o6. Union: o1, o3, o6
    assert_eq!(sorted_ids(&results), vec!["o1", "o3", "o6"]);
}

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

    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
        COLLECTION,
        vec![
            doc! { "_id": "a", "name": "Expired", "ttl": past_ttl() },
            doc! { "_id": "b", "name": "Fresh", "ttl": future_ttl() },
            doc! { "_id": "c", "name": "Permanent" },
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    // Expired docs are immediately invisible (TTL read filtering)
    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, &no_filter_query())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);

    let result = txn.find_by_id(COLLECTION, "a", None).unwrap();
    assert!(result.is_none());

    let count = txn.count(COLLECTION, None).unwrap();
    assert_eq!(count, 2);
}

#[test]
fn ttl_purge_makes_expired_docs_invisible() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
        COLLECTION,
        vec![
            doc! { "_id": "a", "name": "Expired", "ttl": past_ttl() },
            doc! { "_id": "b", "name": "Fresh", "ttl": future_ttl() },
            doc! { "_id": "c", "name": "Permanent" },
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    // Purge removes expired docs
    let deleted = db.purge_expired(COLLECTION).unwrap();
    assert_eq!(deleted, 1);

    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, &no_filter_query())
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
    assert_eq!(names, vec!["Fresh", "Permanent"]);

    let result = txn.find_by_id(COLLECTION, "a", None).unwrap();
    assert!(result.is_none());

    let count = txn.count(COLLECTION, None).unwrap();
    assert_eq!(count, 2);
}

// ── TTL purge tests ─────────────────────────────────────────────

#[test]
fn ttl_purge_deletes_expired_docs() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
        COLLECTION,
        vec![
            doc! { "_id": "a", "name": "Expired", "ttl": past_ttl() },
            doc! { "_id": "b", "name": "Fresh", "ttl": future_ttl() },
            doc! { "_id": "c", "name": "Permanent" },
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    let purged = db.purge_expired(COLLECTION).unwrap();
    assert_eq!(purged, 1);

    // Expired doc is physically gone
    let mut txn = db.begin(true).unwrap();
    // Use a direct scan (count bypasses TTL filter, but purge actually deletes)
    let results = txn
        .find(COLLECTION, &no_filter_query())
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
    assert_eq!(names, vec!["Fresh", "Permanent"]);
}

#[test]
fn ttl_purge_skips_unexpired_docs() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
        COLLECTION,
        vec![
            doc! { "_id": "a", "name": "Fresh", "ttl": future_ttl() },
            doc! { "_id": "b", "name": "Permanent" },
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    let purged = db.purge_expired(COLLECTION).unwrap();
    assert_eq!(purged, 0);
}

#[test]
fn ttl_purge_cleans_user_indexes() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "purge_idx".to_string(),
        indexes: vec!["status".to_string()],
    })
    .unwrap();
    txn.insert_many(
        "purge_idx",
        vec![
            doc! { "_id": "a", "status": "active", "ttl": past_ttl() },
            doc! { "_id": "b", "status": "active", "ttl": future_ttl() },
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    let purged = db.purge_expired("purge_idx").unwrap();
    assert_eq!(purged, 1);

    // Index should only have one entry for "active" (doc "b")
    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", Bson::String("active".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("purge_idx", &query)
        .unwrap()
        .iter()
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

    let old_ttl = future_ttl();
    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "a", "name": "Doc", "ttl": old_ttl },
    )
    .unwrap();
    txn.commit().unwrap();

    // Update ttl to the past
    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("a".into()));
    txn.update_one(COLLECTION, &filter, doc! { "ttl": past_ttl() }, false)
        .unwrap();
    txn.commit().unwrap();

    // Purge should now delete the doc
    let purged = db.purge_expired(COLLECTION).unwrap();
    assert_eq!(purged, 1);

    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, &no_filter_query())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn ttl_purge_multiple_expired() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
        COLLECTION,
        vec![
            doc! { "_id": "a", "ttl": bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 120_000) },
            doc! { "_id": "b", "ttl": bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 60_000) },
            doc! { "_id": "c", "ttl": bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 1_000) },
            doc! { "_id": "d", "ttl": future_ttl() },
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    let purged = db.purge_expired(COLLECTION).unwrap();
    assert_eq!(purged, 3);

    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, &no_filter_query())
        .unwrap()
        .iter()
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

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "x", "name": "Expired", "ttl": past_ttl() },
    )
    .unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "y", "name": "Fresh", "ttl": future_ttl() },
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    assert!(txn.find_by_id(COLLECTION, "x", None).unwrap().is_none());
    assert!(txn.find_by_id(COLLECTION, "y", None).unwrap().is_some());
}

#[test]
fn ttl_update_skips_expired() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "a", "status": "old", "ttl": past_ttl() },
    )
    .unwrap();
    txn.commit().unwrap();

    // update_one should match 0 — expired doc is invisible
    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("a".into()));
    let result = txn
        .update_one(COLLECTION, &filter, doc! { "status": "new" }, false)
        .unwrap();
    assert_eq!(result.modified, 0);
}

#[test]
fn ttl_merge_into_expired_inserts_fresh() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "_id": "a", "old_field": true, "ttl": past_ttl() },
    )
    .unwrap();
    txn.commit().unwrap();

    // Merge with same _id — expired doc treated as non-existent, takes insert path
    let mut txn = db.begin(false).unwrap();
    let result = txn
        .merge_many(
            COLLECTION,
            vec![doc! { "_id": "a", "new_field": true, "ttl": future_ttl() }],
        )
        .unwrap();
    assert_eq!(result.inserted, 1);
    assert_eq!(result.updated, 0);
    txn.commit().unwrap();

    // The new doc should be visible and should NOT contain old_field
    let mut txn = db.begin(true).unwrap();
    let doc = txn.find_by_id(COLLECTION, "a", None).unwrap().unwrap();
    assert!(doc.get("new_field").is_some());
    assert!(doc.get("old_field").is_none());
}

#[test]
fn ttl_no_ttl_field_always_visible() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_many(
        COLLECTION,
        vec![
            doc! { "_id": "a", "name": "Permanent1" },
            doc! { "_id": "b", "name": "Permanent2" },
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    // Docs without ttl are always visible
    let mut txn = db.begin(true).unwrap();
    let results = txn
        .find(COLLECTION, &no_filter_query())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);

    // purge_expired should not touch them
    let purged = db.purge_expired(COLLECTION).unwrap();
    assert_eq!(purged, 0);

    let mut txn = db.begin(true).unwrap();
    let count = txn.count(COLLECTION, None).unwrap();
    assert_eq!(count, 2);
}

// ── Distinct tests ──────────────────────────────────────────────

#[test]
fn distinct_scalar_field() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "active" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "inactive" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "active" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = DistinctQuery {
        field: "status".into(),
        filter: None,
        sort: None,
        skip: None,
        take: None,
    };
    let values = to_bson_vec(txn.distinct(COLLECTION, &query).unwrap());
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::String("active".into())));
    assert!(values.contains(&Bson::String("inactive".into())));
}

#[test]
fn distinct_nested_path() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "address": { "city": "Austin" } })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "address": { "city": "Denver" } })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "address": { "city": "Austin" } })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = DistinctQuery {
        field: "address.city".into(),
        filter: None,
        sort: None,
        skip: None,
        take: None,
    };
    let values = to_bson_vec(txn.distinct(COLLECTION, &query).unwrap());
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::String("Austin".into())));
    assert!(values.contains(&Bson::String("Denver".into())));
}

#[test]
fn distinct_array_field() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "tags": ["rust", "db"] })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "tags": ["db", "perf"] })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = DistinctQuery {
        field: "tags".into(),
        filter: None,
        sort: None,
        skip: None,
        take: None,
    };
    let values = to_bson_vec(txn.distinct(COLLECTION, &query).unwrap());
    assert_eq!(values.len(), 3);
    assert!(values.contains(&Bson::String("rust".into())));
    assert!(values.contains(&Bson::String("db".into())));
    assert!(values.contains(&Bson::String("perf".into())));
}

#[test]
fn distinct_with_filter() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "active", "tier": "gold" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "inactive", "tier": "silver" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "active", "tier": "silver" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = DistinctQuery {
        field: "tier".into(),
        filter: Some(eq_filter("status", Bson::String("active".into()))),
        sort: None,
        skip: None,
        take: None,
    };
    let values = to_bson_vec(txn.distinct(COLLECTION, &query).unwrap());
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::String("gold".into())));
    assert!(values.contains(&Bson::String("silver".into())));
}

#[test]
fn distinct_with_sort_asc() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "cherry" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "apple" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "banana" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = DistinctQuery {
        field: "status".into(),
        filter: None,
        sort: Some(SortDirection::Asc),
        skip: None,
        take: None,
    };
    let values = to_bson_vec(txn.distinct(COLLECTION, &query).unwrap());
    assert_eq!(
        values,
        vec![
            Bson::String("apple".into()),
            Bson::String("banana".into()),
            Bson::String("cherry".into()),
        ]
    );
}

#[test]
fn distinct_with_sort_desc() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "cherry" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "apple" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "banana" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = DistinctQuery {
        field: "status".into(),
        filter: None,
        sort: Some(SortDirection::Desc),
        skip: None,
        take: None,
    };
    let values = to_bson_vec(txn.distinct(COLLECTION, &query).unwrap());
    assert_eq!(
        values,
        vec![
            Bson::String("cherry".into()),
            Bson::String("banana".into()),
            Bson::String("apple".into()),
        ]
    );
}

#[test]
fn distinct_missing_field() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "name": "alice" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "name": "bob" }).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = DistinctQuery {
        field: "nonexistent".into(),
        filter: None,
        sort: None,
        skip: None,
        take: None,
    };
    let values = to_bson_vec(txn.distinct(COLLECTION, &query).unwrap());
    assert!(values.is_empty());
}

#[test]
fn distinct_mixed_presence() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "active" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "name": "bob" }).unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "inactive" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = DistinctQuery {
        field: "status".into(),
        filter: None,
        sort: None,
        skip: None,
        take: None,
    };
    let values = to_bson_vec(txn.distinct(COLLECTION, &query).unwrap());
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::String("active".into())));
    assert!(values.contains(&Bson::String("inactive".into())));
}

#[test]
fn distinct_array_of_sub_documents() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
    })
    .unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "triggers": [{ "type": "email" }, { "type": "sms" }] },
    )
    .unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "triggers": [{ "type": "sms" }, { "type": "push" }] },
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = DistinctQuery {
        field: "triggers.type".into(),
        filter: None,
        sort: Some(SortDirection::Asc),
        skip: None,
        take: None,
    };
    let values = to_bson_vec(txn.distinct(COLLECTION, &query).unwrap());
    assert_eq!(
        values,
        vec![
            Bson::String("email".into()),
            Bson::String("push".into()),
            Bson::String("sms".into()),
        ]
    );
}

#[test]
fn distinct_sub_document() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
    })
    .unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "address": { "city": "Austin", "state": "TX" } },
    )
    .unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "address": { "city": "Denver", "state": "CO" } },
    )
    .unwrap();
    txn.insert_one(
        COLLECTION,
        doc! { "address": { "city": "Austin", "state": "TX" } },
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = DistinctQuery {
        field: "address".into(),
        filter: None,
        sort: None,
        skip: None,
        take: None,
    };
    let values = to_bson_vec(txn.distinct(COLLECTION, &query).unwrap());
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Bson::Document(doc! { "city": "Austin", "state": "TX" })));
    assert!(values.contains(&Bson::Document(doc! { "city": "Denver", "state": "CO" })));
}

#[test]
fn distinct_with_take() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "cherry" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "apple" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "banana" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "date" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = DistinctQuery {
        field: "status".into(),
        filter: None,
        sort: Some(SortDirection::Asc),
        skip: None,
        take: Some(2),
    };
    let values = to_bson_vec(txn.distinct(COLLECTION, &query).unwrap());
    assert_eq!(
        values,
        vec![Bson::String("apple".into()), Bson::String("banana".into()),]
    );
}

#[test]
fn distinct_with_skip_take() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "cherry" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "apple" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "banana" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "date" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = DistinctQuery {
        field: "status".into(),
        filter: None,
        sort: Some(SortDirection::Asc),
        skip: Some(1),
        take: Some(2),
    };
    let values = to_bson_vec(txn.distinct(COLLECTION, &query).unwrap());
    assert_eq!(
        values,
        vec![Bson::String("banana".into()), Bson::String("cherry".into()),]
    );
}

#[test]
fn distinct_with_sort_and_limit() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec![],
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "cherry" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "apple" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "banana" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "date" })
        .unwrap();
    txn.insert_one(COLLECTION, doc! { "status": "elderberry" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    // Sort desc, skip 1, take 2 → ["date", "cherry"]
    let query = DistinctQuery {
        field: "status".into(),
        filter: None,
        sort: Some(SortDirection::Desc),
        skip: Some(1),
        take: Some(2),
    };
    let values = to_bson_vec(txn.distinct(COLLECTION, &query).unwrap());
    assert_eq!(
        values,
        vec![Bson::String("date".into()), Bson::String("cherry".into()),]
    );
}

// ── Index-covered projection type preservation ─────────────────

#[test]
fn index_covered_preserves_int32_type() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLLECTION.to_string(),
        indexes: vec!["score".to_string()],
    })
    .unwrap();
    // Insert with Int32
    txn.insert_one(COLLECTION, doc! { "_id": "rec-1", "score": 100_i32 })
        .unwrap();
    txn.commit().unwrap();

    // Query with Int64 — same encoded bytes, different BSON type
    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("score", Bson::Int64(100))),
        sort: vec![],
        skip: None,
        take: None,
        columns: Some(vec!["score".into()]),
    };
    let results = txn
        .find(COLLECTION, &query)
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
    })
    .unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "rec-1", "status": "active" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(eq_filter("status", Bson::String("active".into()))),
        sort: vec![],
        skip: None,
        take: None,
        columns: Some(vec!["status".into()]),
    };
    let results = txn
        .find(COLLECTION, &query)
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

// ── Upsert Many ─────────────────────────────────────────────────

#[test]
fn upsert_many_inserts_new() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let mut txn = db.begin(false).unwrap();

    let docs = vec![
        doc! { "_id": "u1", "name": "Alice", "status": "active" },
        doc! { "_id": "u2", "name": "Bob", "status": "inactive" },
    ];
    let result = txn.upsert_many(COLLECTION, docs).unwrap();
    assert_eq!(result.inserted, 2);
    assert_eq!(result.updated, 0);

    let found = txn
        .find(COLLECTION, &no_filter_query())
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
    .unwrap();

    // Upsert replaces entirely
    let docs = vec![doc! { "_id": "u1", "name": "Alice Updated", "status": "inactive" }];
    let result = txn.upsert_many(COLLECTION, docs).unwrap();
    assert_eq!(result.inserted, 0);
    assert_eq!(result.updated, 1);

    let doc = txn.find_by_id(COLLECTION, "u1", None).unwrap().unwrap();
    assert_eq!(doc.get_str("name").unwrap(), "Alice Updated");
    assert_eq!(doc.get_str("status").unwrap(), "inactive");
    // score should be gone — full replace
    assert!(doc.get("score").is_none());
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
    .unwrap();

    let docs = vec![
        doc! { "_id": "u1", "name": "Alice v2", "status": "inactive" },
        doc! { "_id": "u2", "name": "Bob", "status": "active" },
    ];
    let result = txn.upsert_many(COLLECTION, docs).unwrap();
    assert_eq!(result.inserted, 1);
    assert_eq!(result.updated, 1);

    let found = txn
        .find(COLLECTION, &no_filter_query())
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
    .unwrap();

    // Verify index works before upsert
    let active = txn
        .find(
            COLLECTION,
            &Query {
                filter: Some(eq_filter("status", Bson::String("active".into()))),
                sort: vec![],
                skip: None,
                take: None,
                columns: None,
            },
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
    .unwrap();

    // Old index entry gone
    let active = txn
        .find(
            COLLECTION,
            &Query {
                filter: Some(eq_filter("status", Bson::String("active".into()))),
                sort: vec![],
                skip: None,
                take: None,
                columns: None,
            },
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
            &Query {
                filter: Some(eq_filter("status", Bson::String("inactive".into()))),
                sort: vec![],
                skip: None,
                take: None,
                columns: None,
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(inactive.len(), 1);
}

// ── Merge Many ──────────────────────────────────────────────────

#[test]
fn merge_many_inserts_new() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let mut txn = db.begin(false).unwrap();

    let docs = vec![
        doc! { "_id": "m1", "name": "Alice", "status": "active" },
        doc! { "_id": "m2", "name": "Bob", "status": "inactive" },
    ];
    let result = txn.merge_many(COLLECTION, docs).unwrap();
    assert_eq!(result.inserted, 2);
    assert_eq!(result.updated, 0);

    let found = txn
        .find(COLLECTION, &no_filter_query())
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(found.len(), 2);
}

#[test]
fn merge_many_merges_existing() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let mut txn = db.begin(false).unwrap();

    txn.insert_one(
        COLLECTION,
        doc! { "_id": "m1", "name": "Alice", "status": "active", "score": 100 },
    )
    .unwrap();

    // Merge only updates status — score should remain
    let docs = vec![doc! { "_id": "m1", "status": "inactive" }];
    let result = txn.merge_many(COLLECTION, docs).unwrap();
    assert_eq!(result.inserted, 0);
    assert_eq!(result.updated, 1);

    let doc = txn.find_by_id(COLLECTION, "m1", None).unwrap().unwrap();
    assert_eq!(doc.get_str("name").unwrap(), "Alice");
    assert_eq!(doc.get_str("status").unwrap(), "inactive");
    assert_eq!(doc.get_i32("score").unwrap(), 100);
}

#[test]
fn merge_many_index_maintenance() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let mut txn = db.begin(false).unwrap();
    txn.create_index(COLLECTION, "status").unwrap();

    txn.insert_one(
        COLLECTION,
        doc! { "_id": "m1", "name": "Alice", "status": "active" },
    )
    .unwrap();

    // Merge changes status
    txn.merge_many(COLLECTION, vec![doc! { "_id": "m1", "status": "inactive" }])
        .unwrap();

    // Old index entry gone
    let active = txn
        .find(
            COLLECTION,
            &Query {
                filter: Some(eq_filter("status", Bson::String("active".into()))),
                sort: vec![],
                skip: None,
                take: None,
                columns: None,
            },
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
            &Query {
                filter: Some(eq_filter("status", Bson::String("inactive".into()))),
                sort: vec![],
                skip: None,
                take: None,
                columns: None,
            },
        )
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(inactive.len(), 1);
}

#[test]
fn merge_many_unchanged_noop() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let mut txn = db.begin(false).unwrap();

    txn.insert_one(
        COLLECTION,
        doc! { "_id": "m1", "name": "Alice", "status": "active" },
    )
    .unwrap();

    // Merge with same values — updated count should still be 1 (we count the attempt, not actual changes)
    // But internally raw_merge_update returns false for no-op, so updated stays at 1 because merge_many
    // always increments updated when the record exists
    let docs = vec![doc! { "_id": "m1", "status": "active" }];
    let result = txn.merge_many(COLLECTION, docs).unwrap();
    assert_eq!(result.inserted, 0);
    assert_eq!(result.updated, 1);
}

#[test]
fn merge_many_adds_new_field() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let mut txn = db.begin(false).unwrap();

    txn.insert_one(COLLECTION, doc! { "_id": "m1", "name": "Alice" })
        .unwrap();

    // Merge adds a new field
    txn.merge_many(COLLECTION, vec![doc! { "_id": "m1", "status": "active" }])
        .unwrap();

    let doc = txn.find_by_id(COLLECTION, "m1", None).unwrap().unwrap();
    assert_eq!(doc.get_str("name").unwrap(), "Alice");
    assert_eq!(doc.get_str("status").unwrap(), "active");
}

#[test]
fn merge_many_mixed_insert_and_merge() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);
    let mut txn = db.begin(false).unwrap();

    txn.insert_one(
        COLLECTION,
        doc! { "_id": "m1", "name": "Alice", "status": "active" },
    )
    .unwrap();

    let docs = vec![
        doc! { "_id": "m1", "status": "inactive" }, // merge
        doc! { "_id": "m2", "name": "Bob", "status": "active" }, // insert
    ];
    let result = txn.merge_many(COLLECTION, docs).unwrap();
    assert_eq!(result.inserted, 1);
    assert_eq!(result.updated, 1);

    let m1 = txn.find_by_id(COLLECTION, "m1", None).unwrap().unwrap();
    assert_eq!(m1.get_str("name").unwrap(), "Alice"); // preserved
    assert_eq!(m1.get_str("status").unwrap(), "inactive"); // merged

    let m2 = txn.find_by_id(COLLECTION, "m2", None).unwrap().unwrap();
    assert_eq!(m2.get_str("name").unwrap(), "Bob");
}

// ── Range scan integration tests ────────────────────────────

#[test]
fn find_gt_on_indexed_field() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "scores".into(),
        indexes: vec!["score".to_string()],
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "score".into(),
                operator: Operator::Gt,
                value: Bson::Int32(75),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("scores", &query)
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    // score >= 70 AND score <= 80
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![
                FilterNode::Condition(Filter {
                    field: "score".into(),
                    operator: Operator::Gte,
                    value: Bson::Int32(70),
                }),
                FilterNode::Condition(Filter {
                    field: "score".into(),
                    operator: Operator::Lte,
                    value: Bson::Int32(80),
                }),
            ],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn
        .find("scores", &query)
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

// ── Mutation operator tests ─────────────────────────────────────

fn get_str_array(doc: &bson::Document, path: &str) -> Vec<String> {
    let segments: Vec<&str> = path.split('.').collect();
    let mut current = doc;
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$set": { "status": "archived", "score": 100 } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$unset": { "score": "" } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
    assert_eq!(d.get_str("name").unwrap(), "Alice");
    assert_eq!(d.get_str("status").unwrap(), "active");
    assert!(!d.contains_key("score"));
}

#[test]
fn mutation_inc_i32() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "score": 10_i32 })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$inc": { "score": 5_i32 } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
    assert_eq!(d.get_i32("score").unwrap(), 15);
}

#[test]
fn mutation_inc_missing_field() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "name": "Alice" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$inc": { "score": 7_i32 } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
    assert_eq!(d.get_i32("score").unwrap(), 7);
}

#[test]
fn mutation_inc_negative_decrement() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "score": 100_i32 })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$inc": { "score": -30_i32 } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
    assert_eq!(d.get_i32("score").unwrap(), 70);
}

#[test]
fn mutation_inc_f64() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "balance": 100.50_f64 })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$inc": { "balance": 25.25_f64 } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$rename": { "old_name": "name" } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
    assert_eq!(d.get_str("name").unwrap(), "Alice");
    assert!(!d.contains_key("old_name"));
    assert_eq!(d.get_str("status").unwrap(), "active");
}

#[test]
fn mutation_push() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "tags": ["rust", "db"] })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$push": { "tags": "perf" } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
    assert_eq!(get_str_array(&d, "tags"), vec!["rust", "db", "perf"]);
}

#[test]
fn mutation_push_creates_array() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "name": "Alice" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$push": { "tags": "new" } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$lpush": { "queue": "first" } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
    assert_eq!(get_str_array(&d, "queue"), vec!["first", "second", "third"]);
}

#[test]
fn mutation_pop() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "stack": ["a", "b", "c"] })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "$pop": { "stack": 1 } }, false)
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! {
            "$set": { "name": "Alice Updated" },
            "$inc": { "score": 5_i32 },
            "$push": { "tags": "b" },
        },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "status": "archived", "score": 99 },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$set": { "address.city": "Denver" } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$inc": { "stats.views": 1_i32 } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
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
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$set": { "address.city": "Austin" } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$unset": { "address.zip": "" } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
    let addr = d.get_document("address").unwrap();
    assert_eq!(addr.get_str("city").unwrap(), "Austin");
    assert_eq!(addr.get_str("state").unwrap(), "TX");
    assert!(!addr.contains_key("zip"));
}

#[test]
fn mutation_dot_path_push() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "data": { "items": ["a"] } })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$push": { "data.items": "b" } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
    assert_eq!(get_str_array(&d, "data.items"), vec!["a", "b"]);
}

#[test]
fn mutation_update_many_with_inc() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("status", Bson::String("active".into()));
    let result = txn
        .update_many(
            COLLECTION,
            &filter,
            doc! { "$inc": { "revenue": 1000.0_f64 } },
        )
        .unwrap();
    assert_eq!(result.matched, 3);
    assert_eq!(result.modified, 3);
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "acct-1", None).unwrap().unwrap();
    assert!((d.get_f64("revenue").unwrap() - 51000.0).abs() < f64::EPSILON);

    let d = txn.find_by_id(COLLECTION, "acct-4", None).unwrap().unwrap();
    assert!((d.get_f64("revenue").unwrap() - 96000.0).abs() < f64::EPSILON);

    let d = txn.find_by_id(COLLECTION, "acct-5", None).unwrap().unwrap();
    assert!((d.get_f64("revenue").unwrap() - 201000.0).abs() < f64::EPSILON);

    let d = txn.find_by_id(COLLECTION, "acct-2", None).unwrap().unwrap();
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        "idx_mut",
        &filter,
        doc! { "$set": { "status": "archived" } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let q = Query {
        filter: Some(eq_filter("status", Bson::String("active".into()))),
        ..no_filter_query()
    };
    let results = txn
        .find("idx_mut", &q)
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);

    let q = Query {
        filter: Some(eq_filter("status", Bson::String("archived".into()))),
        ..no_filter_query()
    };
    let results = txn
        .find("idx_mut", &q)
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
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(
        "idx_unset",
        &filter,
        doc! { "$unset": { "status": "" } },
        false,
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let q = Query {
        filter: Some(eq_filter("status", Bson::String("active".into()))),
        ..no_filter_query()
    };
    let results = txn
        .find("idx_unset", &q)
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);

    let d = txn.find_by_id("idx_unset", "r1", None).unwrap().unwrap();
    assert_eq!(d.get_str("name").unwrap(), "Alice");
    assert!(!d.contains_key("status"));
}

#[test]
fn mutation_push_pop_as_stack() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "name": "stack" })
        .unwrap();
    txn.commit().unwrap();

    for val in ["a", "b", "c"] {
        let mut txn = db.begin(false).unwrap();
        let filter = eq_filter("_id", Bson::String("r1".into()));
        txn.update_one(
            COLLECTION,
            &filter,
            doc! { "$push": { "items": val } },
            false,
        )
        .unwrap();
        txn.commit().unwrap();
    }

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
    assert_eq!(get_str_array(&d, "items"), vec!["a", "b", "c"]);

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "$pop": { "items": 1 } }, false)
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
    assert_eq!(get_str_array(&d, "items"), vec!["a", "b"]);
}

#[test]
fn mutation_lpush_pop_as_queue() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "name": "queue" })
        .unwrap();
    txn.commit().unwrap();

    for val in ["first", "second", "third"] {
        let mut txn = db.begin(false).unwrap();
        let filter = eq_filter("_id", Bson::String("r1".into()));
        txn.update_one(
            COLLECTION,
            &filter,
            doc! { "$lpush": { "items": val } },
            false,
        )
        .unwrap();
        txn.commit().unwrap();
    }

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
    assert_eq!(get_str_array(&d, "items"), vec!["third", "second", "first"]);

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    txn.update_one(COLLECTION, &filter, doc! { "$pop": { "items": 1 } }, false)
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(true).unwrap();
    let d = txn.find_by_id(COLLECTION, "r1", None).unwrap().unwrap();
    assert_eq!(get_str_array(&d, "items"), vec!["third", "second"]);
}

#[test]
fn mutation_unknown_operator_rejected() {
    let (db, _dir) = temp_db();
    create_collection(&db, COLLECTION);

    let mut txn = db.begin(false).unwrap();
    txn.insert_one(COLLECTION, doc! { "_id": "r1", "name": "Alice" })
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    let result = txn.update_one(
        COLLECTION,
        &filter,
        doc! { "$badop": { "name": "Bob" } },
        false,
    );
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
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
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("r1".into()));
    let result = txn.update_one(COLLECTION, &filter, doc! { "$set": { "_id": "r2" } }, false);
    assert!(result.is_err());
}
