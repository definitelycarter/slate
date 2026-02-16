use bson::doc;
use slate_db::{Database, Datasource, FieldDef, FieldType};
use slate_query::{
    Filter, FilterGroup, FilterNode, LogicalOp, Operator, Query, QueryValue, Sort, SortDirection,
};
use slate_store::RocksStore;

const DS_ID: &str = "ds1";
const PARTITION: &str = "test_part";

fn temp_db() -> (Database<RocksStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();
    let db = Database::new(store);
    (db, dir)
}

fn make_datasource() -> Datasource {
    Datasource {
        id: DS_ID.to_string(),
        name: "SBA".to_string(),
        fields: vec![
            FieldDef {
                name: "name".to_string(),
                field_type: FieldType::String,

                indexed: false,
            },
            FieldDef {
                name: "revenue".to_string(),
                field_type: FieldType::Float,

                indexed: false,
            },
            FieldDef {
                name: "status".to_string(),
                field_type: FieldType::String,

                indexed: false,
            },
            FieldDef {
                name: "active".to_string(),
                field_type: FieldType::Bool,

                indexed: false,
            },
        ],
        partition: PARTITION.to_string(),
    }
}

fn make_doc(name: &str, revenue: f64, status: &str, active: bool) -> bson::Document {
    doc! {
        "name": name,
        "revenue": revenue,
        "status": status,
        "active": active,
    }
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

/// Save the datasource and seed 5 records.
fn seed_records(db: &Database<RocksStore>) {
    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    txn.write_record(
        DS_ID,
        "acct-1",
        make_doc("Acme Corp", 50000.0, "active", true),
    )
    .unwrap();
    txn.write_record(
        DS_ID,
        "acct-2",
        make_doc("Globex", 80000.0, "snoozed", true),
    )
    .unwrap();
    txn.write_record(
        DS_ID,
        "acct-3",
        make_doc("Initech", 12000.0, "rejected", false),
    )
    .unwrap();
    txn.write_record(
        DS_ID,
        "acct-4",
        make_doc("Umbrella", 95000.0, "active", true),
    )
    .unwrap();
    txn.write_record(
        DS_ID,
        "acct-5",
        make_doc("Stark Industries", 200000.0, "active", false),
    )
    .unwrap();
    txn.commit().unwrap();
}

// ── Catalog tests ───────────────────────────────────────────────

#[test]
fn catalog_save_and_get() {
    let (db, _dir) = temp_db();
    let ds = make_datasource();
    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&ds).unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let result = txn.get_datasource(DS_ID).unwrap().unwrap();
    assert_eq!(result.id, DS_ID);
    assert_eq!(result.name, "SBA");
    assert_eq!(result.fields.len(), 4);
    assert_eq!(result.partition, PARTITION);
}

#[test]
fn catalog_get_not_found() {
    let (db, _dir) = temp_db();
    let txn = db.begin(true).unwrap();
    assert!(txn.get_datasource("nonexistent").unwrap().is_none());
}

#[test]
fn catalog_list() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&Datasource {
        id: "ds1".into(),
        name: "SBA".into(),
        fields: vec![],
        partition: "a".into(),
        ..make_empty_ds("ds1")
    })
    .unwrap();
    txn.save_datasource(&make_empty_ds("ds2")).unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let result = txn.list_datasources().unwrap();
    assert_eq!(result.len(), 2);
}

#[test]
fn catalog_delete() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    txn.delete_datasource(DS_ID).unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    assert!(txn.get_datasource(DS_ID).unwrap().is_none());
}

fn make_empty_ds(id: &str) -> Datasource {
    Datasource {
        id: id.into(),
        name: id.into(),
        fields: vec![],
        partition: format!("part_{id}"),
    }
}

// ── Write + Read (get_by_id) ────────────────────────────────────

#[test]
fn write_and_get_by_id() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    txn.write_record(DS_ID, "acct-1", make_doc("Acme", 50000.0, "active", true))
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let record = txn.get_by_id(DS_ID, "acct-1", None).unwrap().unwrap();
    assert_eq!(record.get_str("_id").unwrap(), "acct-1");
    assert_eq!(record.get_str("name").unwrap(), "Acme");
    assert_eq!(record.get_f64("revenue").unwrap(), 50000.0);
}

#[test]
fn get_by_id_not_found() {
    let (db, _dir) = temp_db();
    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    assert!(txn.get_by_id(DS_ID, "nonexistent", None).unwrap().is_none());
}

#[test]
fn get_by_id_with_projection() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    txn.write_record(DS_ID, "acct-1", make_doc("Acme", 50000.0, "active", true))
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let record = txn
        .get_by_id(DS_ID, "acct-1", Some(&["name", "status"]))
        .unwrap()
        .unwrap();
    // _id + name + status = 3 keys
    assert_eq!(record.len(), 3);
    assert!(record.contains_key("name"));
    assert!(record.contains_key("status"));
    assert!(!record.contains_key("revenue"));
}

// ── Partial writes ──────────────────────────────────────────────

#[test]
fn partial_write_updates_single_column() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    txn.write_record(DS_ID, "acct-1", make_doc("Acme", 50000.0, "active", true))
        .unwrap();
    txn.commit().unwrap();

    // Partial write: update only status
    let mut txn = db.begin(false).unwrap();
    txn.write_record(DS_ID, "acct-1", doc! { "status": "rejected" })
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let record = txn.get_by_id(DS_ID, "acct-1", None).unwrap().unwrap();
    assert_eq!(record.get_str("status").unwrap(), "rejected");
    // Other fields unchanged
    assert_eq!(record.get_str("name").unwrap(), "Acme");
}

#[test]
fn last_write_wins() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    // First write
    txn.write_record(DS_ID, "acct-1", doc! { "name": "Old Name" })
        .unwrap();
    // Second write overwrites the field
    txn.write_record(DS_ID, "acct-1", doc! { "name": "New Name" })
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let record = txn.get_by_id(DS_ID, "acct-1", None).unwrap().unwrap();
    assert_eq!(record.get_str("name").unwrap(), "New Name");
}

// ── Delete ──────────────────────────────────────────────────────

#[test]
fn delete_record_removes_all_cells() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    txn.write_record(DS_ID, "acct-1", make_doc("Acme", 50000.0, "active", true))
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    txn.delete_record(DS_ID, "acct-1").unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    assert!(txn.get_by_id(DS_ID, "acct-1", None).unwrap().is_none());
}

// ── Write batch ─────────────────────────────────────────────────

#[test]
fn write_batch_multiple_records() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    txn.write_batch(
        DS_ID,
        vec![
            ("acct-1".into(), make_doc("Acme", 50000.0, "active", true)),
            ("acct-2".into(), make_doc("Globex", 80000.0, "active", true)),
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    assert!(txn.get_by_id(DS_ID, "acct-1", None).unwrap().is_some());
    assert!(txn.get_by_id(DS_ID, "acct-2", None).unwrap().is_some());
}

// ── Query tests ─────────────────────────────────────────────────

#[test]
fn query_no_filters() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let results = txn.query(DS_ID, &no_filter_query()).unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn query_eq_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "status".into(),
                operator: Operator::Eq,
                value: QueryValue::String("active".into()),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.query(DS_ID, &query).unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn query_gt_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
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
    let results = txn.query(DS_ID, &query).unwrap();
    assert_eq!(results.len(), 2); // Umbrella (95k) and Stark (200k)
}

#[test]
fn query_isnull_filter() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();

    // Record without "status" column
    txn.write_record(DS_ID, "acct-x", doc! { "name": "NoStatus" })
        .unwrap();
    txn.write_record(DS_ID, "acct-1", make_doc("Acme", 50000.0, "active", true))
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
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
    let results = txn.query(DS_ID, &query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-x");
}

#[test]
fn query_or_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
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
    let results = txn.query(DS_ID, &query).unwrap();
    assert_eq!(results.len(), 2);
}

// ── Sort tests ──────────────────────────────────────────────────

#[test]
fn query_sort_asc() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
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
    let results = txn.query(DS_ID, &query).unwrap();
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-3"); // Initech 12k
    assert_eq!(results[4].get_str("_id").unwrap(), "acct-5"); // Stark 200k
}

#[test]
fn query_sort_desc() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
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
    let results = txn.query(DS_ID, &query).unwrap();
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-5"); // Stark 200k
    assert_eq!(results[4].get_str("_id").unwrap(), "acct-3"); // Initech 12k
}

// ── Pagination ──────────────────────────────────────────────────

#[test]
fn query_skip_and_take() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
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
    let results = txn.query(DS_ID, &query).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-1"); // Acme 50k (skipped Initech 12k)
    assert_eq!(results[1].get_str("_id").unwrap(), "acct-2"); // Globex 80k
}

#[test]
fn query_filter_sort_paginate() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "status".into(),
                operator: Operator::Eq,
                value: QueryValue::String("active".into()),
            })],
        }),
        sort: vec![Sort {
            field: "revenue".into(),
            direction: SortDirection::Desc,
        }],
        skip: Some(1),
        take: Some(1),
        columns: None,
    };
    let results = txn.query(DS_ID, &query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-4"); // Umbrella 95k
}

// ── Projection in query ─────────────────────────────────────────

#[test]
fn query_with_projection() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: Some(vec!["name".into(), "status".into()]),
    };
    let results = txn.query(DS_ID, &query).unwrap();
    assert_eq!(results.len(), 5);
    for record in &results {
        assert!(record.contains_key("name"));
        assert!(record.contains_key("status"));
        assert!(!record.contains_key("revenue"));
        assert!(!record.contains_key("active"));
    }
}

#[test]
fn query_projection_includes_filter_columns() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "status".into(),
                operator: Operator::Eq,
                value: QueryValue::String("active".into()),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: Some(vec!["name".into()]),
    };
    let results = txn.query(DS_ID, &query).unwrap();
    assert_eq!(results.len(), 3);
    for record in &results {
        assert!(record.contains_key("name"));
        assert!(!record.contains_key("status")); // filter col stripped
    }
}

#[test]
fn query_projection_includes_sort_columns() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
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
    let results = txn.query(DS_ID, &query).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get_str("_id").unwrap(), "acct-5"); // Stark 200k
    assert_eq!(results[1].get_str("_id").unwrap(), "acct-4"); // Umbrella 95k
    for record in &results {
        assert!(record.contains_key("name"));
        assert!(!record.contains_key("revenue")); // sort col stripped
    }
}

// ── Datasource isolation ────────────────────────────────────────

#[test]
fn datasource_isolation() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    // Two datasources in different partitions
    txn.save_datasource(&Datasource {
        id: "contacts".into(),
        name: "Contacts".into(),
        fields: vec![FieldDef {
            name: "name".into(),
            field_type: FieldType::String,
            indexed: false,
        }],
        partition: "contacts_part".into(),
    })
    .unwrap();
    txn.save_datasource(&Datasource {
        id: "accounts".into(),
        name: "Accounts".into(),
        fields: vec![FieldDef {
            name: "name".into(),
            field_type: FieldType::String,
            indexed: false,
        }],
        partition: "accounts_part".into(),
    })
    .unwrap();

    txn.write_record("contacts", "c-1", doc! { "name": "Alice" })
        .unwrap();
    txn.write_record("accounts", "a-1", doc! { "name": "Acme" })
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let contacts = txn.query("contacts", &no_filter_query()).unwrap();
    assert_eq!(contacts.len(), 1);
    assert_eq!(contacts[0].get_str("name").unwrap(), "Alice");

    let accounts = txn.query("accounts", &no_filter_query()).unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].get_str("name").unwrap(), "Acme");
}

// ── Catalog + data coexist ──────────────────────────────────────

#[test]
fn catalog_and_data_coexist() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    txn.write_record(DS_ID, "acct-1", make_doc("Acme", 50000.0, "active", true))
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    assert_eq!(txn.list_datasources().unwrap().len(), 1);
    let records = txn.query(DS_ID, &no_filter_query()).unwrap();
    assert_eq!(records.len(), 1);
}

#[test]
fn query_index_scan_on_indexed_field() {
    let (db, _dir) = temp_db();

    // Create datasource with status indexed
    let ds = Datasource {
        id: DS_ID.to_string(),
        name: "Test".to_string(),
        fields: vec![
            FieldDef {
                name: "name".to_string(),
                field_type: FieldType::String,

                indexed: false,
            },
            FieldDef {
                name: "status".to_string(),
                field_type: FieldType::String,

                indexed: true,
            },
        ],
        partition: PARTITION.to_string(),
    };

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&ds).unwrap();
    txn.write_record(DS_ID, "r1", doc! { "name": "Alice", "status": "active" })
        .unwrap();
    txn.write_record(DS_ID, "r2", doc! { "name": "Bob", "status": "rejected" })
        .unwrap();
    txn.write_record(DS_ID, "r3", doc! { "name": "Charlie", "status": "active" })
        .unwrap();
    txn.commit().unwrap();

    // Query with Eq on indexed field — planner should produce IndexScan
    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "status".into(),
                operator: Operator::Eq,
                value: QueryValue::String("active".into()),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.query(DS_ID, &query).unwrap();
    assert_eq!(results.len(), 2);

    let mut names: Vec<_> = results
        .iter()
        .map(|r| r.get_str("name").unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names[0], "Alice");
    assert_eq!(names[1], "Charlie");
}

// ── Nested documents + dot-notation ─────────────────────────────

fn make_nested_datasource() -> Datasource {
    Datasource {
        id: "nested_ds".to_string(),
        name: "Nested".to_string(),
        fields: vec![
            FieldDef {
                name: "name".to_string(),
                field_type: FieldType::String,
                indexed: false,
            },
            FieldDef {
                name: "address".to_string(),
                field_type: FieldType::String, // FieldType doesn't matter for nested docs
                indexed: false,
            },
        ],
        partition: "nested_part".to_string(),
    }
}

#[test]
fn nested_doc_write_and_read() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_nested_datasource()).unwrap();
    txn.write_record(
        "nested_ds",
        "r1",
        doc! {
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

    let txn = db.begin(true).unwrap();
    let record = txn.get_by_id("nested_ds", "r1", None).unwrap().unwrap();
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
    txn.save_datasource(&make_nested_datasource()).unwrap();
    txn.write_record(
        "nested_ds",
        "r1",
        doc! { "name": "Alice", "address": { "city": "Austin", "state": "TX" } },
    )
    .unwrap();
    txn.write_record(
        "nested_ds",
        "r2",
        doc! { "name": "Bob", "address": { "city": "Denver", "state": "CO" } },
    )
    .unwrap();
    txn.write_record(
        "nested_ds",
        "r3",
        doc! { "name": "Charlie", "address": { "city": "Austin", "state": "TX" } },
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "address.city".into(),
                operator: Operator::Eq,
                value: QueryValue::String("Austin".into()),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.query("nested_ds", &query).unwrap();
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
    txn.save_datasource(&make_nested_datasource()).unwrap();
    txn.write_record(
        "nested_ds",
        "r1",
        doc! { "name": "Alice", "address": { "city": "Zurich" } },
    )
    .unwrap();
    txn.write_record(
        "nested_ds",
        "r2",
        doc! { "name": "Bob", "address": { "city": "Austin" } },
    )
    .unwrap();
    txn.write_record(
        "nested_ds",
        "r3",
        doc! { "name": "Charlie", "address": { "city": "Denver" } },
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
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
    let results = txn.query("nested_ds", &query).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get_str("name").unwrap(), "Bob"); // Austin
    assert_eq!(results[1].get_str("name").unwrap(), "Charlie"); // Denver
    assert_eq!(results[2].get_str("name").unwrap(), "Alice"); // Zurich
}

#[test]
fn dot_notation_projection() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_nested_datasource()).unwrap();
    txn.write_record(
        "nested_ds",
        "r1",
        doc! {
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

    // Project only name and address.city
    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: Some(vec!["name".into(), "address.city".into()]),
    };
    let results = txn.query("nested_ds", &query).unwrap();
    assert_eq!(results.len(), 1);
    let record = &results[0];
    assert_eq!(record.get_str("name").unwrap(), "Alice");
    // address should only contain city, not state or zip
    let addr = record.get_document("address").unwrap();
    assert_eq!(addr.get_str("city").unwrap(), "Austin");
    assert!(!addr.contains_key("state"));
    assert!(!addr.contains_key("zip"));
}

#[test]
fn dot_notation_projection_multiple_subfields() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_nested_datasource()).unwrap();
    txn.write_record(
        "nested_ds",
        "r1",
        doc! {
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

    // Project address.city and address.zip (not state)
    let txn = db.begin(true).unwrap();
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
    let results = txn.query("nested_ds", &query).unwrap();
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
    txn.save_datasource(&make_nested_datasource()).unwrap();
    // Record with address
    txn.write_record(
        "nested_ds",
        "r1",
        doc! { "name": "Alice", "address": { "city": "Austin" } },
    )
    .unwrap();
    // Record without address at all
    txn.write_record("nested_ds", "r2", doc! { "name": "Bob" })
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
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
    let results = txn.query("nested_ds", &query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "r2");
}

#[test]
fn dot_notation_deep_nesting() {
    let (db, _dir) = temp_db();

    let ds = Datasource {
        id: "deep_ds".to_string(),
        name: "Deep".to_string(),
        fields: vec![FieldDef {
            name: "data".to_string(),
            field_type: FieldType::String,
            indexed: false,
        }],
        partition: "deep_part".to_string(),
    };

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&ds).unwrap();
    txn.write_record(
        "deep_ds",
        "r1",
        doc! { "data": { "level1": { "level2": { "value": "found" } } } },
    )
    .unwrap();
    txn.commit().unwrap();

    // Filter on 3-level deep path
    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "data.level1.level2.value".into(),
                operator: Operator::Eq,
                value: QueryValue::String("found".into()),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.query("deep_ds", &query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("_id").unwrap(), "r1");
}

#[test]
fn dot_notation_get_by_id_projection() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_nested_datasource()).unwrap();
    txn.write_record(
        "nested_ds",
        "r1",
        doc! {
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

    let txn = db.begin(true).unwrap();
    let record = txn
        .get_by_id("nested_ds", "r1", Some(&["name", "address.city"]))
        .unwrap()
        .unwrap();
    assert_eq!(record.get_str("name").unwrap(), "Alice");
    let addr = record.get_document("address").unwrap();
    assert_eq!(addr.get_str("city").unwrap(), "Austin");
    assert!(!addr.contains_key("state"));
    assert!(!addr.contains_key("zip"));
}

#[test]
fn projection_only_uses_selective_read() {
    // Projection without filter or sort — scan should use selective materialization
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: Some(vec!["name".into()]),
    };
    let results = txn.query(DS_ID, &query).unwrap();
    assert_eq!(results.len(), 5);
    for record in &results {
        assert!(record.contains_key("_id"));
        assert!(record.contains_key("name"));
        assert!(!record.contains_key("revenue"));
        assert!(!record.contains_key("status"));
        assert!(!record.contains_key("active"));
    }
}
