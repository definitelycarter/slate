use slate_db::{CellWrite, Database, Datasource, FieldDef, FieldType, Value};
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
                ttl_seconds: None,
                indexed: false,
            },
            FieldDef {
                name: "revenue".to_string(),
                field_type: FieldType::Float,
                ttl_seconds: None,
                indexed: false,
            },
            FieldDef {
                name: "status".to_string(),
                field_type: FieldType::String,
                ttl_seconds: None,
                indexed: false,
            },
            FieldDef {
                name: "active".to_string(),
                field_type: FieldType::Bool,
                ttl_seconds: None,
                indexed: false,
            },
        ],
        partition: PARTITION.to_string(),
    }
}

fn make_cells(name: &str, revenue: f64, status: &str, active: bool) -> Vec<CellWrite> {
    vec![
        CellWrite {
            column: "name".into(),
            value: Value::String(name.into()),
        },
        CellWrite {
            column: "revenue".into(),
            value: Value::Float(revenue),
        },
        CellWrite {
            column: "status".into(),
            value: Value::String(status.into()),
        },
        CellWrite {
            column: "active".into(),
            value: Value::Bool(active),
        },
    ]
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
        make_cells("Acme Corp", 50000.0, "active", true),
    )
    .unwrap();
    txn.write_record(
        DS_ID,
        "acct-2",
        make_cells("Globex", 80000.0, "snoozed", true),
    )
    .unwrap();
    txn.write_record(
        DS_ID,
        "acct-3",
        make_cells("Initech", 12000.0, "rejected", false),
    )
    .unwrap();
    txn.write_record(
        DS_ID,
        "acct-4",
        make_cells("Umbrella", 95000.0, "active", true),
    )
    .unwrap();
    txn.write_record(
        DS_ID,
        "acct-5",
        make_cells("Stark Industries", 200000.0, "active", false),
    )
    .unwrap();
    txn.commit().unwrap();
}

fn cell_value<'a>(record: &'a slate_db::Record, col: &str) -> Option<&'a Value> {
    record.cells.get(col).map(|c| &c.value)
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
    txn.write_record(DS_ID, "acct-1", make_cells("Acme", 50000.0, "active", true))
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let record = txn.get_by_id(DS_ID, "acct-1", None).unwrap().unwrap();
    assert_eq!(record.id, "acct-1");
    assert_eq!(
        cell_value(&record, "name"),
        Some(&Value::String("Acme".into()))
    );
    assert_eq!(cell_value(&record, "revenue"), Some(&Value::Float(50000.0)));
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
    txn.write_record(DS_ID, "acct-1", make_cells("Acme", 50000.0, "active", true))
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let record = txn
        .get_by_id(DS_ID, "acct-1", Some(&["name", "status"]))
        .unwrap()
        .unwrap();
    assert_eq!(record.cells.len(), 2);
    assert!(record.cells.contains_key("name"));
    assert!(record.cells.contains_key("status"));
    assert!(!record.cells.contains_key("revenue"));
}

// ── Partial writes ──────────────────────────────────────────────

#[test]
fn partial_write_updates_single_column() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    txn.write_record(DS_ID, "acct-1", make_cells("Acme", 50000.0, "active", true))
        .unwrap();
    txn.commit().unwrap();

    // Partial write: update only status
    let mut txn = db.begin(false).unwrap();
    txn.write_record(
        DS_ID,
        "acct-1",
        vec![CellWrite {
            column: "status".into(),
            value: Value::String("rejected".into()),
        }],
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let record = txn.get_by_id(DS_ID, "acct-1", None).unwrap().unwrap();
    assert_eq!(
        cell_value(&record, "status"),
        Some(&Value::String("rejected".into()))
    );
    // Other fields unchanged
    assert_eq!(
        cell_value(&record, "name"),
        Some(&Value::String("Acme".into()))
    );
}

#[test]
fn last_write_wins() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    txn.write_record(
        DS_ID,
        "acct-1",
        vec![
            CellWrite {
                column: "name".into(),
                value: Value::String("Old Name".into()),
            },
            CellWrite {
                column: "name".into(),
                value: Value::String("New Name".into()),
            },
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let record = txn.get_by_id(DS_ID, "acct-1", None).unwrap().unwrap();
    assert_eq!(
        cell_value(&record, "name"),
        Some(&Value::String("New Name".into()))
    );
}

// ── TTL / Expiry ────────────────────────────────────────────────

#[test]
fn ttl_derives_expire_at_from_field_config() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&Datasource {
        id: "ds_ttl".into(),
        name: "TTL Test".into(),
        fields: vec![
            FieldDef {
                name: "name".into(),
                field_type: FieldType::String,
                ttl_seconds: None, // no TTL
                indexed: false,
            },
            FieldDef {
                name: "status".into(),
                field_type: FieldType::String,
                ttl_seconds: Some(3600), // 1 hour
                indexed: false,
            },
        ],
        partition: "ttl_part".into(),
    })
    .unwrap();
    txn.write_record(
        "ds_ttl",
        "acct-1",
        vec![
            CellWrite {
                column: "name".into(),
                value: Value::String("Acme".into()),
            },
            CellWrite {
                column: "status".into(),
                value: Value::String("active".into()),
            },
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let record = txn.get_by_id("ds_ttl", "acct-1", None).unwrap().unwrap();
    // Both cells should be present — status has TTL but was just written so not expired
    assert_eq!(
        cell_value(&record, "name"),
        Some(&Value::String("Acme".into()))
    );
    assert_eq!(
        cell_value(&record, "status"),
        Some(&Value::String("active".into()))
    );
}

#[test]
fn expired_id_hides_record() {
    let (db, _dir) = temp_db();

    // Datasource with _id TTL of 0 seconds — record expires immediately
    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&Datasource {
        id: "ds_expired".into(),
        name: "Expired".into(),
        fields: vec![
            FieldDef {
                name: "_id".into(),
                field_type: FieldType::Bool,
                ttl_seconds: Some(0), // immediate expiry
                indexed: false,
            },
            FieldDef {
                name: "name".into(),
                field_type: FieldType::String,
                ttl_seconds: None,
                indexed: false,
            },
        ],
        partition: "exp_part".into(),
    })
    .unwrap();
    txn.write_record(
        "ds_expired",
        "acct-1",
        vec![CellWrite {
            column: "name".into(),
            value: Value::String("Acme".into()),
        }],
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    // Record should be hidden because _id has expire_at = now (TTL 0 → immediately expired)
    assert!(
        txn.get_by_id("ds_expired", "acct-1", None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn expired_column_hidden_from_record() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&Datasource {
        id: "ds_exp_col".into(),
        name: "Exp Col".into(),
        fields: vec![
            FieldDef {
                name: "name".into(),
                field_type: FieldType::String,
                ttl_seconds: None,
                indexed: false,
            },
            FieldDef {
                name: "status".into(),
                field_type: FieldType::String,
                ttl_seconds: Some(0), // immediate expiry
                indexed: false,
            },
        ],
        partition: "exp_col_part".into(),
    })
    .unwrap();
    txn.write_record(
        "ds_exp_col",
        "acct-1",
        vec![
            CellWrite {
                column: "name".into(),
                value: Value::String("Acme".into()),
            },
            CellWrite {
                column: "status".into(),
                value: Value::String("active".into()),
            },
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let record = txn
        .get_by_id("ds_exp_col", "acct-1", None)
        .unwrap()
        .unwrap();
    // The expired column should be filtered out (TTL 0 → immediately expired)
    assert!(!record.cells.contains_key("status"));
    // Non-expired column still present
    assert_eq!(
        cell_value(&record, "name"),
        Some(&Value::String("Acme".into()))
    );
}

// ── Delete ──────────────────────────────────────────────────────

#[test]
fn delete_record_removes_all_cells() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    txn.write_record(DS_ID, "acct-1", make_cells("Acme", 50000.0, "active", true))
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
            ("acct-1".into(), make_cells("Acme", 50000.0, "active", true)),
            (
                "acct-2".into(),
                make_cells("Globex", 80000.0, "active", true),
            ),
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
    txn.write_record(
        DS_ID,
        "acct-x",
        vec![CellWrite {
            column: "name".into(),
            value: Value::String("NoStatus".into()),
        }],
    )
    .unwrap();
    txn.write_record(DS_ID, "acct-1", make_cells("Acme", 50000.0, "active", true))
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
    assert_eq!(results[0].id, "acct-x");
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
    assert_eq!(results[0].id, "acct-3"); // Initech 12k
    assert_eq!(results[4].id, "acct-5"); // Stark 200k
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
    assert_eq!(results[0].id, "acct-5"); // Stark 200k
    assert_eq!(results[4].id, "acct-3"); // Initech 12k
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
    assert_eq!(results[0].id, "acct-1"); // Acme 50k (skipped Initech 12k)
    assert_eq!(results[1].id, "acct-2"); // Globex 80k
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
    assert_eq!(results[0].id, "acct-4"); // Umbrella 95k
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
        assert!(record.cells.contains_key("name"));
        assert!(record.cells.contains_key("status"));
        assert!(!record.cells.contains_key("revenue"));
        assert!(!record.cells.contains_key("active"));
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
        assert!(record.cells.contains_key("name"));
        assert!(!record.cells.contains_key("status")); // filter col stripped
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
    assert_eq!(results[0].id, "acct-5"); // Stark 200k
    assert_eq!(results[1].id, "acct-4"); // Umbrella 95k
    for record in &results {
        assert!(record.cells.contains_key("name"));
        assert!(!record.cells.contains_key("revenue")); // sort col stripped
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
            ttl_seconds: None,
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
            ttl_seconds: None,
            indexed: false,
        }],
        partition: "accounts_part".into(),
    })
    .unwrap();

    txn.write_record(
        "contacts",
        "c-1",
        vec![CellWrite {
            column: "name".into(),
            value: Value::String("Alice".into()),
        }],
    )
    .unwrap();
    txn.write_record(
        "accounts",
        "a-1",
        vec![CellWrite {
            column: "name".into(),
            value: Value::String("Acme".into()),
        }],
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let contacts = txn.query("contacts", &no_filter_query()).unwrap();
    assert_eq!(contacts.len(), 1);
    assert_eq!(
        cell_value(&contacts[0], "name"),
        Some(&Value::String("Alice".into()))
    );

    let accounts = txn.query("accounts", &no_filter_query()).unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(
        cell_value(&accounts[0], "name"),
        Some(&Value::String("Acme".into()))
    );
}

// ── Catalog + data coexist ──────────────────────────────────────

#[test]
fn catalog_and_data_coexist() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    txn.write_record(DS_ID, "acct-1", make_cells("Acme", 50000.0, "active", true))
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
                ttl_seconds: None,
                indexed: false,
            },
            FieldDef {
                name: "status".to_string(),
                field_type: FieldType::String,
                ttl_seconds: None,
                indexed: true,
            },
        ],
        partition: PARTITION.to_string(),
    };

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&ds).unwrap();
    txn.write_record(
        DS_ID,
        "r1",
        vec![
            CellWrite {
                column: "name".into(),
                value: Value::String("Alice".into()),
            },
            CellWrite {
                column: "status".into(),
                value: Value::String("active".into()),
            },
        ],
    )
    .unwrap();
    txn.write_record(
        DS_ID,
        "r2",
        vec![
            CellWrite {
                column: "name".into(),
                value: Value::String("Bob".into()),
            },
            CellWrite {
                column: "status".into(),
                value: Value::String("rejected".into()),
            },
        ],
    )
    .unwrap();
    txn.write_record(
        DS_ID,
        "r3",
        vec![
            CellWrite {
                column: "name".into(),
                value: Value::String("Charlie".into()),
            },
            CellWrite {
                column: "status".into(),
                value: Value::String("active".into()),
            },
        ],
    )
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

    let mut names: Vec<_> = results.iter().map(|r| cell_value(r, "name")).collect();
    names.sort_by_key(|v| format!("{:?}", v));
    assert_eq!(names[0], Some(&Value::String("Alice".into())));
    assert_eq!(names[1], Some(&Value::String("Charlie".into())));
}
