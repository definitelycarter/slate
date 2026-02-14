use std::collections::HashMap;

use slate_db::{Database, Datasource, FieldDef, FieldType};
use slate_query::{
    Filter, FilterGroup, FilterNode, LogicalOp, Operator, Query, QueryValue, Sort, SortDirection,
};
use slate_store::{Record, RocksStore, Value};

fn temp_db() -> (Database<RocksStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = RocksStore::open(dir.path()).unwrap();
    let db = Database::new(store);
    (db, dir)
}

fn make_datasource() -> Datasource {
    Datasource {
        id: "ds1".to_string(),
        name: "SBA".to_string(),
        fields: vec![
            FieldDef {
                name: "name".to_string(),
                field_type: FieldType::String,
            },
            FieldDef {
                name: "revenue".to_string(),
                field_type: FieldType::Float,
            },
            FieldDef {
                name: "status".to_string(),
                field_type: FieldType::String,
            },
            FieldDef {
                name: "active".to_string(),
                field_type: FieldType::Bool,
            },
            FieldDef {
                name: "created_at".to_string(),
                field_type: FieldType::Date,
            },
        ],
    }
}

fn make_record(id: &str, name: &str, revenue: f64, status: &str, active: bool) -> Record {
    let mut fields = HashMap::new();
    fields.insert("name".to_string(), Value::String(name.to_string()));
    fields.insert("revenue".to_string(), Value::Float(revenue));
    fields.insert("status".to_string(), Value::String(status.to_string()));
    fields.insert("active".to_string(), Value::Bool(active));
    Record {
        id: id.to_string(),
        fields,
    }
}

fn seed_records(db: &Database<RocksStore>) {
    let mut txn = db.begin(false).unwrap();
    txn.insert(make_record("acct-1", "Acme Corp", 50000.0, "active", true))
        .unwrap();
    txn.insert(make_record("acct-2", "Globex", 80000.0, "snoozed", true))
        .unwrap();
    txn.insert(make_record("acct-3", "Initech", 12000.0, "rejected", false))
        .unwrap();
    txn.insert(make_record("acct-4", "Umbrella", 95000.0, "active", true))
        .unwrap();
    txn.insert(make_record(
        "acct-5",
        "Stark Industries",
        200000.0,
        "active",
        false,
    ))
    .unwrap();
    txn.commit().unwrap();
}

// --- Catalog tests ---

#[test]
fn catalog_save_and_get() {
    let (db, _dir) = temp_db();
    let ds = make_datasource();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&ds).unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let result = txn.get_datasource("ds1").unwrap().unwrap();
    assert_eq!(result.id, "ds1");
    assert_eq!(result.name, "SBA");
    assert_eq!(result.fields.len(), 5);
}

#[test]
fn catalog_get_not_found() {
    let (db, _dir) = temp_db();
    let txn = db.begin(true).unwrap();
    let result = txn.get_datasource("nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn catalog_list() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&Datasource {
        id: "ds1".to_string(),
        name: "SBA".to_string(),
        fields: vec![],
    })
    .unwrap();
    txn.save_datasource(&Datasource {
        id: "ds2".to_string(),
        name: "MAS".to_string(),
        fields: vec![],
    })
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let result = txn.list_datasources().unwrap();
    assert_eq!(result.len(), 2);
    let names: Vec<&str> = result.iter().map(|d| d.name.as_str()).collect();
    assert!(names.contains(&"SBA"));
    assert!(names.contains(&"MAS"));
}

#[test]
fn catalog_delete() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&make_datasource()).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin(false).unwrap();
    txn.delete_datasource("ds1").unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let result = txn.get_datasource("ds1").unwrap();
    assert!(result.is_none());
}

#[test]
fn catalog_complex_schema_roundtrip() {
    let (db, _dir) = temp_db();

    let ds = Datasource {
        id: "ds1".to_string(),
        name: "SBA".to_string(),
        fields: vec![FieldDef {
            name: "contacts".to_string(),
            field_type: FieldType::List(Box::new(FieldType::Map(vec![
                FieldDef {
                    name: "name".to_string(),
                    field_type: FieldType::String,
                },
                FieldDef {
                    name: "exhausted".to_string(),
                    field_type: FieldType::Bool,
                },
            ]))),
        }],
    };

    let mut txn = db.begin(false).unwrap();
    txn.save_datasource(&ds).unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let result = txn.get_datasource("ds1").unwrap().unwrap();
    assert_eq!(result.fields.len(), 1);
    assert_eq!(result.fields[0].name, "contacts");
    match &result.fields[0].field_type {
        FieldType::List(inner) => match inner.as_ref() {
            FieldType::Map(fields) => {
                assert_eq!(fields.len(), 2);
            }
            other => panic!("expected Map inside List, got {other:?}"),
        },
        other => panic!("expected List, got {other:?}"),
    }
}

// --- Query tests ---

#[test]
fn query_no_filters() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
    };
    let results = txn.query(&query).unwrap();
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
                field: "status".to_string(),
                operator: Operator::Eq,
                value: QueryValue::String("active".to_string()),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
    };
    let results = txn.query(&query).unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn query_icontains_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "name".to_string(),
                operator: Operator::IContains,
                value: QueryValue::String("corp".to_string()),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
    };
    let results = txn.query(&query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "acct-1");
}

#[test]
fn query_istartswith_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "name".to_string(),
                operator: Operator::IStartsWith,
                value: QueryValue::String("stark".to_string()),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
    };
    let results = txn.query(&query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "acct-5");
}

#[test]
fn query_iendswith_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "name".to_string(),
                operator: Operator::IEndsWith,
                value: QueryValue::String("tech".to_string()),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
    };
    let results = txn.query(&query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "acct-3");
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
                field: "revenue".to_string(),
                operator: Operator::Gt,
                value: QueryValue::Float(80000.0),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
    };
    let results = txn.query(&query).unwrap();
    assert_eq!(results.len(), 2); // Umbrella (95k) and Stark (200k)
}

#[test]
fn query_lte_filter() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "revenue".to_string(),
                operator: Operator::Lte,
                value: QueryValue::Float(50000.0),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
    };
    let results = txn.query(&query).unwrap();
    assert_eq!(results.len(), 2); // Acme (50k) and Initech (12k)
}

#[test]
fn query_isnull_filter() {
    let (db, _dir) = temp_db();

    let mut txn = db.begin(false).unwrap();
    // Insert a record without the "status" field
    let mut fields = HashMap::new();
    fields.insert(
        "name".to_string(),
        Value::String("NoStatus Inc".to_string()),
    );
    txn.insert(Record {
        id: "acct-x".to_string(),
        fields,
    })
    .unwrap();
    txn.insert(make_record("acct-1", "Acme", 50000.0, "active", true))
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "status".to_string(),
                operator: Operator::IsNull,
                value: QueryValue::Bool(true),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
    };
    let results = txn.query(&query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "acct-x");
}

#[test]
fn query_and_multiple_filters() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![
                FilterNode::Condition(Filter {
                    field: "status".to_string(),
                    operator: Operator::Eq,
                    value: QueryValue::String("active".to_string()),
                }),
                FilterNode::Condition(Filter {
                    field: "revenue".to_string(),
                    operator: Operator::Gte,
                    value: QueryValue::Float(90000.0),
                }),
            ],
        }),
        sort: vec![],
        skip: None,
        take: None,
    };
    let results = txn.query(&query).unwrap();
    assert_eq!(results.len(), 2); // Umbrella (95k) and Stark (200k)
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
                    field: "status".to_string(),
                    operator: Operator::Eq,
                    value: QueryValue::String("snoozed".to_string()),
                }),
                FilterNode::Condition(Filter {
                    field: "status".to_string(),
                    operator: Operator::Eq,
                    value: QueryValue::String("rejected".to_string()),
                }),
            ],
        }),
        sort: vec![],
        skip: None,
        take: None,
    };
    let results = txn.query(&query).unwrap();
    assert_eq!(results.len(), 2); // Globex (snoozed) and Initech (rejected)
}

#[test]
fn query_nested_and_or() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    // active AND (revenue > 90000 OR name icontains "acme")
    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![
                FilterNode::Condition(Filter {
                    field: "status".to_string(),
                    operator: Operator::Eq,
                    value: QueryValue::String("active".to_string()),
                }),
                FilterNode::Group(FilterGroup {
                    logical: LogicalOp::Or,
                    children: vec![
                        FilterNode::Condition(Filter {
                            field: "revenue".to_string(),
                            operator: Operator::Gt,
                            value: QueryValue::Float(90000.0),
                        }),
                        FilterNode::Condition(Filter {
                            field: "name".to_string(),
                            operator: Operator::IContains,
                            value: QueryValue::String("acme".to_string()),
                        }),
                    ],
                }),
            ],
        }),
        sort: vec![],
        skip: None,
        take: None,
    };
    let results = txn.query(&query).unwrap();
    // Acme (active, name matches), Umbrella (active, revenue 95k), Stark (active, revenue 200k)
    assert_eq!(results.len(), 3);
}

// --- Sort tests ---

#[test]
fn query_sort_asc() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![Sort {
            field: "revenue".to_string(),
            direction: SortDirection::Asc,
        }],
        skip: None,
        take: None,
    };
    let results = txn.query(&query).unwrap();
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
            field: "revenue".to_string(),
            direction: SortDirection::Desc,
        }],
        skip: None,
        take: None,
    };
    let results = txn.query(&query).unwrap();
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].id, "acct-5"); // Stark 200k
    assert_eq!(results[4].id, "acct-3"); // Initech 12k
}

// --- Pagination tests ---

#[test]
fn query_take() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![Sort {
            field: "revenue".to_string(),
            direction: SortDirection::Desc,
        }],
        skip: None,
        take: Some(2),
    };
    let results = txn.query(&query).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, "acct-5"); // Stark 200k
    assert_eq!(results[1].id, "acct-4"); // Umbrella 95k
}

#[test]
fn query_skip_and_take() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![Sort {
            field: "revenue".to_string(),
            direction: SortDirection::Asc,
        }],
        skip: Some(1),
        take: Some(2),
    };
    let results = txn.query(&query).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, "acct-1"); // Acme 50k (skipped Initech 12k)
    assert_eq!(results[1].id, "acct-2"); // Globex 80k
}

#[test]
fn query_filter_sort_paginate() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    // Active accounts, sorted by revenue desc, skip 1, take 1
    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "status".to_string(),
                operator: Operator::Eq,
                value: QueryValue::String("active".to_string()),
            })],
        }),
        sort: vec![Sort {
            field: "revenue".to_string(),
            direction: SortDirection::Desc,
        }],
        skip: Some(1),
        take: Some(1),
    };
    let results = txn.query(&query).unwrap();
    assert_eq!(results.len(), 1);
    // Active sorted desc: Stark(200k), Umbrella(95k), Acme(50k) → skip 1 → Umbrella
    assert_eq!(results[0].id, "acct-4");
}

// --- Streaming take without sort ---

#[test]
fn query_take_without_sort_streams() {
    let (db, _dir) = temp_db();
    seed_records(&db);

    let txn = db.begin(true).unwrap();
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: Some(2),
    };
    let results = txn.query(&query).unwrap();
    assert_eq!(results.len(), 2);
}
