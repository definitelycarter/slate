use std::sync::{Arc, Barrier};
use std::thread;

use slate_db::{Database, Datasource, FieldDef, FieldType, Value};
use slate_query::*;
use slate_store::RocksStore;

use crate::datagen;
use crate::report::{BenchResult, bench};

pub const DS_ID: &str = "bench";
const PARTITION: &str = "bench_part";
const TS: i64 = 1_700_000_000;

pub struct BenchConfig {
    pub label: &'static str,
    pub batch_size: usize,
    pub batches: usize,
}

impl BenchConfig {
    pub fn total_records(&self) -> usize {
        self.batch_size * self.batches
    }
}

pub const CONFIG_100K: BenchConfig = BenchConfig {
    label: "100k",
    batch_size: 10_000,
    batches: 10,
};

pub const CONFIG_10K: BenchConfig = BenchConfig {
    label: "10k",
    batch_size: 1_000,
    batches: 10,
};

pub fn make_datasource() -> Datasource {
    Datasource {
        id: DS_ID.to_string(),
        name: "Bench".to_string(),
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
                ttl_seconds: None,
                indexed: true,
            },
            FieldDef {
                name: "contacts_count".into(),
                field_type: FieldType::Int,
                ttl_seconds: None,
                indexed: false,
            },
            FieldDef {
                name: "product_recommendation1".into(),
                field_type: FieldType::String,
                ttl_seconds: None,
                indexed: false,
            },
            FieldDef {
                name: "product_recommendation2".into(),
                field_type: FieldType::String,
                ttl_seconds: None,
                indexed: false,
            },
            FieldDef {
                name: "product_recommendation3".into(),
                field_type: FieldType::String,
                ttl_seconds: None,
                indexed: false,
            },
            FieldDef {
                name: "last_contacted_at".into(),
                field_type: FieldType::Date,
                ttl_seconds: None,
                indexed: false,
            },
            FieldDef {
                name: "notes".into(),
                field_type: FieldType::String,
                ttl_seconds: None,
                indexed: false,
            },
        ],
        partition: PARTITION.to_string(),
    }
}

/// Create the datasource in the database (must be called before any data writes).
pub fn setup_datasource(db: &Database<RocksStore>) {
    let mut txn = db.begin(false).expect("begin failed");
    txn.save_datasource(&make_datasource())
        .expect("save_datasource failed");
    txn.commit().expect("commit failed");
}

// --- Phase 1: Bulk Insert ---

pub fn bulk_insert(db: &Database<RocksStore>, user: usize, cfg: &BenchConfig) -> Vec<BenchResult> {
    let mut results = Vec::new();
    let total_records = cfg.total_records();
    let batch_size = cfg.batch_size;
    let batches = cfg.batches;

    let total = bench(
        &format!("bulk insert {total_records} records total"),
        || {
            let mut inserted = 0;
            for batch_idx in 0..batches {
                let start = batch_idx * batch_size;
                let batch_result = bench(
                    &format!(
                        "  batch {} ({start}..{})",
                        batch_idx + 1,
                        start + batch_size
                    ),
                    || {
                        let writes = datagen::generate_batch(user, start, batch_size, TS);
                        let mut txn = db.begin(false).expect("begin failed");
                        txn.write_batch(DS_ID, writes).expect("write_batch failed");
                        txn.commit().expect("commit failed");
                        batch_size
                    },
                );
                batch_result.print();
                results.push(batch_result);
                inserted += batch_size;
            }
            inserted
        },
    );

    results.push(total);
    results
}

// --- Phase 2: Data Integrity Verification ---

pub fn verify_integrity(db: &Database<RocksStore>, user: usize, cfg: &BenchConfig) {
    let total_records = cfg.total_records();
    let txn = db.begin(true).expect("begin failed");
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.query(DS_ID, &query).expect("query failed");

    // Count records for this user (all records in the datasource belong to this user
    // in per-user benchmarks, or we filter by prefix in multi-user)
    let user_prefix = format!("user{user}-");
    let user_records: Vec<_> = results
        .iter()
        .filter(|r| r.id.starts_with(&user_prefix))
        .collect();

    assert_eq!(
        user_records.len(),
        total_records,
        "user {user}: expected {total_records} records, got {}",
        user_records.len()
    );

    let mut has_last_contacted = 0;
    let mut null_last_contacted = 0;
    let mut has_notes = 0;
    let mut null_notes = 0;

    for record in &user_records {
        assert!(record.cells.contains_key("name"), "missing 'name' field");
        assert!(
            record.cells.contains_key("status"),
            "missing 'status' field"
        );
        assert!(
            record.cells.contains_key("contacts_count"),
            "missing 'contacts_count' field"
        );
        assert!(
            record.cells.contains_key("product_recommendation1"),
            "missing 'product_recommendation1' field"
        );

        match record.cells.get("status").map(|c| &c.value) {
            Some(Value::String(s)) => {
                assert!(s == "active" || s == "rejected", "unexpected status: {s}");
            }
            other => panic!("status has wrong type: {other:?}"),
        }

        if record.cells.contains_key("last_contacted_at") {
            has_last_contacted += 1;
        } else {
            null_last_contacted += 1;
        }

        if record.cells.contains_key("notes") {
            has_notes += 1;
        } else {
            null_notes += 1;
        }
    }

    println!(
        "  integrity check passed: {} records verified",
        user_records.len()
    );
    println!(
        "  nullable fields: last_contacted_at ({has_last_contacted} present, {null_last_contacted} null), notes ({has_notes} present, {null_notes} null)"
    );
}

// --- Phase 3: Query Benchmarks ---

pub fn query_benchmarks(
    db: &Database<RocksStore>,
    _user: usize,
    cfg: &BenchConfig,
) -> Vec<BenchResult> {
    let total_records = cfg.total_records();
    let mut results = Vec::new();

    // 1. No filter
    results.push(bench("query: no filter (full scan)", || {
        let txn = db.begin(true).expect("begin failed");
        let query = Query {
            filter: None,
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        let r = txn.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 2. Status filter (IndexScan — status is indexed)
    results.push(bench("query: status = 'active' (indexed)", || {
        let txn = db.begin(true).expect("begin failed");
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
            columns: None,
        };
        let r = txn.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 3. Product recommendation filter
    results.push(bench("query: product_recommendation1 = 'ProductA'", || {
        let txn = db.begin(true).expect("begin failed");
        let query = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![FilterNode::Condition(Filter {
                    field: "product_recommendation1".to_string(),
                    operator: Operator::Eq,
                    value: QueryValue::String("ProductA".to_string()),
                })],
            }),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        let r = txn.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 4. Combined filters: status AND rec1 AND rec2
    results.push(bench("query: status + rec1 + rec2 (AND)", || {
        let txn = db.begin(true).expect("begin failed");
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
                        field: "product_recommendation1".to_string(),
                        operator: Operator::Eq,
                        value: QueryValue::String("ProductA".to_string()),
                    }),
                    FilterNode::Condition(Filter {
                        field: "product_recommendation2".to_string(),
                        operator: Operator::Eq,
                        value: QueryValue::String("ProductX".to_string()),
                    }),
                ],
            }),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        let r = txn.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 5. Filter + sort + pagination
    results.push(bench(
        "query: status='active' + sort contacts_count + skip/take",
        || {
            let txn = db.begin(true).expect("begin failed");
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
                    field: "contacts_count".to_string(),
                    direction: SortDirection::Desc,
                }],
                skip: Some(100),
                take: Some(50),
                columns: None,
            };
            let r = txn.query(DS_ID, &query).expect("query failed");
            r.len()
        },
    ));

    // 6. Filter only (no sort)
    results.push(bench("query: status='active' (no sort)", || {
        let txn = db.begin(true).expect("begin failed");
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
            columns: None,
        };
        let r = txn.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 7. Same filter WITH sort
    results.push(bench(
        "query: status='active' + sort contacts_count",
        || {
            let txn = db.begin(true).expect("begin failed");
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
                    field: "contacts_count".to_string(),
                    direction: SortDirection::Desc,
                }],
                skip: None,
                take: None,
                columns: None,
            };
            let r = txn.query(DS_ID, &query).expect("query failed");
            r.len()
        },
    ));

    // 8. Filter with take(200) — no sort
    results.push(bench("query: status='active' take(200) (no sort)", || {
        let txn = db.begin(true).expect("begin failed");
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
            take: Some(200),
            columns: None,
        };
        let r = txn.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 9. Filter with take(200) — with sort
    results.push(bench("query: status='active' + sort + take(200)", || {
        let txn = db.begin(true).expect("begin failed");
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
                field: "contacts_count".to_string(),
                direction: SortDirection::Desc,
            }],
            skip: None,
            take: Some(200),
            columns: None,
        };
        let r = txn.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 10. IsNull filter on nullable field (~30% null)
    results.push(bench("query: last_contacted_at is null (~30%)", || {
        let txn = db.begin(true).expect("begin failed");
        let query = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![FilterNode::Condition(Filter {
                    field: "last_contacted_at".to_string(),
                    operator: Operator::IsNull,
                    value: QueryValue::Bool(true),
                })],
            }),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        let r = txn.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 11. IsNull filter on nullable field (~50% null)
    results.push(bench("query: notes is null (~50%)", || {
        let txn = db.begin(true).expect("begin failed");
        let query = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![FilterNode::Condition(Filter {
                    field: "notes".to_string(),
                    operator: Operator::IsNull,
                    value: QueryValue::Bool(true),
                })],
            }),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        let r = txn.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 12. IsNull=false
    results.push(bench("query: last_contacted_at is not null (~70%)", || {
        let txn = db.begin(true).expect("begin failed");
        let query = Query {
            filter: Some(FilterGroup {
                logical: LogicalOp::And,
                children: vec![FilterNode::Condition(Filter {
                    field: "last_contacted_at".to_string(),
                    operator: Operator::IsNull,
                    value: QueryValue::Bool(false),
                })],
            }),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        let r = txn.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 13. Combined: status='active' AND notes is null
    results.push(bench("query: status='active' AND notes is null", || {
        let txn = db.begin(true).expect("begin failed");
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
                        field: "notes".to_string(),
                        operator: Operator::IsNull,
                        value: QueryValue::Bool(true),
                    }),
                ],
            }),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        let r = txn.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 14. Point lookups (1000 get_by_id calls)
    results.push(bench("query: 1000 point lookups (get_by_id)", || {
        let txn = db.begin(true).expect("begin failed");
        let mut found = 0;
        for i in (0..total_records).step_by(total_records / 1000) {
            let id = datagen::generate_record_id(_user, i);
            if txn
                .get_by_id(DS_ID, &id, None)
                .expect("get_by_id failed")
                .is_some()
            {
                found += 1;
            }
        }
        found
    }));

    // 15. Projection benchmark: fetch 2 of 8 columns
    results.push(bench("query: projection (name, status only)", || {
        let txn = db.begin(true).expect("begin failed");
        let query = Query {
            filter: None,
            sort: vec![],
            skip: None,
            take: None,
            columns: Some(vec!["name".into(), "status".into()]),
        };
        let r = txn.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    results
}

// --- Phase 4: Concurrency Tests ---

pub fn concurrency_tests(
    db: Arc<Database<RocksStore>>,
    _user: usize,
    cfg: &BenchConfig,
) -> Vec<BenchResult> {
    let total_records = cfg.total_records();
    let mut results = Vec::new();

    // Test 1: Concurrent reads and writes
    results.push(bench("concurrent: 2 writers + 4 readers", || {
        let mut handles = Vec::new();

        // 2 writer threads
        for writer_id in 0..2 {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let base = total_records + writer_id * 5000;
                for batch in 0..5 {
                    let start = base + batch * 1000;
                    let writes = datagen::generate_batch(99, start, 1000, TS);
                    let mut txn = db.begin(false).expect("writer begin failed");
                    txn.write_batch(DS_ID, writes).expect("writer write failed");
                    txn.commit().expect("writer commit failed");
                }
            }));
        }

        // 4 reader threads
        for _ in 0..4 {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for _ in 0..5 {
                    let txn = db.begin(true).expect("reader begin failed");
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
                        take: Some(100),
                        columns: None,
                    };
                    let _ = txn.query(DS_ID, &query).expect("reader query failed");
                }
            }));
        }

        let thread_count = handles.len();
        for h in handles {
            h.join().expect("thread panicked — possible race condition");
        }
        thread_count
    }));

    // Test 2: Write conflict detection
    results.push(bench("concurrent: write conflict detection", || {
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();

        for _ in 0..2 {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || -> Result<(), slate_db::DbError> {
                let mut txn = db.begin(false).expect("conflict begin failed");
                let cells = datagen::generate_cells(99, 999_999, TS);
                let id = datagen::generate_record_id(99, 999_999);
                txn.write_record(DS_ID, &id, cells)
                    .expect("conflict write failed");
                barrier.wait();
                txn.commit()
            }));
        }

        let mut successes = 0;
        let mut conflicts = 0;
        for h in handles {
            match h.join().expect("conflict thread panicked") {
                Ok(()) => successes += 1,
                Err(_) => conflicts += 1,
            }
        }

        println!("    write conflict: {successes} succeeded, {conflicts} conflicted");
        assert!(successes >= 1, "at least one transaction should succeed");
        successes + conflicts
    }));

    results
}

// --- Phase 5: Post-Concurrency Integrity ---

pub fn verify_post_concurrency(db: &Database<RocksStore>, _user: usize, cfg: &BenchConfig) {
    let total_records = cfg.total_records();
    let txn = db.begin(true).expect("begin failed");
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.query(DS_ID, &query).expect("query failed");
    assert!(
        results.len() >= total_records,
        "expected at least {total_records} records after concurrency, got {}",
        results.len()
    );
    println!(
        "  post-concurrency integrity passed: {} records present",
        results.len()
    );
}
