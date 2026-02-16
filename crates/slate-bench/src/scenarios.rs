use std::sync::Arc;
use std::thread;

use slate_db::Database;
use slate_query::*;
use slate_store::Store;

use crate::datagen;
use crate::report::{BenchResult, bench};

pub const COLLECTION: &str = "bench";

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

/// Create an index on `status` for index scan benchmarks.
pub fn setup_collection<S: Store>(db: &Database<S>) {
    let mut txn = db.begin(false).expect("begin failed");
    txn.create_index(COLLECTION, "status")
        .expect("create_index failed");
    txn.commit().expect("commit failed");
}

// --- Phase 1: Bulk Insert ---

pub fn bulk_insert<S: Store>(db: &Database<S>, user: usize, cfg: &BenchConfig) -> Vec<BenchResult> {
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
                        let docs = datagen::generate_batch_docs(user, start, batch_size);
                        let mut txn = db.begin(false).expect("begin failed");
                        txn.insert_many(COLLECTION, docs)
                            .expect("insert_many failed");
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

pub fn verify_integrity<S: Store>(db: &Database<S>, user: usize, cfg: &BenchConfig) {
    let total_records = cfg.total_records();
    let txn = db.begin(true).expect("begin failed");
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).expect("find failed");

    // Count records for this user
    let user_prefix = format!("user{user}-");
    let user_records: Vec<_> = results
        .iter()
        .filter(|r| {
            r.get_str("_id")
                .map(|id| id.starts_with(&user_prefix))
                .unwrap_or(false)
        })
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
        assert!(record.contains_key("name"), "missing 'name' field");
        assert!(record.contains_key("status"), "missing 'status' field");
        assert!(
            record.contains_key("contacts_count"),
            "missing 'contacts_count' field"
        );
        assert!(
            record.contains_key("product_recommendation1"),
            "missing 'product_recommendation1' field"
        );

        match record.get_str("status") {
            Ok(s) => {
                assert!(s == "active" || s == "rejected", "unexpected status: {s}");
            }
            Err(e) => panic!("status has wrong type: {e}"),
        }

        if record.contains_key("last_contacted_at") {
            has_last_contacted += 1;
        } else {
            null_last_contacted += 1;
        }

        if record.contains_key("notes") {
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

pub fn query_benchmarks<S: Store>(
    db: &Database<S>,
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
        let r = txn.find(COLLECTION, &query).expect("find failed");
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
        let r = txn.find(COLLECTION, &query).expect("find failed");
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
        let r = txn.find(COLLECTION, &query).expect("find failed");
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
        let r = txn.find(COLLECTION, &query).expect("find failed");
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
            let r = txn.find(COLLECTION, &query).expect("find failed");
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
        let r = txn.find(COLLECTION, &query).expect("find failed");
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
            let r = txn.find(COLLECTION, &query).expect("find failed");
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
        let r = txn.find(COLLECTION, &query).expect("find failed");
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
        let r = txn.find(COLLECTION, &query).expect("find failed");
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
        let r = txn.find(COLLECTION, &query).expect("find failed");
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
        let r = txn.find(COLLECTION, &query).expect("find failed");
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
        let r = txn.find(COLLECTION, &query).expect("find failed");
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
        let r = txn.find(COLLECTION, &query).expect("find failed");
        r.len()
    }));

    // 14. Point lookups (1000 find_by_id calls)
    results.push(bench("query: 1000 point lookups (find_by_id)", || {
        let txn = db.begin(true).expect("begin failed");
        let mut found = 0;
        for i in (0..total_records).step_by(total_records / 1000) {
            let id = datagen::generate_record_id(_user, i);
            if txn
                .find_by_id(COLLECTION, &id, None)
                .expect("find_by_id failed")
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
        let r = txn.find(COLLECTION, &query).expect("find failed");
        r.len()
    }));

    results
}

// --- Phase 4: Concurrency Tests ---

pub fn concurrency_tests<S: Store + Send + Sync + 'static>(
    db: Arc<Database<S>>,
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
                    let docs = datagen::generate_batch_docs(99, start, 1000);
                    let mut txn = db.begin(false).expect("writer begin failed");
                    txn.insert_many(COLLECTION, docs)
                        .expect("writer insert failed");
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
                    let _ = txn.find(COLLECTION, &query).expect("reader find failed");
                }
            }));
        }

        let thread_count = handles.len();
        for h in handles {
            h.join().expect("thread panicked — possible race condition");
        }
        thread_count
    }));

    results
}

// --- Phase 5: Post-Concurrency Integrity ---

pub fn verify_post_concurrency<S: Store>(db: &Database<S>, _user: usize, cfg: &BenchConfig) {
    let total_records = cfg.total_records();
    let txn = db.begin(true).expect("begin failed");
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = txn.find(COLLECTION, &query).expect("find failed");
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
