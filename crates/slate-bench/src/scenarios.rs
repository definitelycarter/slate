use std::sync::{Arc, Barrier};
use std::thread;

use slate_db::Database;
use slate_query::*;
use slate_store::{Record, RocksStore, Value};

use crate::datagen;
use crate::report::{BenchResult, bench};

fn prefix_records(prefix: &str, records: Vec<Record>) -> Vec<Record> {
    records
        .into_iter()
        .map(|mut r| {
            r.id = format!("{prefix}:{}", r.id);
            r
        })
        .collect()
}

fn prefix_record(prefix: &str, mut record: Record) -> Record {
    record.id = format!("{prefix}:{}", record.id);
    record
}

const BATCH_SIZE: usize = 10_000;
const BATCHES: usize = 10;
const TOTAL_RECORDS: usize = BATCH_SIZE * BATCHES;

// --- Phase 1: Bulk Insert ---

pub fn bulk_insert(db: &Database<RocksStore>, user: usize) -> Vec<BenchResult> {
    let mut results = Vec::new();

    let total = bench(
        &format!("bulk insert {TOTAL_RECORDS} records total"),
        || {
            let mut inserted = 0;
            for batch_idx in 0..BATCHES {
                let start = batch_idx * BATCH_SIZE;
                let batch_result = bench(
                    &format!(
                        "  batch {} ({start}..{})",
                        batch_idx + 1,
                        start + BATCH_SIZE
                    ),
                    || {
                        let prefix = format!("user{user}:bench");
                        let records = datagen::generate_batch(user, start, BATCH_SIZE);
                        let records = prefix_records(&prefix, records);
                        let mut txn = db.begin(false).expect("begin failed");
                        txn.insert_batch(records).expect("insert_batch failed");
                        txn.commit().expect("commit failed");
                        BATCH_SIZE
                    },
                );
                batch_result.print();
                results.push(batch_result);
                inserted += BATCH_SIZE;
            }
            inserted
        },
    );

    results.push(total);
    results
}

// --- Phase 2: Data Integrity Verification ---

pub fn verify_integrity(db: &Database<RocksStore>, user: usize) {
    let prefix = format!("user{user}:bench");
    let txn = db.begin(true).expect("begin failed");
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
    };
    let results = txn.query(&[prefix.as_str()], &query).expect("query failed");

    assert_eq!(
        results.len(),
        TOTAL_RECORDS,
        "user {user}: expected {TOTAL_RECORDS} records, got {}",
        results.len()
    );

    let mut has_last_contacted = 0;
    let mut null_last_contacted = 0;
    let mut has_notes = 0;
    let mut null_notes = 0;

    for record in &results {
        let expected_prefix = format!("user{user}:bench:user{user}-");
        assert!(
            record.id.starts_with(&expected_prefix),
            "unexpected record id: {}",
            record.id
        );
        assert!(record.fields.contains_key("name"), "missing 'name' field");
        assert!(
            record.fields.contains_key("status"),
            "missing 'status' field"
        );
        assert!(
            record.fields.contains_key("contacts_count"),
            "missing 'contacts_count' field"
        );
        assert!(
            record.fields.contains_key("product_recommendation1"),
            "missing 'product_recommendation1' field"
        );
        assert!(
            record.fields.contains_key("product_recommendation2"),
            "missing 'product_recommendation2' field"
        );
        assert!(
            record.fields.contains_key("product_recommendation3"),
            "missing 'product_recommendation3' field"
        );

        match record.fields.get("status") {
            Some(Value::String(s)) => {
                assert!(s == "active" || s == "rejected", "unexpected status: {s}");
            }
            other => panic!("status has wrong type: {other:?}"),
        }

        // Nullable fields: if present, verify correct type
        if let Some(val) = record.fields.get("last_contacted_at") {
            assert!(
                matches!(val, Value::Date(_)),
                "last_contacted_at has wrong type: {val:?}"
            );
            has_last_contacted += 1;
        } else {
            null_last_contacted += 1;
        }

        if let Some(val) = record.fields.get("notes") {
            assert!(
                matches!(val, Value::String(_)),
                "notes has wrong type: {val:?}"
            );
            has_notes += 1;
        } else {
            null_notes += 1;
        }
    }

    println!(
        "  integrity check passed: {} records verified",
        results.len()
    );
    println!(
        "  nullable fields: last_contacted_at ({has_last_contacted} present, {null_last_contacted} null), notes ({has_notes} present, {null_notes} null)"
    );
}

// --- Phase 3: Query Benchmarks ---

pub fn query_benchmarks(db: &Database<RocksStore>, user: usize) -> Vec<BenchResult> {
    let mut results = Vec::new();
    let prefix = format!("user{user}:bench");

    // 1. No filter
    results.push(bench("query: no filter (full scan)", || {
        let txn = db.begin(true).expect("begin failed");
        let query = Query {
            filter: None,
            sort: vec![],
            skip: None,
            take: None,
        };
        let r = txn.query(&[prefix.as_str()], &query).expect("query failed");
        r.len()
    }));

    // 2. Status filter
    results.push(bench("query: status = 'active'", || {
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
        };
        let r = txn.query(&[prefix.as_str()], &query).expect("query failed");
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
        };
        let r = txn.query(&[prefix.as_str()], &query).expect("query failed");
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
        };
        let r = txn.query(&[prefix.as_str()], &query).expect("query failed");
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
            };
            let r = txn.query(&[prefix.as_str()], &query).expect("query failed");
            r.len()
        },
    ));

    // 6. Filter only (no sort) — for comparison with #7
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
        };
        let r = txn.query(&[prefix.as_str()], &query).expect("query failed");
        r.len()
    }));

    // 7. Same filter WITH sort — direct comparison with #6
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
            };
            let r = txn.query(&[prefix.as_str()], &query).expect("query failed");
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
        };
        let r = txn.query(&[prefix.as_str()], &query).expect("query failed");
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
        };
        let r = txn.query(&[prefix.as_str()], &query).expect("query failed");
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
        };
        let r = txn.query(&[prefix.as_str()], &query).expect("query failed");
        r.len()
    }));

    // 7. IsNull filter on nullable field (~50% null)
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
        };
        let r = txn.query(&[prefix.as_str()], &query).expect("query failed");
        r.len()
    }));

    // 8. IsNull=false (records that HAVE the field)
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
        };
        let r = txn.query(&[prefix.as_str()], &query).expect("query failed");
        r.len()
    }));

    // 9. Combined: status='active' AND notes is null
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
        };
        let r = txn.query(&[prefix.as_str()], &query).expect("query failed");
        r.len()
    }));

    // 10. Point lookups (1000 get_by_id calls)
    results.push(bench("query: 1000 point lookups (get_by_id)", || {
        let txn = db.begin(true).expect("begin failed");
        let mut found = 0;
        for i in (0..TOTAL_RECORDS).step_by(TOTAL_RECORDS / 1000) {
            let id = format!("{prefix}:user{user}-{i}");
            if txn.get_by_id(&id).expect("get_by_id failed").is_some() {
                found += 1;
            }
        }
        found
    }));

    results
}

// --- Phase 4: Concurrency Tests ---

pub fn concurrency_tests(db: Arc<Database<RocksStore>>, user: usize) -> Vec<BenchResult> {
    let mut results = Vec::new();
    let prefix = format!("user{user}:bench");

    // Test 1: Concurrent reads and writes
    results.push(bench("concurrent: 2 writers + 4 readers", || {
        let mut handles = Vec::new();

        // 2 writer threads
        for writer_id in 0..2 {
            let db = Arc::clone(&db);
            let prefix = prefix.clone();
            handles.push(thread::spawn(move || {
                let base = TOTAL_RECORDS + writer_id * 5000;
                for batch in 0..5 {
                    let start = base + batch * 1000;
                    let records = datagen::generate_batch(99, start, 1000);
                    let records = prefix_records(&prefix, records);
                    let mut txn = db.begin(false).expect("writer begin failed");
                    txn.insert_batch(records).expect("writer insert failed");
                    txn.commit().expect("writer commit failed");
                }
            }));
        }

        // 4 reader threads
        for _ in 0..4 {
            let db = Arc::clone(&db);
            let prefix = prefix.clone();
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
                    };
                    let _ = txn
                        .query(&[prefix.as_str()], &query)
                        .expect("reader query failed");
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
            let prefix = prefix.clone();
            handles.push(thread::spawn(move || -> Result<(), slate_db::DbError> {
                let mut txn = db.begin(false).expect("conflict begin failed");
                let record = prefix_record(&prefix, datagen::generate_record(99, 999_999));
                txn.insert(record).expect("conflict insert failed");
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

// --- Multi-Prefix Query Benchmarks ---

pub fn multi_prefix_benchmarks(db: &Database<RocksStore>, user_count: usize) -> Vec<BenchResult> {
    let mut results = Vec::new();

    let all_prefixes: Vec<String> = (0..user_count).map(|u| format!("user{u}:bench")).collect();
    let all_refs: Vec<&str> = all_prefixes.iter().map(|s| s.as_str()).collect();

    // Verify: multi-prefix results == sum of individual prefix results
    {
        let txn = db.begin(true).expect("begin failed");
        let no_filter = Query {
            filter: None,
            sort: vec![],
            skip: None,
            take: None,
        };

        let mut individual_total = 0;
        for prefix in &all_refs {
            let count = txn
                .query(&[*prefix], &no_filter)
                .expect("query failed")
                .len();
            assert_eq!(
                count, TOTAL_RECORDS,
                "prefix {prefix}: expected {TOTAL_RECORDS}, got {count}"
            );
            individual_total += count;
        }

        let combined = txn.query(&all_refs, &no_filter).expect("query failed");
        assert_eq!(
            combined.len(),
            individual_total,
            "multi-prefix returned {} but individual scans totaled {individual_total}",
            combined.len()
        );

        // Verify each record belongs to the right prefix
        for record in &combined {
            let has_prefix = all_refs
                .iter()
                .any(|p| record.id.starts_with(&format!("{p}:")));
            assert!(has_prefix, "record {} doesn't match any prefix", record.id);
        }

        println!(
            "  multi-prefix verification passed: {individual_total} records across {user_count} prefixes"
        );
    }

    // Single prefix (baseline)
    let single = &all_refs[..1];
    results.push(bench("multi-prefix: 1 prefix, no filter", || {
        let txn = db.begin(true).expect("begin failed");
        let query = Query {
            filter: None,
            sort: vec![],
            skip: None,
            take: None,
        };
        let r = txn.query(single, &query).expect("query failed");
        r.len()
    }));

    // Two prefixes
    let two = &all_refs[..2];
    results.push(bench("multi-prefix: 2 prefixes, no filter", || {
        let txn = db.begin(true).expect("begin failed");
        let query = Query {
            filter: None,
            sort: vec![],
            skip: None,
            take: None,
        };
        let r = txn.query(two, &query).expect("query failed");
        r.len()
    }));

    // All prefixes
    results.push(bench(
        &format!("multi-prefix: {user_count} prefixes, no filter"),
        || {
            let txn = db.begin(true).expect("begin failed");
            let query = Query {
                filter: None,
                sort: vec![],
                skip: None,
                take: None,
            };
            let r = txn.query(&all_refs, &query).expect("query failed");
            r.len()
        },
    ));

    // All prefixes with filter
    results.push(bench(
        &format!("multi-prefix: {user_count} prefixes, status='active'"),
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
                sort: vec![],
                skip: None,
                take: None,
            };
            let r = txn.query(&all_refs, &query).expect("query failed");
            r.len()
        },
    ));

    // All prefixes with filter + sort + take
    results.push(bench(
        &format!("multi-prefix: {user_count} prefixes, status='active' + sort + take(200)"),
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
                take: Some(200),
            };
            let r = txn.query(&all_refs, &query).expect("query failed");
            r.len()
        },
    ));

    results
}

pub fn verify_post_concurrency(db: &Database<RocksStore>, user: usize) {
    let prefix = format!("user{user}:bench");
    let txn = db.begin(true).expect("begin failed");
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
    };
    let results = txn.query(&[prefix.as_str()], &query).expect("query failed");
    assert!(
        results.len() >= TOTAL_RECORDS,
        "expected at least {TOTAL_RECORDS} records after concurrency, got {}",
        results.len()
    );
    println!(
        "  post-concurrency integrity passed: {} records present",
        results.len()
    );
}
