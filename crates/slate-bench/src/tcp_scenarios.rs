use std::thread;

use slate_client::Client;
use slate_query::*;

use crate::datagen;
use crate::report::{BenchResult, bench};
use crate::scenarios::DS_ID;

const BATCH_SIZE: usize = 10_000;
const BATCHES: usize = 10;
const TOTAL_RECORDS: usize = BATCH_SIZE * BATCHES;

// --- Phase 1: Bulk Insert over TCP ---

pub fn bulk_insert(client: &mut Client, user: usize) -> Vec<BenchResult> {
    let mut results = Vec::new();

    let total = bench(
        &format!("tcp bulk insert {TOTAL_RECORDS} records total"),
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
                        let writes = datagen::generate_batch(user, start, BATCH_SIZE);
                        client
                            .write_batch(DS_ID, writes)
                            .expect("write_batch failed");
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

// --- Phase 2: Data Integrity Verification over TCP ---

pub fn verify_integrity(client: &mut Client, _user: usize) {
    let query = Query {
        filter: None,
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results = client.query(DS_ID, &query).expect("query failed");

    assert!(
        results.len() >= TOTAL_RECORDS,
        "expected at least {TOTAL_RECORDS} records, got {}",
        results.len()
    );

    println!(
        "  integrity check passed: {} records verified",
        results.len()
    );
}

// --- Phase 3: Query Benchmarks over TCP ---

pub fn query_benchmarks(client: &mut Client, user: usize) -> Vec<BenchResult> {
    let mut results = Vec::new();

    // 1. No filter
    results.push(bench("tcp query: no filter (full scan)", || {
        let query = Query {
            filter: None,
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        let r = client.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 2. Status filter
    results.push(bench("tcp query: status = 'active'", || {
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
        let r = client.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 3. Combined filters
    results.push(bench("tcp query: status + rec1 + rec2 (AND)", || {
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
        let r = client.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 4. Filter (no sort)
    results.push(bench("tcp query: status='active' (no sort)", || {
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
        let r = client.query(DS_ID, &query).expect("query failed");
        r.len()
    }));

    // 5. Same filter WITH sort
    results.push(bench(
        "tcp query: status='active' + sort contacts_count",
        || {
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
            let r = client.query(DS_ID, &query).expect("query failed");
            r.len()
        },
    ));

    // 6. Filter + take(200), no sort
    results.push(bench(
        "tcp query: status='active' take(200) (no sort)",
        || {
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
            let r = client.query(DS_ID, &query).expect("query failed");
            r.len()
        },
    ));

    // 7. Filter + sort + take(200)
    results.push(bench(
        "tcp query: status='active' + sort + take(200)",
        || {
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
            let r = client.query(DS_ID, &query).expect("query failed");
            r.len()
        },
    ));

    // 8. Filter + sort + skip/take
    results.push(bench(
        "tcp query: status='active' + sort + skip/take",
        || {
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
            let r = client.query(DS_ID, &query).expect("query failed");
            r.len()
        },
    ));

    // 9. Point lookups
    results.push(bench("tcp query: 1000 point lookups (get_by_id)", || {
        let mut found = 0;
        for i in (0..TOTAL_RECORDS).step_by(TOTAL_RECORDS / 1000) {
            let id = datagen::generate_record_id(user, i);
            if client
                .get_by_id(DS_ID, &id, None)
                .expect("get_by_id failed")
                .is_some()
            {
                found += 1;
            }
        }
        found
    }));

    results
}

// --- Phase 4: Concurrent TCP clients ---

pub fn concurrency_tests(addr: &str, _user: usize) -> Vec<BenchResult> {
    let mut results = Vec::new();

    let addr_owned = addr.to_string();
    results.push(bench("tcp concurrent: 2 writers + 4 readers", || {
        let mut handles = Vec::new();

        // 2 writer threads
        for writer_id in 0..2 {
            let addr = addr_owned.clone();
            handles.push(thread::spawn(move || {
                let mut client = Client::connect(&addr).expect("writer connect failed");
                let base = TOTAL_RECORDS + writer_id * 5000;
                for batch in 0..5 {
                    let start = base + batch * 1000;
                    let writes = datagen::generate_batch(99, start, 1000);
                    client
                        .write_batch(DS_ID, writes)
                        .expect("writer write failed");
                }
            }));
        }

        // 4 reader threads
        for _ in 0..4 {
            let addr = addr_owned.clone();
            handles.push(thread::spawn(move || {
                let mut client = Client::connect(&addr).expect("reader connect failed");
                for _ in 0..5 {
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
                    let _ = client.query(DS_ID, &query).expect("reader query failed");
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
