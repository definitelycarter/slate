use std::borrow::Cow;

use bson::rawdoc;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use slate_db::bench::{ExecutionResult, Executor, PlanNode};
use slate_db::{CollectionConfig, Database, DatabaseConfig};
use slate_store::{MemoryStore, Store, StoreError, Transaction};

// ── NoopTransaction ─────────────────────────────────────────
//
// Implements Transaction with panics. Used for nodes that never
// touch the store (Values, Projection, Limit, Sort, Distinct, Filter).

struct NoopTransaction;

impl Transaction for NoopTransaction {
    type Cf = ();

    fn cf(&mut self, _name: &str) -> Result<Self::Cf, StoreError> {
        panic!("NoopTransaction::cf called");
    }
    fn get<'c>(&self, _cf: &'c Self::Cf, _key: &[u8]) -> Result<Option<Cow<'c, [u8]>>, StoreError> {
        panic!("NoopTransaction::get called");
    }
    fn multi_get<'c>(
        &self,
        _cf: &'c Self::Cf,
        _keys: &[&[u8]],
    ) -> Result<Vec<Option<Cow<'c, [u8]>>>, StoreError> {
        panic!("NoopTransaction::multi_get called");
    }
    fn scan_prefix<'c>(
        &'c self,
        _cf: &'c Self::Cf,
        _prefix: &[u8],
    ) -> Result<
        Box<dyn Iterator<Item = Result<(Cow<'c, [u8]>, Cow<'c, [u8]>), StoreError>> + 'c>,
        StoreError,
    > {
        panic!("NoopTransaction::scan_prefix called");
    }
    fn scan_prefix_rev<'c>(
        &'c self,
        _cf: &'c Self::Cf,
        _prefix: &[u8],
    ) -> Result<
        Box<dyn Iterator<Item = Result<(Cow<'c, [u8]>, Cow<'c, [u8]>), StoreError>> + 'c>,
        StoreError,
    > {
        panic!("NoopTransaction::scan_prefix_rev called");
    }
    fn put(&self, _cf: &Self::Cf, _key: &[u8], _value: &[u8]) -> Result<(), StoreError> {
        panic!("NoopTransaction::put called");
    }
    fn put_batch(&self, _cf: &Self::Cf, _entries: &[(&[u8], &[u8])]) -> Result<(), StoreError> {
        panic!("NoopTransaction::put_batch called");
    }
    fn delete(&self, _cf: &Self::Cf, _key: &[u8]) -> Result<(), StoreError> {
        panic!("NoopTransaction::delete called");
    }
    fn create_cf(&mut self, _name: &str) -> Result<(), StoreError> {
        panic!("NoopTransaction::create_cf called");
    }
    fn drop_cf(&mut self, _name: &str) -> Result<(), StoreError> {
        panic!("NoopTransaction::drop_cf called");
    }
    fn commit(self) -> Result<(), StoreError> {
        Ok(())
    }
    fn rollback(self) -> Result<(), StoreError> {
        Ok(())
    }
}

// ── Helpers ─────────────────────────────────────────────────

fn generate_docs(n: usize) -> Vec<bson::RawDocumentBuf> {
    (0..n)
        .map(|i| {
            rawdoc! {
                "_id": format!("rec-{i}"),
                "name": format!("User {i}"),
                "status": if i % 2 == 0 { "active" } else { "rejected" },
                "contacts_count": (i % 100) as i32,
                "product_recommendation1": "ProductA",
            }
        })
        .collect()
}

fn consume_rows(result: ExecutionResult) -> usize {
    match result {
        ExecutionResult::Rows(iter) => iter.count(),
        _ => panic!("expected Rows"),
    }
}

/// Create a seeded MemoryStore-backed Database with `n` documents and indexes
/// on `status` and `contacts_count`.
fn seeded_db(n: usize) -> Database<MemoryStore> {
    let db = Database::open(MemoryStore::new(), DatabaseConfig::default());
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "test".into(),
        indexes: vec!["status".into(), "contacts_count".into()],
    })
    .unwrap();
    let docs: Vec<bson::Document> = (0..n)
        .map(|i| {
            bson::doc! {
                "_id": format!("rec-{i}"),
                "name": format!("User {i}"),
                "status": if i % 2 == 0 { "active" } else { "rejected" },
                "contacts_count": (i % 100) as i32,
                "product_recommendation1": "ProductA",
            }
        })
        .collect();
    txn.insert_many("test", docs).unwrap();
    txn.commit().unwrap();
    db
}

// ── Store-free benchmarks ───────────────────────────────────

fn bench_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("values");
    for n in [100, 1_000, 10_000] {
        let docs = generate_docs(n);
        let plan = PlanNode::Values { docs };

        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, plan| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn, &());
            b.iter(|| consume_rows(exec.execute(plan).unwrap()))
        });
    }
    group.finish();
}

fn bench_projection(c: &mut Criterion) {
    let mut group = c.benchmark_group("projection");
    for n in [100, 1_000, 10_000] {
        let docs = generate_docs(n);

        let plan = PlanNode::Projection {
            columns: Some(vec!["name".into(), "status".into()]),
            input: Box::new(PlanNode::Values { docs: docs.clone() }),
        };

        group.bench_with_input(BenchmarkId::new("select", n), &plan, |b, plan| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn, &());
            b.iter(|| consume_rows(exec.execute(plan).unwrap()))
        });

        let plan_passthrough = PlanNode::Projection {
            columns: None,
            input: Box::new(PlanNode::Values { docs }),
        };

        group.bench_with_input(
            BenchmarkId::new("passthrough", n),
            &plan_passthrough,
            |b, plan| {
                let txn = NoopTransaction;
                let exec = Executor::new(&txn, &());
                b.iter(|| consume_rows(exec.execute(plan).unwrap()))
            },
        );
    }
    group.finish();
}

fn bench_limit(c: &mut Criterion) {
    let mut group = c.benchmark_group("limit");
    let docs = generate_docs(10_000);

    let plan = PlanNode::Limit {
        skip: 100,
        take: Some(200),
        input: Box::new(PlanNode::Values { docs: docs.clone() }),
    };

    group.bench_with_input(BenchmarkId::new("skip+take", 10_000), &plan, |b, plan| {
        let txn = NoopTransaction;
        let exec = Executor::new(&txn, &());
        b.iter(|| consume_rows(exec.execute(plan).unwrap()))
    });

    let plan_take = PlanNode::Limit {
        skip: 0,
        take: Some(200),
        input: Box::new(PlanNode::Values { docs }),
    };

    group.bench_with_input(BenchmarkId::new("take", 10_000), &plan_take, |b, plan| {
        let txn = NoopTransaction;
        let exec = Executor::new(&txn, &());
        b.iter(|| consume_rows(exec.execute(plan).unwrap()))
    });

    group.finish();
}

fn bench_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort");
    for n in [100, 1_000, 10_000] {
        let docs = generate_docs(n);

        let plan = PlanNode::Sort {
            sorts: vec![slate_query::Sort {
                field: "contacts_count".into(),
                direction: slate_query::SortDirection::Desc,
            }],
            input: Box::new(PlanNode::Values { docs: docs.clone() }),
        };

        group.bench_with_input(BenchmarkId::new("single", n), &plan, |b, plan| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn, &());
            b.iter(|| consume_rows(exec.execute(plan).unwrap()))
        });

        let plan_multi = PlanNode::Sort {
            sorts: vec![
                slate_query::Sort {
                    field: "status".into(),
                    direction: slate_query::SortDirection::Asc,
                },
                slate_query::Sort {
                    field: "contacts_count".into(),
                    direction: slate_query::SortDirection::Desc,
                },
            ],
            input: Box::new(PlanNode::Values { docs }),
        };

        group.bench_with_input(BenchmarkId::new("multi", n), &plan_multi, |b, plan| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn, &());
            b.iter(|| consume_rows(exec.execute(plan).unwrap()))
        });
    }
    group.finish();
}

fn bench_distinct(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct");
    for n in [100, 1_000, 10_000] {
        let docs = generate_docs(n);

        let plan = PlanNode::Distinct {
            field: "status".into(),
            input: Box::new(PlanNode::Projection {
                columns: Some(vec!["status".into()]),
                input: Box::new(PlanNode::Values { docs: docs.clone() }),
            }),
        };

        group.bench_with_input(BenchmarkId::new("low_card", n), &plan, |b, plan| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn, &());
            b.iter(|| consume_rows(exec.execute(plan).unwrap()))
        });

        let plan_sort = PlanNode::Sort {
            sorts: vec![slate_query::Sort {
                field: "status".into(),
                direction: slate_query::SortDirection::Asc,
            }],
            input: Box::new(PlanNode::Distinct {
                field: "status".into(),
                input: Box::new(PlanNode::Projection {
                    columns: Some(vec!["status".into()]),
                    input: Box::new(PlanNode::Values { docs: docs.clone() }),
                }),
            }),
        };

        group.bench_with_input(BenchmarkId::new("sorted", n), &plan_sort, |b, plan| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn, &());
            b.iter(|| consume_rows(exec.execute(plan).unwrap()))
        });

        let plan_sort_hc = PlanNode::Sort {
            sorts: vec![slate_query::Sort {
                field: "contacts_count".into(),
                direction: slate_query::SortDirection::Desc,
            }],
            input: Box::new(PlanNode::Distinct {
                field: "contacts_count".into(),
                input: Box::new(PlanNode::Projection {
                    columns: Some(vec!["contacts_count".into()]),
                    input: Box::new(PlanNode::Values { docs }),
                }),
            }),
        };

        group.bench_with_input(
            BenchmarkId::new("sorted_hc", n),
            &plan_sort_hc,
            |b, plan| {
                let txn = NoopTransaction;
                let exec = Executor::new(&txn, &());
                b.iter(|| consume_rows(exec.execute(plan).unwrap()))
            },
        );
    }
    group.finish();
}

fn bench_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter");
    for n in [100, 1_000, 10_000] {
        let docs = generate_docs(n);

        let plan_eq = PlanNode::Filter {
            predicate: slate_query::FilterGroup {
                logical: slate_query::LogicalOp::And,
                children: vec![slate_query::FilterNode::Condition(slate_query::Filter {
                    field: "status".into(),
                    operator: slate_query::Operator::Eq,
                    value: bson::Bson::String("active".into()),
                })],
            },
            input: Box::new(PlanNode::Values { docs: docs.clone() }),
        };

        group.bench_with_input(BenchmarkId::new("eq", n), &plan_eq, |b, plan| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn, &());
            b.iter(|| consume_rows(exec.execute(plan).unwrap()))
        });

        let plan_and = PlanNode::Filter {
            predicate: slate_query::FilterGroup {
                logical: slate_query::LogicalOp::And,
                children: vec![
                    slate_query::FilterNode::Condition(slate_query::Filter {
                        field: "status".into(),
                        operator: slate_query::Operator::Eq,
                        value: bson::Bson::String("active".into()),
                    }),
                    slate_query::FilterNode::Condition(slate_query::Filter {
                        field: "contacts_count".into(),
                        operator: slate_query::Operator::Gt,
                        value: bson::Bson::Int32(50),
                    }),
                ],
            },
            input: Box::new(PlanNode::Values { docs }),
        };

        group.bench_with_input(BenchmarkId::new("and", n), &plan_and, |b, plan| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn, &());
            b.iter(|| consume_rows(exec.execute(plan).unwrap()))
        });
    }
    group.finish();
}

// ── Store-backed benchmarks ─────────────────────────────────

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");
    for n in [100, 1_000, 10_000] {
        let db = seeded_db(n);
        let plan = PlanNode::Scan {
            collection: "test".into(),
        };

        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, plan| {
            let mut txn = db.store().begin(true).unwrap();
            let cf = txn.cf("test").unwrap();
            b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan).unwrap()))
        });
    }
    group.finish();
}

fn bench_index_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_scan");
    for n in [100, 1_000, 10_000] {
        let db = seeded_db(n);

        // Eq scan: status = "active" (~50% match)
        let plan_eq = PlanNode::IndexScan {
            collection: "test".into(),
            column: "status".into(),
            value: Some(bson::Bson::String("active".into())),
            direction: slate_query::SortDirection::Asc,
            limit: None,
            complete_groups: false,
        };

        group.bench_with_input(BenchmarkId::new("eq", n), &plan_eq, |b, plan| {
            let mut txn = db.store().begin(true).unwrap();
            let cf = txn.cf("test").unwrap();
            b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan).unwrap()))
        });

        // Full column scan (no value filter)
        let plan_full = PlanNode::IndexScan {
            collection: "test".into(),
            column: "contacts_count".into(),
            value: None,
            direction: slate_query::SortDirection::Asc,
            limit: None,
            complete_groups: false,
        };

        group.bench_with_input(BenchmarkId::new("full", n), &plan_full, |b, plan| {
            let mut txn = db.store().begin(true).unwrap();
            let cf = txn.cf("test").unwrap();
            b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan).unwrap()))
        });

        // Desc with limit
        let plan_desc_limit = PlanNode::IndexScan {
            collection: "test".into(),
            column: "contacts_count".into(),
            value: None,
            direction: slate_query::SortDirection::Desc,
            limit: Some(50),
            complete_groups: false,
        };

        group.bench_with_input(
            BenchmarkId::new("desc_limit", n),
            &plan_desc_limit,
            |b, plan| {
                let mut txn = db.store().begin(true).unwrap();
                let cf = txn.cf("test").unwrap();
                b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan).unwrap()))
            },
        );
    }
    group.finish();
}

fn bench_read_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_record");
    for n in [100, 1_000, 10_000] {
        let db = seeded_db(n);

        // ReadRecord over Scan (passthrough — Scan already yields full docs)
        let plan_scan = PlanNode::ReadRecord {
            input: Box::new(PlanNode::Scan {
                collection: "test".into(),
            }),
        };

        group.bench_with_input(BenchmarkId::new("scan", n), &plan_scan, |b, plan| {
            let mut txn = db.store().begin(true).unwrap();
            let cf = txn.cf("test").unwrap();
            b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan).unwrap()))
        });

        // ReadRecord over IndexScan (fetches full doc by id)
        let plan_idx = PlanNode::ReadRecord {
            input: Box::new(PlanNode::IndexScan {
                collection: "test".into(),
                column: "status".into(),
                value: Some(bson::Bson::String("active".into())),
                direction: slate_query::SortDirection::Asc,
                limit: None,
                complete_groups: false,
            }),
        };

        group.bench_with_input(BenchmarkId::new("index", n), &plan_idx, |b, plan| {
            let mut txn = db.store().begin(true).unwrap();
            let cf = txn.cf("test").unwrap();
            b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan).unwrap()))
        });
    }
    group.finish();
}

fn bench_index_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_merge");
    for n in [100, 1_000, 10_000] {
        let db = seeded_db(n);

        // OR merge: status="active" OR contacts_count=50
        let plan_or = PlanNode::IndexMerge {
            logical: slate_query::LogicalOp::Or,
            lhs: Box::new(PlanNode::IndexScan {
                collection: "test".into(),
                column: "status".into(),
                value: Some(bson::Bson::String("active".into())),
                direction: slate_query::SortDirection::Asc,
                limit: None,
                complete_groups: false,
            }),
            rhs: Box::new(PlanNode::IndexScan {
                collection: "test".into(),
                column: "contacts_count".into(),
                value: Some(bson::Bson::Int32(50)),
                direction: slate_query::SortDirection::Asc,
                limit: None,
                complete_groups: false,
            }),
        };

        group.bench_with_input(BenchmarkId::new("or", n), &plan_or, |b, plan| {
            let mut txn = db.store().begin(true).unwrap();
            let cf = txn.cf("test").unwrap();
            b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan).unwrap()))
        });

        // AND merge: status="active" AND contacts_count=50
        let plan_and = PlanNode::IndexMerge {
            logical: slate_query::LogicalOp::And,
            lhs: Box::new(PlanNode::IndexScan {
                collection: "test".into(),
                column: "status".into(),
                value: Some(bson::Bson::String("active".into())),
                direction: slate_query::SortDirection::Asc,
                limit: None,
                complete_groups: false,
            }),
            rhs: Box::new(PlanNode::IndexScan {
                collection: "test".into(),
                column: "contacts_count".into(),
                value: Some(bson::Bson::Int32(50)),
                direction: slate_query::SortDirection::Asc,
                limit: None,
                complete_groups: false,
            }),
        };

        group.bench_with_input(BenchmarkId::new("and", n), &plan_and, |b, plan| {
            let mut txn = db.store().begin(true).unwrap();
            let cf = txn.cf("test").unwrap();
            b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan).unwrap()))
        });
    }
    group.finish();
}

// ── Mutation benchmarks ─────────────────────────────────────
//
// Mutations modify state, so we use iter_batched to get a fresh
// write transaction per iteration. We don't commit — changes stay
// local to the dropped transaction.

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    for n in [100, 1_000] {
        // Empty db with collection + indexes, docs inserted per iteration
        let db = {
            let db = Database::open(MemoryStore::new(), DatabaseConfig::default());
            let mut txn = db.begin(false).unwrap();
            txn.create_collection(&CollectionConfig {
                name: "test".into(),
                indexes: vec!["status".into(), "contacts_count".into()],
            })
            .unwrap();
            txn.commit().unwrap();
            db
        };

        let docs = generate_docs(n);
        let plan = PlanNode::InsertIndex {
            indexed_fields: vec!["status".into(), "contacts_count".into()],
            input: Box::new(PlanNode::InsertRecord {
                input: Box::new(PlanNode::Values { docs }),
            }),
        };

        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, plan| {
            b.iter_batched(
                || {
                    let mut txn = db.store().begin(false).unwrap();
                    let cf = txn.cf("test").unwrap();
                    (txn, cf)
                },
                |(txn, cf)| {
                    let result = Executor::new(&txn, &cf).execute(plan).unwrap();
                    match result {
                        ExecutionResult::Insert { ids } => ids.len(),
                        _ => panic!("expected Insert"),
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("update");
    for n in [100, 1_000] {
        let db = seeded_db(n);

        // Update: set status = "updated" for all docs via Scan → ReadRecord → Update
        let plan = PlanNode::InsertIndex {
            indexed_fields: vec!["status".into(), "contacts_count".into()],
            input: Box::new(PlanNode::Update {
                update: bson::doc! { "$set": { "status": "updated" } },
                input: Box::new(PlanNode::ReadRecord {
                    input: Box::new(PlanNode::Scan {
                        collection: "test".into(),
                    }),
                }),
            }),
        };

        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, plan| {
            b.iter_batched(
                || {
                    let mut txn = db.store().begin(false).unwrap();
                    let cf = txn.cf("test").unwrap();
                    (txn, cf)
                },
                |(txn, cf)| {
                    let result = Executor::new(&txn, &cf).execute(plan).unwrap();
                    match result {
                        ExecutionResult::Update { modified, .. } => modified,
                        _ => panic!("expected Update"),
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");
    for n in [100, 1_000] {
        let db = seeded_db(n);

        // Delete all docs: Scan → ReadRecord → DeleteIndex → Delete
        let plan = PlanNode::Delete {
            input: Box::new(PlanNode::DeleteIndex {
                indexed_fields: vec!["status".into(), "contacts_count".into()],
                input: Box::new(PlanNode::ReadRecord {
                    input: Box::new(PlanNode::Scan {
                        collection: "test".into(),
                    }),
                }),
            }),
        };

        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, plan| {
            b.iter_batched(
                || {
                    let mut txn = db.store().begin(false).unwrap();
                    let cf = txn.cf("test").unwrap();
                    (txn, cf)
                },
                |(txn, cf)| {
                    let result = Executor::new(&txn, &cf).execute(plan).unwrap();
                    match result {
                        ExecutionResult::Delete { deleted } => deleted,
                        _ => panic!("expected Delete"),
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_replace(c: &mut Criterion) {
    let mut group = c.benchmark_group("replace");
    for n in [100, 1_000] {
        let db = seeded_db(n);

        // Replace all docs with a new document
        let plan = PlanNode::InsertIndex {
            indexed_fields: vec!["status".into(), "contacts_count".into()],
            input: Box::new(PlanNode::Replace {
                replacement: bson::doc! {
                    "name": "Replaced",
                    "status": "replaced",
                    "contacts_count": 0,
                },
                input: Box::new(PlanNode::ReadRecord {
                    input: Box::new(PlanNode::Scan {
                        collection: "test".into(),
                    }),
                }),
            }),
        };

        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, plan| {
            b.iter_batched(
                || {
                    let mut txn = db.store().begin(false).unwrap();
                    let cf = txn.cf("test").unwrap();
                    (txn, cf)
                },
                |(txn, cf)| {
                    let result = Executor::new(&txn, &cf).execute(plan).unwrap();
                    match result {
                        ExecutionResult::Update { modified, .. } => modified,
                        _ => panic!("expected Update"),
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_values,
    bench_projection,
    bench_limit,
    bench_sort,
    bench_distinct,
    bench_filter,
    bench_scan,
    bench_index_scan,
    bench_read_record,
    bench_index_merge,
    bench_insert,
    bench_update,
    bench_delete,
    bench_replace,
);
criterion_main!(benches);
