use std::borrow::Cow;

use bson::raw::RawBsonRef;
use bson::rawdoc;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use slate_db::CollectionConfig;
use slate_db::bench::{Engine, ExecutionResult, Executor, IndexFilter, PlanNode};
use slate_query::*;
use slate_store::{MemoryStore, Store, StoreError, Transaction};

// ── NoopTransaction ─────────────────────────────────────────
//
// Implements Transaction with panics. Used for nodes that never
// touch the store (Values, Projection, Limit, Sort, Distinct, Filter).

struct NoopTransaction;

impl Transaction for NoopTransaction {
    type Cf = ();

    fn cf(&self, _name: &str) -> Result<Self::Cf, StoreError> {
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

/// Create a seeded MemoryStore-backed Engine with `n` documents and indexes
/// on `status` and `contacts_count`.
fn seeded_engine(n: usize) -> Engine<MemoryStore> {
    let engine = Engine::new(MemoryStore::new());
    let mut txn = engine.begin(false).unwrap();
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
    engine
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
            b.iter(|| consume_rows(exec.execute(plan.clone()).unwrap()))
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
            b.iter(|| consume_rows(exec.execute(plan.clone()).unwrap()))
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
                b.iter(|| consume_rows(exec.execute(plan.clone()).unwrap()))
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
        b.iter(|| consume_rows(exec.execute(plan.clone()).unwrap()))
    });

    let plan_take = PlanNode::Limit {
        skip: 0,
        take: Some(200),
        input: Box::new(PlanNode::Values { docs }),
    };

    group.bench_with_input(BenchmarkId::new("take", 10_000), &plan_take, |b, plan| {
        let txn = NoopTransaction;
        let exec = Executor::new(&txn, &());
        b.iter(|| consume_rows(exec.execute(plan.clone()).unwrap()))
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
            b.iter(|| consume_rows(exec.execute(plan.clone()).unwrap()))
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
            b.iter(|| consume_rows(exec.execute(plan.clone()).unwrap()))
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
            b.iter(|| consume_rows(exec.execute(plan.clone()).unwrap()))
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
            b.iter(|| consume_rows(exec.execute(plan.clone()).unwrap()))
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
                b.iter(|| consume_rows(exec.execute(plan.clone()).unwrap()))
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
            predicate: Expression::Eq("status", RawBsonRef::String("active")),
            input: Box::new(PlanNode::Values { docs: docs.clone() }),
        };

        group.bench_with_input(BenchmarkId::new("eq", n), &plan_eq, |b, plan| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn, &());
            b.iter(|| consume_rows(exec.execute(plan.clone()).unwrap()))
        });

        let plan_and = PlanNode::Filter {
            predicate: Expression::And(vec![
                Expression::Eq("status", RawBsonRef::String("active")),
                Expression::Gt("contacts_count", RawBsonRef::Int32(50)),
            ]),
            input: Box::new(PlanNode::Values { docs }),
        };

        group.bench_with_input(BenchmarkId::new("and", n), &plan_and, |b, plan| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn, &());
            b.iter(|| consume_rows(exec.execute(plan.clone()).unwrap()))
        });
    }
    group.finish();
}

// ── Store-backed benchmarks ─────────────────────────────────

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");
    for n in [100, 1_000, 10_000] {
        let engine = seeded_engine(n);
        let plan = PlanNode::Scan;

        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, plan| {
            let txn = engine.store().begin(true).unwrap();
            let cf = txn.cf("test").unwrap();
            b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan.clone()).unwrap()))
        });
    }
    group.finish();
}

fn bench_index_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_scan");
    for n in [100, 1_000, 10_000] {
        let engine = seeded_engine(n);

        // Eq scan: status = "active" (~50% match)
        let plan_eq = PlanNode::IndexScan {
            column: "status".into(),
            filter: Some(IndexFilter::Eq(RawBsonRef::String("active"))),
            direction: slate_query::SortDirection::Asc,
            limit: None,
            complete_groups: false,
            covered: false,
        };

        group.bench_with_input(BenchmarkId::new("eq", n), &plan_eq, |b, plan| {
            let txn = engine.store().begin(true).unwrap();
            let cf = txn.cf("test").unwrap();
            b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan.clone()).unwrap()))
        });

        // Full column scan (no value filter)
        let plan_full = PlanNode::IndexScan {
            column: "contacts_count".into(),
            filter: None,
            direction: slate_query::SortDirection::Asc,
            limit: None,
            complete_groups: false,
            covered: false,
        };

        group.bench_with_input(BenchmarkId::new("full", n), &plan_full, |b, plan| {
            let txn = engine.store().begin(true).unwrap();
            let cf = txn.cf("test").unwrap();
            b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan.clone()).unwrap()))
        });

        // Desc with limit
        let plan_desc_limit = PlanNode::IndexScan {
            column: "contacts_count".into(),
            filter: None,
            direction: slate_query::SortDirection::Desc,
            limit: Some(50),
            complete_groups: false,
            covered: false,
        };

        group.bench_with_input(
            BenchmarkId::new("desc_limit", n),
            &plan_desc_limit,
            |b, plan| {
                let txn = engine.store().begin(true).unwrap();
                let cf = txn.cf("test").unwrap();
                b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan.clone()).unwrap()))
            },
        );
    }
    group.finish();
}

fn bench_read_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_record");
    for n in [100, 1_000, 10_000] {
        let engine = seeded_engine(n);

        // ReadRecord over Scan (passthrough — Scan already yields full docs)
        let plan_scan = PlanNode::ReadRecord {
            input: Box::new(PlanNode::Scan),
        };

        group.bench_with_input(BenchmarkId::new("scan", n), &plan_scan, |b, plan| {
            let txn = engine.store().begin(true).unwrap();
            let cf = txn.cf("test").unwrap();
            b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan.clone()).unwrap()))
        });

        // ReadRecord over IndexScan (fetches full doc by id)
        let plan_idx = PlanNode::ReadRecord {
            input: Box::new(PlanNode::IndexScan {
                column: "status".into(),
                filter: Some(IndexFilter::Eq(RawBsonRef::String("active"))),
                direction: slate_query::SortDirection::Asc,
                limit: None,
                complete_groups: false,
                covered: false,
            }),
        };

        group.bench_with_input(BenchmarkId::new("index", n), &plan_idx, |b, plan| {
            let txn = engine.store().begin(true).unwrap();
            let cf = txn.cf("test").unwrap();
            b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan.clone()).unwrap()))
        });
    }
    group.finish();
}

fn bench_index_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_merge");
    for n in [100, 1_000, 10_000] {
        let engine = seeded_engine(n);

        // OR merge: status="active" OR contacts_count=50
        let plan_or = PlanNode::IndexMerge {
            logical: slate_query::LogicalOp::Or,
            lhs: Box::new(PlanNode::IndexScan {
                column: "status".into(),
                filter: Some(IndexFilter::Eq(RawBsonRef::String("active"))),
                direction: slate_query::SortDirection::Asc,
                limit: None,
                complete_groups: false,
                covered: false,
            }),
            rhs: Box::new(PlanNode::IndexScan {
                column: "contacts_count".into(),
                filter: Some(IndexFilter::Eq(RawBsonRef::Int32(50))),
                direction: slate_query::SortDirection::Asc,
                limit: None,
                complete_groups: false,
                covered: false,
            }),
        };

        group.bench_with_input(BenchmarkId::new("or", n), &plan_or, |b, plan| {
            let txn = engine.store().begin(true).unwrap();
            let cf = txn.cf("test").unwrap();
            b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan.clone()).unwrap()))
        });

        // AND merge: status="active" AND contacts_count=50
        let plan_and = PlanNode::IndexMerge {
            logical: slate_query::LogicalOp::And,
            lhs: Box::new(PlanNode::IndexScan {
                column: "status".into(),
                filter: Some(IndexFilter::Eq(RawBsonRef::String("active"))),
                direction: slate_query::SortDirection::Asc,
                limit: None,
                complete_groups: false,
                covered: false,
            }),
            rhs: Box::new(PlanNode::IndexScan {
                column: "contacts_count".into(),
                filter: Some(IndexFilter::Eq(RawBsonRef::Int32(50))),
                direction: slate_query::SortDirection::Asc,
                limit: None,
                complete_groups: false,
                covered: false,
            }),
        };

        group.bench_with_input(BenchmarkId::new("and", n), &plan_and, |b, plan| {
            let txn = engine.store().begin(true).unwrap();
            let cf = txn.cf("test").unwrap();
            b.iter(|| consume_rows(Executor::new(&txn, &cf).execute(plan.clone()).unwrap()))
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
        // Empty engine with collection + indexes, docs inserted per iteration
        let engine = {
            let engine = Engine::new(MemoryStore::new());
            let mut txn = engine.begin(false).unwrap();
            txn.create_collection(&CollectionConfig {
                name: "test".into(),
                indexes: vec!["status".into(), "contacts_count".into()],
            })
            .unwrap();
            txn.commit().unwrap();
            engine
        };

        let docs = generate_docs(n);
        let fields = vec!["status".into(), "contacts_count".into()];
        let plan = PlanNode::InsertIndex {
            indexed_fields: &fields,
            input: Box::new(PlanNode::InsertRecord {
                input: Box::new(PlanNode::Values { docs }),
            }),
        };

        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, plan| {
            b.iter_batched(
                || {
                    let txn = engine.store().begin(false).unwrap();
                    let cf = txn.cf("test").unwrap();
                    (txn, cf)
                },
                |(txn, cf)| {
                    let result = Executor::new(&txn, &cf).execute(plan.clone()).unwrap();
                    match result {
                        ExecutionResult::Insert { ids } => ids.len(),
                        _ => panic!("expected Insert"),
                    }
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

fn bench_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("update");
    for n in [100, 1_000] {
        let engine = seeded_engine(n);

        // Update: set status = "updated" for all docs via Scan → ReadRecord → Update
        let fields = vec!["status".into(), "contacts_count".into()];
        let plan = PlanNode::InsertIndex {
            indexed_fields: &fields,
            input: Box::new(PlanNode::Update {
                mutation: slate_query::parse_mutation(
                    &bson::rawdoc! { "$set": { "status": "updated" } },
                )
                .unwrap(),
                input: Box::new(PlanNode::ReadRecord {
                    input: Box::new(PlanNode::Scan),
                }),
            }),
        };

        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, plan| {
            b.iter_batched(
                || {
                    let txn = engine.store().begin(false).unwrap();
                    let cf = txn.cf("test").unwrap();
                    (txn, cf)
                },
                |(txn, cf)| {
                    let result = Executor::new(&txn, &cf).execute(plan.clone()).unwrap();
                    match result {
                        ExecutionResult::Update { modified, .. } => modified,
                        _ => panic!("expected Update"),
                    }
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");
    for n in [100, 1_000] {
        let engine = seeded_engine(n);

        // Delete all docs: Scan → ReadRecord → DeleteIndex → Delete
        let fields = vec!["status".into(), "contacts_count".into()];
        let plan = PlanNode::Delete {
            input: Box::new(PlanNode::DeleteIndex {
                indexed_fields: &fields,
                input: Box::new(PlanNode::ReadRecord {
                    input: Box::new(PlanNode::Scan),
                }),
            }),
        };

        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, plan| {
            b.iter_batched(
                || {
                    let txn = engine.store().begin(false).unwrap();
                    let cf = txn.cf("test").unwrap();
                    (txn, cf)
                },
                |(txn, cf)| {
                    let result = Executor::new(&txn, &cf).execute(plan.clone()).unwrap();
                    match result {
                        ExecutionResult::Delete { deleted } => deleted,
                        _ => panic!("expected Delete"),
                    }
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

fn bench_replace(c: &mut Criterion) {
    let mut group = c.benchmark_group("replace");
    for n in [100, 1_000] {
        let engine = seeded_engine(n);

        // Replace all docs with a new document
        let fields = vec!["status".into(), "contacts_count".into()];
        let plan = PlanNode::InsertIndex {
            indexed_fields: &fields,
            input: Box::new(PlanNode::Replace {
                replacement: bson::rawdoc! {
                    "name": "Replaced",
                    "status": "replaced",
                    "contacts_count": 0,
                },
                input: Box::new(PlanNode::ReadRecord {
                    input: Box::new(PlanNode::Scan),
                }),
            }),
        };

        group.bench_with_input(BenchmarkId::from_parameter(n), &plan, |b, plan| {
            b.iter_batched(
                || {
                    let txn = engine.store().begin(false).unwrap();
                    let cf = txn.cf("test").unwrap();
                    (txn, cf)
                },
                |(txn, cf)| {
                    let result = Executor::new(&txn, &cf).execute(plan.clone()).unwrap();
                    match result {
                        ExecutionResult::Update { modified, .. } => modified,
                        _ => panic!("expected Update"),
                    }
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

fn bench_upsert_replace(c: &mut Criterion) {
    use slate_db::bench::UpsertMode;

    let mut group = c.benchmark_group("upsert_replace");
    let indexed = vec!["status".into(), "contacts_count".into()];

    for n in [100, 1_000] {
        let engine = seeded_engine(n);
        let raw_docs: Vec<bson::RawDocumentBuf> = (0..n)
            .map(|i| {
                rawdoc! {
                    "_id": format!("rec-{i}"),
                    "name": format!("Replaced {i}"),
                    "status": "replaced",
                    "contacts_count": 0,
                }
            })
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {
                    let txn = engine.store().begin(false).unwrap();
                    let cf = txn.cf("test").unwrap();
                    let plan = PlanNode::InsertIndex {
                        indexed_fields: &indexed,
                        input: Box::new(PlanNode::Upsert {
                            mode: UpsertMode::Replace,
                            indexed_fields: &indexed,
                            input: Box::new(PlanNode::Values {
                                docs: raw_docs.clone(),
                            }),
                        }),
                    };
                    (txn, cf, plan)
                },
                |(txn, cf, plan)| {
                    let result = Executor::new(&txn, &cf).execute(plan.clone()).unwrap();
                    match result {
                        ExecutionResult::Upsert { updated, .. } => updated,
                        _ => panic!("expected Upsert"),
                    }
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

fn bench_upsert_merge(c: &mut Criterion) {
    use slate_db::bench::UpsertMode;

    let mut group = c.benchmark_group("upsert_merge");
    let indexed = vec!["status".into(), "contacts_count".into()];

    for n in [100, 1_000] {
        let engine = seeded_engine(n);
        let raw_docs: Vec<bson::RawDocumentBuf> = (0..n)
            .map(|i| {
                rawdoc! {
                    "_id": format!("rec-{i}"),
                    "email": format!("user{i}@test.com"),
                }
            })
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {
                    let txn = engine.store().begin(false).unwrap();
                    let cf = txn.cf("test").unwrap();
                    let plan = PlanNode::InsertIndex {
                        indexed_fields: &indexed,
                        input: Box::new(PlanNode::Upsert {
                            mode: UpsertMode::Merge,
                            indexed_fields: &indexed,
                            input: Box::new(PlanNode::Values {
                                docs: raw_docs.clone(),
                            }),
                        }),
                    };
                    (txn, cf, plan)
                },
                |(txn, cf, plan)| {
                    let result = Executor::new(&txn, &cf).execute(plan.clone()).unwrap();
                    match result {
                        ExecutionResult::Upsert { updated, .. } => updated,
                        _ => panic!("expected Upsert"),
                    }
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

fn bench_upsert_insert(c: &mut Criterion) {
    use slate_db::bench::UpsertMode;

    let mut group = c.benchmark_group("upsert_insert");
    let indexed = vec!["status".into(), "contacts_count".into()];

    for n in [100, 1_000] {
        // Empty collection — upsert acts as pure insert
        let engine = {
            let engine = Engine::new(MemoryStore::new());
            let mut txn = engine.begin(false).unwrap();
            txn.create_collection(&CollectionConfig {
                name: "test".into(),
                indexes: vec!["status".into(), "contacts_count".into()],
            })
            .unwrap();
            txn.commit().unwrap();
            engine
        };

        let raw_docs: Vec<bson::RawDocumentBuf> = (0..n)
            .map(|i| {
                rawdoc! {
                    "_id": format!("new-{i}"),
                    "name": format!("New {i}"),
                    "status": "pending",
                    "contacts_count": (i % 100) as i32,
                }
            })
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {
                    let txn = engine.store().begin(false).unwrap();
                    let cf = txn.cf("test").unwrap();
                    let plan = PlanNode::InsertIndex {
                        indexed_fields: &indexed,
                        input: Box::new(PlanNode::Upsert {
                            mode: UpsertMode::Replace,
                            indexed_fields: &indexed,
                            input: Box::new(PlanNode::Values {
                                docs: raw_docs.clone(),
                            }),
                        }),
                    };
                    (txn, cf, plan)
                },
                |(txn, cf, plan)| {
                    let result = Executor::new(&txn, &cf).execute(plan.clone()).unwrap();
                    match result {
                        ExecutionResult::Upsert { inserted, .. } => inserted,
                        _ => panic!("expected Upsert"),
                    }
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

// ── Engine API benchmarks ───────────────────────────────────
//
// These benchmark the full Engine API path: engine.begin() → txn.find() / txn.insert_many() etc.
// They use realistic data with nullable fields, arrays, and multiple indexed fields.

const STATUSES: &[&str] = &["active", "rejected"];
const REC1: &[&str] = &["ProductA", "ProductB", "ProductC"];
const REC2: &[&str] = &["ProductX", "ProductY", "ProductZ"];
const REC3: &[&str] = &["Widget1", "Widget2", "Widget3"];
const TAGS: &[&str] = &[
    "renewal_due",
    "high_value",
    "churning",
    "new_customer",
    "enterprise",
];

fn generate_realistic_doc(rng: &mut StdRng, seq: usize) -> bson::Document {
    let mut doc = bson::doc! {
        "_id": format!("rec-{seq}"),
        "name": format!("Company-{seq}"),
        "status": STATUSES[rng.gen_range(0..STATUSES.len())],
        "contacts_count": rng.gen_range(0_i32..100),
        "product_recommendation1": REC1[rng.gen_range(0..REC1.len())],
        "product_recommendation2": REC2[rng.gen_range(0..REC2.len())],
        "product_recommendation3": REC3[rng.gen_range(0..REC3.len())],
    };

    let tag_count = rng.gen_range(2..=4);
    let tags: Vec<&str> = (0..tag_count)
        .map(|_| TAGS[rng.gen_range(0..TAGS.len())])
        .collect();
    doc.insert("tags", tags);

    if rng.gen_ratio(7, 10) {
        let epoch_secs = rng.gen_range(1_700_000_000_i64..1_740_000_000);
        doc.insert(
            "last_contacted_at",
            bson::Bson::DateTime(bson::DateTime::from_millis(epoch_secs * 1000)),
        );
    }

    if rng.gen_bool(0.5) {
        doc.insert("notes", format!("Note for {seq}"));
    }

    doc
}

fn generate_realistic_batch(count: usize) -> Vec<bson::Document> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..count)
        .map(|i| generate_realistic_doc(&mut rng, i))
        .collect()
}

fn realistic_seeded_engine(n: usize) -> Engine<MemoryStore> {
    let engine = Engine::new(MemoryStore::new());
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "bench".into(),
        indexes: vec!["status".into(), "contacts_count".into()],
    })
    .unwrap();
    let docs = generate_realistic_batch(n);
    for chunk in docs.chunks(1000) {
        txn.insert_many("bench", chunk.to_vec()).unwrap();
    }
    txn.commit().unwrap();
    engine
}

// ── Bulk Insert ─────────────────────────────────────────────

fn bench_bulk_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_insert");
    for n in [1_000, 10_000] {
        let engine = {
            let engine = Engine::new(MemoryStore::new());
            let mut txn = engine.begin(false).unwrap();
            txn.create_collection(&CollectionConfig {
                name: "bench".into(),
                indexes: vec!["status".into(), "contacts_count".into()],
            })
            .unwrap();
            txn.commit().unwrap();
            engine
        };
        let docs = generate_realistic_batch(n);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {
                    let txn = engine.begin(false).unwrap();
                    (txn, docs.clone())
                },
                |(mut txn, docs)| {
                    txn.insert_many("bench", docs).unwrap();
                    // Don't commit — let txn drop so engine stays empty for next iteration
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

// ── Query Benchmarks ────────────────────────────────────────

fn bench_query_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_scan");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = Query {
            filter: None,
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", query.clone())
                    .unwrap()
                    .iter()
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

fn bench_query_indexed_eq(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_indexed_eq");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = Query {
            filter: Some(rawdoc! { "status": "active" }),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", query.clone())
                    .unwrap()
                    .iter()
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

fn bench_query_indexed_eq_projection(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_indexed_eq_proj");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = Query {
            filter: Some(rawdoc! { "status": "active" }),
            sort: vec![],
            skip: None,
            take: None,
            columns: Some(vec!["status".into()]),
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", query.clone())
                    .unwrap()
                    .iter()
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

fn bench_query_multi_field_and(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_multi_and");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = Query {
            filter: Some(rawdoc! {
                "status": "active",
                "product_recommendation1": "ProductA",
                "product_recommendation2": "ProductX",
            }),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", query.clone())
                    .unwrap()
                    .iter()
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

fn bench_query_null_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_null_filter");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = Query {
            filter: Some(rawdoc! { "last_contacted_at": { "$exists": false } }),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", query.clone())
                    .unwrap()
                    .iter()
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

fn bench_query_sort_indexed(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_sort_indexed");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = Query {
            filter: Some(rawdoc! { "status": "active" }),
            sort: vec![Sort {
                field: "contacts_count".into(),
                direction: SortDirection::Desc,
            }],
            skip: None,
            take: None,
            columns: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", query.clone())
                    .unwrap()
                    .iter()
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

fn bench_query_sort_indexed_take(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_sort_indexed_take");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = Query {
            filter: None,
            sort: vec![Sort {
                field: "contacts_count".into(),
                direction: SortDirection::Desc,
            }],
            skip: None,
            take: Some(200),
            columns: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", query.clone())
                    .unwrap()
                    .iter()
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

fn bench_query_sort_multi(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_sort_multi");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = Query {
            filter: None,
            sort: vec![
                Sort {
                    field: "contacts_count".into(),
                    direction: SortDirection::Desc,
                },
                Sort {
                    field: "name".into(),
                    direction: SortDirection::Asc,
                },
            ],
            skip: None,
            take: Some(200),
            columns: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", query.clone())
                    .unwrap()
                    .iter()
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

fn bench_query_pagination(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_pagination");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = Query {
            filter: Some(rawdoc! { "status": "active" }),
            sort: vec![Sort {
                field: "contacts_count".into(),
                direction: SortDirection::Desc,
            }],
            skip: Some(100),
            take: Some(50),
            columns: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", query.clone())
                    .unwrap()
                    .iter()
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

fn bench_query_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_point_lookup");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        // Pick 100 evenly-spaced IDs to look up
        let ids: Vec<String> = (0..n)
            .step_by(n / 100)
            .map(|i| format!("rec-{i}"))
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &ids, |b, ids| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                let mut found = 0usize;
                for id in ids {
                    if txn.find_by_id("bench", id, None).unwrap().is_some() {
                        found += 1;
                    }
                }
                found
            })
        });
    }
    group.finish();
}

fn bench_query_projection(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_projection");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = Query {
            filter: None,
            sort: vec![],
            skip: None,
            take: None,
            columns: Some(vec!["name".into(), "status".into()]),
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", query.clone())
                    .unwrap()
                    .iter()
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

fn bench_query_array_match(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_array_match");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = Query {
            filter: Some(rawdoc! { "tags": "renewal_due" }),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", query.clone())
                    .unwrap()
                    .iter()
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

// ── Distinct Benchmarks ─────────────────────────────────────

fn bench_distinct_indexed_low(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct_indexed_low");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = DistinctQuery {
            field: "status".into(),
            filter: None,
            sort: None,
            skip: None,
            take: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.distinct("bench", query.clone()).unwrap()
            })
        });
    }
    group.finish();
}

fn bench_distinct_indexed_high(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct_indexed_high");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = DistinctQuery {
            field: "contacts_count".into(),
            filter: None,
            sort: None,
            skip: None,
            take: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.distinct("bench", query.clone()).unwrap()
            })
        });
    }
    group.finish();
}

fn bench_distinct_non_indexed(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct_non_indexed");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = DistinctQuery {
            field: "product_recommendation1".into(),
            filter: None,
            sort: None,
            skip: None,
            take: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.distinct("bench", query.clone()).unwrap()
            })
        });
    }
    group.finish();
}

fn bench_distinct_with_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct_with_filter");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let query = DistinctQuery {
            field: "product_recommendation1".into(),
            filter: Some(rawdoc! { "status": "active" }),
            sort: None,
            skip: None,
            take: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.distinct("bench", query.clone()).unwrap()
            })
        });
    }
    group.finish();
}

// ── Range filter benchmarks ─────────────────────────────────
//
// These capture the before/after for index range scans.
// Before: range predicates on indexed fields fall back to Scan + Filter.
// After: planner pushes them into IndexScan with range bounds.

fn bench_query_indexed_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_indexed_range");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        // contacts_count > 50 on indexed field
        let query = Query {
            filter: Some(rawdoc! { "contacts_count": { "$gt": 50 } }),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", query.clone())
                    .unwrap()
                    .iter()
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

fn bench_query_indexed_range_dual(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_indexed_range_dual");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        // contacts_count > 20 AND contacts_count < 80 on indexed field
        let query = Query {
            filter: Some(rawdoc! { "contacts_count": { "$gt": 20, "$lt": 80 } }),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", query.clone())
                    .unwrap()
                    .iter()
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

fn bench_query_indexed_eq_plus_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_indexed_eq_plus_range");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        // status = "active" AND contacts_count > 50 (Eq wins index, range is residual)
        let query = Query {
            filter: Some(rawdoc! { "status": "active", "contacts_count": { "$gt": 50 } }),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &query, |b, query| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", query.clone())
                    .unwrap()
                    .iter()
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    // Executor node benchmarks (store-free)
    bench_values,
    bench_projection,
    bench_limit,
    bench_sort,
    bench_distinct,
    bench_filter,
    // Executor node benchmarks (store-backed)
    bench_scan,
    bench_index_scan,
    bench_read_record,
    bench_index_merge,
    // Executor mutation benchmarks
    bench_insert,
    bench_update,
    bench_delete,
    bench_replace,
    bench_upsert_replace,
    bench_upsert_merge,
    bench_upsert_insert,
    // Engine API benchmarks
    bench_bulk_insert,
    bench_query_scan,
    bench_query_indexed_eq,
    bench_query_indexed_eq_projection,
    bench_query_multi_field_and,
    bench_query_null_filter,
    bench_query_sort_indexed,
    bench_query_sort_indexed_take,
    bench_query_sort_multi,
    bench_query_pagination,
    bench_query_point_lookup,
    bench_query_projection,
    bench_query_array_match,
    bench_distinct_indexed_low,
    bench_distinct_indexed_high,
    bench_distinct_non_indexed,
    bench_distinct_with_filter,
    // Range filter benchmarks
    bench_query_indexed_range,
    bench_query_indexed_range_dual,
    bench_query_indexed_eq_plus_range,
);
criterion_main!(benches);
