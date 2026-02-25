use bson::raw::RawDocumentBuf;
use bson::rawdoc;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use slate_db::CollectionConfig;
use slate_db::bench::{
    Executor, Expression, IndexScanRange, LogicalOp, Node, Plan, RawIter, ScanDirection,
    SlateEngine,
};
use slate_engine::{
    BsonValue, Catalog, CollectionHandle, Engine, EngineError, EngineTransaction, IndexEntry,
    IndexRange,
};
use slate_query::*;
use slate_store::MemoryStore;

// ── NoopTransaction ─────────────────────────────────────────
//
// Implements EngineTransaction with panics. Used for nodes that never
// touch the store (Values, Projection, Limit, Sort, Distinct, Filter).

struct NoopTransaction;

impl EngineTransaction for NoopTransaction {
    type Cf = ();

    fn get(
        &self,
        _handle: &CollectionHandle<Self::Cf>,
        _doc_id: &BsonValue<'_>,
        _ttl: i64,
    ) -> Result<Option<RawDocumentBuf>, EngineError> {
        panic!("NoopTransaction::get called");
    }

    fn put(
        &self,
        _handle: &CollectionHandle<Self::Cf>,
        _doc: &bson::raw::RawDocument,
        _doc_id: &BsonValue<'_>,
    ) -> Result<(), EngineError> {
        panic!("NoopTransaction::put called");
    }

    fn delete(
        &self,
        _handle: &CollectionHandle<Self::Cf>,
        _doc_id: &BsonValue<'_>,
    ) -> Result<(), EngineError> {
        panic!("NoopTransaction::delete called");
    }

    fn scan<'a>(
        &'a self,
        _handle: &CollectionHandle<Self::Cf>,
        _ttl: i64,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(BsonValue<'a>, RawDocumentBuf), EngineError>> + 'a>,
        EngineError,
    > {
        panic!("NoopTransaction::scan called");
    }

    fn scan_index<'a>(
        &'a self,
        _handle: &CollectionHandle<Self::Cf>,
        _field: &str,
        _range: IndexRange<'_>,
        _reverse: bool,
        _ttl: i64,
    ) -> Result<Box<dyn Iterator<Item = Result<IndexEntry<'a>, EngineError>> + 'a>, EngineError>
    {
        panic!("NoopTransaction::scan_index called");
    }

    fn commit(self) -> Result<(), EngineError> {
        Ok(())
    }

    fn rollback(self) -> Result<(), EngineError> {
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

fn consume_rows(iter: RawIter) -> usize {
    iter.count()
}

/// Create a seeded MemoryStore-backed Engine with `n` documents and indexes
/// on `status` and `contacts_count`.
fn seeded_engine(n: usize) -> SlateEngine<MemoryStore> {
    let engine = SlateEngine::new(MemoryStore::new());
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

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn);
            b.iter_batched(
                || Plan::Find(Node::Values(docs.clone())),
                |plan| consume_rows(exec.execute(plan).unwrap()),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_projection(c: &mut Criterion) {
    let mut group = c.benchmark_group("projection");
    for n in [100, 1_000, 10_000] {
        let docs = generate_docs(n);

        group.bench_with_input(BenchmarkId::new("select", n), &n, |b, _| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn);
            b.iter_batched(
                || {
                    Plan::Find(Node::Projection {
                        columns: Some(vec!["name".into(), "status".into()]),
                        source: Box::new(Node::Values(docs.clone())),
                    })
                },
                |plan| consume_rows(exec.execute(plan).unwrap()),
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("passthrough", n), &n, |b, _| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn);
            b.iter_batched(
                || {
                    Plan::Find(Node::Projection {
                        columns: None,
                        source: Box::new(Node::Values(docs.clone())),
                    })
                },
                |plan| consume_rows(exec.execute(plan).unwrap()),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_limit(c: &mut Criterion) {
    let mut group = c.benchmark_group("limit");
    let docs = generate_docs(10_000);

    group.bench_with_input(BenchmarkId::new("skip+take", 10_000), &10_000, |b, _| {
        let txn = NoopTransaction;
        let exec = Executor::new(&txn);
        b.iter_batched(
            || {
                Plan::Find(Node::Limit {
                    skip: 100,
                    take: Some(200),
                    source: Box::new(Node::Values(docs.clone())),
                })
            },
            |plan| consume_rows(exec.execute(plan).unwrap()),
            BatchSize::SmallInput,
        )
    });

    group.bench_with_input(BenchmarkId::new("take", 10_000), &10_000, |b, _| {
        let txn = NoopTransaction;
        let exec = Executor::new(&txn);
        b.iter_batched(
            || {
                Plan::Find(Node::Limit {
                    skip: 0,
                    take: Some(200),
                    source: Box::new(Node::Values(docs.clone())),
                })
            },
            |plan| consume_rows(exec.execute(plan).unwrap()),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort");
    for n in [100, 1_000, 10_000] {
        let docs = generate_docs(n);

        group.bench_with_input(BenchmarkId::new("single", n), &n, |b, _| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn);
            b.iter_batched(
                || {
                    Plan::Find(Node::Sort {
                        sorts: vec![slate_query::Sort {
                            field: "contacts_count".into(),
                            direction: slate_query::SortDirection::Desc,
                        }],
                        source: Box::new(Node::Values(docs.clone())),
                    })
                },
                |plan| consume_rows(exec.execute(plan).unwrap()),
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("multi", n), &n, |b, _| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn);
            b.iter_batched(
                || {
                    Plan::Find(Node::Sort {
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
                        source: Box::new(Node::Values(docs.clone())),
                    })
                },
                |plan| consume_rows(exec.execute(plan).unwrap()),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_distinct(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct");
    for n in [100, 1_000, 10_000] {
        let docs = generate_docs(n);

        group.bench_with_input(BenchmarkId::new("low_card", n), &n, |b, _| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn);
            b.iter_batched(
                || {
                    Plan::Find(Node::Distinct {
                        field: "status".into(),
                        source: Box::new(Node::Projection {
                            columns: Some(vec!["status".into()]),
                            source: Box::new(Node::Values(docs.clone())),
                        }),
                    })
                },
                |plan| consume_rows(exec.execute(plan).unwrap()),
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("sorted", n), &n, |b, _| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn);
            b.iter_batched(
                || {
                    Plan::Find(Node::Sort {
                        sorts: vec![slate_query::Sort {
                            field: "status".into(),
                            direction: slate_query::SortDirection::Asc,
                        }],
                        source: Box::new(Node::Distinct {
                            field: "status".into(),
                            source: Box::new(Node::Projection {
                                columns: Some(vec!["status".into()]),
                                source: Box::new(Node::Values(docs.clone())),
                            }),
                        }),
                    })
                },
                |plan| consume_rows(exec.execute(plan).unwrap()),
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("sorted_hc", n), &n, |b, _| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn);
            b.iter_batched(
                || {
                    Plan::Find(Node::Sort {
                        sorts: vec![slate_query::Sort {
                            field: "contacts_count".into(),
                            direction: slate_query::SortDirection::Desc,
                        }],
                        source: Box::new(Node::Distinct {
                            field: "contacts_count".into(),
                            source: Box::new(Node::Projection {
                                columns: Some(vec!["contacts_count".into()]),
                                source: Box::new(Node::Values(docs.clone())),
                            }),
                        }),
                    })
                },
                |plan| consume_rows(exec.execute(plan).unwrap()),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter");
    for n in [100, 1_000, 10_000] {
        let docs = generate_docs(n);

        group.bench_with_input(BenchmarkId::new("eq", n), &n, |b, _| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn);
            b.iter_batched(
                || {
                    Plan::Find(Node::Filter {
                        predicate: Expression::Eq(
                            "status".into(),
                            bson::Bson::String("active".into()),
                        ),
                        source: Box::new(Node::Values(docs.clone())),
                    })
                },
                |plan| consume_rows(exec.execute(plan).unwrap()),
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("and", n), &n, |b, _| {
            let txn = NoopTransaction;
            let exec = Executor::new(&txn);
            b.iter_batched(
                || {
                    Plan::Find(Node::Filter {
                        predicate: Expression::And(vec![
                            Expression::Eq("status".into(), bson::Bson::String("active".into())),
                            Expression::Gt("contacts_count".into(), bson::Bson::Int32(50)),
                        ]),
                        source: Box::new(Node::Values(docs.clone())),
                    })
                },
                |plan| consume_rows(exec.execute(plan).unwrap()),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

// ── Store-backed benchmarks ─────────────────────────────────

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");
    for n in [100, 1_000, 10_000] {
        let engine = seeded_engine(n);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            let txn = engine.kv_engine().begin(true).unwrap();
            let collection = txn.collection("test").unwrap();
            b.iter_batched(
                || {
                    Plan::Find(Node::Scan {
                        collection: collection.clone(),
                    })
                },
                |plan| {
                    let exec = Executor::new(&txn);
                    consume_rows(exec.execute(plan).unwrap())
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_index_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_scan");
    for n in [100, 1_000, 10_000] {
        let engine = seeded_engine(n);

        // Eq scan: status = "active" (~50% match)
        group.bench_with_input(BenchmarkId::new("eq", n), &n, |b, _| {
            let txn = engine.kv_engine().begin(true).unwrap();
            let collection = txn.collection("test").unwrap();
            b.iter_batched(
                || {
                    Plan::Find(Node::IndexScan {
                        collection: collection.clone(),
                        field: "status".into(),
                        range: IndexScanRange::Eq(bson::Bson::String("active".into())),
                        direction: ScanDirection::Forward,
                        limit: None,
                        covered: false,
                    })
                },
                |plan| {
                    let exec = Executor::new(&txn);
                    consume_rows(exec.execute(plan).unwrap())
                },
                BatchSize::SmallInput,
            )
        });

        // Full column scan (no value filter)
        group.bench_with_input(BenchmarkId::new("full", n), &n, |b, _| {
            let txn = engine.kv_engine().begin(true).unwrap();
            let collection = txn.collection("test").unwrap();
            b.iter_batched(
                || {
                    Plan::Find(Node::IndexScan {
                        collection: collection.clone(),
                        field: "contacts_count".into(),
                        range: IndexScanRange::Full,
                        direction: ScanDirection::Forward,
                        limit: None,
                        covered: false,
                    })
                },
                |plan| {
                    let exec = Executor::new(&txn);
                    consume_rows(exec.execute(plan).unwrap())
                },
                BatchSize::SmallInput,
            )
        });

        // Desc with limit
        group.bench_with_input(BenchmarkId::new("desc_limit", n), &n, |b, _| {
            let txn = engine.kv_engine().begin(true).unwrap();
            let collection = txn.collection("test").unwrap();
            b.iter_batched(
                || {
                    Plan::Find(Node::IndexScan {
                        collection: collection.clone(),
                        field: "contacts_count".into(),
                        range: IndexScanRange::Full,
                        direction: ScanDirection::Reverse,
                        limit: Some(50),
                        covered: false,
                    })
                },
                |plan| {
                    let exec = Executor::new(&txn);
                    consume_rows(exec.execute(plan).unwrap())
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_read_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_record");
    for n in [100, 1_000, 10_000] {
        let engine = seeded_engine(n);

        // KeyLookup over Scan (passthrough — Scan already yields full docs)
        group.bench_with_input(BenchmarkId::new("scan", n), &n, |b, _| {
            let txn = engine.kv_engine().begin(true).unwrap();
            let collection = txn.collection("test").unwrap();
            b.iter_batched(
                || {
                    Plan::Find(Node::KeyLookup {
                        collection: collection.clone(),
                        source: Box::new(Node::Scan {
                            collection: collection.clone(),
                        }),
                    })
                },
                |plan| {
                    let exec = Executor::new(&txn);
                    consume_rows(exec.execute(plan).unwrap())
                },
                BatchSize::SmallInput,
            )
        });

        // KeyLookup over IndexScan (fetches full doc by id)
        group.bench_with_input(BenchmarkId::new("index", n), &n, |b, _| {
            let txn = engine.kv_engine().begin(true).unwrap();
            let collection = txn.collection("test").unwrap();
            b.iter_batched(
                || {
                    Plan::Find(Node::KeyLookup {
                        collection: collection.clone(),
                        source: Box::new(Node::IndexScan {
                            collection: collection.clone(),
                            field: "status".into(),
                            range: IndexScanRange::Eq(bson::Bson::String("active".into())),
                            direction: ScanDirection::Forward,
                            limit: None,
                            covered: false,
                        }),
                    })
                },
                |plan| {
                    let exec = Executor::new(&txn);
                    consume_rows(exec.execute(plan).unwrap())
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_index_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_merge");
    for n in [100, 1_000, 10_000] {
        let engine = seeded_engine(n);

        // OR merge: status="active" OR contacts_count=50
        group.bench_with_input(BenchmarkId::new("or", n), &n, |b, _| {
            let txn = engine.kv_engine().begin(true).unwrap();
            let collection = txn.collection("test").unwrap();
            b.iter_batched(
                || {
                    Plan::Find(Node::IndexMerge {
                        logical: LogicalOp::Or,
                        lhs: Box::new(Node::IndexScan {
                            collection: collection.clone(),
                            field: "status".into(),
                            range: IndexScanRange::Eq(bson::Bson::String("active".into())),
                            direction: ScanDirection::Forward,
                            limit: None,
                            covered: false,
                        }),
                        rhs: Box::new(Node::IndexScan {
                            collection: collection.clone(),
                            field: "contacts_count".into(),
                            range: IndexScanRange::Eq(bson::Bson::Int32(50)),
                            direction: ScanDirection::Forward,
                            limit: None,
                            covered: false,
                        }),
                    })
                },
                |plan| {
                    let exec = Executor::new(&txn);
                    consume_rows(exec.execute(plan).unwrap())
                },
                BatchSize::SmallInput,
            )
        });

        // AND merge: status="active" AND contacts_count=50
        group.bench_with_input(BenchmarkId::new("and", n), &n, |b, _| {
            let txn = engine.kv_engine().begin(true).unwrap();
            let collection = txn.collection("test").unwrap();
            b.iter_batched(
                || {
                    Plan::Find(Node::IndexMerge {
                        logical: LogicalOp::And,
                        lhs: Box::new(Node::IndexScan {
                            collection: collection.clone(),
                            field: "status".into(),
                            range: IndexScanRange::Eq(bson::Bson::String("active".into())),
                            direction: ScanDirection::Forward,
                            limit: None,
                            covered: false,
                        }),
                        rhs: Box::new(Node::IndexScan {
                            collection: collection.clone(),
                            field: "contacts_count".into(),
                            range: IndexScanRange::Eq(bson::Bson::Int32(50)),
                            direction: ScanDirection::Forward,
                            limit: None,
                            covered: false,
                        }),
                    })
                },
                |plan| {
                    let exec = Executor::new(&txn);
                    consume_rows(exec.execute(plan).unwrap())
                },
                BatchSize::SmallInput,
            )
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
            let engine = SlateEngine::new(MemoryStore::new());
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

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {
                    let txn = engine.kv_engine().begin(false).unwrap();
                    let collection = txn.collection("test").unwrap();
                    let plan = Plan::Insert {
                        collection,
                        source: Node::Values(docs.clone()),
                    };
                    (txn, plan)
                },
                |(txn, plan)| {
                    let exec = Executor::new(&txn);
                    let iter = exec.execute(plan).unwrap();
                    iter.count()
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

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {
                    let txn = engine.kv_engine().begin(false).unwrap();
                    let collection = txn.collection("test").unwrap();
                    let plan = Plan::Update {
                        collection: collection.clone(),
                        mutation: slate_query::parse_mutation(
                            &bson::rawdoc! { "$set": { "status": "updated" } },
                        )
                        .unwrap(),
                        source: Node::Scan { collection },
                    };
                    (txn, plan)
                },
                |(txn, plan)| {
                    let exec = Executor::new(&txn);
                    let iter = exec.execute(plan).unwrap();
                    let modified: u64 =
                        iter.map(|r| r.unwrap()).filter(|opt| opt.is_some()).count() as u64;
                    modified
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

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {
                    let txn = engine.kv_engine().begin(false).unwrap();
                    let collection = txn.collection("test").unwrap();
                    let plan = Plan::Delete {
                        collection: collection.clone(),
                        source: Node::Scan { collection },
                    };
                    (txn, plan)
                },
                |(txn, plan)| {
                    let exec = Executor::new(&txn);
                    let iter = exec.execute(plan).unwrap();
                    let deleted: u64 = iter.map(|r| r.unwrap()).count() as u64;
                    deleted
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

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {
                    let txn = engine.kv_engine().begin(false).unwrap();
                    let collection = txn.collection("test").unwrap();
                    let plan = Plan::Replace {
                        collection: collection.clone(),
                        replacement: bson::rawdoc! {
                            "name": "Replaced",
                            "status": "replaced",
                            "contacts_count": 0,
                        },
                        source: Node::Scan { collection },
                    };
                    (txn, plan)
                },
                |(txn, plan)| {
                    let exec = Executor::new(&txn);
                    let iter = exec.execute(plan).unwrap();
                    let modified: u64 =
                        iter.map(|r| r.unwrap()).filter(|opt| opt.is_some()).count() as u64;
                    modified
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

fn bench_upsert_replace(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert_replace");

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
                    let txn = engine.kv_engine().begin(false).unwrap();
                    let collection = txn.collection("test").unwrap();
                    let plan = Plan::Upsert {
                        collection,
                        source: Node::Values(raw_docs.clone()),
                    };
                    (txn, plan)
                },
                |(txn, plan)| {
                    let exec = Executor::new(&txn);
                    let iter = exec.execute(plan).unwrap();
                    let mut count = 0u64;
                    for r in iter {
                        r.unwrap();
                        count += 1;
                    }
                    count
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

fn bench_upsert_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert_merge");

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
                    let txn = engine.kv_engine().begin(false).unwrap();
                    let collection = txn.collection("test").unwrap();
                    let plan = Plan::Merge {
                        collection,
                        source: Node::Values(raw_docs.clone()),
                    };
                    (txn, plan)
                },
                |(txn, plan)| {
                    let exec = Executor::new(&txn);
                    let iter = exec.execute(plan).unwrap();
                    let mut count = 0u64;
                    for r in iter {
                        r.unwrap();
                        count += 1;
                    }
                    count
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

fn bench_upsert_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert_insert");

    for n in [100, 1_000] {
        // Empty collection — upsert acts as pure insert
        let engine = {
            let engine = SlateEngine::new(MemoryStore::new());
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
                    let txn = engine.kv_engine().begin(false).unwrap();
                    let collection = txn.collection("test").unwrap();
                    let plan = Plan::Upsert {
                        collection,
                        source: Node::Values(raw_docs.clone()),
                    };
                    (txn, plan)
                },
                |(txn, plan)| {
                    let exec = Executor::new(&txn);
                    let iter = exec.execute(plan).unwrap();
                    let mut count = 0u64;
                    for r in iter {
                        r.unwrap();
                        count += 1;
                    }
                    count
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

fn realistic_seeded_engine(n: usize) -> SlateEngine<MemoryStore> {
    let engine = SlateEngine::new(MemoryStore::new());
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
            let engine = SlateEngine::new(MemoryStore::new());
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

fn bench_query_indexed_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_indexed_range");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
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
