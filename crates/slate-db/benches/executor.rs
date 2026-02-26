mod common;
use common::*;

use bson::raw::RawDocumentBuf;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use slate_db::bench::{Executor, Expression, IndexScanRange, LogicalOp, Node, Plan, ScanDirection};
use slate_engine::{
    BsonValue, Catalog, CollectionHandle, Engine, EngineError, EngineTransaction, IndexEntry,
    IndexRange,
};

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

    fn put_nx(
        &self,
        _handle: &CollectionHandle<Self::Cf>,
        _doc: &bson::raw::RawDocument,
        _doc_id: &BsonValue<'_>,
        _ttl: i64,
    ) -> Result<(), EngineError> {
        panic!("NoopTransaction::put_nx called");
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

criterion_group!(
    benches,
    // Store-free
    bench_values,
    bench_projection,
    bench_limit,
    bench_sort,
    bench_distinct,
    bench_filter,
    // Store-backed
    bench_scan,
    bench_index_scan,
    bench_read_record,
    bench_index_merge,
);
criterion_main!(benches);
