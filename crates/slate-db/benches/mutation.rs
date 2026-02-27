mod common;
use common::*;

use bson::raw::RawDocumentBuf;
use bson::rawdoc;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use slate_db::bench::{Database, Executor, Node, Plan};
use slate_db::{CollectionConfig, DatabaseConfig};
use slate_engine::{Catalog, Engine};
use slate_store::MemoryStore;

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
            let engine = Database::open(MemoryStore::new(), DatabaseConfig::default());
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
                        mutation: slate_db::bench::parse_mutation(
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
        let raw_docs: Vec<RawDocumentBuf> = (0..n)
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
        let raw_docs: Vec<RawDocumentBuf> = (0..n)
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
            let engine = Database::open(MemoryStore::new(), DatabaseConfig::default());
            let mut txn = engine.begin(false).unwrap();
            txn.create_collection(&CollectionConfig {
                name: "test".into(),
                indexes: vec!["status".into(), "contacts_count".into()],
            })
            .unwrap();
            txn.commit().unwrap();
            engine
        };

        let raw_docs: Vec<RawDocumentBuf> = (0..n)
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

criterion_group!(
    benches,
    bench_insert,
    bench_update,
    bench_delete,
    bench_replace,
    bench_upsert_replace,
    bench_upsert_merge,
    bench_upsert_insert,
);
criterion_main!(benches);
