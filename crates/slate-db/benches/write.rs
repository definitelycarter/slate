mod common;
use common::*;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use slate_db::DatabaseBuilder;
use slate_db::v2::IndexOptions;
use slate_store::MemoryStore;

// ── Bulk Insert ─────────────────────────────────────────────

fn bench_bulk_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_insert");
    for n in [1_000, 10_000] {
        let engine = {
            let engine = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
            let txn = engine.begin(false).unwrap();
            engine.collections().create("bench").execute(&txn).unwrap();
            engine
                .collection("bench")
                .indexes()
                .create("status", IndexOptions::default())
                .execute(&txn)
                .unwrap();
            engine
                .collection("bench")
                .indexes()
                .create("contacts_count", IndexOptions::default())
                .execute(&txn)
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
                |(txn, docs)| {
                    engine
                        .collection("bench")
                        .insert_many(docs)
                        .execute(&txn)
                        .unwrap();
                    // Don't commit — let txn drop so engine stays empty for next iteration
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

// ── Insert with a native validator ──────────────────────────

fn bench_insert_validated(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_validated");
    let n = 1_000;
    let docs = generate_realistic_batch(n);
    // `none` = inserts with no validator (the untouched write path); `native` =
    // the same inserts behind one bound pass-through validator. The delta is the
    // per-write native-validator overhead (resolve once, then per-row
    // ctx + catch_unwind + trait call).
    for (label, validated) in [("none", false), ("native", true)] {
        let engine = insert_validator_engine(validated);
        group.bench_function(label, |b| {
            b.iter_batched(
                || (engine.begin(false).unwrap(), docs.clone()),
                |(txn, docs)| {
                    engine
                        .collection("bench")
                        .insert_many(docs)
                        .execute(&txn)
                        .unwrap();
                    // Don't commit — let txn drop so the engine stays empty.
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

// ── Insert with a native trigger ────────────────────────────

fn bench_insert_triggered(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_triggered");
    let n = 1_000;
    let docs = generate_realistic_batch(n);
    // `none` = inserts with no trigger (the untouched write path); `native` = the
    // same inserts firing one bound pass-through trigger (twice per insert:
    // `inserting` + `inserted`). The delta is the per-write native-trigger
    // overhead (resolve once, then per-fire ctx + CfScopedTxn + catch_unwind +
    // trait call).
    for (label, triggered) in [("none", false), ("native", true)] {
        let engine = insert_trigger_engine(triggered);
        group.bench_function(label, |b| {
            b.iter_batched(
                || (engine.begin(false).unwrap(), docs.clone()),
                |(txn, docs)| {
                    engine
                        .collection("bench")
                        .insert_many(docs)
                        .execute(&txn)
                        .unwrap();
                    // Don't commit — let txn drop so the engine stays empty.
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_bulk_insert,
    bench_insert_validated,
    bench_insert_triggered
);
criterion_main!(benches);
