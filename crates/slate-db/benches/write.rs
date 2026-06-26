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

criterion_group!(benches, bench_bulk_insert);
criterion_main!(benches);
