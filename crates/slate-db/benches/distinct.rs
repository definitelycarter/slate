mod common;
use common::*;

use bson::rawdoc;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

// ── Distinct Benchmarks ─────────────────────────────────────

fn bench_distinct_indexed_low(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct_indexed_low");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .find(rawdoc! {})
                    .distinct("status")
                    .collect(&txn)
                    .unwrap()
            })
        });
    }
    group.finish();
}

fn bench_distinct_indexed_high(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct_indexed_high");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .find(rawdoc! {})
                    .distinct("contacts_count")
                    .collect(&txn)
                    .unwrap()
            })
        });
    }
    group.finish();
}

fn bench_distinct_non_indexed(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct_non_indexed");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .find(rawdoc! {})
                    .distinct("product_recommendation1")
                    .collect(&txn)
                    .unwrap()
            })
        });
    }
    group.finish();
}

fn bench_distinct_with_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct_with_filter");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let filter = rawdoc! { "status": "active" };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .find(filter.clone())
                    .distinct("product_recommendation1")
                    .collect(&txn)
                    .unwrap()
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_distinct_indexed_low,
    bench_distinct_indexed_high,
    bench_distinct_non_indexed,
    bench_distinct_with_filter,
);
criterion_main!(benches);
