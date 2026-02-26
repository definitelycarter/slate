mod common;
use common::*;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use slate_store::{MemoryStore, Store, Transaction};

const CF: &str = "bench";

fn new_store() -> MemoryStore {
    let store = MemoryStore::new();
    store.create_cf(CF).unwrap();
    store
}

fn seeded_store(n: usize) -> MemoryStore {
    let store = new_store();
    seed_store(&store, CF, n);
    store
}

// ── Put ─────────────────────────────────────────────────────

fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory/put");
    for n in [100, 1_000] {
        let store = new_store();
        let pairs = generate_kv_pairs(n);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {
                    let txn = store.begin(false).unwrap();
                    let cf = txn.cf(CF).unwrap();
                    (txn, cf, pairs.clone())
                },
                |(txn, cf, pairs)| {
                    for (k, v) in &pairs {
                        txn.put(&cf, k, v).unwrap();
                    }
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

fn bench_put_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory/put_batch");
    for n in [100, 1_000] {
        let store = new_store();
        let pairs = generate_kv_pairs(n);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {
                    let txn = store.begin(false).unwrap();
                    let cf = txn.cf(CF).unwrap();
                    (txn, cf, pairs.clone())
                },
                |(txn, cf, pairs)| {
                    let refs: Vec<(&[u8], &[u8])> =
                        pairs.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
                    txn.put_batch(&cf, &refs).unwrap();
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

// ── Get ─────────────────────────────────────────────────────

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory/get");
    for n in [100, 1_000] {
        let store = seeded_store(n);
        let pairs = generate_kv_pairs(n);
        let keys: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_slice()).collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = store.begin(true).unwrap();
                let cf = txn.cf(CF).unwrap();
                let mut found = 0usize;
                for key in &keys {
                    if txn.get(&cf, key).unwrap().is_some() {
                        found += 1;
                    }
                }
                found
            })
        });
    }
    group.finish();
}

fn bench_multi_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory/multi_get");
    for n in [100, 1_000] {
        let store = seeded_store(n);
        let pairs = generate_kv_pairs(n);
        let keys: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_slice()).collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = store.begin(true).unwrap();
                let cf = txn.cf(CF).unwrap();
                txn.multi_get(&cf, &keys).unwrap()
            })
        });
    }
    group.finish();
}

// ── Scan ────────────────────────────────────────────────────

fn bench_scan_prefix(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory/scan_prefix");
    for n in [100, 1_000] {
        let store = seeded_store(n);
        let prefix = b"r\x00test\x00";

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = store.begin(true).unwrap();
                let cf = txn.cf(CF).unwrap();
                txn.scan_prefix(&cf, prefix).unwrap().count()
            })
        });
    }
    group.finish();
}

// ── Delete ──────────────────────────────────────────────────

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory/delete");
    for n in [100, 1_000] {
        let store = seeded_store(n);
        let pairs = generate_kv_pairs(n);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {
                    let txn = store.begin(false).unwrap();
                    let cf = txn.cf(CF).unwrap();
                    (txn, cf, pairs.clone())
                },
                |(txn, cf, pairs)| {
                    for (k, _) in &pairs {
                        txn.delete(&cf, k).unwrap();
                    }
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

fn bench_delete_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory/delete_batch");
    for n in [100, 1_000] {
        let store = seeded_store(n);
        let pairs = generate_kv_pairs(n);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {
                    let txn = store.begin(false).unwrap();
                    let cf = txn.cf(CF).unwrap();
                    (txn, cf, pairs.clone())
                },
                |(txn, cf, pairs)| {
                    let keys: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_slice()).collect();
                    txn.delete_batch(&cf, &keys).unwrap();
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_put,
    bench_put_batch,
    bench_get,
    bench_multi_get,
    bench_scan_prefix,
    bench_delete,
    bench_delete_batch,
);
criterion_main!(benches);
