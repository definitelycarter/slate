//! Commit throughput parameterized by durability level.
//!
//! Thread A of the Durability & Crash Safety RFC asks the spike to measure
//! `Strict` vs `Buffered` (vs `Relaxed`) commit cost per backend — the numbers
//! that decide whether the per-transaction override is worth the trait change.
//!
//! Each iteration begins a write transaction at a fixed [`Durability`] level,
//! writes a small batch, and commits — so the measured time includes the
//! backend's flush behavior at that level. RocksDB `Strict` fsyncs the WAL;
//! redb `Strict` (`Immediate`) fsyncs the data file. Run serially:
//!
//! ```bash
//! cargo bench -p slate-store --bench durability --features rocksdb,redb
//! ```
//!
//! Bench IDs:
//! - `durability/rocks/commit/{strict,buffered,relaxed}/{1,16}`
//! - `durability/redb/commit/{strict,buffered,relaxed}/{1,16}`

mod common;
use common::generate_kv_pairs;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use slate_store::{Durability, RedbStore, RocksStore, Store, Transaction};

const CF: &str = "bench";

const LEVELS: &[(&str, Durability)] = &[
    ("strict", Durability::Strict),
    ("buffered", Durability::Buffered),
    ("relaxed", Durability::Relaxed),
];

/// Batch sizes: a single small commit (the latency the fsync dominates) and a
/// modest batch (amortized cost).
const BATCHES: &[usize] = &[1, 16];

fn bench_rocks(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability/rocks/commit");
    for (label, level) in LEVELS {
        for &n in BATCHES {
            let dir = tempfile::tempdir().unwrap();
            let store = RocksStore::open(dir.path()).unwrap();
            store.create_cf(CF).unwrap();
            let pairs = generate_kv_pairs(n);

            group.bench_with_input(BenchmarkId::new(*label, n), &n, |b, _| {
                b.iter_batched(
                    || pairs.clone(),
                    |pairs| {
                        let txn = store.begin_with_durability(*level).unwrap();
                        let cf = txn.cf(CF).unwrap();
                        for (k, v) in &pairs {
                            txn.put(&cf, k, v).unwrap();
                        }
                        txn.commit().unwrap();
                    },
                    BatchSize::PerIteration,
                )
            });
        }
    }
    group.finish();
}

fn bench_redb(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability/redb/commit");
    for (label, level) in LEVELS {
        for &n in BATCHES {
            let dir = tempfile::tempdir().unwrap();
            let store = RedbStore::open(&dir.path().join("bench.redb")).unwrap();
            store.create_cf(CF).unwrap();
            let pairs = generate_kv_pairs(n);

            group.bench_with_input(BenchmarkId::new(*label, n), &n, |b, _| {
                b.iter_batched(
                    || pairs.clone(),
                    |pairs| {
                        let txn = store.begin_with_durability(*level).unwrap();
                        let cf = txn.cf(CF).unwrap();
                        for (k, v) in &pairs {
                            txn.put(&cf, k, v).unwrap();
                        }
                        txn.commit().unwrap();
                    },
                    BatchSize::PerIteration,
                )
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_rocks, bench_redb);
criterion_main!(benches);
