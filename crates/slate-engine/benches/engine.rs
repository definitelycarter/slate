use bson::raw::RawBsonRef;
use bson::rawdoc;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use slate_engine::{
    BsonValue, Catalog, Engine, EngineTransaction, IndexRange, KvEngine,
};
use slate_store::MemoryStore;

// ── Helpers ─────────────────────────────────────────────────

fn str_id(s: &str) -> BsonValue<'static> {
    BsonValue::from_raw_bson_ref(RawBsonRef::String(s))
        .unwrap()
        .into_owned()
}

fn generate_docs(n: usize) -> Vec<bson::RawDocumentBuf> {
    (0..n)
        .map(|i| {
            rawdoc! {
                "_id": format!("rec-{i}"),
                "name": format!("User {i}"),
                "status": if i % 2 == 0 { "active" } else { "rejected" },
                "age": (i % 80) as i32,
            }
        })
        .collect()
}

/// Create a MemoryStore-backed engine with `n` documents in "bench" collection
/// and indexes on "status" and "age".
fn seeded_engine(n: usize) -> KvEngine<MemoryStore> {
    let engine = KvEngine::new(MemoryStore::new());
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "bench").unwrap();
    txn.create_index("bench", "status").unwrap();
    txn.create_index("bench", "age").unwrap();
    let handle = txn.collection("bench").unwrap();
    for doc in generate_docs(n) {
        let id_val = BsonValue::extract(&doc, "_id").unwrap();
        txn.put(&handle, &doc, &id_val).unwrap();
    }
    txn.commit().unwrap();
    engine
}

// ── Point read ──────────────────────────────────────────────

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    for n in [100, 1_000, 10_000] {
        let engine = seeded_engine(n);
        let ids: Vec<BsonValue<'static>> = (0..100)
            .map(|i| str_id(&format!("rec-{}", i * (n / 100))))
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &ids, |b, ids| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                let handle = txn.collection("bench").unwrap();
                let mut found = 0usize;
                for id in ids {
                    if txn.get(&handle, id).unwrap().is_some() {
                        found += 1;
                    }
                }
                txn.rollback().unwrap();
                found
            })
        });
    }
    group.finish();
}

// ── Scan ────────────────────────────────────────────────────

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");
    for n in [100, 1_000, 10_000] {
        let engine = seeded_engine(n);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                let handle = txn.collection("bench").unwrap();
                let count = txn.scan(&handle).unwrap().count();
                txn.rollback().unwrap();
                count
            })
        });
    }
    group.finish();
}

// ── Index scan ──────────────────────────────────────────────

fn bench_index_scan_eq(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_scan_eq");
    for n in [100, 1_000, 10_000] {
        let engine = seeded_engine(n);
        let active = BsonValue::from_raw_bson_ref(RawBsonRef::String("active"))
            .unwrap()
            .to_vec();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                let handle = txn.collection("bench").unwrap();
                let count = txn
                    .scan_index(&handle, "status", IndexRange::Eq(&active), false)
                    .unwrap()
                    .count();
                txn.rollback().unwrap();
                count
            })
        });
    }
    group.finish();
}

fn bench_index_scan_full(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_scan_full");
    for n in [100, 1_000, 10_000] {
        let engine = seeded_engine(n);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                let handle = txn.collection("bench").unwrap();
                let count = txn
                    .scan_index(&handle, "age", IndexRange::Full, false)
                    .unwrap()
                    .count();
                txn.rollback().unwrap();
                count
            })
        });
    }
    group.finish();
}

fn bench_index_scan_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_scan_range");
    for n in [100, 1_000, 10_000] {
        let engine = seeded_engine(n);
        let lower = BsonValue::from_raw_bson_ref(RawBsonRef::Int32(20))
            .unwrap()
            .to_vec();
        let upper = BsonValue::from_raw_bson_ref(RawBsonRef::Int32(60))
            .unwrap()
            .to_vec();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                let handle = txn.collection("bench").unwrap();
                let count = txn
                    .scan_index(
                        &handle,
                        "age",
                        IndexRange::Range {
                            lower: Some(&lower),
                            lower_inclusive: true,
                            upper: Some(&upper),
                            upper_inclusive: false,
                        },
                        false,
                    )
                    .unwrap()
                    .count();
                txn.rollback().unwrap();
                count
            })
        });
    }
    group.finish();
}

// ── Put (insert) ────────────────────────────────────────────

fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("put");
    for n in [100, 1_000] {
        // Empty collection with indexes — fresh inserts each iteration.
        let engine = {
            let engine = KvEngine::new(MemoryStore::new());
            let mut txn = engine.begin(false).unwrap();
            txn.create_collection(None, "bench").unwrap();
            txn.create_index("bench", "status").unwrap();
            txn.create_index("bench", "age").unwrap();
            txn.commit().unwrap();
            engine
        };
        let docs = generate_docs(n);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || docs.clone(),
                |docs| {
                    let txn = engine.begin(false).unwrap();
                    let handle = txn.collection("bench").unwrap();
                    for doc in &docs {
                        let id = BsonValue::extract(doc, "_id").unwrap();
                        txn.put(&handle, doc, &id).unwrap();
                    }
                    // Don't commit — keep engine empty for next iteration.
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

// ── Put (overwrite / upsert) ────────────────────────────────

fn bench_put_overwrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_overwrite");
    for n in [100, 1_000] {
        let engine = seeded_engine(n);
        let updated_docs: Vec<bson::RawDocumentBuf> = (0..n)
            .map(|i| {
                rawdoc! {
                    "_id": format!("rec-{i}"),
                    "name": format!("Updated {i}"),
                    "status": if i % 3 == 0 { "active" } else { "archived" },
                    "age": ((i + 10) % 80) as i32,
                }
            })
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || updated_docs.clone(),
                |docs| {
                    let txn = engine.begin(false).unwrap();
                    let handle = txn.collection("bench").unwrap();
                    for doc in &docs {
                        let id = BsonValue::extract(doc, "_id").unwrap();
                        txn.put(&handle, doc, &id).unwrap();
                    }
                    // Don't commit — engine stays seeded for next iteration.
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

// ── Delete ──────────────────────────────────────────────────

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");
    for n in [100, 1_000] {
        let engine = seeded_engine(n);
        let ids: Vec<BsonValue<'static>> = (0..n)
            .map(|i| str_id(&format!("rec-{i}")))
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || ids.clone(),
                |ids| {
                    let txn = engine.begin(false).unwrap();
                    let handle = txn.collection("bench").unwrap();
                    for id in &ids {
                        txn.delete(&handle, id).unwrap();
                    }
                    // Don't commit.
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

// ── Catalog ─────────────────────────────────────────────────

fn bench_create_index_backfill(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_index_backfill");
    for n in [100, 1_000] {
        // Engine with docs but no index on "name".
        let engine = {
            let engine = KvEngine::new(MemoryStore::new());
            let mut txn = engine.begin(false).unwrap();
            txn.create_collection(None, "bench").unwrap();
            let handle = txn.collection("bench").unwrap();
            for doc in generate_docs(n) {
                let id = BsonValue::extract(&doc, "_id").unwrap();
                txn.put(&handle, &doc, &id).unwrap();
            }
            txn.commit().unwrap();
            engine
        };

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {},
                |_| {
                    let mut txn = engine.begin(false).unwrap();
                    txn.create_index("bench", "name").unwrap();
                    // Don't commit — index doesn't persist.
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    // Read
    bench_get,
    bench_scan,
    // Index reads
    bench_index_scan_eq,
    bench_index_scan_full,
    bench_index_scan_range,
    // Writes
    bench_put,
    bench_put_overwrite,
    bench_delete,
    // Catalog
    bench_create_index_backfill,
);
criterion_main!(benches);
