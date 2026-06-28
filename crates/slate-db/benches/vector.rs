mod common;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use slate_db::DatabaseBuilder;
use slate_db::VectorMetric;
use slate_db::bench::Database;
use slate_db::v2::VectorIndexOptions;
use slate_store::MemoryStore;

// ── Vector kNN benchmarks ───────────────────────────────────
//
// A flat vector index of `DIMS` dimensions over `N` random embeddings, queried as
// `ORDER BY VECTORDISTANCE(...) LIMIT K`, across the three stored widths. The
// float32 index scans exact and emits the top-k directly; the quantized widths
// scan an approximate copy, then rescore the over-sampled shortlist against each
// document's exact float32 (so they trade a bounded set of point reads for a
// 2-4x smaller footprint). The footprint per width is printed once per run.

const DIMS: usize = 256;
const K: usize = 10;
const N: usize = 10_000;

#[derive(Clone, Copy)]
enum Width {
    Float32,
    Float16,
    Int8,
}

impl Width {
    fn label(self) -> &'static str {
        match self {
            Width::Float32 => "float32",
            Width::Float16 => "float16",
            Width::Int8 => "int8",
        }
    }

    fn options(self) -> VectorIndexOptions {
        match self {
            Width::Float32 => VectorIndexOptions::float32(DIMS as u32, VectorMetric::Cosine),
            Width::Float16 => VectorIndexOptions::float16(DIMS as u32, VectorMetric::Cosine),
            Width::Int8 => VectorIndexOptions::int8(DIMS as u32, VectorMetric::Cosine),
        }
    }

    /// Stored bytes per vector (the index keyspace footprint, excluding KV
    /// framing): float32 `4·d`, float16 `2·d`, int8 `d + 4` (a per-vector scale).
    fn bytes_per_vector(self) -> usize {
        match self {
            Width::Float32 => DIMS * 4,
            Width::Float16 => DIMS * 2,
            Width::Int8 => DIMS + 4,
        }
    }
}

fn random_vec(rng: &mut StdRng) -> Vec<f64> {
    (0..DIMS).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

/// A MemoryStore-backed db with `N` random `DIMS`-dim embeddings under a vector
/// index of the given width (built after the inserts, exercising the backfill).
fn vector_engine(width: Width) -> Database<MemoryStore> {
    let mut rng = StdRng::seed_from_u64(42);
    let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
    let txn = db.begin(false).unwrap();
    db.collections().create("vec").execute(&txn).unwrap();
    let docs: Vec<bson::Document> = (0..N)
        .map(|i| bson::doc! { "_id": format!("d-{i}"), "embedding": random_vec(&mut rng) })
        .collect();
    for chunk in docs.chunks(1_000) {
        db.collection("vec")
            .insert_many(chunk.to_vec())
            .execute(&txn)
            .unwrap();
    }
    db.collection("vec")
        .indexes()
        .create("embedding", width.options())
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();
    db
}

fn knn_sql(query: &[f64]) -> String {
    let arr = query
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT VALUE c._id FROM c ORDER BY VECTORDISTANCE(c.embedding, [{arr}]) DESC LIMIT {K}"
    )
}

fn bench_vector_knn(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_vector_knn");
    let query = random_vec(&mut StdRng::seed_from_u64(7));
    let sql = knn_sql(&query);
    for width in [Width::Float32, Width::Float16, Width::Int8] {
        let db = vector_engine(width);
        // Footprint is deterministic, not a timed metric — print it once so the
        // run records the 2-4x reduction alongside the latency numbers.
        eprintln!(
            "[footprint] {:<8} {:>4} bytes/vec  ({} total over {N} vectors)",
            width.label(),
            width.bytes_per_vector(),
            width.bytes_per_vector() * N,
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(width.label()),
            &width,
            |b, _| {
                b.iter(|| {
                    let txn = db.begin(true).unwrap();
                    let count = db
                        .collection("vec")
                        .query(&sql)
                        .iter::<String>(&txn)
                        .unwrap()
                        .count();
                    txn.rollback().unwrap();
                    count
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_vector_knn);
criterion_main!(benches);
