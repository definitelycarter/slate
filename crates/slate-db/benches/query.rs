mod common;
use common::*;

use bson::rawdoc;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use slate_db::bench::Database;
use slate_db::{CollectionConfig, DatabaseConfig};
use slate_query::*;
use slate_store::MemoryStore;

// ── Bulk Insert ─────────────────────────────────────────────

fn bench_bulk_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_insert");
    for n in [1_000, 10_000] {
        let engine = {
            let engine = Database::open(MemoryStore::new(), DatabaseConfig::default());
            let mut txn = engine.begin(false).unwrap();
            txn.create_collection(&CollectionConfig {
                name: "bench".into(),
                indexes: vec!["status".into(), "contacts_count".into()],
        ..Default::default()
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
                    txn.insert_many("bench", docs).unwrap().drain().unwrap();
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
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", rawdoc! {}, FindOptions::default())
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
        let filter = rawdoc! { "status": "active" };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", filter.clone(), FindOptions::default())
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
        let filter = rawdoc! { "status": "active" };
        let options = FindOptions {
            columns: Some(vec!["status".into()]),
            ..FindOptions::default()
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", filter.clone(), options.clone())
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
        let filter = rawdoc! {
            "status": "active",
            "product_recommendation1": "ProductA",
            "product_recommendation2": "ProductX",
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", filter.clone(), FindOptions::default())
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
        let filter = rawdoc! { "last_contacted_at": { "$exists": false } };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", filter.clone(), FindOptions::default())
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
        let filter = rawdoc! { "status": "active" };
        let options = FindOptions {
            sort: vec![Sort {
                field: "contacts_count".into(),
                direction: SortDirection::Desc,
            }],
            ..FindOptions::default()
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", filter.clone(), options.clone())
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
        let options = FindOptions {
            sort: vec![Sort {
                field: "contacts_count".into(),
                direction: SortDirection::Desc,
            }],
            take: Some(200),
            ..FindOptions::default()
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", rawdoc! {}, options.clone())
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
        let options = FindOptions {
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
            take: Some(200),
            ..FindOptions::default()
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", rawdoc! {}, options.clone())
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
        let filter = rawdoc! { "status": "active" };
        let options = FindOptions {
            sort: vec![Sort {
                field: "contacts_count".into(),
                direction: SortDirection::Desc,
            }],
            skip: Some(100),
            take: Some(50),
            ..FindOptions::default()
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", filter.clone(), options.clone())
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
                    if txn
                        .find_one("bench", rawdoc! { "_id": id.as_str() })
                        .unwrap()
                        .is_some()
                    {
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
        let options = FindOptions {
            columns: Some(vec!["name".into(), "status".into()]),
            ..FindOptions::default()
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", rawdoc! {}, options.clone())
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
        let filter = rawdoc! { "tags": "renewal_due" };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", filter.clone(), FindOptions::default())
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
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.distinct("bench", "status", rawdoc! {}, DistinctOptions::default())
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
                txn.distinct(
                    "bench",
                    "contacts_count",
                    rawdoc! {},
                    DistinctOptions::default(),
                )
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
                txn.distinct(
                    "bench",
                    "product_recommendation1",
                    rawdoc! {},
                    DistinctOptions::default(),
                )
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
                txn.distinct(
                    "bench",
                    "product_recommendation1",
                    filter.clone(),
                    DistinctOptions::default(),
                )
                .unwrap()
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
        let filter = rawdoc! { "contacts_count": { "$gt": 50 } };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", filter.clone(), FindOptions::default())
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
        let filter = rawdoc! { "contacts_count": { "$gt": 20, "$lt": 80 } };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", filter.clone(), FindOptions::default())
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
        let filter = rawdoc! { "status": "active", "contacts_count": { "$gt": 50 } };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                txn.find("bench", filter.clone(), FindOptions::default())
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
    bench_query_indexed_range,
    bench_query_indexed_range_dual,
    bench_query_indexed_eq_plus_range,
);
criterion_main!(benches);
