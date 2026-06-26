mod common;
use common::*;

use bson::rawdoc;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use slate_query::*;

// ── Query Benchmarks ────────────────────────────────────────

fn bench_query_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_scan");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .find(rawdoc! {})
                    .iter(&txn)
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
                engine
                    .collection("bench")
                    .find(filter.clone())
                    .iter(&txn)
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
                let columns = options.columns.clone().unwrap();
                engine
                    .collection("bench")
                    .find(filter.clone())
                    .project(columns)
                    .iter(&txn)
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
                engine
                    .collection("bench")
                    .find(filter.clone())
                    .iter(&txn)
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
                engine
                    .collection("bench")
                    .find(filter.clone())
                    .iter(&txn)
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
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .find(filter.clone())
                    .sort("contacts_count", SortDirection::Desc)
                    .iter(&txn)
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
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .find(rawdoc! {})
                    .sort("contacts_count", SortDirection::Desc)
                    .limit(200)
                    .iter(&txn)
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
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .find(rawdoc! {})
                    .sort("contacts_count", SortDirection::Desc)
                    .sort("name", SortDirection::Asc)
                    .limit(200)
                    .iter(&txn)
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
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .find(filter.clone())
                    .sort("contacts_count", SortDirection::Desc)
                    .offset(100)
                    .limit(50)
                    .iter(&txn)
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
                    if engine
                        .collection("bench")
                        .find(rawdoc! { "_id": id.as_str() })
                        .first(&txn)
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
                let columns = options.columns.clone().unwrap();
                engine
                    .collection("bench")
                    .find(rawdoc! {})
                    .project(columns)
                    .iter(&txn)
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
                engine
                    .collection("bench")
                    .find(filter.clone())
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

// ── Range filter benchmarks ─────────────────────────────────

/// Numeric `Eq` on the indexed `Int32` field `contacts_count` (values `0..100`,
/// so `= 50` matches ~1% of rows). Today numeric `Eq` is not sargable as a tight
/// seek — it routes to a full scan + `CoercingFilter` — so this measures the
/// full-scan cost the unified numeric key is meant to turn into a seek.
fn bench_query_indexed_eq_numeric(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_indexed_eq_numeric");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let filter = rawdoc! { "contacts_count": 50 };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .find(filter.clone())
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

/// Covered numeric projection: numeric `Eq` projecting only `contacts_count`, so
/// the value is served from the index — exercises numeric-key decode under the
/// unified key.
fn bench_query_indexed_eq_numeric_projection(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_indexed_eq_numeric_proj");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let filter = rawdoc! { "contacts_count": 50 };
        let options = FindOptions {
            columns: Some(vec!["contacts_count".into()]),
            ..FindOptions::default()
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                let columns = options.columns.clone().unwrap();
                engine
                    .collection("bench")
                    .find(filter.clone())
                    .project(columns)
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

fn bench_query_indexed_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_indexed_range");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let filter = rawdoc! { "contacts_count": { "$gt": 50 } };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .find(filter.clone())
                    .iter(&txn)
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
                engine
                    .collection("bench")
                    .find(filter.clone())
                    .iter(&txn)
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
                engine
                    .collection("bench")
                    .find(filter.clone())
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

/// IN-style disjunction on an indexed field (`$or` of several `{field: value}`
/// equalities). Should plan to `IndexMerge(Or)` over the indexed candidates, not
/// a full scan.
fn bench_query_or_indexed(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_or_indexed");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let filter = rawdoc! {
            "$or": [
                { "contacts_count": 5 },
                { "contacts_count": 25 },
                { "contacts_count": 50 },
                { "contacts_count": 75 },
            ]
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .find(filter.clone())
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_query_scan,
    bench_query_indexed_eq,
    bench_query_indexed_eq_projection,
    bench_query_indexed_eq_numeric,
    bench_query_indexed_eq_numeric_projection,
    bench_query_multi_field_and,
    bench_query_or_indexed,
    bench_query_null_filter,
    bench_query_sort_indexed,
    bench_query_sort_indexed_take,
    bench_query_sort_multi,
    bench_query_pagination,
    bench_query_point_lookup,
    bench_query_projection,
    bench_query_array_match,
    bench_query_indexed_range,
    bench_query_indexed_range_dual,
    bench_query_indexed_eq_plus_range,
);
criterion_main!(benches);
