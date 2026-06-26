mod common;
use common::*;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

// ── SQL Query Benchmarks ────────────────────────────────────

/// Covered *dotted* projection: index on the dotted path `meta.note`, query reads
/// only `c.meta.note` (+ pk). On the branch the scan covers (synthesizes
/// `{meta: {note}}`, no `KeyLookup`); on `main` it fetches each matched document.
/// Mirrors `query_indexed_eq_proj`'s selectivity for a direct comparison.
fn bench_query_indexed_eq_dotted_projection(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_indexed_eq_dotted_proj");
    let sql = r#"SELECT VALUE c.meta.note FROM c WHERE c.meta.note = "active""#;
    for n in [1_000, 10_000] {
        let engine = nested_indexed_engine(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .query(sql)
                    .iter(&txn)
                    .unwrap()
                    .map(|v| bson::Bson::try_from(v.unwrap().as_raw_bson_ref()).unwrap())
                    .count()
            })
        });
    }
    group.finish();
}

/// `ARRAY_CONTAINS(c.tags, "rare")` over a `tags.[]` multikey index versus the
/// same query with no index (full scan). Pins the increment-A win: a selective
/// containment test should beat the full scan on an array-valued corpus.
fn bench_query_array_contains(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_array_contains");
    let sql = "SELECT VALUE c FROM c WHERE ARRAY_CONTAINS(c.tags, \"rare\")";
    for n in [1_000, 10_000] {
        let indexed = array_tags_engine(n, true);
        let unindexed = array_tags_engine(n, false);
        group.bench_with_input(BenchmarkId::new("multikey_index", n), &n, |b, _| {
            b.iter(|| {
                let txn = indexed.begin(true).unwrap();
                indexed
                    .collection("bench")
                    .query(sql)
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
        group.bench_with_input(BenchmarkId::new("full_scan", n), &n, |b, _| {
            b.iter(|| {
                let txn = unindexed.begin(true).unwrap();
                unindexed
                    .collection("bench")
                    .query(sql)
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

// ── String predicate pushdown (sargability B/C) ─────────────

/// `STRINGEQUALS(c.name, "Company-500")` over a scalar `name` index versus the
/// same query with no index (full scan). Pins the increment-C win: a 2-arg
/// `STRINGEQUALS` becomes a tight `Eq` seek instead of a `Scan → Filter`.
fn bench_query_string_equals(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_string_equals");
    let sql = r#"SELECT VALUE c FROM c WHERE STRINGEQUALS(c.name, "Company-500")"#;
    for n in [1_000, 10_000] {
        let indexed = string_index_engine(n, true);
        let unindexed = string_index_engine(n, false);
        group.bench_with_input(BenchmarkId::new("indexed", n), &n, |b, _| {
            b.iter(|| {
                let txn = indexed.begin(true).unwrap();
                indexed
                    .collection("bench")
                    .query(sql)
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
        group.bench_with_input(BenchmarkId::new("full_scan", n), &n, |b, _| {
            b.iter(|| {
                let txn = unindexed.begin(true).unwrap();
                unindexed
                    .collection("bench")
                    .query(sql)
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

/// `STARTSWITH(c.name, "Company-99")` over a scalar `name` index versus the same
/// query with no index (full scan). Pins the increment-B win: a prefix predicate
/// becomes a `[pre, pre⁺)` range scan instead of a `Scan → Filter`.
fn bench_query_string_startswith(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_string_startswith");
    let sql = r#"SELECT VALUE c FROM c WHERE STARTSWITH(c.name, "Company-99")"#;
    for n in [1_000, 10_000] {
        let indexed = string_index_engine(n, true);
        let unindexed = string_index_engine(n, false);
        group.bench_with_input(BenchmarkId::new("indexed", n), &n, |b, _| {
            b.iter(|| {
                let txn = indexed.begin(true).unwrap();
                indexed
                    .collection("bench")
                    .query(sql)
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
        group.bench_with_input(BenchmarkId::new("full_scan", n), &n, |b, _| {
            b.iter(|| {
                let txn = unindexed.begin(true).unwrap();
                unindexed
                    .collection("bench")
                    .query(sql)
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

/// `c.name LIKE "Company-99%"` — the same prefix range as `STARTSWITH`, reached
/// via the LIKE → anchored-regex desugaring.
fn bench_query_string_like(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_string_like");
    let sql = r#"SELECT VALUE c FROM c WHERE c.name LIKE "Company-99%""#;
    for n in [1_000, 10_000] {
        let indexed = string_index_engine(n, true);
        let unindexed = string_index_engine(n, false);
        group.bench_with_input(BenchmarkId::new("indexed", n), &n, |b, _| {
            b.iter(|| {
                let txn = indexed.begin(true).unwrap();
                indexed
                    .collection("bench")
                    .query(sql)
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
        group.bench_with_input(BenchmarkId::new("full_scan", n), &n, |b, _| {
            b.iter(|| {
                let txn = unindexed.begin(true).unwrap();
                unindexed
                    .collection("bench")
                    .query(sql)
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

/// End-to-end CosmosDB-style SQL (`query()`): lex + parse + lower + execute +
/// value iteration, over an indexed `WHERE`. The find-equivalent is
/// `find_indexed_eq`; the delta is mostly the SQL front-end (lex/parse) vs the
/// Mongo translation.
fn bench_query_sql(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_sql");
    for n in [1_000, 10_000] {
        let engine = realistic_seeded_engine(n);
        let sql = r#"SELECT VALUE c.name FROM c WHERE c.status = "active""#;
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .query(sql)
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

/// Leading-equality on a compound index `(status, contacts_count)`:
/// `WHERE c.status = "active"` plans as `CompoundIndexScan[status=active] →
/// KeyLookup`. Part A of the covering-index RFC drops the residual `Filter
/// status` the planner used to keep above the lookup (the compound scan node
/// already rechecks the equality against the entry), so this is the before/after
/// for that change. SQL (not a Mongo find) so the equality is a plain atom the
/// compound-scan path claims.
fn bench_query_compound_eq(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_compound_eq");
    let sql = r#"SELECT VALUE c FROM c WHERE c.status = "active""#;
    for n in [1_000, 10_000] {
        let engine = compound_indexed_engine(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .query(sql)
                    .iter(&txn)
                    .unwrap()
                    .count()
            })
        });
    }
    group.finish();
}

/// Covered compound projection over `(status, contacts_count)`: reading only the
/// two index components (`SELECT c.status, c.contacts_count … WHERE c.status =
/// "active"`) is served entirely from the index entries — the planner drops the
/// `KeyLookup` and the executor synthesizes each row from the entry's per-component
/// values (RFC Part B, phase 2). Same engine as `query_compound_eq`, but a *covered
/// projection* rather than the whole-row read that bails to a fetch, so this is the
/// before/after for compound covering. The realistic (heavy) documents make the
/// skipped fetch material — on tiny docs the synthesis cost ≈ the fetch saved.
fn bench_query_compound_covering(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_compound_covering");
    let sql = r#"SELECT c.status, c.contacts_count FROM c WHERE c.status = "active""#;
    for n in [1_000, 10_000] {
        let engine = compound_indexed_engine(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = engine.begin(true).unwrap();
                engine
                    .collection("bench")
                    .query(sql)
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
    bench_query_indexed_eq_dotted_projection,
    bench_query_sql,
    bench_query_compound_eq,
    bench_query_compound_covering,
    bench_query_array_contains,
    bench_query_string_equals,
    bench_query_string_startswith,
    bench_query_string_like,
);
criterion_main!(benches);
