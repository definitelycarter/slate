//! Per-node micro-benchmarks for `slate-executor` — each physical node executor
//! driven *in isolation*, off the full `find`/SQL pipeline, so per-node perf work
//! has a stable signal.
//!
//! Mirrors `slate-eval/benches/apply.rs` in shape: builder helpers up top, named
//! scenarios, one `"nodes"` group, `BenchmarkId::from_parameter(name)` per case,
//! `black_box` around inputs/outputs, `harness = false`. Scenario names are
//! `node_variant/<size>`, so Criterion groups a node's sizes together. The
//! node executors and a seeded-engine fixture are reached through
//! `slate_executor::bench` (the `bench-internals` feature), which re-exports thin
//! `pub` wrappers over the otherwise `pub(crate)` `execute(...)` functions.
//!
//! ## What is timed
//!
//! Node iterators are lazy — a node does no work until drained — so each case
//! builds the node's inputs *outside* the timed closure and, *inside* `b.iter`,
//! constructs the iterator and fully drains it with [`bench::collect`].
//!
//! - **Source nodes** (`Scan`, `IndexScan`) and the storage transforms
//!   (`KeyLookup`, `IndexMerge`) read storage: the engine + read transaction are
//!   built once outside (the `txn` must outlive the iterator), and only the
//!   open-iterator-plus-drain is timed.
//! - **Transform nodes** consume their source `ValueIter`, so the source must be
//!   rebuilt every iteration. The input documents are built once as a
//!   `Vec<RawBson>`; inside the loop a fresh source is created via the `values`
//!   (or `bind`) wrapper — `bench::values(docs.clone())` — then the transform,
//!   then the drain. A `values_passthrough/<size>` baseline is included at each
//!   size so a transform's *marginal* cost is `transform − passthrough` (the
//!   `clone` + `values` drain is the shared overhead both pay; `distinct`/`unwind`
//!   read differently-shaped inputs but the same rebuild mechanism).
//!
//! Sizes sweep 100 / 1k / 10k rows. The storage fixture indexes `age` (numeric,
//! `i % 100`) and `status` (low-cardinality string) on a `people` collection.

use std::hint::black_box;

use bson::{Bson, RawBson};
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main};
use slate_ast::{BinOp, Expression, Literal, OrderByItem, SelectClause};
use slate_engine::Engine;
use slate_executor::bench;
use slate_planner::{
    AggregateExpr, GroupKey, IndexIntersectPart, IndexScanRange, LogicalOp, RowBinding,
    ScanDirection,
};

const SIZES: &[usize] = &[100, 1_000, 10_000];
const TAGS: &[&str] = &["renewal", "high_value", "churning", "new", "enterprise"];

// ── Expression parse helpers (ported from nodes::test_support) ───────────────

/// The projection expression of `SELECT VALUE <src> FROM c`.
fn sv(src: &str) -> Expression {
    let q = slate_sql::parse(&format!("SELECT VALUE {src} FROM c")).unwrap();
    let SelectClause::Value(e) = q.select else {
        panic!("expected SELECT VALUE");
    };
    e
}

/// The `WHERE` predicate of `SELECT VALUE c FROM c WHERE <src>`.
fn pred(src: &str) -> Expression {
    slate_sql::parse(&format!("SELECT VALUE c FROM c WHERE {src}"))
        .unwrap()
        .filter
        .unwrap()
}

/// The `ORDER BY` keys of `SELECT VALUE c FROM c ORDER BY <src>`.
fn order_by(src: &str) -> Vec<OrderByItem> {
    slate_sql::parse(&format!("SELECT VALUE c FROM c ORDER BY {src}"))
        .unwrap()
        .order_by
}

// ── Input builders ──────────────────────────────────────────────────────────

/// A bare document (the single-alias pipeline shape: read in
/// `RowBinding::Alias("c")` mode, no enclosing `Bind`).
fn bare_doc(i: usize) -> RawBson {
    RawBson::Document(bson::rawdoc! {
        "_id": format!("rec-{i}"),
        "name": format!("User {i}"),
        "status": if i % 2 == 0 { "active" } else { "rejected" },
        "age": (i % 100) as i32,
        "score": (i % 1000) as i32,
    })
}

fn bare_docs(n: usize) -> Vec<RawBson> {
    (0..n).map(bare_doc).collect()
}

fn pick_tags(i: usize) -> Vec<&'static str> {
    (0..3).map(|k| TAGS[(i + k) % TAGS.len()]).collect()
}

/// Environment documents `{ c: { _id, tags: [...] } }` — the source shape `Unwind`
/// reads (its top-level fields are the bindings), built once so the timed loop
/// only rebuilds the `values` source.
fn tag_env_docs(n: usize) -> Vec<RawBson> {
    (0..n)
        .map(|i| {
            let inner = bson::doc! { "_id": format!("rec-{i}"), "tags": pick_tags(i) };
            let env = bson::doc! { "c": inner };
            RawBson::try_from(Bson::Document(env)).unwrap()
        })
        .collect()
}

/// Bare arrays `["renewal", …]` — the `distinct(flatten = true)` (Mongo multikey)
/// input; elements dedup across rows.
fn tag_arrays(n: usize) -> Vec<RawBson> {
    (0..n)
        .map(|i| {
            let arr: Vec<Bson> = pick_tags(i)
                .into_iter()
                .map(|t| Bson::String(t.into()))
                .collect();
            RawBson::try_from(Bson::Array(arr)).unwrap()
        })
        .collect()
}

/// Scalars `0..100` repeated — the `distinct(flatten = false)` input (100 distinct
/// values).
fn ages(n: usize) -> Vec<RawBson> {
    (0..n).map(|i| RawBson::Int32((i % 100) as i32)).collect()
}

/// Bare `_id` strings — a `KeyLookup` source (every doc in the fixture).
fn ids(n: usize) -> Vec<RawBson> {
    (0..n)
        .map(|i| RawBson::String(format!("rec-{i}")))
        .collect()
}

/// An `age`-bounded [`IndexScanRange::Range`] over `Int32` bounds.
fn range(lower: Option<(i32, bool)>, upper: Option<(i32, bool)>) -> IndexScanRange {
    IndexScanRange::Range {
        lower: lower.map(|(v, incl)| (Bson::Int32(v), incl)),
        upper: upper.map(|(v, incl)| (Bson::Int32(v), incl)),
    }
}

// ── Storage-backed nodes (Scan, IndexScan, KeyLookup, IndexMerge) ────────────

fn bench_storage(group: &mut BenchmarkGroup<'_, WallTime>) {
    let coll = bench::collection_ref();

    // IndexScan ranges over `age` (built once; the engine post-filters numerics).
    let full = IndexScanRange::Full;
    let eq = IndexScanRange::Eq(Bson::Int32(50));
    let ge50 = range(Some((50, true)), None);
    let lt50 = range(None, Some((50, false)));
    let r25_75 = range(Some((25, true)), Some((75, false)));
    let ge25 = range(Some((25, true)), None);
    let lt75 = range(None, Some((75, false)));
    let index_scans: [(&str, &IndexScanRange); 5] = [
        ("index_scan_full", &full),
        ("index_scan_eq", &eq),
        ("index_scan_range_lower", &ge50),
        ("index_scan_range_upper", &lt50),
        ("index_scan_range_both", &r25_75),
    ];

    for &n in SIZES {
        let engine = bench::seeded_engine(n);
        let txn = engine.begin(true).unwrap();
        let id_vec = ids(n);

        // Scan — full collection.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("scan/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let it = bench::scan(&txn, &coll).unwrap();
                    black_box(bench::collect(it).unwrap())
                })
            },
        );

        // IndexScan — Eq / Range (lower, upper, both) / Full, forward.
        for (label, scan_range) in index_scans {
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{label}/{n}")),
                &n,
                |b, _| {
                    b.iter(|| {
                        let it = bench::index_scan(
                            &txn,
                            &coll,
                            "age".into(),
                            scan_range,
                            ScanDirection::Forward,
                            None,
                        )
                        .unwrap();
                        black_box(bench::collect(it).unwrap())
                    })
                },
            );
        }

        // KeyLookup — point-read every id off a `values` id stream.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("key_lookup/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src = bench::values(id_vec.clone());
                    let it = bench::key_lookup(&txn, &coll, src).unwrap();
                    black_box(bench::collect(it).unwrap())
                })
            },
        );

        // IndexMerge(Or) — {age = 50} ∪ {age >= 50}, dedup.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("index_merge_or/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let l = bench::index_scan(
                        &txn,
                        &coll,
                        "age".into(),
                        &eq,
                        ScanDirection::Forward,
                        None,
                    )
                    .unwrap();
                    let r = bench::index_scan(
                        &txn,
                        &coll,
                        "age".into(),
                        &ge50,
                        ScanDirection::Forward,
                        None,
                    )
                    .unwrap();
                    let it = bench::index_merge(&txn, &coll, LogicalOp::Or, l, r).unwrap();
                    black_box(bench::collect(it).unwrap())
                })
            },
        );

        // IndexMerge(And) — {age >= 25} ∩ {age < 75}, intersection.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("index_merge_and/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let l = bench::index_scan(
                        &txn,
                        &coll,
                        "age".into(),
                        &ge25,
                        ScanDirection::Forward,
                        None,
                    )
                    .unwrap();
                    let r = bench::index_scan(
                        &txn,
                        &coll,
                        "age".into(),
                        &lt75,
                        ScanDirection::Forward,
                        None,
                    )
                    .unwrap();
                    let it = bench::index_merge(&txn, &coll, LogicalOp::And, l, r).unwrap();
                    black_box(bench::collect(it).unwrap())
                })
            },
        );
    }
}

// ── In-memory transform nodes ────────────────────────────────────────────────

fn bench_transforms(group: &mut BenchmarkGroup<'_, WallTime>) {
    let alias = || RowBinding::Alias("c".into());

    for &n in SIZES {
        let docs = bare_docs(n);
        let scalars = ages(n);
        let arrays = tag_arrays(n);
        let envs = tag_env_docs(n);

        // Built once per size; the per-row programs are cloned into each fresh
        // node (the executors take their inputs by value).
        let cheap = pred("c.age >= 50");
        let expensive = pred(
            "c.age * 3 + c.score > 200 AND (c.status = \"active\" OR c.age < 10) \
             AND c.score - c.age < 900",
        );
        let identity = sv("c");
        let computed = sv(r#"{ "n": c.name, "a2": c.age * 2, "s": c.status }"#);
        let sort_one = order_by("c.age ASC");
        let sort_two = order_by("c.status ASC, c.age DESC");
        let unwind_arr = sv("c.tags");
        let count_keys = vec![GroupKey {
            slot: "$key0".into(),
            expr: sv("c.status"),
        }];
        let count_aggs = vec![AggregateExpr {
            func: "COUNT".into(),
            arg: sv("1"),
            slot: "$agg0".into(),
        }];
        let sum_keys = vec![GroupKey {
            slot: "$key0".into(),
            expr: sv("c.age"),
        }];
        let sum_aggs = vec![AggregateExpr {
            func: "SUM".into(),
            arg: sv("c.score"),
            slot: "$agg0".into(),
        }];
        // ARRAY_AGG(score) by status (2 groups): gathers each group's scores into
        // an array — the n/2-element collection cost dominates over COUNT/SUM.
        let array_agg_aggs = vec![AggregateExpr {
            func: "ARRAY_AGG".into(),
            arg: sv("c.score"),
            slot: "$agg0".into(),
        }];
        // HAVING over the COUNT-by-status aggregate: `$agg0 > <half>` keeps the
        // groups whose count exceeds half the rows (here, both, so it measures the
        // post-group Filter pass without dropping its source). `$agg0` is the slot
        // the Aggregate binds COUNT into; the parser has no `$`-ident syntax, so
        // the predicate is built directly.
        let having_pred = Expression::Binary {
            op: BinOp::Gt,
            lhs: Box::new(Expression::Identifier("$agg0".into())),
            rhs: Box::new(Expression::Literal(Literal::Int((n / 4) as i64))),
        };

        // Baseline: rebuild + drain a `values` source — the shared per-iter cost.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("values_passthrough/{n}")),
            &n,
            |b, _| b.iter(|| black_box(bench::collect(bench::values(docs.clone())).unwrap())),
        );

        // Bind — wrap each value as `{c: value}`.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("bind/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src = bench::bind("c".into(), bench::values(docs.clone()));
                    black_box(bench::collect(src).unwrap())
                })
            },
        );

        // Filter — cheap single comparison vs. a heavier compound predicate.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("filter_cheap/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src = bench::filter(cheap.clone(), alias(), bench::values(docs.clone()));
                    black_box(bench::collect(src).unwrap())
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("filter_expensive/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src =
                        bench::filter(expensive.clone(), alias(), bench::values(docs.clone()));
                    black_box(bench::collect(src).unwrap())
                })
            },
        );

        // Project — identity fast path vs. a computed object projection.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("project_identity/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src =
                        bench::project(identity.clone(), alias(), bench::values(docs.clone()));
                    black_box(bench::collect(src).unwrap())
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("project_computed/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src =
                        bench::project(computed.clone(), alias(), bench::values(docs.clone()));
                    black_box(bench::collect(src).unwrap())
                })
            },
        );

        // Sort — single key vs. multi-key (blocking).
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("sort_single/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src = bench::sort(sort_one.clone(), alias(), bench::values(docs.clone()))
                        .unwrap();
                    black_box(bench::collect(src).unwrap())
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("sort_multi/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src = bench::sort(sort_two.clone(), alias(), bench::values(docs.clone()))
                        .unwrap();
                    black_box(bench::collect(src).unwrap())
                })
            },
        );

        // Limit — skip then take.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("limit_skip_take/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src = bench::limit(n / 10, Some(n / 2), bench::values(docs.clone()));
                    black_box(bench::collect(src).unwrap())
                })
            },
        );

        // Distinct — flatten=false (whole values) vs. flatten=true (array multikey).
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("distinct_flatten_false/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src = bench::distinct(bench::values(scalars.clone()), false);
                    black_box(bench::collect(src).unwrap())
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("distinct_flatten_true/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src = bench::distinct(bench::values(arrays.clone()), true);
                    black_box(bench::collect(src).unwrap())
                })
            },
        );

        // Unwind — one row per `c.tags` element (env-shaped source).
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("unwind/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src =
                        bench::unwind("t".into(), unwind_arr.clone(), bench::values(envs.clone()));
                    black_box(bench::collect(src).unwrap())
                })
            },
        );

        // Aggregate — COUNT(1) by status (2 groups) and SUM(score) by age (100 groups).
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("aggregate_count/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src = bench::aggregate(
                        count_keys.clone(),
                        count_aggs.clone(),
                        alias(),
                        bench::values(docs.clone()),
                    )
                    .unwrap();
                    black_box(bench::collect(src).unwrap())
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("aggregate_sum/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src = bench::aggregate(
                        sum_keys.clone(),
                        sum_aggs.clone(),
                        alias(),
                        bench::values(docs.clone()),
                    )
                    .unwrap();
                    black_box(bench::collect(src).unwrap())
                })
            },
        );

        // ARRAY_AGG(score) by status — array-gathering aggregate (2 groups).
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("aggregate_array_agg/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let src = bench::aggregate(
                        count_keys.clone(),
                        array_agg_aggs.clone(),
                        alias(),
                        bench::values(docs.clone()),
                    )
                    .unwrap();
                    black_box(bench::collect(src).unwrap())
                })
            },
        );

        // HAVING — a post-aggregation Filter (Env-bound) over the COUNT-by-status
        // group rows. Times the full Aggregate→Filter pipeline the planner emits.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("having/{n}")),
            &n,
            |b, _| {
                b.iter(|| {
                    let agg = bench::aggregate(
                        count_keys.clone(),
                        count_aggs.clone(),
                        alias(),
                        bench::values(docs.clone()),
                    )
                    .unwrap();
                    let src = bench::filter(having_pred.clone(), RowBinding::Env, agg);
                    black_box(bench::collect(src).unwrap())
                })
            },
        );
    }
}

// ── Index intersection (galloping skip-merge vs hash IndexMerge(And)) ─────────

/// The all-equality `AND` intersection two ways over the same corpus: the new
/// galloping [`IndexIntersect`](slate_planner::Node::IndexIntersect) (bounded by
/// the smaller side) and the hash `IndexMerge(And)` it replaces (reads both sides
/// fully). The `intersect_*` / `hashmerge_*` ratio at each size is the win.
///
/// - **skew**: `big` (≈n/2) ∩ `sel` (≈n/50) — the motivating case; the hash merge
///   reads all of `big`, the skip-merge is bounded by `sel`.
/// - **balanced**: `big` (≈n/2) ∩ `tri` (≈n/3), heavily interleaved — galloping's
///   worst case (the gate that it must not regress vs the hash merge).
fn bench_intersect(group: &mut BenchmarkGroup<'_, WallTime>) {
    let coll = bench::intersect_collection_ref();
    let eq_y = IndexScanRange::Eq(Bson::String("y".into()));
    let part = |field: &str| IndexIntersectPart {
        field: field.into(),
        value: Bson::String("y".into()),
    };
    // (label, the two equality fields whose `= "y"` streams are intersected).
    let cases: [(&str, &str, &str); 2] = [("skew", "big", "sel"), ("balanced", "big", "tri")];

    for &n in SIZES {
        let engine = bench::intersect_engine(n);
        let txn = engine.begin(true).unwrap();
        for (label, f1, f2) in cases {
            let parts = [part(f1), part(f2)];
            // Galloping skip-merge.
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("intersect_{label}/{n}")),
                &n,
                |b, _| {
                    b.iter(|| {
                        let it = bench::index_intersect(&txn, &coll, &parts).unwrap();
                        black_box(bench::collect(it).unwrap())
                    })
                },
            );
            // Hash IndexMerge(And) over the same two `Eq` scans.
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("hashmerge_{label}/{n}")),
                &n,
                |b, _| {
                    b.iter(|| {
                        let l = bench::index_scan(
                            &txn,
                            &coll,
                            f1.into(),
                            &eq_y,
                            ScanDirection::Forward,
                            None,
                        )
                        .unwrap();
                        let r = bench::index_scan(
                            &txn,
                            &coll,
                            f2.into(),
                            &eq_y,
                            ScanDirection::Forward,
                            None,
                        )
                        .unwrap();
                        let it = bench::index_merge(&txn, &coll, LogicalOp::And, l, r).unwrap();
                        black_box(bench::collect(it).unwrap())
                    })
                },
            );
        }
    }
}

fn bench_nodes(c: &mut Criterion) {
    let mut group = c.benchmark_group("nodes");
    bench_storage(&mut group);
    bench_transforms(&mut group);
    bench_intersect(&mut group);
    group.finish();
}

criterion_group!(benches, bench_nodes);
criterion_main!(benches);
