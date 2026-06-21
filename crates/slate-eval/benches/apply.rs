//! Benchmarks for `slate_eval::apply_assignments` — the UPDATE apply step.
//!
//! Each scenario is a `(document, assignments)` pair, then `apply_assignments`
//! is timed over it. The assignments are built directly as the AST the Mongo
//! front-end emits — `$set` → `Value`, `$inc` → `inc(c.f, k)`, `$push` →
//! `rpush(c.f, v)`, `$unset` → `= undefined` — so the bench stays self-contained
//! in `slate-ast`/`slate-eval` without pulling in a query-surface crate (the
//! assignments are constructed once, outside the timed loop, anyway).
//!
//! Scenarios span representative assignment shapes crossed with a few document
//! sizes (small flat, ~20-field flat, large array, nested sub-doc), so the
//! before/after of the in-place byte-edit pass is visible per shape.

use std::hint::black_box;

use bson::{Bson, Document, RawDocument, RawDocumentBuf};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use slate_ast::{Assignment, Expression};

const ALIAS: &str = "c";

// ── Assignment builders (mirror slate_query::translate) ─────────────────────

/// `c.<field>` member access — the field reference the operators read through.
fn field_ref(field: &str) -> Expression {
    Expression::Member {
        base: Box::new(Expression::Identifier(ALIAS.into())),
        field: field.into(),
    }
}

fn path(field: &str) -> Vec<String> {
    field.split('.').map(String::from).collect()
}

/// `$set` (and bare-field) → write a literal value to the path.
fn set(field: &str, value: Bson) -> Assignment {
    Assignment {
        path: path(field),
        value: Expression::Value(value),
    }
}

/// `$unset` → assign `undefined`, which removes the field.
fn unset(field: &str) -> Assignment {
    Assignment {
        path: path(field),
        value: Expression::Identifier("undefined".into()),
    }
}

/// `$inc` → type-preserving `inc(c.field, delta)`.
fn inc(field: &str, delta: Bson) -> Assignment {
    Assignment {
        path: path(field),
        value: Expression::Function {
            name: "inc".into(),
            args: vec![field_ref(field), Expression::Value(delta)],
        },
    }
}

/// `$push` → `rpush(c.field, value)`.
fn push(field: &str, value: Bson) -> Assignment {
    Assignment {
        path: path(field),
        value: Expression::Function {
            name: "rpush".into(),
            args: vec![field_ref(field), Expression::Value(value)],
        },
    }
}

// ── Document builders ───────────────────────────────────────────────────────

fn raw(doc: Document) -> RawDocumentBuf {
    RawDocumentBuf::try_from(&doc).expect("valid document")
}

/// A flat document with `_id`, a string `name`, an i32 `score`, and `n` filler
/// fields `f0..fn` of mixed types — the "wide" shape.
fn wide_doc(n: usize) -> Document {
    let mut doc = Document::new();
    doc.insert("_id", "row-1");
    doc.insert("name", "Alice");
    doc.insert("score", 10_i32);
    for i in 0..n {
        match i % 3 {
            0 => doc.insert(format!("f{i}"), i as i32),
            1 => doc.insert(format!("f{i}"), format!("val-{i}")),
            _ => doc.insert(format!("f{i}"), (i as f64) + 0.5),
        };
    }
    doc
}

/// A document carrying a `tags` array of `n` integers (plus a couple of fields).
fn array_doc(n: usize) -> Document {
    let tags: Vec<Bson> = (0..n).map(|i| Bson::Int32(i as i32)).collect();
    let mut doc = Document::new();
    doc.insert("_id", "row-1");
    doc.insert("name", "Alice");
    doc.insert("tags", tags);
    doc
}

/// A named apply scenario: a document and the assignments to apply to it.
struct Scenario {
    name: &'static str,
    old: RawDocumentBuf,
    assignments: Vec<Assignment>,
}

fn scenarios() -> Vec<Scenario> {
    vec![
        // ── flat $set to a scalar, same type + width (i32 → i32) ────────────
        Scenario {
            name: "set_i32_same_width/flat_small",
            old: raw(bson::doc! { "_id": "1", "a": 1_i32, "b": 2_i32, "name": "x" }),
            assignments: vec![set("a", Bson::Int32(7))],
        },
        Scenario {
            name: "set_i32_same_width/flat_wide",
            old: raw(wide_doc(20)),
            assignments: vec![set("score", Bson::Int32(7))],
        },
        // Scalar set into a large document: isolates the asymptotic win — the
        // rebuild re-encodes every field, the in-place edit touches one slot.
        Scenario {
            name: "set_i32_same_width/big_array_doc",
            old: raw(array_doc(200)),
            assignments: vec![set("name", Bson::String("Bob__".into()))],
        },
        // ── flat $set that changes width / type ─────────────────────────────
        Scenario {
            name: "set_widen_i32_to_string/flat_small",
            old: raw(bson::doc! { "_id": "1", "a": 1_i32, "b": 2_i32 }),
            assignments: vec![set("a", Bson::String("a longer string value".into()))],
        },
        Scenario {
            name: "set_grow_string/flat_small",
            old: raw(bson::doc! { "_id": "1", "name": "Al", "b": 2_i32 }),
            assignments: vec![set(
                "name",
                Bson::String("Alexander the considerably longer".into()),
            )],
        },
        // ── $inc on a numeric field ─────────────────────────────────────────
        Scenario {
            name: "inc_i32/flat_small",
            old: raw(bson::doc! { "_id": "1", "a": 1_i32, "n": 41_i32 }),
            assignments: vec![inc("n", Bson::Int32(1))],
        },
        Scenario {
            name: "inc_i32/flat_wide",
            old: raw(wide_doc(20)),
            assignments: vec![inc("score", Bson::Int32(5))],
        },
        // ── $unset of a field ───────────────────────────────────────────────
        Scenario {
            name: "unset/flat_wide",
            old: raw(wide_doc(20)),
            assignments: vec![unset("f10")],
        },
        // ── $push to an existing array ──────────────────────────────────────
        Scenario {
            name: "push/small_array",
            old: raw(array_doc(20)),
            assignments: vec![push("tags", Bson::Int32(99))],
        },
        Scenario {
            name: "push/big_array",
            old: raw(array_doc(200)),
            assignments: vec![push("tags", Bson::Int32(99))],
        },
        // ── nested dotted-path $set (a.b.c = …) — falls back to rebuild ──────
        Scenario {
            name: "nested_set_dotted/subdoc",
            old: raw(bson::doc! {
                "_id": "1",
                "a": { "b": { "c": 1_i32, "d": "keep" }, "e": "keep" },
                "name": "Alice",
            }),
            assignments: vec![set("a.b.c", Bson::Int32(2))],
        },
        // ── a multi-assignment update ───────────────────────────────────────
        Scenario {
            name: "multi/flat_wide",
            old: raw(wide_doc(20)),
            assignments: vec![
                inc("score", Bson::Int32(5)),
                set("name", Bson::String("renamed".into())),
                unset("f5"),
            ],
        },
    ]
}

fn bench_apply(c: &mut Criterion) {
    let params = Document::new();
    let scenarios = scenarios();
    let mut group = c.benchmark_group("apply");
    for s in &scenarios {
        let old: &RawDocument = &s.old;
        group.bench_with_input(BenchmarkId::from_parameter(s.name), s, |b, s| {
            b.iter(|| {
                let out = slate_eval::apply_assignments(
                    black_box(old),
                    ALIAS,
                    black_box(&s.assignments),
                    &params,
                )
                .expect("apply ok");
                black_box(out)
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_apply);
criterion_main!(benches);
