//! Benchmarks for the `slate-rawbson` scan hot path.
//!
//! These are the regression baseline for the rawbson-robustness bounds-check
//! hardening: the bounds checks added to `skip_bson_value` and `RawField::value`
//! sit on the per-row field-lookup path (`raweval` calls `RawField::get` once
//! per row), so any cost shows up here.
//!
//! Bench IDs (stable — do not rename; they are the before/after baseline):
//!   * `rawfield_get/first`     — locate a field near the front of the document
//!   * `rawfield_get/last`      — locate a field at the back (full skip walk)
//!   * `rawfield_get/missing`   — scan the whole doc, find nothing (worst case)
//!   * `rawfield_get_value/last`— locate + parse the value (full pipeline)
//!   * `skip_bson_value/<type>` — single-arm micro-bench per fixed-width type

use std::hint::black_box;

use bson::{Document, RawDocumentBuf, doc};
use criterion::{Criterion, criterion_group, criterion_main};
use slate_rawbson::{RawField, skip_bson_value};

/// A representative ~12-field flat document, mirroring the kind of row the
/// engine reads per-row in a filter/projection scan.
fn sample_doc() -> RawDocumentBuf {
    let d: Document = doc! {
        "_id": "rec-000123",
        "name": "Alice Johnson",
        "age": 34_i32,
        "score": 87.5_f64,
        "active": true,
        "created": bson::DateTime::from_millis(1_700_000_000_000_i64),
        "visits": 4096_i64,
        "tier": "gold",
        "balance": 12_345_i32,
        "country": "US",
        "rank": 7_i32,
        "last": "tail-value",
    };
    let bytes = bson::serialize_to_vec(&d).expect("serialize");
    RawDocumentBuf::from_bytes(bytes).expect("valid")
}

fn bench_rawfield_get(c: &mut Criterion) {
    let doc = sample_doc();
    let bytes = doc.as_bytes();

    let mut group = c.benchmark_group("rawfield_get");
    group.bench_function("first", |b| {
        b.iter(|| black_box(RawField::get(black_box(bytes), black_box("_id"))).is_some())
    });
    group.bench_function("last", |b| {
        b.iter(|| black_box(RawField::get(black_box(bytes), black_box("last"))).is_some())
    });
    group.bench_function("missing", |b| {
        b.iter(|| black_box(RawField::get(black_box(bytes), black_box("nope"))).is_some())
    });
    group.finish();

    let mut group = c.benchmark_group("rawfield_get_value");
    group.bench_function("last", |b| {
        b.iter(|| {
            black_box(RawField::get(black_box(bytes), black_box("last")).and_then(|f| f.value()))
                .is_some()
        })
    });
    group.finish();
}

fn bench_skip_bson_value(c: &mut Criterion) {
    // A 24-byte buffer big enough for every fixed-width arm.
    let buf = [0u8; 24];
    let cases: &[(&str, u8)] = &[
        ("double", 0x01),
        ("objectid", 0x07),
        ("bool", 0x08),
        ("datetime", 0x09),
        ("int32", 0x10),
        ("timestamp", 0x11),
        ("int64", 0x12),
        ("decimal128", 0x13),
    ];

    let mut group = c.benchmark_group("skip_bson_value");
    for &(name, tb) in cases {
        group.bench_function(name, |b| {
            b.iter(|| {
                black_box(skip_bson_value(
                    black_box(tb),
                    black_box(&buf),
                    black_box(0),
                ))
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_rawfield_get, bench_skip_bson_value);
criterion_main!(benches);
