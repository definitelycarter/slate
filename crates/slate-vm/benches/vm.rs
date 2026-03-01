use std::sync::Arc;

use bson::{Bson, rawdoc};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use slate_vm::{LuaVm, Vm, VmCallback};

// ── Helpers ─────────────────────────────────────────────────

fn vm_with(name: &str, source: &[u8]) -> LuaVm {
    let mut vm = LuaVm::new().unwrap();
    vm.register(name, source).unwrap();
    vm
}

fn small_doc() -> bson::RawDocumentBuf {
    rawdoc! {
        "name": "alice",
        "age": 30_i32,
        "active": true,
    }
}

fn medium_doc() -> bson::RawDocumentBuf {
    rawdoc! {
        "first": "Ada",
        "last": "Lovelace",
        "email": "ada@example.com",
        "age": 36_i32,
        "active": true,
        "score": 98.5,
        "tags": ["math", "cs", "engineering"],
        "address": {
            "city": "London",
            "country": "UK",
        },
    }
}

// ── Identity (baseline: conversion overhead) ────────────────

fn bench_identity(c: &mut Criterion) {
    let mut group = c.benchmark_group("identity");

    let vm = vm_with("f", b"return function(doc) return doc end");

    group.bench_function("small_doc", |b| {
        let input = small_doc();
        b.iter(|| vm.call("f", &input).unwrap())
    });

    group.bench_function("medium_doc", |b| {
        let input = medium_doc();
        b.iter(|| vm.call("f", &input).unwrap())
    });

    group.finish();
}

// ── Validator ───────────────────────────────────────────────

fn bench_validator(c: &mut Criterion) {
    let mut group = c.benchmark_group("validator");

    // Simple: check one field exists
    let vm_simple = vm_with(
        "f",
        br#"return function(doc)
            if doc.name ~= nil then
                return { ok = true }
            else
                return { ok = false, reason = "name is required" }
            end
        end"#,
    );

    group.bench_function("simple_pass", |b| {
        let input = small_doc();
        b.iter(|| vm_simple.call("f", &input).unwrap())
    });

    group.bench_function("simple_fail", |b| {
        let input = rawdoc! { "status": "active" };
        b.iter(|| vm_simple.call("f", &input).unwrap())
    });

    // Multi-field: check several constraints
    let vm_multi = vm_with(
        "f",
        br#"return function(doc)
            if doc.name == nil then
                return { ok = false, reason = "name is required" }
            end
            if doc.email == nil then
                return { ok = false, reason = "email is required" }
            end
            if doc.age == nil or doc.age < 0 then
                return { ok = false, reason = "age must be non-negative" }
            end
            return { ok = true }
        end"#,
    );

    group.bench_function("multi_field", |b| {
        let input = medium_doc();
        b.iter(|| vm_multi.call("f", &input).unwrap())
    });

    group.finish();
}

// ── Computed fields ─────────────────────────────────────────

fn bench_computed_field(c: &mut Criterion) {
    let mut group = c.benchmark_group("computed_field");

    // String concatenation
    let vm_concat = vm_with(
        "f",
        br#"return function(doc)
            return { value = doc.first .. " " .. doc.last }
        end"#,
    );

    group.bench_function("string_concat", |b| {
        let input = rawdoc! { "first": "Ada", "last": "Lovelace" };
        b.iter(|| vm_concat.call("f", &input).unwrap())
    });

    // Arithmetic
    let vm_arith = vm_with(
        "f",
        br#"return function(doc)
            return { value = (doc.score * 100 + 0.5) }
        end"#,
    );

    group.bench_function("arithmetic", |b| {
        let input = rawdoc! { "score": 0.875 };
        b.iter(|| vm_arith.call("f", &input).unwrap())
    });

    // Build a derived document
    let vm_derive = vm_with(
        "f",
        br#"return function(doc)
            return {
                full_name = doc.first .. " " .. doc.last,
                display = doc.first .. " " .. doc.last .. " <" .. doc.email .. ">",
            }
        end"#,
    );

    group.bench_function("derived_doc", |b| {
        let input = medium_doc();
        b.iter(|| vm_derive.call("f", &input).unwrap())
    });

    group.finish();
}

// ── Trigger (simulates side-effect-free trigger logic) ──────

fn bench_trigger(c: &mut Criterion) {
    let mut group = c.benchmark_group("trigger");

    // Minimal: just return empty doc (trigger that inspects nothing)
    let vm_noop = vm_with("f", b"return function(doc) return {} end");

    group.bench_function("noop", |b| {
        let input = small_doc();
        b.iter(|| vm_noop.call("f", &input).unwrap())
    });

    // Read-heavy: inspect multiple fields, return audit-style output
    let vm_audit = vm_with(
        "f",
        br#"return function(doc)
            return {
                action = "insert",
                collection = "users",
                key_fields = { doc.name, doc.email },
            }
        end"#,
    );

    group.bench_function("audit_log", |b| {
        let input = medium_doc();
        b.iter(|| vm_audit.call("f", &input).unwrap())
    });

    group.finish();
}

// ── Trigger with injected db callbacks ──────────────────────

fn bench_trigger_with_db(c: &mut Criterion) {
    let mut group = c.benchmark_group("trigger_with_db");

    // Single db.put call
    let vm_single = vm_with(
        "f",
        br#"return function(doc)
            db.put("audit", doc.name)
            return {}
        end"#,
    );
    let cb_noop: VmCallback = Arc::new(|_| Ok(Bson::Null));
    vm_single.inject("db", "put", cb_noop).unwrap();

    group.bench_function("single_put", |b| {
        let input = small_doc();
        b.iter(|| vm_single.call("f", &input).unwrap())
    });

    // Multiple db calls
    let vm_multi = vm_with(
        "f",
        br#"return function(doc)
            db.put("audit", doc.name)
            db.put("metrics", "insert_count")
            db.put("changelog", doc.name)
            return {}
        end"#,
    );
    let cb_noop2: VmCallback = Arc::new(|_| Ok(Bson::Null));
    vm_multi.inject("db", "put", cb_noop2).unwrap();

    group.bench_function("three_puts", |b| {
        let input = small_doc();
        b.iter(|| vm_multi.call("f", &input).unwrap())
    });

    // put + get round-trip
    let vm_rw = vm_with(
        "f",
        br#"return function(doc)
            db.put("users", doc.name)
            local result = db.get("users", doc.name)
            return { found = result }
        end"#,
    );
    let cb_put: VmCallback = Arc::new(|_| Ok(Bson::Null));
    let cb_get: VmCallback = Arc::new(|_| Ok(Bson::String("stored".into())));
    vm_rw.inject("db", "put", cb_put).unwrap();
    vm_rw.inject("db", "get", cb_get).unwrap();

    group.bench_function("put_and_get", |b| {
        let input = small_doc();
        b.iter(|| vm_rw.call("f", &input).unwrap())
    });

    // Realistic trigger: read doc fields, call db.put with a table arg
    let vm_real = vm_with(
        "f",
        br#"return function(doc)
            db.put("audit", {
                action = "insert",
                target = "users",
                key = doc.name,
                email = doc.email,
            })
            return {}
        end"#,
    );
    let cb_real: VmCallback = Arc::new(|_| Ok(Bson::Null));
    vm_real.inject("db", "put", cb_real).unwrap();

    group.bench_function("realistic_audit", |b| {
        let input = medium_doc();
        b.iter(|| vm_real.call("f", &input).unwrap())
    });

    group.finish();
}

// ── BSON type round-trip overhead ───────────────────────────

fn bench_type_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("type_roundtrip");

    let vm = vm_with("f", b"return function(doc) return doc end");

    // DateTime
    group.bench_function("datetime", |b| {
        let ts = bson::DateTime::from_millis(1_700_000_000_000);
        let input = rawdoc! { "ts": ts };
        b.iter(|| vm.call("f", &input).unwrap())
    });

    // ObjectId
    group.bench_function("objectid", |b| {
        let oid = bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        let input = rawdoc! { "_id": oid };
        b.iter(|| vm.call("f", &input).unwrap())
    });

    // Int32 (preserved, not widened)
    group.bench_function("i32", |b| {
        let input = rawdoc! { "n": 42_i32 };
        b.iter(|| vm.call("f", &input).unwrap())
    });

    // Int64
    group.bench_function("i64", |b| {
        let input = rawdoc! { "n": 42_i64 };
        b.iter(|| vm.call("f", &input).unwrap())
    });

    group.finish();
}

// ── VM lifecycle ────────────────────────────────────────────

fn bench_lifecycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("lifecycle");

    group.bench_function("vm_create", |b| {
        b.iter(|| LuaVm::new().unwrap())
    });

    group.bench_function("register", |b| {
        b.iter_batched(
            || LuaVm::new().unwrap(),
            |mut vm| {
                vm.register(
                    "f",
                    br#"return function(doc)
                        if doc.name ~= nil then return { ok = true }
                        else return { ok = false } end
                    end"#,
                )
                .unwrap();
            },
            BatchSize::PerIteration,
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_identity,
    bench_validator,
    bench_computed_field,
    bench_trigger,
    bench_trigger_with_db,
    bench_type_roundtrip,
    bench_lifecycle,
);
criterion_main!(benches);
