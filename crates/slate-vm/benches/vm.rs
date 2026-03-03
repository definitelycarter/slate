use bson::{Bson, rawdoc};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use slate_vm::{LuaScriptRuntime, ScriptCapabilities, ScriptHandle, ScriptRuntime, ScopedMethod, VmError};

// ── Helpers ─────────────────────────────────────────────────

fn load(name: &str, source: &[u8]) -> Box<dyn ScriptHandle> {
    let runtime = LuaScriptRuntime::new();
    runtime.load(name, source).unwrap()
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

    let handle = load("f", b"return function(ctx, doc) return doc end");

    group.bench_function("small_doc", |b| {
        let input = small_doc();
        b.iter(|| handle.call(&input, &ScriptCapabilities::Pure).unwrap())
    });

    group.bench_function("medium_doc", |b| {
        let input = medium_doc();
        b.iter(|| handle.call(&input, &ScriptCapabilities::Pure).unwrap())
    });

    group.finish();
}

// ── Validator ───────────────────────────────────────────────

fn bench_validator(c: &mut Criterion) {
    let mut group = c.benchmark_group("validator");

    // Simple: check one field exists
    let handle_simple = load(
        "f",
        br#"return function(ctx, doc)
            if doc.name ~= nil then
                return { ok = true }
            else
                return { ok = false, reason = "name is required" }
            end
        end"#,
    );

    group.bench_function("simple_pass", |b| {
        let input = small_doc();
        b.iter(|| handle_simple.call(&input, &ScriptCapabilities::Pure).unwrap())
    });

    group.bench_function("simple_fail", |b| {
        let input = rawdoc! { "status": "active" };
        b.iter(|| handle_simple.call(&input, &ScriptCapabilities::Pure).unwrap())
    });

    // Multi-field: check several constraints
    let handle_multi = load(
        "f",
        br#"return function(ctx, doc)
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
        b.iter(|| handle_multi.call(&input, &ScriptCapabilities::Pure).unwrap())
    });

    group.finish();
}

// ── Computed fields ─────────────────────────────────────────

fn bench_computed_field(c: &mut Criterion) {
    let mut group = c.benchmark_group("computed_field");

    // String concatenation
    let handle_concat = load(
        "f",
        br#"return function(ctx, doc)
            return { value = doc.first .. " " .. doc.last }
        end"#,
    );

    group.bench_function("string_concat", |b| {
        let input = rawdoc! { "first": "Ada", "last": "Lovelace" };
        b.iter(|| handle_concat.call(&input, &ScriptCapabilities::Pure).unwrap())
    });

    // Arithmetic
    let handle_arith = load(
        "f",
        br#"return function(ctx, doc)
            return { value = (doc.score * 100 + 0.5) }
        end"#,
    );

    group.bench_function("arithmetic", |b| {
        let input = rawdoc! { "score": 0.875 };
        b.iter(|| handle_arith.call(&input, &ScriptCapabilities::Pure).unwrap())
    });

    // Build a derived document
    let handle_derive = load(
        "f",
        br#"return function(ctx, doc)
            return {
                full_name = doc.first .. " " .. doc.last,
                display = doc.first .. " " .. doc.last .. " <" .. doc.email .. ">",
            }
        end"#,
    );

    group.bench_function("derived_doc", |b| {
        let input = medium_doc();
        b.iter(|| handle_derive.call(&input, &ScriptCapabilities::Pure).unwrap())
    });

    group.finish();
}

// ── Trigger (simulates side-effect-free trigger logic) ──────

fn bench_trigger(c: &mut Criterion) {
    let mut group = c.benchmark_group("trigger");

    // Minimal: just return empty doc (trigger that inspects nothing)
    let handle_noop = load("f", b"return function(ctx, doc) return {} end");

    group.bench_function("noop", |b| {
        let input = small_doc();
        b.iter(|| handle_noop.call(&input, &ScriptCapabilities::Pure).unwrap())
    });

    // Read-heavy: inspect multiple fields, return audit-style output
    let handle_audit = load(
        "f",
        br#"return function(ctx, doc)
            return {
                action = "insert",
                collection = "users",
                key_fields = { doc.name, doc.email },
            }
        end"#,
    );

    group.bench_function("audit_log", |b| {
        let input = medium_doc();
        b.iter(|| handle_audit.call(&input, &ScriptCapabilities::Pure).unwrap())
    });

    group.finish();
}

// ── Trigger with injected db callbacks ──────────────────────

fn bench_trigger_with_db(c: &mut Criterion) {
    let mut group = c.benchmark_group("trigger_with_db");

    let noop_cb = |_: Vec<Bson>| -> Result<Bson, VmError> { Ok(Bson::Null) };

    // Single ctx.put call
    let handle_single = load(
        "f",
        br#"return function(ctx, doc)
            ctx.put("audit", doc.name)
            return {}
        end"#,
    );

    group.bench_function("single_put", |b| {
        let input = small_doc();
        let methods = [ScopedMethod { name: "put", callback: &noop_cb }];
        let caps = ScriptCapabilities::ReadWrite { methods: &methods };
        b.iter(|| handle_single.call(&input, &caps).unwrap())
    });

    // Multiple ctx calls
    let handle_multi = load(
        "f",
        br#"return function(ctx, doc)
            ctx.put("audit", doc.name)
            ctx.put("metrics", "insert_count")
            ctx.put("changelog", doc.name)
            return {}
        end"#,
    );

    group.bench_function("three_puts", |b| {
        let input = small_doc();
        let methods = [ScopedMethod { name: "put", callback: &noop_cb }];
        let caps = ScriptCapabilities::ReadWrite { methods: &methods };
        b.iter(|| handle_multi.call(&input, &caps).unwrap())
    });

    // put + get round-trip
    let handle_rw = load(
        "f",
        br#"return function(ctx, doc)
            ctx.put("users", doc.name)
            local result = ctx.get("users", doc.name)
            return { found = result }
        end"#,
    );

    let get_cb = |_: Vec<Bson>| -> Result<Bson, VmError> { Ok(Bson::String("stored".into())) };

    group.bench_function("put_and_get", |b| {
        let input = small_doc();
        let methods = [
            ScopedMethod { name: "put", callback: &noop_cb },
            ScopedMethod { name: "get", callback: &get_cb },
        ];
        let caps = ScriptCapabilities::ReadWrite { methods: &methods };
        b.iter(|| handle_rw.call(&input, &caps).unwrap())
    });

    // Realistic trigger: read doc fields, call ctx.put with a table arg
    let handle_real = load(
        "f",
        br#"return function(ctx, doc)
            ctx.put("audit", {
                action = "insert",
                target = "users",
                key = doc.name,
                email = doc.email,
            })
            return {}
        end"#,
    );

    group.bench_function("realistic_audit", |b| {
        let input = medium_doc();
        let methods = [ScopedMethod { name: "put", callback: &noop_cb }];
        let caps = ScriptCapabilities::ReadWrite { methods: &methods };
        b.iter(|| handle_real.call(&input, &caps).unwrap())
    });

    group.finish();
}

// ── BSON type round-trip overhead ───────────────────────────

fn bench_type_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("type_roundtrip");

    let handle = load("f", b"return function(ctx, doc) return doc end");

    // DateTime
    group.bench_function("datetime", |b| {
        let ts = bson::DateTime::from_millis(1_700_000_000_000);
        let input = rawdoc! { "ts": ts };
        b.iter(|| handle.call(&input, &ScriptCapabilities::Pure).unwrap())
    });

    // ObjectId
    group.bench_function("objectid", |b| {
        let oid = bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        let input = rawdoc! { "_id": oid };
        b.iter(|| handle.call(&input, &ScriptCapabilities::Pure).unwrap())
    });

    // Int32 (preserved, not widened)
    group.bench_function("i32", |b| {
        let input = rawdoc! { "n": 42_i32 };
        b.iter(|| handle.call(&input, &ScriptCapabilities::Pure).unwrap())
    });

    // Int64
    group.bench_function("i64", |b| {
        let input = rawdoc! { "n": 42_i64 };
        b.iter(|| handle.call(&input, &ScriptCapabilities::Pure).unwrap())
    });

    group.finish();
}

// ── Script lifecycle ─────────────────────────────────────────

fn bench_lifecycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("lifecycle");

    let runtime = LuaScriptRuntime::new();

    group.bench_function("runtime_load", |b| {
        b.iter(|| {
            runtime
                .load(
                    "f",
                    br#"return function(ctx, doc)
                        if doc.name ~= nil then return { ok = true }
                        else return { ok = false } end
                    end"#,
                )
                .unwrap()
        })
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
