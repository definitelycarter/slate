# Slate

A document database built in Rust. Schema-flexible BSON documents with pluggable storage, query execution, indexing, and column-family-scoped collections — from embedded in a Swift app to standalone server.

## Features

- **BSON document storage** — schema-flexible documents with zero-copy reads and no deserialization in the query pipeline
- **Atomic mutations** — `$set`, `$inc`, `$unset`, `$rename`, `$push`, `$pop`, `$lpush` with dot-path support — no read-modify-write required
- **Query engine** — filters, sorts, projections, pagination, distinct queries, dot-notation paths, and array element matching
- **Indexed queries** — single-field indexes with automatic plan optimization (index scans, covered projections)
- **Lua scripting** — triggers, validators, and UDFs with sandboxed execution, BSON type preservation, and snapshot-isolated hook resolution
- **Three storage backends** — RocksDB (fast, default), redb (pure Rust, no C dependencies), in-memory (ephemeral)
- **Swift/Apple embedding** — UniFFI bindings, XCFramework builds for macOS and iOS
- **WebAssembly** — wasm-bindgen bindings with JS-native object interface (no BSON library required)
- **Sub-millisecond indexed queries** at 100k records across all backends

## Crate Structure

```
slate/
  ├── slate-store            → Store/Transaction traits, RocksDB + redb + MemoryStore backends
  ├── slate-engine           → Storage engine: key encoding, TTL, indexes, catalog, record format
  ├── slate-query            → Query model: FindOptions, DistinctOptions, Sort, Mutation (pure data structures)
  ├── slate-vm               → Scripting engine: runtime-agnostic VM pool, Lua runtime (feature-gated)
  ├── slate-db               → Database layer: filter parser, expression tree, query planner + executor
  ├── slate-uniffi           → UniFFI bindings for Swift/Kotlin (XCFramework builds)
  └── slate-wasm             → wasm-bindgen bindings for JavaScript/WebAssembly
```

## Quick Start

```bash
# Build
cargo build

# Run tests
cargo test --workspace

# Run database benchmarks (criterion suite)
cargo bench -p slate-db --features bench-internals

# Run store benchmarks (500k x 10KB records)
cargo run --release -p slate-store-bench

# Run examples
cargo run --example basic -p slate-db
cargo run --example triggers -p slate-db
cargo run --example validators -p slate-db
```

## Usage

```rust
use bson::{doc, rawdoc};
use slate_db::{DatabaseBuilder, DEFAULT_CF};
use slate_query::FindOptions;
use slate_store::RocksStore; // or RedbStore for pure-Rust (no C deps)

let store = RocksStore::open("/tmp/slate-data")?;
let db = DatabaseBuilder::new().open(store)?;

// Insert
let mut txn = db.begin(false)?;
txn.insert_one(DEFAULT_CF, "accounts", doc! {
    "_id": "acct-1",
    "name": "Acme Corp",
    "status": "active",
    "revenue": 50000.0
})?;
txn.commit()?;

// Query
let txn = db.begin(true)?;
let cursor = txn.find(DEFAULT_CF, "accounts", rawdoc! {}, FindOptions::default())?;
for doc in cursor.iter()? {
    let doc = doc?; // RawDocumentBuf
}
let doc = txn.find_one(DEFAULT_CF, "accounts", rawdoc! { "_id": "acct-1" })?;
let count = txn.count(DEFAULT_CF, "accounts", rawdoc! {})?;

// Update — atomic mutations, no read-modify-write
let mut txn = db.begin(false)?;
txn.update_one(DEFAULT_CF, "accounts",
    rawdoc! { "status": "active" },
    rawdoc! { "$set": { "status": "archived" }, "$inc": { "revenue": 5000.0 } },
)?.drain()?;
txn.commit()?;

// Indexes
let mut txn = db.begin(false)?;
txn.create_index(DEFAULT_CF, "accounts", "status")?;
txn.commit()?;
```

### Triggers and Validators

Attach Lua scripts to collections for side effects (triggers) and constraints (validators). Requires `DatabaseBuilder` with a scripting pool.

```rust
use std::sync::Arc;
use slate_db::{DatabaseBuilder, RuntimeRegistry, VmPool};
use slate_vm::{LuaScriptRuntime, RuntimeKind};

let mut reg = RuntimeRegistry::new();
reg.register(RuntimeKind::Lua, Arc::new(LuaScriptRuntime::new()));
let db = DatabaseBuilder::new()
    .with_scripting(VmPool::new(reg))
    .open(store)?;

// Validator — reject documents missing a "name" field
let mut txn = db.begin(false)?;
txn.register_validator("app", "accounts", "require_name", r#"
    return function(event)
      if type(event.doc.name) ~= "string" or event.doc.name == "" then
        return { ok = false, reason = "name is required" }
      end
      return { ok = true }
    end
"#)?;

// Trigger — log every mutation to an audit collection
txn.register_trigger("app", "accounts", "audit_log", r#"
    return function(ctx, event)
      ctx.put("audit", {
        _id       = tostring(event.doc._id) .. ":" .. event.action,
        action    = event.action,
        doc_id    = event.doc._id,
        timestamp = bson.now(),
      })
      return event
    end
"#)?;
txn.commit()?;
```

## Performance

See [benchmarks](book/src/benchmarks.md) for full results including MemoryStore vs RocksDB vs redb comparisons across bulk inserts, queries, indexes, and concurrency tests.

## Documentation

Architecture docs are in the `book/` directory, built with [mdBook](https://rust-lang.github.io/mdBook/):

```bash
cd book && mdbook serve
```
