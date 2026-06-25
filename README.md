# Slate

A document database built in Rust. Schema-flexible BSON documents with pluggable storage, query execution, indexing, and column-family-scoped collections — embedded in-process, from a native Swift app to the browser via WebAssembly.

## Features

- **BSON document storage** — schema-flexible documents with zero-copy reads and no deserialization in the query pipeline
- **Atomic mutations** — `$set`, `$inc`, `$unset`, `$rename`, `$push`, `$pop`, `$lpush` with dot-path support — no read-modify-write required
- **Query engine** — filters, sorts, projections, pagination, distinct queries, dot-notation paths, and array element matching
- **Two query surfaces** — a MongoDB-style `find` and a CosmosDB-style SQL (`SELECT * | VALUE <expr> | <cols>  FROM c [JOIN ...] [WHERE ...] [ORDER BY ...]` via `txn.query()`) that lower to one shared planner/executor
- **Indexed queries** — single-field and unique indexes with automatic plan optimization (index scans, index-merge for AND/OR)
- **Lua scripting** — triggers, validators, and UDFs with sandboxed execution, BSON type preservation, and snapshot-isolated hook resolution
- **Online backup** — `db.backup(path)` for hot snapshots (RocksDB checkpoint, redb file copy)
- **Logical export / import** — `db.export(path)` / `db.import(path)` write a portable BSON dump (manifest + per-collection document streams) that rebuilds indexes on load, for cross-backend migration (e.g. redb → RocksDB), seeding, and recovery
- **Three storage backends** — RocksDB (fast), redb (pure Rust, no C dependencies), in-memory (ephemeral, default)
- **Swift/Apple embedding** — UniFFI bindings, XCFramework builds for macOS and iOS
- **WebAssembly** — wasm-bindgen bindings with JS-native object interface (no BSON library required), powering an in-browser [query playground](book/src/playground.md) in the docs
- **Interactive shell** — `slate`, a REPL to create collections, insert documents, and run SQL against a live (in-memory or persistent) database
- **Sub-millisecond indexed queries** at 10k records across all backends

## Crate Structure

```
slate/
  ├── slate-store            → Store/Transaction traits, RocksDB + redb + MemoryStore backends
  ├── slate-rawbson          → Fast raw byte-level BSON field scanner + document merge (shared leaf)
  ├── slate-engine           → Storage engine: key encoding, TTL, indexes, catalog, record format
  ├── slate-ast              → Shared query AST — the IR both query surfaces target
  ├── slate-query            → MongoDB find front-end: FindOptions/Sort + filter→AST translation
  ├── slate-sql              → CosmosDB-style SQL front-end: SQL text → AST
  ├── slate-eval             → Evaluation semantics for the AST (owned + zero-copy evaluators)
  ├── slate-planner          → Logical planning: AST → Plan/Node IR (sargability, index choice)
  ├── slate-executor         → Physical execution: streams a Plan against a transaction
  ├── slate-vm               → Scripting engine: runtime-agnostic VM pool, Lua runtime (feature-gated)
  ├── slate-db               → Database layer: public API, query planning + execution
  ├── slate-uniffi           → UniFFI bindings for Swift/Kotlin (XCFramework builds)
  ├── slate-wasm             → wasm-bindgen bindings for JavaScript/WebAssembly
  └── slate-cli              → `slate`, an interactive shell (REPL) over the public API
```

The `slate-ast` … `slate-executor` crates are the query stack: two surfaces — a MongoDB `find` and a CosmosDB-style SQL — lower to one shared AST, planner, and executor, so they can't drift.

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

# Launch the interactive shell (in-memory; --rocksdb <path> to persist)
cargo run -p slate-cli
```

In the shell, lines starting with `.` are commands (`.help` lists them) and
everything else is run as SQL against the active collection. A SQL statement is
terminated by `;` and may span multiple lines:

```
slate> .seed
slate(sample)> SELECT c.city, COUNT(1) AS n
          ...> FROM c GROUP BY c.city;
slate(sample)> .insert {"_id":"5","name":"linus","city":"Helsinki"}
```

## Usage

```rust
use bson::{doc, rawdoc};
use slate_db::{DatabaseBuilder, DEFAULT_CF, FindOptions};
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
for doc in cursor.iter::<MyStruct>()? {   // deserializes into T
    let doc = doc?;
}
// or keep raw bytes:
for raw in cursor.iter_raw()? {
    let raw = raw?; // RawDocumentBuf — zero deserialization
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
txn.create_unique_index(DEFAULT_CF, "accounts", "email")?; // rejects duplicate emails
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
mdbook serve book   # local preview at http://localhost:3000
mdbook build book   # render to book/book (what CI verifies)
```

The book includes an in-browser **[Playground](book/src/playground.md)** —
runnable SQL cells that execute real slate queries client-side via the
`slate-wasm` build, with no backend. The playground fetches its WebAssembly
module over HTTP, so it only activates under `mdbook serve` (not when opening an
`.html` file from disk); blocks degrade to static examples otherwise.

The playground ships a prebuilt wasm bundle (`book/src/playground/pkg`, checked
in) so `mdbook build` needs no Rust/wasm toolchain. Rebuild it after changing
`crates/slate-wasm`:

```bash
./book/build-playground.sh   # requires wasm-pack + the wasm32 target
```
