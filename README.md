# Slate

A document database built in Rust. Schema-flexible BSON documents with pluggable storage, query execution, indexing, and column-family-scoped collections — embedded in-process, from a native Swift app to the browser via WebAssembly.

## Features

- **BSON document storage** — schema-flexible documents; the query pipeline filters, projects, and sorts directly over raw BSON bytes, with no serde deserialization
- **Atomic mutations** — `$set`, `$inc`, `$unset`, `$rename`, `$push`, `$pop`, `$lpush` with dot-path support — no read-modify-write required
- **Multi-statement transactions** — `db.begin(read_only)` groups many operations into one atomic commit, with a documented cross-backend contract: snapshot isolation taken at `begin`, read-your-writes, and a commit-time write-write conflict possible on every backend (`DbError::Conflict`). `db.transact(|txn| …)` runs a closure and retries on conflict — bounded and `wasm32`-safe by default, with an optional injectable backoff. See the [transaction & concurrency contract](book/src/architecture-database.md#concurrency-and-the-transaction-contract)
- **Query engine** — filters, sorts, projections, pagination, distinct queries, dot-notation paths, and array element matching
- **Two query surfaces** — a MongoDB-style `find` and a CosmosDB-style SQL (`SELECT * | VALUE <expr> | <cols>  FROM c [JOIN ...] [WHERE ...] [ORDER BY ...]` via `collection.query()`) that lower to one shared planner/executor
- **Change detection (watch queries)** — register a filter on a collection and get matching inserts/updates/deletes delivered as they commit, by callback (`watch`/`watch_query`) or a pull cursor (`stream`/`stream_query`); BSON or SQL filter, coalesced per-commit and recast to set enter/leave events — in-process reactivity for live UIs, IoT rules, and sync, with no server or polling
- **Indexed queries** — single-field, compound (multi-field, leftmost-prefix), and unique indexes with automatic plan optimization (index scans, index-merge for OR, and a galloping skip-merge that intersects an all-equality AND bounded by its most selective index)
- **Vector search** — flat (exact, brute-force) k-nearest-neighbour over embedding fields: `ORDER BY VECTORDISTANCE(c.embedding, @q) LIMIT k` seeks a per-field vector index (cosine / dot-product / euclidean), with `WHERE` pre-filtering before the top-k; on-device RAG / semantic search (the app supplies embeddings, Slate stores and searches them). Optional **quantized storage** — `float16` (2× smaller) or `int8` (~4×) via `VectorIndexOptions::float16`/`::int8` — stays effectively exact by rescoring the shortlist against each document's full-precision vector
- **Observability** — `EXPLAIN` plus `EXPLAIN ANALYZE` (the plan tree annotated with per-node `rows=`/`examined=` counts), a `stats()` size/cardinality surface, and feature-gated `tracing` spans (off by default, zero-cost when off)
- **Native functions (UDFs)** — register Rust functions in a database-scoped bag (`with_udf` / `functions().register`) and bind them per-collection (`functions().create`), callable as `udf.name(...)` in SQL; the binding resolves at plan build (a dangling binding fails before execution, with per-collection isolation) and runs behind a panic boundary
- **Native validators** — register Rust functions in a database-scoped bag and bind them per collection (`with_validator` / `validators().register` + `validators().create`) to gate writes: a validator returns `Verdict::accept`/`reject`, runs behind a panic boundary, and a dangling binding fails writes safely (the binding resolves once per query, then runs per row)
- **Native triggers** — register Rust functions in a database-scoped bag and bind them per collection (`with_trigger` / `triggers().register` + `triggers().create`) to run side effects on writes: a trigger sees the action and candidate document and can read/write other documents in the same column family, runs behind a panic boundary, and a dangling binding fails writes safely (the binding resolves once per query)
- **Online backup** — `db.backup(path)` for hot snapshots (RocksDB checkpoint, redb file copy)
- **Logical export / import** — `db.export(path)` / `db.import(path)` write a portable BSON dump (manifest + per-collection document streams) that rebuilds indexes on load, for cross-backend migration (e.g. redb → RocksDB), seeding, and recovery
- **Encryption at rest via the OS** — relies on the device's full-disk / file-level encryption (iOS Data Protection, FileVault/APFS, equivalents), which protects the whole on-disk file — keys, values, and `_id`s — on a locked or powered-off device; no app-level crypto. See [the guarantee and threat model](book/src/architecture-storage.md#encryption-at-rest)
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
  ├── slate-value            → The shared value domain (`Value`) — function crates speak it without the evaluator
  ├── slate-udf              → Native UDF trait + the database-scoped code bag (baked into the plan/executor)
  ├── slate-validator        → Native validator trait + the database-scoped validator bag (write-path gate)
  ├── slate-trigger          → Native trigger trait + the database-scoped trigger bag (write-path side effects)
  ├── slate-eval             → Evaluation semantics for the AST (owned + zero-copy evaluators)
  ├── slate-planner          → Logical planning: AST → Plan/Node IR (sargability, index choice)
  ├── slate-executor         → Physical execution: streams a Plan against a transaction
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
slate(sample)> .explain analyze SELECT VALUE c.name FROM c WHERE c.age > 30
slate(sample)> .stats sample
```

`.explain <query>` shows a query's plan; `.explain analyze <query>` runs it and
annotates each plan node with `rows=`/`examined=` counts; `.stats [collection]`
reports document, index-entry, and cardinality numbers.

## Usage

```rust
use bson::doc;
use slate_db::DatabaseBuilder;
use slate_db::v2::IndexOptions;
use slate_store::RocksStore; // or RedbStore for pure-Rust (no C deps)

let store = RocksStore::open("/tmp/slate-data")?;
let db = DatabaseBuilder::new().open(store)?;

// Insert
let txn = db.begin(false)?;
db.collections().create("accounts").execute(&txn)?;
db.collection("accounts").insert_one(doc! {
    "_id": "acct-1",
    "name": "Acme Corp",
    "status": "active",
    "revenue": 50000.0
}).execute(&txn)?;
txn.commit()?;

// Query
let txn = db.begin(true)?;
let accounts = db.collection("accounts");
for doc in accounts.find(doc! {}).iter::<MyStruct>(&txn)? {   // deserializes into T
    let doc = doc?;
}
// or keep raw bytes:
for raw in accounts.find(doc! {}).iter_raw(&txn)? {
    let raw = raw?; // RawDocumentBuf — zero deserialization
}
let one = accounts.find(doc! { "_id": "acct-1" }).iter::<MyStruct>(&txn)?.next().transpose()?;
let count = accounts.find(doc! {}).iter_raw(&txn)?.count();

// Update — atomic mutations, no read-modify-write
let txn = db.begin(false)?;
db.collection("accounts").find(doc! { "status": "active" })
    .update(doc! { "$set": { "status": "archived" }, "$inc": { "revenue": 5000.0 } })
    .execute(&txn)?;
txn.commit()?;

// Indexes
let txn = db.begin(false)?;
let accounts = db.collection("accounts");
accounts.indexes().create("status", IndexOptions::default()).execute(&txn)?;
accounts.indexes().create("email", IndexOptions::unique()).execute(&txn)?; // rejects duplicate emails
accounts.indexes().create(["status", "created_at"], IndexOptions::default()).execute(&txn)?; // leftmost-prefix compound
txn.commit()?;
```

### Triggers and Validators

Both validators and triggers are **native Rust functions**: register one in the
database-scoped bag and bind it per collection. Both resolve from a snapshot at
plan time and run behind a panic boundary — there is no embedded VM.

```rust
use slate_db::{DatabaseBuilder, TriggerCtx, ValidatorCtx, Verdict};
use slate_db::v2::{TriggerFunction, ValidatorFunction};

let db = DatabaseBuilder::new()
    // Native validator — reject documents missing a "name" field
    .with_validator("require_name", |ctx: &ValidatorCtx| {
        match ctx.doc().get_str("name") {
            Ok(name) if !name.is_empty() => Ok(Verdict::Accept),
            _ => Ok(Verdict::reject("name is required")),
        }
    })
    // Native trigger — mirror every mutation into an audit collection (same cf)
    .with_trigger("audit", |ctx: &TriggerCtx| {
        let id = ctx.doc().get_str("_id").unwrap_or("?");
        ctx.put("audit", &bson::rawdoc! {
            "_id": format!("{id}:{}", ctx.action()),
            "action": ctx.action(),
            "doc_id": id,
        })?;
        Ok(())
    })
    .open(store)?;

let accounts = db.collection("accounts");

let txn = db.begin(false)?;
// Bind the registered native functions to this collection
accounts.validators()
    .create("require_name", ValidatorFunction::from_name("require_name"))
    .execute(&txn)?;
accounts.triggers()
    .create("audit_log", TriggerFunction::from_name("audit"))
    .execute(&txn)?;
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
