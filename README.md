# Slate

A document database built in Rust. Schema-flexible BSON documents with pluggable storage, query execution, indexing, and multiple deployment modes — from embedded in a Swift app to running in Kubernetes.

## Features

- **BSON document storage** — schema-flexible documents with zero-copy reads and no deserialization in the query pipeline
- **Atomic mutations** — `$set`, `$inc`, `$unset`, `$rename`, `$push`, `$pop`, `$lpush` with dot-path support — no read-modify-write required
- **Query engine** — filters, sorts, projections, pagination, distinct queries, dot-notation paths, and array element matching
- **Indexed queries** — single-field indexes with automatic plan optimization (index scans, covered projections)
- **Three storage backends** — RocksDB (fast, default), redb (pure Rust, no C dependencies), in-memory (ephemeral)
- **Swift/Apple embedding** — UniFFI bindings, XCFramework builds for macOS and iOS
- **Sub-millisecond indexed queries** at 100k records across all backends

## Crate Structure

```
slate/
  ├── slate-store            → Store/Transaction traits, RocksDB + redb + MemoryStore backends
  ├── slate-engine           → Storage engine: key encoding, TTL, indexes, catalog, record format
  ├── slate-query            → Query model: FindOptions, DistinctOptions, Sort, Mutation (pure data structures)
  ├── slate-db               → Database layer: filter parser, expression tree, query planner + executor
  └─── slate-uniffi           → UniFFI bindings for Swift/Kotlin (XCFramework builds)
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
```

## Usage

```rust
use bson::{doc, rawdoc};
use slate_db::{Database, DatabaseConfig};
use slate_query::FindOptions;
use slate_store::RocksStore; // or RedbStore for pure-Rust (no C deps)

let store = RocksStore::open("/tmp/slate-data")?;
let db = Database::open(store, DatabaseConfig::default());

// Insert
let mut txn = db.begin(false)?;
txn.insert_one("accounts", doc! {
    "_id": "acct-1",
    "name": "Acme Corp",
    "status": "active",
    "revenue": 50000.0
})?;
txn.commit()?;

// Query
let txn = db.begin(true)?;
let cursor = txn.find("accounts", rawdoc! {}, FindOptions::default())?;
for doc in cursor.iter()? {
    let doc = doc?; // RawDocumentBuf
}
let doc = txn.find_one("accounts", rawdoc! { "_id": "acct-1" })?;
let count = txn.count("accounts", rawdoc! {})?;

// Update — atomic mutations, no read-modify-write
let mut txn = db.begin(false)?;
txn.update_one("accounts",
    rawdoc! { "status": "active" },
    rawdoc! { "$set": { "status": "archived" }, "$inc": { "revenue": 5000.0 } },
)?.drain()?;
txn.commit()?;

// Indexes
let mut txn = db.begin(false)?;
txn.create_index("accounts", "status")?;
txn.commit()?;
```

## Performance

See [benchmarks](book/src/benchmarks.md) for full results including MemoryStore vs RocksDB vs redb comparisons across bulk inserts, queries, indexes, and concurrency tests.

## Documentation

Architecture docs are in the `book/` directory, built with [mdBook](https://rust-lang.github.io/mdBook/):

```bash
cd book && mdbook serve
```
