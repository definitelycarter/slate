# Slate

A document database built in Rust. Schema-flexible BSON documents with pluggable storage, query execution, indexing, and multiple deployment modes — from embedded in a Swift app to running in Kubernetes.

## Features

- **BSON document storage** — schema-flexible documents with zero-copy reads and lazy materialization
- **Query engine** — filters, sorts, projections, pagination, distinct queries, dot-notation paths, and array element matching
- **Indexed queries** — single-field indexes with automatic plan optimization (index scans, covered projections)
- **Three storage backends** — RocksDB (fast, default), redb (pure Rust, no C dependencies), in-memory (ephemeral)
- **TCP server/client** — MessagePack wire protocol, thread-per-connection, connection pooling
- **HTTP API** — framework-agnostic CRUD endpoints with a standalone server
- **Swift/Apple embedding** — UniFFI bindings, XCFramework builds for macOS and iOS
- **Kubernetes operator** — declarative Server and Collection CRDs with full lifecycle management
- **Sub-millisecond indexed queries** at 100k records across all backends

## Crate Structure

```
slate/
  ├── slate-store            → Store/Transaction traits, RocksDB + redb + MemoryStore backends
  ├── slate-query            → Query model: filters, operators, sorting (pure data structures)
  ├── slate-db               → Database layer: collections, indexes, query planner + executor
  ├── slate-server           → TCP server with MessagePack wire protocol, thread-per-connection
  ├── slate-client           → TCP client with connection pool
  ├── slate-server-init      → CLI tool to initialize collections on a running server
  ├── slate-collection       → HTTP handler for collection CRUD (framework-agnostic)
  ├── slate-collection-http  → Standalone HTTP server wrapping slate-collection
  ├── slate-operator         → Kubernetes operator for Server + Collection CRDs
  ├── slate-bench            → Database-level benchmark suite (embedded + TCP, both backends)
  └── slate-store-bench      → Store-level benchmark suite (raw read/write throughput)
```

## Quick Start

```bash
# Build
cargo build

# Run tests
cargo test --workspace

# Run database benchmarks (3 users x 100k records each)
cargo run --release -p slate-bench

# Run store benchmarks (500k x 10KB records)
cargo run --release -p slate-store-bench
```

## Usage

### Embedded

```rust
use bson::doc;
use slate_db::{Database, DatabaseConfig};
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
let mut txn = db.begin(true)?;
let results = txn.find("accounts", &query)?;
let doc = txn.find_by_id("accounts", "acct-1", None)?;
let count = txn.count("accounts", None)?;

// Update
let mut txn = db.begin(false)?;
txn.update_one("accounts", &filter, doc! { "status": "archived" }, false)?;
txn.commit()?;

// Indexes
let mut txn = db.begin(false)?;
txn.create_index("accounts", "status")?;
txn.commit()?;
```

### TCP Server

```rust
use slate_server::Server;

let server = Server::new(db, "127.0.0.1:9600");
server.serve()?;
```

### TCP Client

```rust
use bson::doc;
use slate_client::{Client, ClientPool};

let mut client = Client::connect("127.0.0.1:9600")?;
client.insert_one("accounts", doc! { "name": "Acme" })?;
let results = client.find("accounts", &query)?;
let doc = client.find_by_id("accounts", "acct-1", None)?;

// Connection pool
let pool = ClientPool::new("127.0.0.1:9600", 10)?;
let mut client = pool.get()?;
client.find("accounts", &query)?;
```

## Performance

See [benchmarks](book/src/benchmarks.md) for full results including MemoryStore vs RocksDB vs redb comparisons across bulk inserts, queries, indexes, and concurrency tests.

## Documentation

Architecture docs are in the `book/` directory, built with [mdBook](https://rust-lang.github.io/mdBook/):

```bash
cd book && mdbook serve
```
