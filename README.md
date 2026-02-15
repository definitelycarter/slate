# Slate

A dynamic storage engine built in Rust. Schema-flexible records on top of RocksDB with query execution, a TCP server/client, and native + web frontends (planned).

## Crate Structure

```
slate/
  ├── slate-store   → Store/Transaction traits, Record/Value types, RocksDB impl
  ├── slate-query   → Query model: filters, operators, sorting (pure data structures)
  ├── slate-db      → Database layer: query execution (Volcano/iterator), datasource catalog
  ├── slate-server  → TCP server with bincode wire protocol, thread-per-connection
  ├── slate-client  → TCP client with connection pool
  └── slate-bench   → Benchmark suite (embedded + TCP)
```

## Quick Start

```bash
# Build
cargo build

# Run tests
cargo test

# Run benchmarks (3 users × 100k records each)
cargo run --release -p slate-bench
```

## Usage

### Embedded

```rust
use slate_db::Database;
use slate_store::{RocksStore, Record, Value};

let store = RocksStore::new("/tmp/slate-data");
let db = Database::new(store);

let mut txn = db.begin(false)?;
txn.insert(Record::new("acct-1", [
    ("name", Value::String("Acme Corp".into())),
    ("status", Value::String("active".into())),
]))?;
txn.commit()?;
```

### TCP Server

```rust
use slate_server::Server;

let server = Server::new(db, "127.0.0.1:9600");
server.serve(); // blocks
```

### TCP Client

```rust
use slate_client::{Client, ClientPool};

// Single connection
let mut client = Client::connect("127.0.0.1:9600")?;
client.insert(record)?;
let results = client.query(&query)?;

// Connection pool
let pool = ClientPool::new("127.0.0.1:9600", 10)?;
let mut client = pool.get()?; // blocks if all in use, returns on drop
client.query(&query)?;
```

## Performance (100k records, Apple Silicon)

| Operation | Embedded | TCP |
|-----------|----------|-----|
| Bulk insert (100k) | 194ms | 283ms |
| Full scan | 74ms | 148ms |
| Filter (~50k results) | 80ms | 123ms |
| Filter + take(200) | 69ms | 73ms |
| Filter + sort + take(200) | 127ms | 131ms |
| Point lookup (get_by_id) | 0.001ms | 0.04ms |

See [full benchmark results](book/src/benchmarks.md) for details.

## Documentation

Architecture docs are in the `book/` directory, built with [mdBook](https://rust-lang.github.io/mdBook/):

```bash
cd book && mdbook serve
```
