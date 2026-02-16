# Architecture

## Overview

A three-tiered system with a dynamic storage engine, a database layer with query execution, a REST API, and native + web frontends sharing a common WASM engine.

```
rust core (lib crate)
  └── wasm-bindgen → WASM module
        ├── SwiftUI app (embedded WASM runtime)
        ├── Web app (browser WASM runtime)
        └── REST API (cloud deployment, native Rust)
```

## Crate Structure

```
slate/
  ├── slate-store        → Store/Transaction traits, RocksDB + MemoryStore impls (feature-gated)
  ├── slate-query        → Query model, Filter, Operator, Sort, QueryValue (pure data structures)
  ├── slate-db           → Database, Catalog, query execution, depends on slate-store + slate-query
  ├── slate-server       → TCP server, bincode wire protocol, thread-per-connection
  ├── slate-client       → TCP client, connection pool (crossbeam channel)
  ├── slate-bench        → DB-level benchmark suite (embedded + TCP, both backends)
  └── slate-store-bench  → Store-level stress test (500k records, race conditions, data integrity)
```

## Tier 1: Storage Layer (`slate-store`)

### Overview

The store is a dumb, schema-unaware key-value storage layer. It stores and retrieves raw bytes within column-family-scoped transactions. It knows nothing about records, datasources, users, schemas, or query optimization — those are higher-level concerns handled by `slate-db`.

### Store Trait

The `Store` trait provides column family management, range deletion, and transaction creation. Uses a GAT for the transaction lifetime.

```rust
pub trait Store {
    type Txn<'a>: Transaction where Self: 'a;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, StoreError>;
    fn create_cf(&self, name: &str) -> Result<(), StoreError>;
    fn drop_cf(&self, name: &str) -> Result<(), StoreError>;
    fn delete_range(&self, cf: &str, range: impl RangeBounds<Vec<u8>>) -> Result<(), StoreError>;
}
```

### Transaction Trait

All read/write operations go through a transaction. Read-only transactions return errors on write operations (enforced at runtime). Everything is raw bytes — serialization is the caller's responsibility.

```rust
pub trait Transaction {
    // Reads
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Box<[u8]>>, StoreError>;
    fn multi_get(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Box<[u8]>>>, StoreError>;
    fn scan_prefix(&self, cf: &str, prefix: &[u8])
        -> Result<Box<dyn Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), StoreError>> + '_>, StoreError>;

    // Writes
    fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StoreError>;
    fn put_batch(&mut self, cf: &str, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError>;
    fn delete(&mut self, cf: &str, key: &[u8]) -> Result<(), StoreError>;

    // Lifecycle
    fn commit(self) -> Result<(), StoreError>;
    fn rollback(self) -> Result<(), StoreError>;
}
```

### Error Type

Custom `StoreError` enum with variants: `TransactionConsumed`, `ReadOnly`, `Serialization`, `Storage`.

### Implementation: RocksDB

The default implementation uses RocksDB (feature-gated). RocksDB provides:

- Embedded storage, no separate server process
- Native transaction support (`OptimisticTransactionDB`) with begin/commit/rollback
- Snapshot isolation for read-only transactions
- MVCC-like behavior built in — no need to implement our own

Other implementations can be added behind feature flags.

### Implementation: In-Memory (`MemoryStore`)

The in-memory implementation (feature-gated behind `memory`) is designed for ephemeral cache workloads where data is populated from an upstream source and doesn't need to survive process restarts.

**Why not RocksDB for caching?** RocksDB is a disk-backed LSM engine — it pays for durability (WAL, compaction, fsync) that an ephemeral cache doesn't need. At 500k records, MemoryStore writes are 2x faster and reads are 1.5-1.9x faster.

**Why not an external database (MongoDB, Redis)?** The data model is already document-shaped (`Record` with nested `Value` types). Adding an external database means shipping a server dependency, managing connections, and translating between type systems. An embedded in-memory store gives zero operational overhead, no network round-trips, and direct Rust type access.

**Why not partial deserialization with BSON?** BSON supports skipping nested fields without full deserialization. But at 50 fields with small nested arrays, bincode full deserialization is already fast enough that partial reads don't pay for themselves. The overhead of a self-describing format (field names stored per document) is a constant tax on every read/write.

**Architecture** (inspired by SurrealDB's [echodb](https://github.com/surrealdb/echodb)):

- **`imbl::OrdMap`** per column family — immutable B-tree with structural sharing. Snapshot clones are O(1), not O(n). Ordered keys give sorted iteration for `scan_prefix` and `delete_range`.
- **`arc_swap::ArcSwap`** per column family — lock-free atomic pointer swap. Readers load the current pointer without blocking. Writers swap in a new pointer on commit.
- **`std::sync::Mutex`** write lock — serializes writers. Acquired at `begin(false)`, released on commit/rollback.

**Concurrency model:**

- Readers snapshot via `ArcSwap::load` (lock-free) and see a consistent point-in-time view.
- Writers acquire the mutex, clone the OrdMap (cheap via structural sharing), mutate locally, and atomically swap on commit.
- Multiple concurrent readers never block each other or writers.
- A reader that started before a commit continues seeing old data (snapshot isolation).

**Memory footprint:** ~5-6 KB per record (50 fields, small nested arrays). At 500k records, ~2.5-3 GB — fits comfortably in an 8 GB ECS container with headroom for request handling, sorting, and transient allocations.

## Tier 2: Query Layer (`slate-query`)

### Overview

Pure data structures representing queries. No dependencies on storage or execution — transport-agnostic. Can be constructed from query strings, JSON, GraphQL, or programmatically.

### Query Model

```rust
pub struct Query {
    pub filter: Option<FilterGroup>,
    pub sort: Vec<Sort>,
    pub skip: Option<usize>,
    pub take: Option<usize>,
}
```

### Filters

Django-style field + operator model. Operators: `Eq`, `IContains`, `IStartsWith`, `IEndsWith`, `Gt`, `Gte`, `Lt`, `Lte`, `IsNull`.

Filter groups support arbitrary nesting with `And`/`Or` logical operators:

```rust
pub enum FilterNode {
    Condition(Filter),
    Group(FilterGroup),
}

pub struct FilterGroup {
    pub logical: LogicalOp,  // And | Or
    pub children: Vec<FilterNode>,
}
```

### Query Values

Scalar types only — no `Map` or `List` (those are storage concerns, not query concerns):

```rust
pub enum QueryValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Date(i64),
    Null,
}
```

## Tier 3: Database Layer (`slate-db`)

### Overview

The database layer sits on top of `slate-store` and `slate-query`. It provides datasource catalog management, query execution, and the record model. This is where raw bytes become typed data — `slate-db` owns serialization (bincode) and the `Record`/`Value` types.

### Record Model

Records have a string ID and a dynamic set of fields. Field values are represented as a `Value` enum supporting multiple types including nested structures. Absent fields are treated as null — there is no explicit `Null` variant.

```rust
pub struct Record {
    pub id: String,
    pub fields: HashMap<String, Value>,
}

pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Date(i64),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
}
```

### Database and Transactions

The `Database` struct owns a store and provides a `begin()` method that returns a `DatabaseTransaction`. All catalog and data operations go through the transaction:

```rust
let db = Database::new(store);

let mut txn = db.begin(false)?;
txn.save_datasource(&datasource)?;
txn.insert(record)?;
txn.insert_batch(records)?;
txn.commit()?;

let txn = db.begin(true)?;
let results = txn.query(&query)?;
```

### Datasource Catalog

The `Catalog` is a unit struct owned by `Database`. It stores datasource definitions as records in the same store, separated by a key prefix (`__ds__`). Catalog methods take a transaction reference — no separate store needed.

```rust
pub struct Datasource {
    pub id: String,
    pub name: String,
    pub fields: Vec<FieldDef>,
}

pub enum FieldType {
    String, Int, Float, Bool, Date,
    List(Box<FieldType>),
    Map(Vec<FieldDef>),
}
```

### Query Execution (Volcano/Iterator Model)

Query execution uses a streaming iterator pipeline inspired by the Volcano model:

```
RocksDB scan (streaming iterator)
  → FilterIterator (streaming, only passes matching records)
    → Sort (collects filtered results only, if needed)
      → Skip/Take (streaming on sorted or filtered results)
```

Key properties:
- **Filters are pushed down** — applied during scan, not after collecting all records.
- **No sort needed** — filter + skip + take are fully streaming. Only matching records are materialized.
- **Sort needed** — only the filtered set is collected into memory for sorting.
- **Errors propagate** — scan and deserialization errors are not silently swallowed.

### Future: Arrow Materialized Views

Arrow-based materialized views may be added later as a query optimization. They would:
- Live in the db layer, not the store
- Be materialized per partition after a transaction commits
- Use double-buffer swapping for concurrent read access
- Be gated behind a per-partition mutex to prevent duplicate materialization

For now, queries scan directly from RocksDB through the iterator pipeline.

### Partitioning (Future)

The db layer will manage partitioning — grouping data by a partition key. The store has no concept of partitions. The db layer will decide how to compose partition keys from domain concepts (user, datasource, etc.).

## TCP Interface (`slate-server` + `slate-client`)

### Overview

Slate can run as a standalone TCP server, allowing clients in separate processes to interact with the database over the network. The wire protocol uses bincode serialization with length-prefixed framing — fast, compact, and Rust-native. This enables architectures where the REST API (or other consumers) run as separate services.

```
┌──────────────┐         TCP (bincode)        ┌──────────────────┐
│ slate-client │  ◄──────────────────────────► │  slate-server    │
│ Client       │                               │  thread-per-conn │
└──────────────┘                               │  Session         │
                                               │    └─ Database   │
                                               │        └─ Store  │
                                               └──────────────────┘
```

### Wire Protocol

Messages are length-prefixed: a 4-byte big-endian length followed by a bincode-serialized payload. All types on the wire implement `serde::Serialize + Deserialize`.

```rust
enum Request {
    Insert(Record),
    InsertBatch(Vec<Record>),
    Delete(String),
    GetById(String),
    Query(Query),
    SaveDatasource(Datasource),
    GetDatasource(String),
    ListDatasources,
    DeleteDatasource(String),
}

enum Response {
    Ok,
    Record(Option<Record>),
    Records(Vec<Record>),
    Datasource(Option<Datasource>),
    Datasources(Vec<Datasource>),
    Error(String),
}
```

### Server

The server binds to a TCP address and spawns a thread per client connection. Each connection gets a `Session` that holds an `Arc<Database<RocksStore>>`. Every request is auto-committed — the session opens a transaction, performs the operation, and commits. No multi-request transactions over the wire (keeps the protocol stateless).

```rust
let server = Server::new(db, "127.0.0.1:9600");
server.serve(); // blocks, listens for connections
```

### Client

The client opens a TCP connection and provides methods that mirror the database API.

```rust
let mut client = Client::connect("127.0.0.1:9600")?;

// Data operations
client.insert(record)?;
client.insert_batch(records)?;
client.get_by_id("acct-1")?;     // -> Option<Record>
client.delete("acct-1")?;
client.query(&query)?;            // -> Vec<Record>

// Catalog operations
client.save_datasource(&ds)?;
client.get_datasource("ds1")?;    // -> Option<Datasource>
client.list_datasources()?;       // -> Vec<Datasource>
client.delete_datasource("ds1")?;
```

### Connection Pool

`ClientPool` manages a fixed set of pre-connected `Client` instances using a `crossbeam` bounded channel as a blocking queue. Connections are checked out with `pool.get()` and returned automatically on drop.

```rust
let pool = ClientPool::new("127.0.0.1:9600", 10)?; // 10 connections

let mut client = pool.get()?; // blocks if all in use
client.query(&query)?;
// returns to pool on drop
```

Internally, `PooledClient` wraps an `Option<Client>` and implements `Deref`/`DerefMut` for transparent access. On `Drop`, the client is returned to the channel. The `Option` + `take()` + `expect("BUG: ...")` pattern (borrowed from sqlx) handles the provably-unreachable None case.

## Tier 4: API Layer

A REST API that can run locally during development and be deployed to the cloud later.

### Datasource Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/datasources` | Create a datasource (schema definition) |
| GET | `/datasources` | List datasources |
| GET | `/datasources/:id` | Get a datasource and its schema |
| PUT | `/datasources/:id` | Update a datasource schema |
| DELETE | `/datasources/:id` | Delete a datasource |

### Item Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/datasources/:id/items` | Bulk load items for a user |
| GET | `/datasources/:id/items` | Query items (filters, sorting) |
| DELETE | `/datasources/:id/items` | Invalidate/clear a user's cached items |

### List Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/lists` | Create a list |
| GET | `/lists` | Get user's lists |
| GET | `/lists/:id/items` | Execute list query, return matching items |

### List Model

Lists are filtered views over items. They are a **top-level resource** with an optional `datasource_id`:

- **`datasource_id` is set** — list is scoped to a single datasource. Full schema fields available for filtering, sorting, and column display.
- **`datasource_id` is null** — list spans all of a user's datasources. Only fields common across all datasources (schema intersection) are available.

Two default lists ship out of the box:
- **Snoozed** — `datasource_id = null`, filters by `status = 'snoozed'` across all datasources.
- **Rejected** — `datasource_id = null`, filters by `status = 'rejected'` across all datasources.

These are not special-cased — they are regular list configurations that happen to be pre-created. Users cannot create their own cross-datasource lists.

### Query Syntax

Django-style filter syntax via query parameters:

```
name=foo                    → exact match
name__icontains=foo         → case-insensitive contains
revenue__gt=50000           → greater than
created_at__gte=1707900000  → date range
```

For `OR` groups and complex filters, JSON request bodies are used instead of query strings.

### List Column Configuration

Each list has a column configuration that controls the UI presentation:

- **Columns** — an ordered list of columns, each referencing a datasource field.
- **Display config** — per-column rendering configuration. Default is `toString`.
- **Visibility** — columns are explicitly opted in.
- **Sort** — available sort options are dictated by the field type.

Column configuration is a **UI-layer concern**.

## Tier 5: Frontend

### Shared WASM Engine

The Rust core compiles to a WASM module via `wasm-bindgen`. Both frontends consume the same WASM engine, guaranteeing behavioral parity.

### SwiftUI (macOS)

- Native macOS application using SwiftUI.
- Embeds the WASM module via an in-process WASM runtime (e.g., WasmKit, Wasmtime).
- Native look and feel, leveraging SwiftUI's `Table` and list components.

### Web

- Browser-based frontend.
- Loads the WASM module via the browser's native WASM runtime.
- JS/TS UI layer on top (e.g., ag-grid for table rendering).

### Cloud API

- The REST API can also be deployed as a cloud service.
- Runs native Rust (no WASM overhead on the server side).
