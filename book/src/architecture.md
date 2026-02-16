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
  ├── slate-server       → TCP server, MessagePack wire protocol, thread-per-connection
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

**Why not an external database (MongoDB, Redis)?** The data model is already document-shaped (`bson::Document` with nested types). Adding an external database means shipping a server dependency, managing connections, and translating between type systems. An embedded in-memory store gives zero operational overhead, no network round-trips, and direct Rust type access.

**Architecture** (inspired by SurrealDB's [echodb](https://github.com/surrealdb/echodb)):

- **`imbl::OrdMap`** per column family — immutable B-tree with structural sharing. Snapshot clones are O(1), not O(n). Ordered keys give sorted iteration for `scan_prefix` and `delete_range`.
- **`arc_swap::ArcSwap`** per column family — lock-free atomic pointer swap. Readers load the current pointer without blocking. Writers swap in a new pointer on commit.
- **`std::sync::Mutex`** write lock — serializes writers. Acquired at `begin(false)`, released on commit/rollback.

**Concurrency model:**

- Readers snapshot via `ArcSwap::load` (lock-free) and see a consistent point-in-time view.
- Writers acquire the mutex, clone the OrdMap (cheap via structural sharing), mutate locally, and atomically swap on commit.
- Multiple concurrent readers never block each other or writers.
- A reader that started before a commit continues seeing old data (snapshot isolation).

**Memory footprint:** ~1.2 KB per record on disk/in-store (960 bytes BSON data + keys + index entries for a 50-field document). At 500k records, ~0.7 GB; at 1M records, ~1.4 GB including BTreeMap overhead — fits comfortably in a 2-4 GB container.

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
    pub columns: Option<Vec<String>>,  // column projection
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

The database layer sits on top of `slate-store` and `slate-query`. It provides datasource catalog management, query execution, and document storage. Records are stored as raw BSON bytes (`bson::to_vec` of a `bson::Document`), with no intermediate type system — the `bson::Document` is the record.

### Document Model

Records are `bson::Document` values. There is no custom `Record`, `Cell`, or `Value` type — BSON's native types (`String`, `Int64`, `Double`, `Boolean`, `DateTime`, `Array`, `Document`) are used directly. This means nested documents and arrays are first-class citizens.

```rust
let doc = doc! {
    "name": "Alice",
    "status": "active",
    "contacts_count": 42,
    "address": {
        "city": "Austin",
        "state": "TX",
        "zip": "78701"
    }
};
```

Writes use a read-modify-write pattern — the new document is merged with the existing record so that unspecified fields are preserved. Each record is stored as a single key-value pair (`d:{partition}:{record_id}` → raw BSON bytes).

### Database and Transactions

The `Database` struct is generic over `Store` and provides a `begin()` method that returns a `DatabaseTransaction`. All catalog and data operations go through the transaction:

```rust
let db = Database::new(store);

let mut txn = db.begin(false)?;
txn.save_datasource(&datasource)?;
txn.write_record("ds1", "rec-1", doc! {
    "name": "Alice",
    "status": "active",
})?;
txn.write_batch("ds1", vec![
    ("rec-2".into(), doc! { "name": "Bob" }),
])?;
txn.delete_record("ds1", "rec-1")?;
txn.commit()?;

let txn = db.begin(true)?;
let record = txn.get_by_id("ds1", "rec-1", None)?;           // all columns
let record = txn.get_by_id("ds1", "rec-1", Some(&["name"]))?; // projection
let results = txn.query("ds1", &query)?;
```

### Datasource Catalog

The `Catalog` is a unit struct owned by `Database`. It stores datasource definitions in the `_sys` column family with a `__ds__:` key prefix. Catalog methods take a transaction reference.

```rust
pub struct Datasource {
    pub id: String,
    pub name: String,
    pub fields: Vec<FieldDef>,
    pub partition: String,  // column family name for this datasource's data
}

pub struct FieldDef {
    pub name: String,
    pub field_type: FieldType,
    pub indexed: bool,  // maintain index on writes
}

pub enum FieldType {
    String, Int, Float, Bool, Date,
    List(Box<FieldType>),
    Map(Vec<FieldDef>),
}
```

Each datasource gets its own column family (`partition`). The catalog itself lives in the shared `_sys` column family.

### Query Execution (Planner + Volcano Iterator)

Query execution has two phases: planning and execution.

**Planning** builds a tree of `PlanNode`s:

```
Projection (optional column selection)
  → Limit (skip/take)
    → Sort (collects if needed)
      → Filter (streaming residual filters)
        → Scan { columns } or IndexScan { columns } (base operation)
```

**Column pushdown:** The planner computes the set of required columns (union of projection + filter + sort fields, mapped to top-level keys for dot-notation paths) and passes them to the Scan/IndexScan node. When present, only those fields are materialized from `RawDocumentBuf`; otherwise full document deserialization.

**Index optimization:** For top-level AND groups, the planner looks for `Eq` conditions on indexed columns and rewrites them as `IndexScan` nodes. The consumed condition is removed from the filter, and remaining conditions become a residual `Filter` node. OR groups cannot use indexes and always fall back to full scan.

**Execution** is lazy/streaming where possible:
- `Scan` / `IndexScan` yield records one at a time, with selective materialization from `RawDocumentBuf`
- `Filter` passes through matching records (streaming). Supports dot-notation paths (e.g. `"address.city"`) via `get_path()`
- `Sort` materializes the filtered set (necessary for ordering). Supports dot-notation sort keys
- `Limit` applies skip/take (streaming)
- `Projection` strips unneeded columns. For dot-notation paths, trims nested documents to only requested sub-paths (e.g. `"address.city"` returns `{ "address": { "city": "Austin" } }`)

Key properties:
- **Selective materialization** — scan nodes only deserialize the fields needed for the query, not the entire document
- **Dot-notation field access** — filters, sorts, and projections support paths like `"address.city"` for nested document traversal
- **Filters are pushed down** — applied during scan, not after collecting all records
- **Index pushdown** — indexed `Eq` conditions narrow the scan to matching keys
- **Errors propagate** — scan and deserialization errors are not silently swallowed

## TCP Interface (`slate-server` + `slate-client`)

### Overview

Slate can run as a standalone TCP server, allowing clients in separate processes to interact with the database over the network. The wire protocol uses MessagePack serialization (rmp-serde) with length-prefixed framing. MessagePack was chosen over BSON for the wire because it's more compact for structured enum data — benchmarks showed BSON wire format 32-76% slower for bulk transfers (see Benchmarks). This enables architectures where the REST API (or other consumers) run as separate services.

```
┌──────────────┐       TCP (MessagePack)      ┌──────────────────┐
│ slate-client │  ◄──────────────────────────► │  slate-server    │
│ Client       │                               │  thread-per-conn │
└──────────────┘                               │  Session         │
                                               │    └─ Database   │
                                               │        └─ Store  │
                                               └──────────────────┘
```

### Wire Protocol

Messages are length-prefixed: a 4-byte big-endian length followed by a MessagePack-serialized payload (via `rmp_serde`). All types on the wire implement `serde::Serialize + Deserialize`.

```rust
enum Request {
    // Data (all scoped to a datasource)
    WriteRecord { datasource_id: String, record_id: String, doc: bson::Document },
    WriteBatch { datasource_id: String, writes: Vec<(String, bson::Document)> },
    DeleteRecord { datasource_id: String, record_id: String },
    GetById { datasource_id: String, record_id: String, columns: Option<Vec<String>> },
    Query { datasource_id: String, query: Query },
    // Catalog (global)
    SaveDatasource(Datasource),
    GetDatasource(String),
    ListDatasources,
    DeleteDatasource(String),
}

enum Response {
    Ok,
    Record(Option<bson::Document>),
    Records(Vec<bson::Document>),
    Datasource(Option<Datasource>),
    Datasources(Vec<Datasource>),
    Error(String),
}
```

### Server

The server binds to a TCP address and spawns a thread per client connection. Each connection gets a `Session` that holds an `Arc<Database<S>>`. Every request is auto-committed — the session opens a transaction, performs the operation, and commits. No multi-request transactions over the wire (keeps the protocol stateless).

```rust
let server = Server::new(db, "127.0.0.1:9600");
server.serve(); // blocks, listens for connections
```

### Client

The client opens a TCP connection and provides methods that mirror the database API. All data operations are scoped to a datasource.

```rust
let mut client = Client::connect("127.0.0.1:9600")?;

// Data operations (scoped to datasource)
client.write_record("ds1", "rec-1", doc! { "name": "Alice", "status": "active" })?;
client.write_batch("ds1", vec![("rec-2".into(), doc! { "name": "Bob" })])?;
client.get_by_id("ds1", "rec-1", None)?;          // -> Option<bson::Document>
client.get_by_id("ds1", "rec-1", Some(&["name"]))?; // with projection
client.delete_record("ds1", "rec-1")?;
client.query("ds1", &query)?;                      // -> Vec<bson::Document>

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
