# Architecture

## Overview

A layered system with a dynamic storage engine, a database layer with query execution, a TCP interface, and an HTTP layer for collection access.

## Crate Structure

```
slate/
  ├── slate-store            → Store/Transaction traits, RocksDB + redb + MemoryStore impls (feature-gated)
  ├── slate-query            → Query model, Filter, Operator, Sort, QueryValue (pure data structures)
  ├── slate-db               → Database, Catalog, query execution, depends on slate-store + slate-query
  ├── slate-server           → TCP server, MessagePack wire protocol, thread-per-connection
  ├── slate-client           → TCP client, connection pool (crossbeam channel)
  ├── slate-server-init      → CLI tool to initialize collections on a running server
  ├── slate-collection       → HTTP handler for collection CRUD (framework-agnostic)
  ├── slate-collection-http  → Standalone HTTP server wrapping slate-collection
  ├── slate-operator         → Kubernetes operator for Server + Collection CRDs
  ├── slate-bench            → DB-level benchmark suite (embedded + TCP, both backends)
  └── slate-store-bench      → Store-level stress test (500k records, race conditions, data integrity)
```

## Tier 1: Storage Layer (`slate-store`)

### Overview

The store is a dumb, schema-unaware key-value storage layer. It stores and retrieves raw bytes within column-family-scoped transactions. It knows nothing about records, collections, or query optimization — those are higher-level concerns handled by `slate-db`.

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

### Implementation: redb (`RedbStore`)

The redb implementation (feature-gated behind `redb`) is a pure-Rust embedded key-value store designed for environments where C dependencies are problematic — notably Apple platforms (macOS/iOS) where RocksDB's C toolchain complicates cross-compilation and distribution.

**Why redb?** RocksDB is faster (2-4x on raw throughput) but requires a C compiler, `libclang`, and platform-specific build configuration. redb is ~15k LOC of pure Rust with no C dependencies — it compiles cleanly for all Apple targets and produces smaller binaries. For frontend applications with user-generated data, redb's performance is more than adequate (sub-15ms for all operations at 10k records).

**Architecture:**

- **Copy-on-write B-trees** with MVCC. Single writer, multiple concurrent readers.
- **`redb::Database`** — single file on disk, created via `Database::create(path)`.
- **Tables as column families** — each `cf` string maps to a `TableDefinition<&[u8], &[u8]>`. Tables are opened inline per operation (lightweight handle, no caching needed).
- **Separate transaction types** — redb has distinct `ReadTransaction` / `WriteTransaction` types, wrapped in an `Inner` enum inside `RedbTransaction`.

**Key differences from RocksDB:**

- **No native `delete_range`** — implemented via iterate-and-delete within a write transaction.
- **Eager scan collection** — redb iterators borrow the table handle and can't outlive the method. Prefix scans collect results into a `Vec` before returning. Acceptable because prefix scans in slate are bounded by collection size.
- **Returns `Cow::Owned`** — like RocksDB, values must be copied out of redb's `AccessGuard`. No zero-copy borrows across the transaction boundary.

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

The database layer sits on top of `slate-store` and `slate-query`. It provides collection management, query execution, and document storage. Records are stored as raw BSON bytes (`bson::to_vec` of a `bson::Document`), with no intermediate type system — the `bson::Document` is the record.

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

Each record is stored as a single key-value pair (`d:{record_id}` → raw BSON bytes) within the collection's column family. `_id` is stored in the key, not in the BSON value.

### Collections

A collection is a named group of documents backed by its own column family. Collections are managed through a `Catalog` that stores metadata in the `_sys` column family.

```rust
pub struct CollectionConfig {
    pub name: String,
    pub indexes: Vec<String>,  // field names to index
}
```

Indexes are maintained automatically on writes. Each indexed field gets entries in the format `i:{field}:{value}:{record_id}` for O(1) lookups by field value.

### Database and Transactions

The `Database` struct is generic over `Store` and provides a `begin()` method that returns a `DatabaseTransaction`. All operations go through the transaction:

```rust
let db = Database::open(store, DatabaseConfig::default());

// Create a collection with indexes
let mut txn = db.begin(false)?;
txn.create_collection(&CollectionConfig {
    name: "users".into(),
    indexes: vec!["status".into()],
})?;

// Insert
let result = txn.insert_one("users", doc! {
    "name": "Alice",
    "status": "active",
})?;  // result.id = auto-generated UUID (or use _id in doc)

txn.insert_many("users", vec![
    doc! { "_id": "bob", "name": "Bob" },
    doc! { "_id": "carol", "name": "Carol" },
])?;

// Query
let results = txn.find("users", &query)?;               // -> Vec<Document>
let one = txn.find_one("users", &query)?;                // -> Option<Document>
let record = txn.find_by_id("users", "bob", None)?;      // -> Option<Document>
let record = txn.find_by_id("users", "bob",
    Some(&["name"]))?;                                    // with projection

// Update (merge — preserves unspecified fields)
txn.update_one("users", &filter,
    doc! { "status": "archived" }, false)?;               // -> UpdateResult
txn.update_many("users", &filter,
    doc! { "status": "archived" })?;

// Replace (full document swap)
txn.replace_one("users", &filter,
    doc! { "name": "Alice", "status": "inactive" })?;

// Delete
txn.delete_one("users", &filter)?;                       // -> DeleteResult
txn.delete_many("users", &filter)?;

// Count
let n = txn.count("users", Some(&filter))?;              // -> u64

// Index management
txn.create_index("users", "email")?;  // backfills existing records
txn.drop_index("users", "email")?;
txn.list_indexes("users")?;           // -> Vec<String>

// Collection management
txn.list_collections()?;              // -> Vec<String>
txn.drop_collection("users")?;        // removes data, indexes, metadata

txn.commit()?;
```

### Query Execution

Query execution uses a three-tier plan tree with lazy materialization. See [Querying](./querying.md) for the full reference with all plan scenarios.

**Planning** builds a pipeline:

```
Projection → Limit → Sort → Filter → ReadRecord → IndexScan / IndexMerge / Scan
```

**Three tiers:**

1. **ID tier** — `Scan`, `IndexScan`, `IndexMerge` produce record IDs without touching document bytes.
2. **Raw tier** — `ReadRecord`, `Filter`, `Sort`, `Limit` operate on `Cow<[u8]>` bytes with borrowed `&RawDocument` views, accessing individual fields lazily. Records that fail a filter are never cloned or deserialized.
3. **Document tier** — `Projection` is the single materialization point, selectively converting only the requested columns from raw bytes to `bson::Document`.

**Key properties:**

- **Lazy materialization** — 30-80% improvement on filtered queries. Rejected records never pay deserialization cost.
- **Priority-based index selection** — `CollectionConfig.indexes` order determines which index is preferred for AND groups.
- **Index union for OR** — OR queries with indexed branches use `IndexMerge(Or)` to combine ID sets, avoiding full scans.
- **Dot-notation field access** — filters, sorts, and projections support nested paths like `"address.city"`.

## TCP Interface (`slate-server` + `slate-client`)

### Overview

Slate can run as a standalone TCP server, allowing clients in separate processes to interact with the database over the network. The wire protocol uses MessagePack serialization (rmp-serde) with length-prefixed framing. MessagePack was chosen over BSON for the wire because it's more compact for structured enum data — benchmarks showed BSON wire format 32-76% slower for bulk transfers (see Benchmarks).

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
    // Insert
    InsertOne { collection: String, doc: bson::Document },
    InsertMany { collection: String, docs: Vec<bson::Document> },
    // Query
    Find { collection: String, query: Query },
    FindOne { collection: String, query: Query },
    FindById { collection: String, id: String, columns: Option<Vec<String>> },
    // Update
    UpdateOne { collection: String, filter: FilterGroup, update: bson::Document, upsert: bool },
    UpdateMany { collection: String, filter: FilterGroup, update: bson::Document },
    ReplaceOne { collection: String, filter: FilterGroup, doc: bson::Document },
    // Delete
    DeleteOne { collection: String, filter: FilterGroup },
    DeleteMany { collection: String, filter: FilterGroup },
    // Count
    Count { collection: String, filter: Option<FilterGroup> },
    // Index management
    CreateIndex { collection: String, field: String },
    DropIndex { collection: String, field: String },
    ListIndexes { collection: String },
    // Collection management
    CreateCollection { config: CollectionConfig },
    ListCollections,
    DropCollection { collection: String },
}

enum Response {
    Ok,
    Insert(InsertResult),
    Inserts(Vec<InsertResult>),
    Record(Option<bson::Document>),
    Records(Vec<bson::Document>),
    Update(UpdateResult),
    Delete(DeleteResult),
    Count(u64),
    Indexes(Vec<String>),
    Collections(Vec<String>),
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

The client opens a TCP connection and provides methods that mirror the database API. All operations are scoped to a collection.

```rust
let mut client = Client::connect("127.0.0.1:9600")?;

// Insert
client.insert_one("users", doc! { "name": "Alice", "status": "active" })?;
client.insert_many("users", vec![doc! { "name": "Bob" }])?;

// Query
client.find("users", &query)?;                         // -> Vec<Document>
client.find_one("users", &query)?;                     // -> Option<Document>
client.find_by_id("users", "rec-1", None)?;            // -> Option<Document>
client.find_by_id("users", "rec-1", Some(&["name"]))?; // with projection

// Update
client.update_one("users", &filter, doc! { "status": "archived" }, false)?;
client.update_many("users", &filter, doc! { "status": "archived" })?;
client.replace_one("users", &filter, doc! { "name": "Alice" })?;

// Delete
client.delete_one("users", &filter)?;
client.delete_many("users", &filter)?;

// Count
client.count("users", Some(&filter))?;

// Index management
client.create_index("users", "email")?;
client.drop_index("users", "email")?;
client.list_indexes("users")?;

// Collection management
client.create_collection(&config)?;
client.list_collections()?;
client.drop_collection("users")?;
```

### Connection Pool

`ClientPool` manages a fixed set of pre-connected `Client` instances using a `crossbeam` bounded channel as a blocking queue. Connections are checked out with `pool.get()` and returned automatically on drop.

```rust
let pool = ClientPool::new("127.0.0.1:9600", 10)?; // 10 connections

let mut client = pool.get()?; // blocks if all in use
client.find("users", &query)?;
// returns to pool on drop
```

Internally, `PooledClient` wraps an `Option<Client>` and implements `Deref`/`DerefMut` for transparent access. On `Drop`, the client is returned to the channel. The `Option` + `take()` + `expect("BUG: ...")` pattern (borrowed from sqlx) handles the provably-unreachable None case.

## HTTP Layer (`slate-collection` + `slate-collection-http`)

### Overview

`slate-collection` provides a framework-agnostic HTTP handler (`CollectionHttp`) for CRUD operations on a single collection. It uses raw `http::Request`/`http::Response` from the `http` crate, with no dependency on any web framework. `slate-collection-http` wraps it in a standalone hyper-based HTTP server.

### CollectionHttp

```rust
pub struct CollectionHttp {
    collection: String,
    pool: ClientPool,
}
```

The handler is configured with a collection name and a TCP client pool. All requests are proxied to the slate-server via the pool.

**Endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| POST | `/query` | Execute query, return matching records |
| POST | `/query/distinct` | Execute distinct query on a field |
| POST | `/data` | Insert new records |
| PUT | `/data` | Upsert records (insert or replace) |
| PATCH | `/data` | Merge records (insert or partial update) |
| DELETE | `/data` | Delete records matching a filter |

**Request context:**
- Request body — `QueryRequest` JSON (filters, sort, skip, take, columns) for `/query`. Empty body returns all records.
- Request body — `Vec<bson::Document>` (JSON array) for POST/PUT/PATCH `/data`.
- Request body — `{ "filter": {...} }` for DELETE `/data`.

The `handle` method takes `http::Request<Vec<u8>>` and returns `http::Response<Vec<u8>>`.
