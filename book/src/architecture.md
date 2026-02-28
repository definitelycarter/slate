# Architecture

## Overview

A layered system with a key-value storage backend, an engine layer for key encoding and index maintenance, and a database layer with query planning and execution.

## Crate Structure

```
slate/
  ├── slate-store            → Store/Transaction traits, RocksDB + redb + MemoryStore impls (feature-gated)
  ├── slate-engine           → Storage engine: BSON key encoding, TTL, indexes, catalog, record format
  ├── slate-query            → Query model: FindOptions, DistinctOptions, Sort, Mutation (pure data structures)
  ├── slate-db               → Filter parser, expression tree, query planner + executor
  └── slate-uniffi           → UniFFI bindings for Swift/Kotlin (XCFramework builds)
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
    type Cf: Clone;
    fn cf(&self, name: &str) -> Result<Self::Cf, StoreError>;

    // Reads
    fn get(&self, cf: &Self::Cf, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError>;
    fn multi_get(&self, cf: &Self::Cf, keys: &[&[u8]])
        -> Result<Vec<Option<Vec<u8>>>, StoreError>;
    fn scan_prefix<'a>(&'a self, cf: &Self::Cf, prefix: &[u8])
        -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'a>, StoreError>;
    fn scan_prefix_rev<'a>(&'a self, cf: &Self::Cf, prefix: &[u8])
        -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'a>, StoreError>;

    // Writes
    fn put(&self, cf: &Self::Cf, key: &[u8], value: &[u8]) -> Result<(), StoreError>;
    fn put_batch(&self, cf: &Self::Cf, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError>;
    fn delete(&self, cf: &Self::Cf, key: &[u8]) -> Result<(), StoreError>;

    // Schema
    fn create_cf(&mut self, name: &str) -> Result<(), StoreError>;
    fn drop_cf(&mut self, name: &str) -> Result<(), StoreError>;

    // Lifecycle
    fn commit(self) -> Result<(), StoreError>;
    fn rollback(self) -> Result<(), StoreError>;
}
```

### Error Type

Custom `StoreError` enum with variants: `TransactionConsumed`, `ReadOnly`, `Storage`.

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
- **Returns owned data** — like RocksDB, values must be copied out of redb's `AccessGuard`. No zero-copy borrows across the transaction boundary.

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

Pure data structures representing queries and mutations. No dependencies on storage or execution — transport-agnostic. Can be constructed from query strings, JSON, GraphQL, or programmatically.

### Query Model

```rust
pub struct FindOptions {
    pub sort: Vec<Sort>,
    pub skip: Option<usize>,
    pub take: Option<usize>,
    pub columns: Option<Vec<String>>,     // column projection
}

pub struct DistinctOptions {
    pub sort: Option<SortDirection>,
    pub skip: Option<usize>,
    pub take: Option<usize>,
}
```

Filters are passed separately as raw BSON bytes (`impl IntoRawDocumentBuf`) at the database API layer — `slate-query` only defines options for pagination, sorting, and projection. The filter document is parsed into an `Expression` tree by `slate-db`'s parser at plan time.

### Sort

```rust
pub struct Sort {
    pub field: String,
    pub direction: SortDirection,  // Asc | Desc
}
```

### Mutations

`slate-query` exports the `Mutation` model: `parse_mutation` converts a BSON update document (with `$set`, `$inc`, `$unset`, etc.) into a `Vec<FieldMutation>` — a flat list of field-level operations. This is used by the executor's mutation pipeline.

## Tier 2.5: Engine Layer (`slate-engine`)

### Overview

The engine layer sits between `slate-store` and `slate-db`. It owns the on-disk format: BSON key encoding, record serialization (with TTL metadata), index maintenance, and the collection catalog. It provides an `EngineTransaction` trait that `slate-db` programs against, hiding all key encoding and storage layout details.

### Key Encoding

All keys use a tag-prefixed binary format with sort-preserving BSON value encoding:

- **Collection metadata** — `c:{name}` stores collection config in the `_sys_` CF.
- **Index config** — `x:{collection}:{field}` stores index metadata.
- **Record** — `d:{collection}:{doc_id}` → encoded `Record` (BSON bytes + optional TTL).
- **Index** — `i:{collection}:{field}:{value}:{doc_id}` → `IndexMeta` (type byte + TTL).
- **IndexMap** — `m:{collection}:{field}:{doc_id}:{value}` → index key bytes (reverse lookup for O(1) cleanup).

### TTL

TTL filtering is handled inside the engine. `get()`, `scan()`, and `scan_index()` accept a `now_millis` timestamp and skip expired records transparently — callers never see expired data.

### Index Maintenance

On `put()`, the engine uses IndexMap reverse lookups to delete old index entries without reading the previous document, then inserts new entries. On `delete()`, the same IndexMap scan removes all index entries for the doc.

### Catalog

The `Catalog` trait provides collection and index lifecycle: `create_collection`, `drop_collection`, `create_index` (with backfill), `drop_index`, and `collection` handle resolution.

## Tier 3: Database Layer (`slate-db`)

### Overview

The database layer sits on top of `slate-engine` and `slate-query`. It provides query planning, execution, and the user-facing `Database`/`Transaction` API. Storage operations (record reads, writes, scans, index lookups) are delegated to `slate-engine`'s `EngineTransaction` trait.

### Document Model

Records are `bson::Document` values. There is no custom `Cell` or `Value` type — BSON's native types (`String`, `Int64`, `Double`, `Boolean`, `DateTime`, `Array`, `Document`) are used directly. This means nested documents and arrays are first-class citizens.

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

### Database and Transactions

The `Database` struct is generic over `Store` and provides a `begin()` method that returns a `DatabaseTransaction`. All operations go through the transaction:

```rust
use slate_query::FindOptions;

let db = Database::open(store, DatabaseConfig::default());

// Create a collection with indexes
let mut txn = db.begin(false)?;
txn.create_collection(&CollectionConfig {
    name: "users".into(),
    indexes: vec!["status".into()],
    ..Default::default()
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

// Query — find() returns a Cursor for lazy iteration
let cursor = txn.find("users", rawdoc! {}, FindOptions::default())?;
for doc in cursor.iter()? {              // CursorIter yields RawDocumentBuf
    let doc = doc?;
}

let one = txn.find_one("users", rawdoc! { "_id": "bob" })?; // -> Option<RawDocumentBuf>

// Update (merge — preserves unspecified fields)
txn.update_one("users", filter,
    rawdoc! { "$set": { "status": "archived" } })?.drain()?;
txn.update_many("users", filter,
    rawdoc! { "$set": { "status": "archived" } })?.drain()?;

// Replace (full document swap)
txn.replace_one("users", filter,
    rawdoc! { "name": "Alice", "status": "inactive" })?.drain()?;

// Delete
txn.delete_one("users", filter)?.drain()?;
txn.delete_many("users", filter)?.drain()?;

// Count
let n = txn.count("users", rawdoc! {})?;  // -> u64

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

Query execution uses a two-tier plan tree with lazy materialization. See [Querying](./querying.md) for the full reference with all plan scenarios.

**Planning** builds a pipeline:

```
Projection → Limit → Sort → Filter → ReadRecord → IndexScan / IndexMerge / Scan
```

**Two tiers:**

1. **ID tier** — `Scan`, `IndexScan`, `IndexMerge` produce record IDs without touching document bytes.
2. **Raw tier** — everything above `ReadRecord` operates on `Option<RawBson>`. Filter, Sort, and Limit construct `&RawDocument` views to access individual fields lazily. Projection builds `RawDocumentBuf` output using `append()` for selective field copying — no `bson::Document` materialization anywhere in the pipeline. `find()` returns a `Cursor` whose iterator yields `RawDocumentBuf` lazily.

For index-covered queries, the index value is carried directly as `RawBson` — no document fetch needed.

**Key properties:**

- **No deserialization** — the entire pipeline stays in raw bytes. Records that fail a filter are never cloned or materialized.
- **Priority-based index selection** — `CollectionConfig.indexes` order determines which index is preferred for AND groups.
- **Index union for OR** — OR queries with indexed branches use `IndexMerge(Or)` to combine ID sets, avoiding full scans.
- **Dot-notation field access** — filters, sorts, and projections support nested paths like `"address.city"`.
