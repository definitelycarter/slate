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
  ├── slate-store   → Store/Transaction traits, Record/Value types, RocksDB impl (feature-gated)
  ├── slate-query   → Query model, Filter, Operator, Sort, QueryValue (pure data structures)
  └── slate-db      → Database, Catalog, query execution, depends on slate-store + slate-query
```

## Tier 1: Storage Layer (`slate-store`)

### Overview

The store is a dumb, schema-unaware key-value storage layer. It stores and retrieves `Record`s within transactions. It knows nothing about datasources, users, schemas, or query optimization — those are higher-level concerns handled by `slate-db`.

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

### Store Trait

The `Store` trait provides a minimal interface: begin a transaction (read-write or read-only). Uses a GAT for the transaction lifetime.

```rust
pub trait Store {
    type Txn<'a>: Transaction where Self: 'a;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, StoreError>;
}
```

### Transaction Trait

All operations go through a transaction. Read-only transactions return errors on write operations (enforced at runtime).

```rust
pub trait Transaction {
    type Iter: Iterator<Item = Result<Record, StoreError>>;

    fn get_by_id(&self, id: &str) -> Result<Option<Record>, StoreError>;
    fn scan(&self) -> Result<Self::Iter, StoreError>;

    fn insert(&mut self, record: Record) -> Result<(), StoreError>;
    fn delete(&mut self, id: &str) -> Result<(), StoreError>;

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

The database layer sits on top of `slate-store` and `slate-query`. It provides datasource catalog management and query execution.

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
