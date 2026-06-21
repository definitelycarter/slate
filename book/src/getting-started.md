# Getting Started

Slate is an embedded document database with a CosmosDB-style SQL surface and a
Mongo-style `find`/update surface over the same engine. It runs in-process — no
server, no network — against a pluggable storage backend.

This chapter walks from an empty project to querying. For the full query grammar
see [Querying](./querying.md); for the operators and functions you'll call, see
[Mongo Operators](./mongo-operators.md) and the [Function Reference](./functions.md).

## Add the dependencies

Slate is a Rust workspace; add the database crate plus a storage backend:

```toml
[dependencies]
slate-db = { path = "…/crates/slate-db" }
slate-store = { path = "…/crates/slate-store" }   # backends
slate-query = { path = "…/crates/slate-query" }   # FindOptions, filters
bson = "3"
```

Pick a backend via `slate-store`'s features:

- `memory` (default) — in-process, ephemeral. Great for tests.
- `redb` — persistent, **pure Rust** (no C dependencies; the choice for `wasm32`).
- `rocksdb` — persistent, RocksDB-backed.

## Open a database

A `Database` wraps a store. Everything goes through `DatabaseBuilder`:

```rust
use slate_db::DatabaseBuilder;
use slate_store::MemoryStore;            // or RedbStore / RocksStore

let store = MemoryStore::new();          // RedbStore::open(path)? to persist
let db = DatabaseBuilder::new().open(store)?;
```

## Transactions

Every operation runs inside a transaction. `begin(read_only)` returns one;
`commit` makes writes durable, and dropping (or `rollback`) discards them.

```rust
let txn = db.begin(false)?;   // false = read-write
// … writes …
txn.commit()?;

let txn = db.begin(true)?;    // true = read-only snapshot
// … reads …
```

A write transaction is exclusive; readers see a consistent snapshot.

## Collections and the primary key

Documents live in **collections**, addressed within a column family (use the
built-in `DEFAULT_CF`). A collection has a **primary key path** (default `_id`)
and an optional TTL path:

```rust
use slate_db::{CollectionConfig, DEFAULT_CF};

let txn = db.begin(false)?;
txn.create_collection(&CollectionConfig {
    name: "accounts".into(),
    ..Default::default()             // pk_path defaults to "_id"
})?;
txn.commit()?;
```

To match CosmosDB's system key, set `pk_path: "id".into()`.

## Insert documents

```rust
use bson::doc;

let txn = db.begin(false)?;
txn.insert_one(DEFAULT_CF, "accounts", doc! {
    "_id": "acct-1",
    "name": "Acme Corp",
    "status": "active",
    "revenue": 50000.0,
})?.drain()?;
txn.commit()?;
```

`insert_many` takes an iterable of documents. Write operations return a result
you `drain()` to surface per-document errors (e.g. a unique-index violation).

## Query

Two surfaces, one engine. Use whichever fits.

**SQL** (CosmosDB dialect — `FROM c` binds the rows of the collection to `c`):

```rust
let txn = db.begin(true)?;
let cursor = txn.query(DEFAULT_CF, "accounts",
    "SELECT c.name, c.revenue FROM c WHERE c.status = 'active' ORDER BY c.revenue DESC")?;
for row in cursor.iter_raw()? {
    let row = row?;   // RawDocumentBuf, zero-copy
}
```

**Mongo `find`** (a BSON filter; see [Mongo Operators](./mongo-operators.md)):

```rust
use bson::rawdoc;
use slate_query::FindOptions;

let cursor = txn.find(DEFAULT_CF, "accounts",
    rawdoc! { "status": "active" }, FindOptions::default())?;

for doc in cursor.iter::<serde_json::Value>()? {   // deserialize into any T
    let doc = doc?;
}

let one = txn.find_one(DEFAULT_CF, "accounts", rawdoc! { "_id": "acct-1" })?;
let n   = txn.count(DEFAULT_CF, "accounts", rawdoc! {})?;
```

`iter::<T>()` deserializes each result into `T`; `iter_raw()` hands back
`RawDocumentBuf` with no deserialization.

## Indexes

Indexes are per-field and speed up equality, range, and sort. A unique index also
enforces a constraint:

```rust
let txn = db.begin(false)?;
txn.create_index(DEFAULT_CF, "accounts", "status")?;
txn.create_unique_index(DEFAULT_CF, "accounts", "email")?;
txn.commit()?;
```

The planner uses indexes automatically — see the plan scenarios in
[Querying](./querying.md).

## Update and delete

Updates take a filter and a Mongo update document — atomic field mutations, no
read-modify-write (see [Mongo Operators](./mongo-operators.md)):

```rust
let txn = db.begin(false)?;
txn.update_one(DEFAULT_CF, "accounts",
    rawdoc! { "_id": "acct-1" },
    rawdoc! { "$set": { "status": "archived" }, "$inc": { "revenue": 5000.0 } },
)?.drain()?;

txn.delete_one(DEFAULT_CF, "accounts", rawdoc! { "_id": "acct-1" })?.drain()?;
txn.commit()?;
```

`update_many` / `delete_many` apply to every match.

## Try it interactively

The `slate-cli` shell is the fastest way to explore. Lines starting with `.` are
commands (`.help` lists them); everything else is SQL terminated by `;`:

```bash
cargo run -p slate-cli            # in-memory
cargo run -p slate-cli -- --rocksdb /tmp/slate   # or persist
```

```
slate> .seed
slate(sample)> SELECT c.city, COUNT(1) AS n
          ...> FROM c GROUP BY c.city;
slate(sample)> .schema sample
slate(sample)> .backup /tmp/snapshot      # rocksdb/redb only
```

## Where next

- [Querying](./querying.md) — the query model, plan scenarios, distinct, subqueries.
- [Mongo Operators](./mongo-operators.md) — find filters and update operators.
- [Function Reference](./functions.md) — every built-in function.
- [Architecture](./architecture.md) — how the crates fit together, and pluggable scripting.
