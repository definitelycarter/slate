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

Documents live in **collections**. A collection has a **primary key path**
(default `_id`) and an optional TTL path. Create one through the `collections()`
namespace:

```rust
let txn = db.begin(false)?;
db.collections().create("accounts").execute(&txn)?;   // pk_path defaults to "_id"
txn.commit()?;
```

To match CosmosDB's system key, add `.pk_path("id")` before `.execute`. A
collection lives in a column family; `db.collections()` targets the default one,
and `db.cf("other").collections()` targets another.

## Insert documents

```rust
use bson::doc;

let txn = db.begin(false)?;
db.collection("accounts").insert_one(doc! {
    "_id": "acct-1",
    "name": "Acme Corp",
    "status": "active",
    "revenue": 50000.0,
}).execute(&txn)?;
txn.commit()?;
```

`insert_many` takes an iterable of documents. A write builder runs with
`.execute(&txn)` and returns a `WriteResult { affected }`; a failed write (e.g. a
unique-index violation) surfaces as an `Err`.

## Query

Two surfaces, one engine. Use whichever fits.

**SQL** (CosmosDB dialect — `FROM c` binds the rows of the collection to `c`):

```rust
let txn = db.begin(true)?;
let accounts = db.collection("accounts");
for row in accounts
    .query("SELECT c.name, c.revenue FROM c WHERE c.status = 'active' ORDER BY c.revenue DESC")
    .iter_raw(&txn)?
{
    let row = row?;   // RawBson value — no deserialization
}
```

**Mongo `find`** (a BSON filter; see [Mongo Operators](./mongo-operators.md)):

```rust
let accounts = db.collection("accounts");

for doc in accounts.find(doc! { "status": "active" }).iter::<serde_json::Value>(&txn)? {
    let doc = doc?;   // deserialize into any T
}

let one = accounts.find(doc! { "_id": "acct-1" })
    .iter::<serde_json::Value>(&txn)?.next().transpose()?;
let n   = accounts.find(doc! {}).iter_raw(&txn)?.count();
```

`iter::<T>(&txn)` deserializes each result into `T`; `iter_raw(&txn)` hands back
raw bytes with no deserialization (`RawDocumentBuf` for `find`, a `RawBson` value
for SQL).

## Indexes

Indexes are per-field and speed up equality, range, and sort. A unique index also
enforces a constraint:

```rust
use slate_db::v2::IndexOptions;

let txn = db.begin(false)?;
let accounts = db.collection("accounts");
accounts.indexes().create("status", IndexOptions::default()).execute(&txn)?;
accounts.indexes().create("email", IndexOptions::unique()).execute(&txn)?;
txn.commit()?;
```

The planner uses indexes automatically — see the plan scenarios in
[Querying](./querying.md).

## Update and delete

Updates take a filter and a Mongo update document — atomic field mutations, no
read-modify-write (see [Mongo Operators](./mongo-operators.md)):

```rust
let txn = db.begin(false)?;
let accounts = db.collection("accounts");
accounts.find(doc! { "_id": "acct-1" })
    .update(doc! { "$set": { "status": "archived" }, "$inc": { "revenue": 5000.0 } })
    .one().execute(&txn)?;

accounts.find(doc! { "_id": "acct-1" }).delete().one().execute(&txn)?;
txn.commit()?;
```

Drop the `.one()` to apply to every match (the default for `update`/`delete`).

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
slate(sample)> .explain SELECT VALUE c.name FROM c WHERE c.city = 'London'
slate(sample)> .schema sample
slate(sample)> .backup /tmp/snapshot      # physical snapshot; rocksdb/redb only
slate(sample)> .export /tmp/dump          # logical dump; any backend
slate(sample)> .import /tmp/dump          # reload a dump (errors on collision)
```

## Loading data

`.seed` with no argument loads the tiny built-in sample. To explore against
realistic data, point `.seed` at a file:

```
slate> .seed movies.jsonl          # JSONL: one JSON document per line
slate(movies)> .seed cities.json   # or a single [ … ] array of documents
```

The format is auto-detected: a leading `[` is read as one JSON array of
documents, anything else is treated as **JSONL/NDJSON** (one document per line —
`mongoexport`'s default output). The collection name defaults to the file's stem
(`movies.jsonl` → `movies`) and becomes the active collection, so you can query
it right away:

```
slate(movies)> SELECT VALUE COUNT(1) FROM c;
```

Documents are coerced exactly as `.insert` does, so MongoDB **extended JSON**
(`{"$oid": …}`, `{"$date": …}`) is understood and a missing `_id` is filled in
with a generated ObjectId. JSONL is streamed line by line, so a large export
loads without being read fully into memory; a malformed line stops the import
(naming the line number) rather than loading half a file.

### Datasets worth trying

The [MongoDB Atlas sample datasets](https://www.mongodb.com/docs/atlas/sample-data/)
are rich, deeply-nested documents — ideal for exercising functions, `JOIN` over
arrays, and `GROUP BY`. Export a collection to JSONL with `mongoexport`, then
seed it:

```bash
mongoexport --uri "<atlas-uri>" --db sample_mflix \
  --collection movies --out movies.jsonl
mongoexport --uri "<atlas-uri>" --db sample_restaurants \
  --collection restaurants --out restaurants.jsonl
```

```
slate> .seed movies.jsonl
slate(movies)> SELECT c.title, c.year FROM c WHERE c.year > 2000 ORDER BY c.year;
```

To keep the loaded data, open a persistent backend first
(`cargo run -p slate-cli -- --rocksdb /tmp/slate`); the import survives across
sessions.

### Export & import (logical dump)

`.seed` loads documents from JSON/JSONL into one collection. To move a **whole
database** — every collection, with its indexes, `pk_path`, and `ttl_path` — use
`.export` / `.import`:

```
slate> .export /tmp/dump      # write a portable dump directory
slate> .import /tmp/dump      # reload it (errors if a collection collides)
```

A dump is a directory holding a `manifest.bson` (the collection definitions) plus
one `<cf>.<collection>.bson` document-stream file per collection. The format is
**BSON canonical and lossless** — ObjectId, DateTime, and Decimal128 all survive a
round-trip, unlike the JSON/JSONL `.seed` path, which is lossy on those
BSON-specific types (the same `$oid` / `$date` caveats noted above). Indexes are
not stored in the dump; `.import` **rebuilds them from the records**, so the
imported database always has correctly-encoded indexes for its backend. That is
what makes a dump portable **across backends** — export from a redb database,
import into a RocksDB one. This is distinct from `.backup`, which is a fast
*physical* copy that only the same backend can restore.

(At the API level these are `Database::export` / `Database::import`, which also
support per-collection scope and `Overwrite` / `Skip` collision modes; the CLI
commands are whole-database and error on collision. A JSONL interop *export* is
not yet available — use `mongoexport` / `.seed` for the lossy JSONL path.)

## Where next

- [Querying](./querying.md) — the query model, plan scenarios, distinct, subqueries.
- [Mongo Operators](./mongo-operators.md) — find filters and update operators.
- [Function Reference](./functions.md) — every built-in function.
- [Architecture](./architecture.md) — how the crates fit together, and native triggers/validators/UDFs.
