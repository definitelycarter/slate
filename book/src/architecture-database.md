# Database & Bindings

## Tier 3: Database Layer (`slate-db`)

### Overview

The database layer composes the query stack (Tier 2) over `slate-engine` and exposes the user-facing `Database`/`Transaction` API. It translates a request through a front-end, lowers it with `slate-planner`, and runs it with `slate-executor`; storage operations (record reads, writes, scans, index lookups) are delegated to `slate-engine`'s `EngineTransaction` trait.

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

The `Database` is generic over `Store`. `begin(read_only)` returns a transaction;
every collection-scoped operation hangs off a `db.collection(name)` handle (or
`db.cf(cf).collection(name)` for a non-default column family) and runs against the
transaction you pass to its terminal. Operations are **lazy builders** — nothing
runs until a terminal (`.execute(&txn)`, `.iter::<T>(&txn)`, `.iter_raw(&txn)`).

```rust
use slate_db::{DatabaseBuilder, SortDirection};
use slate_db::v2::IndexOptions;
use bson::doc;

let db = DatabaseBuilder::new().open(store)?;

// Create a collection (default column family; pk_path defaults to "_id")
let txn = db.begin(false)?;
db.collections().create("users").execute(&txn)?;

// A reusable handle to the collection
let users = db.collection("users");

// Insert — builders finished with .execute(&txn) → WriteResult { affected }
users.insert_one(doc! { "name": "Alice", "status": "active" }).execute(&txn)?;
users.insert_many(vec![
    doc! { "_id": "bob",   "name": "Bob" },
    doc! { "_id": "carol", "name": "Carol" },
]).execute(&txn)?;

// Query — find() is a lazy read; a terminal streams it as a std Iterator
for doc in users.find(doc! {}).iter::<User>(&txn)? {  // iter::<T> — deserializes into T
    let doc = doc?;
}
// or raw access, no deserialization:
for raw in users.find(doc! {}).iter_raw(&txn)? {      // yields RawDocumentBuf
    let raw = raw?;
}

// find_one — take the first row of the iterator
let one = users.find(doc! { "_id": "bob" }).iter::<User>(&txn)?.next().transpose()?;

// Count — the std Iterator's own count (usize)
let n = users.find(doc! {}).iter_raw(&txn)?.count();

let filter = doc! { "status": "active" };

// Update (merge — preserves unspecified fields). Defaults to ALL matches;
// .one() restricts to the first.
users.find(filter.clone())
    .update(doc! { "$set": { "status": "archived" } }).execute(&txn)?;
users.find(filter.clone())
    .update(doc! { "$set": { "status": "archived" } }).one().execute(&txn)?;

// Replace (full document swap, single row, pk preserved)
users.find(filter.clone())
    .replace(doc! { "name": "Alice", "status": "inactive" }).execute(&txn)?;

// Delete — ALL matches, or .one() for the first
users.find(filter.clone()).delete().execute(&txn)?;
users.find(filter).delete().one().execute(&txn)?;

// Index management
users.indexes().create("email", IndexOptions::default()).execute(&txn)?; // backfills
users.indexes().remove("email").execute(&txn)?;
users.indexes().list(&txn)?;                       // -> Vec<String>

// Triggers — native: register a Rust fn in the bag (with_trigger /
// triggers().register), then bind it to the collection here.
users.triggers()
    .create("audit_trigger", TriggerFunction::from_name("audit"))
    .execute(&txn)?;
// Validators — native: register a Rust fn in the bag (with_validator /
// validators().register), then bind it to the collection here.
users.validators()
    .create("require_name", ValidatorFunction::from_name("require_name"))
    .execute(&txn)?;

// Collection management
db.collections().list(&txn)?;                      // -> Vec<String> (this column family)
db.collections().remove("users").execute(&txn)?;   // removes data, indexes, metadata

txn.commit()?;
```

> **Still flat (db-level API).** A few operations have no builder yet and remain
> methods on `Database`/`Transaction`, pending the db-level API redesign:
> `db.verify(cf, coll)` / `db.repair(cf, coll)` (integrity), `db.stats()`, and the
> db-global `db.list_collections()` (every column family, as `(cf, name)` pairs —
> distinct from the cf-scoped `collections().list()` above). `list_collections`
> will move to a handle in a later pass.

### Query Execution

`slate-planner` lowers a request to a `Plan` (a tree of `Node`s); `slate-executor` streams it with lazy materialization. See [Querying](./querying.md) for the full reference with all plan scenarios.

**Planning** builds a pipeline (a join-free `find` reads bottom-to-top):

```
Scan / (IndexScan|IndexMerge → KeyLookup) → [Bind/Unwind] → Filter → Sort → Project → Limit
```

**Two tiers:**

1. **ID tier** — `Scan` streams documents; `IndexScan` and `IndexMerge` produce document IDs without touching document bytes, and `KeyLookup` fetches the documents for an ID stream.
2. **Value tier** — the binding-aware nodes (`Filter`, `Sort`, `Project`) operate on `Option<RawBson>` values via the `slate-eval` raw evaluator, borrowing individual fields out of the row's bytes (through the `slate-rawbson` scanner) with no `bson::Document` materialization. `Project` builds `RawDocumentBuf` output with `append()`, copying selected fields by reference. A `find` terminal `.iter::<T>(&txn)` deserializes each row into `T`, or `.iter_raw(&txn)` yields `RawDocumentBuf` with no deserialization.

For a join-free query the planner binds the whole row to a single alias (`RowBinding::Alias`) with no per-row environment wrapper; only joins materialize a multi-binding row environment via `Bind`/`Unwind`.

**Key properties:**

- **No deserialization** — the entire pipeline stays in raw bytes. Rows that fail a filter are never cloned or materialized.
- **Compiled expressions** — `Filter`/`Project` compile their expression once per query (`raweval::compile`) and evaluate the resolved form per row, rather than re-walking the AST.
- **Sargability** — equality/range conjuncts on indexed fields push into an `IndexScan`; primary-key equality becomes a direct `KeyLookup`; the rest stays a residual `Filter`.
- **Index union for OR** — OR queries with indexed branches use `IndexMerge(Or)` to combine ID sets, avoiding full scans.
- **Dot-notation field access** — filters, sorts, and projections support nested paths like `"address.city"`.
- **Plan-time hook resolution** — triggers and validators are resolved from a snapshot at plan time and wired into the plan tree as `Node::Trigger`, `Node::Validate`, and `Plan::Trigger` nodes. Zero overhead for collections without hooks. See [Querying — Mutation Pipeline](./querying.md#mutation-pipeline--triggers-and-validators).
- **Native validators and triggers** — both are native Rust (`dyn Validator` / `dyn Trigger` from a database-scoped bag, resolved once per query and run behind a panic boundary); validators are `Pure` (candidate-only), triggers get a column-family-confined read-write context. There is no embedded VM, so the whole query stack builds without `mlua` and compiles to `wasm32`. Register functions before open via `DatabaseBuilder::with_trigger` / `with_validator`, or at runtime via `triggers().register` / `validators().register`.

### Logical Export / Import

`slate-db` also owns logical dump/reload — `Database::export(...)` and
`Database::import(...)` — the backend-neutral counterpart to the physical
`BackupStore::backup` in the [storage tier](./architecture-storage.md). It lives
here because only this tier sees the collection catalog (`pk_path`, `ttl_path`,
indexes and their unique subset); the store layer sees only keys and bytes.

A dump is a directory:

```
dump/
  manifest.bson            ← versioned collection definitions
  <cf>.<collection>.bson   ← one streamed document file per collection (concatenated raw BSON)
```

`export` walks each collection through a read-only snapshot and streams native raw
BSON. `import` recreates collections from the manifest, reloads documents, and
**rebuilds index entries from the records** — index entries are never dumped. That
is the same "records are the source of truth" contract used by repair and the
encoding migration, and it means the target backend always gets correctly-encoded
indexes for *its* current encoding version. Consequently a dump round-trips across
backends (e.g. redb → RocksDB), which physical backup cannot do.

Scope is whole-DB or per-collection in both directions. Collision behavior on an
existing target is `OnCollision::Error` (default) / `Overwrite` / `Skip`. The BSON
canonical format is lossless — ObjectId, DateTime, and Decimal128 survive a
round-trip. Public types (`Manifest`, `CollectionDef`, `ExportOptions`,
`ImportOptions`, `ExportReport`, `ImportReport`, `OnCollision`) are re-exported
from `slate-db`. See the
[Logical Export / Import RFC](./rfcs/logical-export-import.md). JSONL interop export
is deferred; `.seed` already ingests JSONL.

## Platform Bindings

### `slate-uniffi` — Swift/Kotlin

UniFFI bindings for Apple and Android platforms. Wraps `Database<StoreImpl>` in a `SlateDatabase` object with auto-commit `read()`/`write()` helpers. Documents cross the FFI boundary as raw BSON bytes (`Vec<u8>`). Feature-gated store selection: `memory`, `redb`, `rocksdb`.

### `slate-wasm` — JavaScript/WebAssembly

wasm-bindgen bindings for browser and Node.js. Wraps `Database<MemoryStore>` in a `SlateDb` struct exported to JavaScript. Accepts and returns plain JS objects — BSON conversion happens internally via `serde-wasm-bindgen`, so no BSON library is needed on the JS side.

```js
const db = new SlateDb();
db.create_collection("users");
db.insert_one("users", { _id: "u1", name: "Alice", age: 30 });

const docs = db.find("users", { name: "Alice" });
// [{ _id: "u1", name: "Alice", age: 30 }]
```

**Key properties:**

- **Auto-commit** — each method creates a transaction, executes, and commits/rolls back. Same pattern as `slate-uniffi`.
- **JS-native interface** — `JsValue` in/out via `serde-wasm-bindgen`. No `Uint8Array` encoding required.
- **Mutations return documents** — insert, update, delete all return the affected documents as an `Array` of JS objects. Use `.length` for count.
- **MemoryStore only** — no filesystem access on wasm32. Persistent storage (OPFS, IndexedDB) is future work.
- **Clock injection** — uses `Date.now()` via `js_sys` since `SystemTime::now()` panics on wasm32. Injected through `DatabaseBuilder::with_clock()`.
- **No native runtime deps** — `slate-db` is pulled with `default-features = false`, excluding the native PRNG/TTL pieces (`rand`/`getrandom`, which don't target wasm32). Triggers, validators, and UDFs are native Rust functions with no embedded VM, so they cross to wasm with no extra wiring — the host registers closures via `with_trigger` / `with_validator` / `with_udf` like any other build. The `wasm32-unknown-unknown` build of this crate is guarded in CI.
