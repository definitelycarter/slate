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

The `Database` struct is generic over `Store` and provides a `begin()` method that returns a `DatabaseTransaction`. All operations go through the transaction:

```rust
use slate_db::{DatabaseBuilder, DEFAULT_CF, CollectionConfig, FindOptions};

let db = DatabaseBuilder::new().open(store)?;

// Create a collection (uses DEFAULT_CF by default)
let mut txn = db.begin(false)?;
txn.create_collection(&CollectionConfig {
    name: "users".into(),
    ..Default::default()
})?;

// Insert — all operations take (cf, collection, ...) as first params
txn.insert_one(DEFAULT_CF, "users", doc! {
    "name": "Alice",
    "status": "active",
})?;

txn.insert_many(DEFAULT_CF, "users", vec![
    doc! { "_id": "bob", "name": "Bob" },
    doc! { "_id": "carol", "name": "Carol" },
])?;

// Query — find() returns a Cursor for lazy iteration
let cursor = txn.find(DEFAULT_CF, "users", rawdoc! {}, FindOptions::default())?;
for doc in cursor.iter::<User>()? {      // CursorIter<T> — deserializes into T
    let doc = doc?;
}
// or raw access, no deserialization:
for raw in cursor.iter_raw()? {          // RawCursorIter — yields RawDocumentBuf
    let raw = raw?;
}

let one = txn.find_one(DEFAULT_CF, "users", rawdoc! { "_id": "bob" })?;

// Update (merge — preserves unspecified fields)
txn.update_one(DEFAULT_CF, "users", filter,
    rawdoc! { "$set": { "status": "archived" } })?.drain()?;
txn.update_many(DEFAULT_CF, "users", filter,
    rawdoc! { "$set": { "status": "archived" } })?.drain()?;

// Replace (full document swap)
txn.replace_one(DEFAULT_CF, "users", filter,
    rawdoc! { "name": "Alice", "status": "inactive" })?.drain()?;

// Delete
txn.delete_one(DEFAULT_CF, "users", filter)?.drain()?;
txn.delete_many(DEFAULT_CF, "users", filter)?.drain()?;

// Count
let n = txn.count(DEFAULT_CF, "users", rawdoc! {})?;  // -> u64

// Index management
txn.create_index(DEFAULT_CF, "users", "email")?;  // backfills existing records
txn.drop_index(DEFAULT_CF, "users", "email")?;
txn.list_indexes(DEFAULT_CF, "users")?;            // -> Vec<String>

// Triggers and validators (requires DatabaseBuilder with scripting)
txn.register_trigger("app", "users", "audit_trigger", r#"
    return function(ctx, event)
      ctx.put("audit", { _id = event.doc._id .. ":" .. event.action, action = event.action })
      return event
    end
"#)?;
txn.register_validator("app", "users", "require_name", r#"
    return function(event)
      if type(event.doc.name) ~= "string" then
        return { ok = false, reason = "name is required" }
      end
      return { ok = true }
    end
"#)?;

// Collection management
txn.list_collections()?;                           // -> Vec<String>
txn.drop_collection(DEFAULT_CF, "users")?;         // removes data, indexes, metadata

txn.commit()?;
```

### Query Execution

`slate-planner` lowers a request to a `Plan` (a tree of `Node`s); `slate-executor` streams it with lazy materialization. See [Querying](./querying.md) for the full reference with all plan scenarios.

**Planning** builds a pipeline (a join-free `find` reads bottom-to-top):

```
Scan / (IndexScan|IndexMerge → KeyLookup) → [Bind/Unwind] → Filter → Sort → Project → Limit
```

**Two tiers:**

1. **ID tier** — `Scan` streams documents; `IndexScan` and `IndexMerge` produce document IDs without touching document bytes, and `KeyLookup` fetches the documents for an ID stream.
2. **Value tier** — the binding-aware nodes (`Filter`, `Sort`, `Project`) operate on `Option<RawBson>` values via the `slate-eval` raw evaluator, borrowing individual fields out of the row's bytes (through the `slate-rawbson` scanner) with no `bson::Document` materialization. `Project` builds `RawDocumentBuf` output with `append()`, copying selected fields by reference. `find()` returns a `Cursor` whose `.iter::<T>()` deserializes into `T`, or `.iter_raw()` yields `RawDocumentBuf` with no deserialization.

For a join-free query the planner binds the whole row to a single alias (`RowBinding::Alias`) with no per-row environment wrapper; only joins materialize a multi-binding row environment via `Bind`/`Unwind`.

**Key properties:**

- **No deserialization** — the entire pipeline stays in raw bytes. Rows that fail a filter are never cloned or materialized.
- **Compiled expressions** — `Filter`/`Project` compile their expression once per query (`raweval::compile`) and evaluate the resolved form per row, rather than re-walking the AST.
- **Sargability** — equality/range conjuncts on indexed fields push into an `IndexScan`; primary-key equality becomes a direct `KeyLookup`; the rest stays a residual `Filter`.
- **Index union for OR** — OR queries with indexed branches use `IndexMerge(Or)` to combine ID sets, avoiding full scans.
- **Dot-notation field access** — filters, sorts, and projections support nested paths like `"address.city"`.
- **Plan-time hook resolution** — triggers and validators are resolved from a snapshot at plan time and wired into the plan tree as `Node::Trigger`, `Node::Validate`, and `Plan::Trigger` nodes. Zero overhead for collections without hooks. See [Querying — Mutation Pipeline](./querying.md#mutation-pipeline--triggers-and-validators).
- **Runtime-agnostic scripting** — trigger/validator dispatch goes through `slate-vm`'s trait objects (`VmPool`, `dyn ScriptRuntime`/`ScriptHandle`, `VmError`); the executor never names a concrete runtime, so it builds without `mlua` and the whole query stack compiles to `wasm32`. Scripting is injected from above via `DatabaseBuilder::with_scripting`; tests register a `LuaScriptRuntime` through a dev-dependency.

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
- **No Lua runtime** — `slate-db` is pulled with `default-features = false`, so the Lua runtime (and mlua's vendored C, which cannot target wasm32) is excluded. Scripting stays pluggable: register a wasm-safe `ScriptRuntime` (the `js` backend bridges to a JS-side Lua engine) into a `VmPool` and inject it via `DatabaseBuilder::with_scripting`. The `wasm32-unknown-unknown` build of this crate is guarded in CI.
