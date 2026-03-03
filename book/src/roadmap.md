# Roadmap

## Collect Node (Plan Materialization Barrier)

### Concept

A `Collect` plan node that drains its child stream and emits the result as a single
materialized batch. This makes materialization points explicit in the plan tree instead
of hidden inside executor implementations.

```
Sort                    IndexMerge(And)
  Collect                 Collect            ← materialized into a set
    Scan                    IndexScan(status)
                          IndexScan(user_id) ← streamed, probed against set
```

### Motivation

Today, `Sort` and `IndexMerge` secretly materialize their inputs internally. The plan
tree shows `Sort(Scan)` which looks like a streaming pipeline, but the executor collects
everything into a `Vec` before sorting. With `Collect` as an explicit node, the plan
tree is honest about where memory grows.

### Current nodes that collect internally

- **`IndexMerge`** — collects both `lhs` and `rhs` into `Vec`s for set intersection/union
- **`Sort`** — collects all records into a `Vec` to sort in memory
- **`Distinct`** — iterates source eagerly to build a dedup set

### Asymmetric IndexMerge (And)

Postgres-style optimization: for `And`, collect only one side into a hash set, then
stream the other side and probe against it. This avoids materializing both sides.
For `Or`, both sides still need collection (full union).

### Benefits

- **Plan legibility** — materialization is visible in the plan tree
- **Reusable** — any node needing a materialized input wraps its child in `Collect`
- **EXPLAIN** — `Collect` nodes tell the user where memory grows
- **Future stages** — natural boundary for spill-to-disk, distributed execution, caching

### Performance note

This is primarily a **composability win**, not a performance win. The same work happens
either way. The real performance opportunity is the asymmetric `IndexMerge(And)` path,
which is an algorithm change that could be implemented with or without `Collect` as a
plan node.

---

## Dynamic Primary Key Path — Done

All executor nodes (`insert_record`, `upsert`, `replace`, `delete`, `read_record`,
`index_scan`, `index_merge`, `projection`) and the mutation parser now use
`CollectionHandle::pk_path()` instead of hardcoded `"_id"`.

**Constraint:** `pk_path` must be a top-level scalar field. Dot-paths are rejected at
collection creation time. This matches MongoDB (where `_id` is always top-level) and
avoids significant complexity in the append/copy/skip patterns used by insert, upsert,
replace, and projection nodes.

**TTL path** supports dot-paths (e.g. `"meta.expires_at"`) since it is read-only —
the engine resolves nested DateTime values during record encoding via a byte-level
path scanner.

---

## Backup — Done (Hot Backup)

### Hot backup

Online snapshots via `Database::backup(path)`. Delegates to the store backend behind
the `BackupStore` trait — the caller doesn't need to know which backend is in use.

- **RocksDB** — `rocksdb::Checkpoint` (hardlinks SST files, near-instant)
- **redb** — `std::fs::copy` (CoW B-tree keeps the file crash-consistent)
- **MemoryStore** — returns an error (nothing to back up)

`backup()` is only available when `S: BackupStore`. Restore is offline — open the
backup directory/file as a fresh store.

### Export / Import (Not yet implemented)

Logical backup. Dump collections as BSON or JSON, reload into the same or a different
instance. The DB layer owns this because it understands collection metadata, index
configuration, and PK paths. The engine just sees keys and bytes.

Use cases: moving data between instances, seeding dev environments, cross-backend
migration (e.g. redb → RocksDB).

---

## Change Detection (Watch Queries)

### Concept

Register a filter against a collection. On every insert, update, or delete, the
written document is evaluated against registered filters. If it matches, the
registered handler fires with the change event.

```rust
db.watch("users", filter!{ "status": "active" }, |event| {
    // event contains: operation (insert/update/delete), old doc, new doc
});
```

### Why it fits an embedded DB

In a client-server database, change streams are a networking concern (oplog tailing,
WebSocket push). In an embedded DB, writes happen in-process — the write path can
evaluate filters synchronously and dispatch to callbacks with zero serialization
overhead.

### Design considerations

- **Filter evaluation** — reuse the existing query filter logic from the planner/executor.
  A registered watch is essentially a compiled filter predicate.
- **Granularity** — fire on insert, update, delete, or any combination. Include both old
  and new document for updates so the handler can diff.
- **Lifecycle** — return a handle that can be dropped to unregister. Watches should not
  prevent collection drops but should be cleaned up gracefully.
- **Threading** — handlers run on the writer's thread by default (synchronous). Async
  dispatch (channel-based) as an option for handlers that shouldn't block writes.
- **Index-aware fast path** — if the watch filter matches an indexed field with an Eq
  predicate, skip evaluation for writes that don't touch that field.

---

## User-Defined Logic

### Implemented

**Triggers** and **validators** are implemented. Scripts are registered per-collection,
resolved at plan time via `HookSnapshot`, and executed as typed plan nodes
(`Node::Validate`, `Node::Trigger`, `Plan::Trigger`). See the
[mutation pipeline](./querying.md) documentation for details.

**Lua runtime** (`mlua`) is the default scripting backend. Sandboxed execution with
instruction limits, BSON type preservation, and scoped transaction callbacks
(`ctx.get`, `ctx.put`, `ctx.delete`).

**JS runtime** (`wasm-bindgen`) provides the same `ScriptRuntime`/`ScriptHandle`
traits on wasm32 targets, delegating script execution to the JS host.

### Remaining hook points

- **Computed fields** — derive a field value from the rest of the document on
  insert/update. The result is stored in the document, making it indexable and
  queryable like any real field.
- **Custom index key extractors** — produce a synthetic index key from document fields
  (e.g. a normalized/lowercased string, a composite key). The engine indexes the
  output; the function defines *what* to index.
- **Partial index filters** — a Lua predicate that controls whether a document is
  included in an index. Evaluated on every insert/update during index maintenance.
  See the Partial Indexes section below.
- **Transform pipelines** — chain multiple functions on a document before storage.
  Schema migration, field normalization, enrichment.

### Future runtime: Wasm (wasmtime / wasmi)

Polyglot — users write functions in any language that compiles to wasm32 (Rust, Swift,
Go, JS via QuickJS, AssemblyScript). Sandboxed by default with no filesystem, network,
or memory access beyond what's explicitly granted. Fuel metering provides hard
computation bounds. The `RuntimeKind::Wasm` variant and `wasm` feature flag are
reserved for this.

---

## WebAssembly Support

### What works today

The full database stack (`slate-store`, `slate-engine`, `slate-query`, `slate-vm`,
`slate-db`) compiles to `wasm32-unknown-unknown`. The MemoryStore backend works
out of the box — no C dependencies, no filesystem, no threads.

```bash
cargo build -p slate-db --target wasm32-unknown-unknown --no-default-features --features js
```

This produces a `slate_db.wasm` binary with the complete query engine, planner,
executor, index support, and JS scripting bridge.

**Scripting on wasm32:** The native Lua runtime (`mlua`) vendors C code and cannot
compile to wasm. The `js` feature in `slate-vm` provides an alternative:
`JsScriptRuntime` and `JsScriptHandle` implement the `ScriptRuntime`/`ScriptHandle`
traits via `wasm-bindgen`, delegating script execution to the JS host. The JS side
plugs in any Lua engine (e.g. wasmoon for Lua 5.4 compiled to wasm, or Fengari for
a pure-JS Lua VM).

The bridge exposes three wasm-bindgen contract points:

- **`slate_vm_load(name, source) → handle_id`** — JS compiles script source, returns
  an integer handle
- **`slate_vm_call(handle_id, input_bson, caps) → output_bson`** — JS executes the
  compiled script with BSON input
- **`slate_vm_invoke_method(name, args_bson) → result_bson`** — exported from Rust,
  called by JS when a script invokes `ctx.get()`, `ctx.put()`, or `ctx.delete()`,
  routing back into the Rust transaction layer

Feature flags:

| Feature | Runtime | Target | Use case |
|---------|---------|--------|----------|
| `lua`   | mlua (C Lua 5.4) | Native | Default for Rust/Swift apps |
| `js`    | wasm-bindgen → JS host | wasm32 | Browser, Node.js |
| `wasm`  | (reserved) | Any | Future: wasmtime/wasmi for embedded wasm modules |

### Remaining work

#### Platform adapters

`slate-db` currently owns two platform-specific concerns: a `SystemTime::now()` clock
and a background sweep thread (`std::thread::spawn`). These need to be gated for wasm.

Gate platform-specific code behind a `runtime` feature in `slate-db` (default on).
With `runtime` enabled, `Database::open` uses `SystemTime::now()` as the clock and
spawns a background sweep thread — batteries included for native Rust consumers.
Without it, the caller provides a clock function and handles sweep manually via
`purge_expired()`.

```
slate-db (features = ["runtime"])   → default: SystemTime clock, sweep thread
slate-db (default-features = false) → pure logic, caller provides clock, no sweep

slate-uniffi  → depends on slate-db (default features → runtime on)
                UniFFI bindings for Swift/Kotlin

slate-wasm    → depends on slate-db with default-features = false
                wasm-bindgen, injects Date.now(), no sweep (or JS setInterval)
```

Runtime blockers (compile succeeds, but these panic at runtime on wasm32):

- **`SystemTime::now()`** — `KvEngine::with_clock()` escape hatch already exists
- **`std::thread::spawn`** — sweep is already a no-op when
  `ttl_sweep_interval_secs = u64::MAX`
- **`getrandom`** — needs `features = ["js"]` for `crypto.getRandomValues()` entropy
  (used by bson for ObjectId generation)

#### Browser storage

MemoryStore works on wasm32 but is ephemeral — data is lost on page reload. Two
browser-native storage options could provide persistence:

**OPFS (Origin Private File System)** — `createSyncAccessHandle()` in Web Workers
provides synchronous file I/O. This maps directly to the existing `Store` trait
without any API changes. Could potentially run redb on top of it since redb is
file-backed. Limited to Web Workers (not the main thread).

**IndexedDB** — transactional key-value store built into all browsers. No size limits,
supports binary data. The catch: IndexedDB is async, and the `Store`/`Transaction`
traits are synchronous. Supporting IndexedDB would require making the Store trait
async, which is a significant but worthwhile change — it would also benefit
server-mode deployments where async I/O matters.

#### slate-wasm crate

A thin binding crate (similar to `slate-uniffi`) that wraps `Database`, `Transaction`,
and `Cursor` with `wasm-bindgen` exports. Depends on `slate-db` with
`default-features = false`. Injects `Date.now()` as the clock. Exposes
`purge_expired()` for manual or `setInterval`-driven cleanup.

---

## Browser Playground

### Concept

Ship a single-page app powered by `slate-wasm` where users can create collections,
insert documents, write queries, attach Lua hooks, and see query plans — all
client-side with no backend.

An interactive playground sells an embedded DB better than docs or benchmarks. Users
can feel it working: insert a document, watch a Lua validator reject bad data, edit a
computed field function, re-query, see results update instantly.

### Architecture

```
Browser
  ├── slate_db.wasm          ← database engine (MemoryStore)
  ├── wasmoon / fengari      ← Lua VM in JS
  └── UI (editor + results)  ← query input, document viewer, plan visualizer

JS glue wires wasmoon into the wasm-bindgen bridge:
  slate_vm_load  → wasmoon.loadString(source)
  slate_vm_call  → wasmoon.callFunction(handle, input)
  ctx.get/put/delete → slate_vm_invoke_method → Rust transaction layer
```

Depends on the platform adapter work and the `slate-wasm` crate above.

---

## Compound Indexes

### Problem

Today, each index covers a single field. A query like `{ "status": "active", "created_at": { "$gt": "2024-01-01" } }` uses one index for `status` (Eq scan) and either a full scan or a second index for `created_at`, merged via `IndexMerge(And)`. This works but requires materializing one side into a hash set for the probe — two index scans plus a set intersection.

### Design

A compound index covers multiple fields in a defined order. The key layout extends naturally:

```
i\0{collection}\0{field1}+{field2}\0{value1_bytes}{value2_bytes}{doc_id_lp}
```

The planner recognizes when a compound index satisfies multiple predicates in a single scan. For the query above, a compound index on `["status", "created_at"]` produces a prefix scan on `status = "active"` followed by a range filter on `created_at` — one index walk, no merge.

### Key prefix rules

Compound indexes follow the leftmost prefix rule (same as MongoDB, MySQL):

- Index on `["a", "b", "c"]` can satisfy queries on `{a}`, `{a, b}`, or `{a, b, c}`
- Cannot satisfy `{b}` or `{b, c}` alone — the leading field must be present
- Range predicates on a field terminate prefix usage — fields after the range use in-memory filtering

### Work

- Extend `CollectionConfig.indexes` to accept `Vec<String>` per index (single field is `vec!["field"]`)
- Update key encoding to concatenate multiple value bytes with length prefixes
- Extend `IndexDiff` to compute entries for multi-field keys
- Planner: score compound indexes by how many query predicates they cover
- Backfill: `create_index` with a compound spec re-indexes existing documents

### Interaction with partial indexes

Compound indexes can be combined with Lua-based partial index filters — e.g. a compound index on `["status", "priority"]` that only indexes documents where `is_archived == false`.

---

## SQL Query Surface

### Concept

A SQL-like query language for aggregation and complex reads, inspired by CosmosDB's SQL
dialect. This is a query surface — it compiles down to the same plan tree nodes that the
filter/find API uses, plus new aggregation nodes.

```sql
SELECT c.status, COUNT(1) AS total, AVG(c.score) AS avg_score
FROM users c
WHERE c.active = true
GROUP BY c.status
ORDER BY total DESC
```

### Why SQL

SQL is universally understood. Offering a SQL surface for aggregation queries lowers the
learning curve — users don't need to learn a custom pipeline DSL. The document model stays
BSON; SQL is just the query language.

### Sub-document joins (CosmosDB-style)

CosmosDB supports `JOIN` within a single document's sub-arrays, not across collections.
This is a natural fit for an embedded document DB:

```sql
SELECT c.name, t.tag
FROM users c
JOIN t IN c.tags
WHERE t.tag = "rust"
```

This flattens the `tags` array, producing one row per element. No cross-collection joins,
no foreign keys — just array unwinding expressed in SQL syntax.

### Aggregation functions

- **`COUNT`**, **`SUM`**, **`AVG`**, **`MIN`**, **`MAX`** — standard aggregates
- **`GROUP BY`** — groups by one or more fields, produces one output row per group
- **`HAVING`** — filter on aggregate results (post-group)
- **`ARRAY_AGG`** / **`COLLECT`** — gather grouped values into an array

### Execution

Aggregation introduces new plan nodes:

- **`GroupBy`** — materializes input, groups by key fields, computes aggregates
- **`Having`** — post-group filter (operates on aggregate outputs)
- **`ArrayUnwind`** — flattens a sub-array for `JOIN ... IN` syntax

These compose with existing nodes (`Filter`, `Sort`, `Limit`, `Projection`, `IndexScan`).

### Parser

A lightweight SQL parser (hand-written recursive descent or `sqlparser-rs`) that emits the
existing plan tree. The SQL surface is purely additive — the filter/find API continues to
work unchanged.

---

## Partial Indexes

### Concept

Index only a subset of documents in a collection, controlled by a filter predicate. Reduces
index size and write amplification for collections where most queries target a known subset.

```rust
txn.create_index("orders", IndexConfig {
    fields: vec!["customer_id".into()],
    filter: Some("status ~= 'cancelled'"),  // Lua expression
})?;
```

### Integration with Lua hooks

The filter predicate is a Lua expression evaluated against each document on insert/update.
If it returns `false`, the document is skipped during index maintenance — no entry is
written. This reuses the Lua runtime from the user-defined logic system.

### Use cases

- Index `orders.customer_id` only for non-cancelled orders
- Index `users.email` only for verified users
- Sparse indexes: skip documents missing the indexed field entirely

### Work

- Extend `IndexConfig` with an optional filter expression (stored in index metadata)
- `IndexDiff` evaluates the filter before generating index entries
- `create_index` backfill respects the filter
- Planner: only consider a partial index when the query's filter is a superset of the
  index filter (the query must logically guarantee all matching documents are in the index)

---

## Test Coverage

### Error paths

Test behavior for: malformed queries, missing collections, invalid filter operators.

### Encoding edge cases

Negative ints in index keys, empty strings, special characters in record IDs, very long values.

### ClientPool

Test connection pooling: checkout/return, behavior under contention, handling of dropped connections.
