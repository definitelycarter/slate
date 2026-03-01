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

## Dynamic Primary Key Path

### Problem

The engine layer supports configurable primary keys via `CollectionHandle::pk_path()`,
but the executor nodes in `slate-db` hard-code `"_id"` in ~18 places across
`insert_record`, `upsert`, `replace`, `delete`, `read_record`, `index_scan`,
`index_merge`, and `projection`.

### Work

- Thread `CollectionHandle` (or just `pk_path: &str`) into each executor node so they
  use the configured path instead of `"_id"`.
- `pk_path` should support dot-notated paths (e.g. `"user.id"`) for nested primary keys.
  `RawDocument::get()` only does top-level lookups, so a dot-path traversal helper is
  needed.
- Auto-generation of missing PKs (`insert_record`, `upsert`) needs to work with nested
  paths — creating intermediate subdocuments if necessary.
- `projection` always includes `_id` unconditionally — should include whatever `pk_path`
  resolves to instead.

---

## Backup

### Hot backup

Snapshot the storage while the DB is running. Delegates to the store backend
(RocksDB `CreateCheckpoint`, redb `savepoint`) behind a unified DB-layer API so the
caller doesn't need to know which backend is in use.

This is the primary backup path — run on a schedule or before a risky migration.

### Export / Import

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

### Concept

Register sandboxed functions that the DB calls at defined hook points in the write and
read paths. Functions take BSON bytes in and return BSON bytes out.

### Hook points

- **Computed fields** — derive a field value from the rest of the document on
  insert/update. The result is stored in the document, making it indexable and
  queryable like any real field.
- **Validation** — run a predicate before a write is accepted. Return an error to
  reject the document. Replaces ad-hoc application-side validation.
- **Custom index key extractors** — produce a synthetic index key from document fields
  (e.g. a normalized/lowercased string, a composite key). The engine indexes the
  output; the function defines *what* to index.
- **Transform pipelines** — chain multiple functions on a document before storage.
  Schema migration, field normalization, enrichment.

### Runtime options

#### Wasm (wasmtime / wasmi)

Polyglot — users write functions in any language that compiles to wasm32 (Rust, Swift,
Go, JS via QuickJS, AssemblyScript). Sandboxed by default with no filesystem, network,
or memory access beyond what's explicitly granted. Fuel metering provides hard
computation bounds.

wasmtime uses JIT/AOT compilation (fast, but JIT is blocked on iOS). wasmi is a pure
interpreter (works everywhere, slower for heavy computation). AOT pre-compilation is a
middle ground — compile at build time, load the native artifact at runtime.

#### Lua (mlua)

Single language, but a much smaller footprint (~200KB runtime). Pure interpreter with
no platform restrictions. Battle-tested embedding pattern (Redis, Nginx, Neovim).
Lower barrier for simple validation rules and field transforms. Sandboxing is manual —
strip `os`, `io`, `require` from the environment.

#### Recommendation

Lua is the more practical default for slate's primary use case (short functions that
compute a field or validate a document). Wasm is the better choice when polyglot
support or heavy computation is needed. Both can coexist behind a common trait —
the hook points don't care which runtime evaluates the function.

---

## Test Coverage

### Error paths

Test behavior for: malformed queries, missing collections, invalid filter operators.

### Encoding edge cases

Negative ints in index keys, empty strings, special characters in record IDs, very long values.

### ClientPool

Test connection pooling: checkout/return, behavior under contention, handling of dropped connections.
