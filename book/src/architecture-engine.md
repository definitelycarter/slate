# Query Stack, Engine & Scripting

## Tier 2: Query Stack (v2)

### Overview

Two query surfaces lower to **one** shared AST, planner, and executor, so they can't drift in semantics:

```
Mongo find ─► slate-query ─┐
                           ├─► slate-ast ─► slate-planner ─► slate-executor
SQL text   ─► slate-sql  ──┘                  (slate-eval gives the AST meaning)
```

A "document" is just the case where the value flowing through execution happens to be a document. That single row model lets one executor — and crucially one `WHERE`/expression evaluator — serve both a traditional `find` and a CosmosDB-style `SELECT VALUE`. `find` is the degenerate case: a single binding `c`, an identity projection, no join/unwind.

### Shared AST (`slate-ast`)

A dependency-free leaf crate: the single intermediate representation every surface targets (`Query`, `ScalarExpr`, `Literal`, `BinOp`, …). `slate-sql` parses SQL text into it, `slate-query` translates a Mongo find into it, `slate-planner` lowers it, and `slate-eval` evaluates it — none of them depend on a sibling surface.

### Front-ends (`slate-query`, `slate-sql`)

**`slate-query`** — the MongoDB find front-end. Owns the find DTOs and translates a find request (a `$`-operator filter document plus options) into a `slate_ast::Query`. `translate_filter` is the *single* definition of what a Mongo filter means, reused by the write APIs that select documents with a filter.

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

pub struct Sort {
    pub field: String,
    pub direction: SortDirection,  // Asc | Desc
}
```

Filters are passed as `impl Serialize` at the database API layer (typically a `bson::doc!`, `rawdoc!`, or any `Serialize` struct), serialized to a `RawDocumentBuf`, and translated to the AST by `translate_filter`. Mongo-only constructs that Cosmos SQL doesn't share (array-distributing path access, explicit `.[]` multikey equality) are dedicated AST variants (`PathGet`, `MultikeyEq`) rather than overloaded member access or magic functions — so SQL functions stay to the CosmosDB spec.

**`slate-sql`** — the CosmosDB-style SQL front-end. Lexes and parses SQL text into the same AST. Surface: `SELECT VALUE <expr> FROM <alias> [JOIN <alias> IN <array-expr>]* [WHERE <expr>] [ORDER BY ...] [OFFSET/LIMIT]`. It also ships a small in-memory `exec` engine (over a `&[bson::Bson]` source) for iterating on language semantics without touching storage.

### Evaluation (`slate-eval`)

The meaning of the AST: an `owned` evaluator (`eval`, walks `bson::Bson`) and a zero-copy `raweval` (walks raw bytes, borrowing from the input). Every leaf rule — comparison, numeric coercion, three-valued logic, function dispatch — is a single shared definition reused by both, pinned by a differential test so they can't drift. `raweval::compile` resolves an expression once per query (function dispatch, path segments, single-alias field reads, the `f = v OR ARRAY_CONTAINS(f, v)` idiom, borrowed constants) and `eval_compiled` runs the resolved form per row.

### Planning + execution (`slate-planner`, `slate-executor`)

**`slate-planner`** makes the *decisions*: it lowers `slate_ast::Query` to a `Plan`/`Node` IR, choosing a scan source via sargability (`c.<indexed-field> <cmp> <literal>` conjuncts push into an `IndexScan`; pk equality becomes a direct `KeyLookup`; everything else stays a residual `Filter`). It is fed the collection's index metadata (`CollectionMeta`) so it can choose an index.

**`slate-executor`** runs the plan: pull-based streaming, one small per-node function per `Node`, mirroring the engine's `RawIter`. The stream item is `Result<Option<RawBson>, ExecError>` — the `Option` is the *undefined* channel (a `None` row is dropped at the output boundary), and carrying raw `RawBson` keeps `find` deserialization-free (the pipeline never re-materializes a typed `bson::Document`).

### Observability (`trace` feature, EXPLAIN ANALYZE, `stats()`)

Two opt-in instruments sit beside the planner/executor; both are zero-cost on the normal path.

**EXPLAIN ANALYZE — per-node row counters.** `Executor::execute_analyze(plan)` runs the plan and returns `(Vec<RawBson>, Rc<PlanStats>)`, where `PlanStats` (in `slate-planner`) holds one row counter per node keyed by **pre-order index**. The analyze walk and the renderer (`Plan::explain_analyze(&stats)`) advance that index in lock-step, so each counter lines up with its rendered line — `rows=` (emitted) and, for non-source nodes, `examined=` (the child's emitted count). The counting wrappers are built **only** when `execute_analyze` set the collector; the plain `execute` never reads it, so ordinary queries pay nothing per row. `Transaction::explain_analyze` is the `slate-db` surface (clones the plan once — off the hot path — to render after execution consumes it); the CLI exposes `.explain analyze`.

**`tracing` spans/events (`trace` feature).** An off-by-default cargo feature on both `slate-executor` and `slate-db` (enabling it on `slate-db` cascades to the executor). With it off, `tracing` is not a dependency and the `trace_span!`/`trace_event!` call sites (`slate-executor/src/trace.rs`, `slate-db/src/trace.rs`) expand to nothing — zero cost, wasm-safe. With it on, the executor emits a span/event around `execute` (keyed by plan kind) and the database emits an event on commit; the host wires its own subscriber (slate bundles no exporter).

**`stats()` — catalog & storage statistics.** `Transaction::collection_stats(cf, collection)` and `Transaction::stats()` (with `Database::{collection_stats, stats}` convenience wrappers that open and roll back their own read snapshot) report a collection's live `document_count` and, per index, `entry_count` and distinct-value `cardinality`, rolled up into a `DatabaseStats` with `total_documents` and an optional `disk_size_bytes`. Counts are computed by **scanning** (distinct values deduped on canonical BSON bytes), so they are exact as of the snapshot — no write-path counter, no drift — at an O(rows + index entries) cost (an introspection call, not a hot path). The `approximate` flag is reserved (always `false` today) for a future maintained-counter fast path, and `disk_size_bytes` is `None` pending per-backend plumbing through the store trait. CLI: `.stats [collection]`.

### Mutations (`slate-eval`, `slate-rawbson`)

`UPDATE` assignments are applied by `slate_eval::apply_assignments`: a raw byte-edit fast path splices each evaluated value into the document in place for the simple shapes (single-segment scalar writes, `$push`/`$pop` on the field's own array), falling back to a deserialize-mutate-reserialize rebuild for dotted paths, whole document/array values, and `$lpush`. Upsert *merges* — overlaying an update document's fields onto an existing one — use `slate_rawbson::raw_merge`, which overwrites in place when a value keeps its BSON type and width and splices otherwise. Both paths return "unchanged" so the write node can drop an untouched row.

## Tier 2.5: Native Hooks (`slate-udf`, `slate-validator`, `slate-trigger`)

### Overview

Slate extends database behavior with three kinds of **native Rust function** — a **UDF** (called from SQL), a **validator** (a write-path gate), and a **trigger** (a write-path side effect). Each role is its own low crate (`slate-udf`, `slate-validator`, `slate-trigger`) holding a trait, a context type, and a database-scoped **bag**. There is no embedded VM: the core links none of `mlua`/wasmtime/etc., so the whole stack compiles to `wasm32`. A scripting language can return one of these role-typed functions as a companion adapter (see the [native-functions RFC](./rfcs/native-functions.md)), but the core never links one.

### The bag + binding model

Two levels, think dynamic linking:

- **The bag (code)** — a flat, database-scoped `name -> Arc<dyn Role>` map, populated by `register(name, closure)` (no transaction) or the pre-open `with_udf` / `with_validator` / `with_trigger`. This is the *shared library*: one function, reused everywhere.
- **The binding (intent)** — a per-collection, durable `(cf, collection, name) -> func_name` mapping in the catalog, written by `create(name, func)` (transactional) and stored as a `runtime_tag::NATIVE` row whose bytes are the target function name. This is the *symbol reference*.

Resolution links the two — a binding's target name is looked up in the bag at plan build (UDFs) or once per query (validators, triggers). A dangling binding (bound but never registered) is an unresolved symbol: a UDF fails its query, a validator or trigger aborts the write (fail-safe). Every role runs behind a `catch_unwind` panic boundary, so user code at the seam can't violate the no-panic contract.

### Per-role contracts

- **UDF** — `Fn(&[Value]) -> Result<Value, UdfError>`, pure and contextless. Documented as a pure function of its arguments, so the engine may call it zero or more times and cache the result.
- **Validator** — `Fn(&ValidatorCtx) -> Result<Verdict, ValidatorError>`, `Pure`: `ctx.doc()` exposes only the candidate document, so a validator *structurally cannot* write.
- **Trigger** — `Fn(&TriggerCtx) -> Result<(), TriggerError>`, `ReadWrite`: `ctx.action()` / `ctx.doc()` plus `ctx.get` / `ctx.put` / `ctx.delete` / `ctx.merge` (field-merge upsert) over the transaction, **confined to the firing column family** (the context names a collection, never a cf, so cross-cf access is structurally impossible).

### Hook Registry and Snapshot Isolation

At the database layer (`slate-db`), hooks are managed by a `HookRegistry` backed by `ArcSwap<HookSnapshot>`:

- **`HookSnapshot`** — a frozen per-collection map, built by scanning the catalog: `(name, native_function)` bindings for triggers and validators (the code lives in the respective bag, not here), plus UDF bindings. All three are native — the snapshot holds name mappings, never code.
- **`HookRegistry`** — wraps `ArcSwap` for lock-free snapshot reads. Transactions capture a snapshot at `begin()` time and see a consistent view regardless of concurrent hook modifications.
- **Invalidation** — when a transaction that modified hooks (register/drop) commits, a fresh snapshot is loaded and swapped in. Subsequent transactions see the updated hooks.

This gives snapshot isolation for hook resolution — a long-running read transaction won't see hooks that were registered after it began.

## Tier 2.5: Engine Layer (`slate-engine`)

### Overview

The engine layer sits between `slate-store` and `slate-db`. It owns the on-disk format: BSON key encoding, record serialization (with TTL metadata), index maintenance, and the collection catalog. It provides an `EngineTransaction` trait that `slate-db` programs against, hiding all key encoding and storage layout details.

### CollectionHandle

A cheap-to-clone (`Arc`-backed) handle returned by the catalog when resolving a
collection. Carries the collection's column family name (`cf_name`), column family
reference, index field names, primary key path (`pk_path`), and TTL field path
(`ttl_path`). Passed to all engine operations so the caller never needs to know
about key encoding or storage layout.

**`pk_path`** — configurable primary key field name. Must be a top-level scalar field
(dot-paths are rejected at collection creation time). Defaults to `"_id"`. Can be any
top-level property (e.g. `"email"`, `"sku"`, `"user_id"`). If absent on insert, an
`ObjectId` is auto-generated at the configured field name.

**`ttl_path`** — the field used for TTL expiry. Supports dot-paths for nested DateTime
fields (e.g. `"meta.expires_at"`). Defaults to `"ttl"`. The engine extracts the
DateTime value during record encoding and stores it in the record header for O(1)
expiry checks.

### Key Encoding

All keys use a tag-prefixed binary format with `\x00` separators and sort-preserving
BSON value encoding. Doc IDs are length-prefixed (`[type:1][len:2 BE][bytes]`) so
they can be embedded in index keys without ambiguity.

- **Collection metadata** — `c\x00{cf}\x00{name}` stores collection config in the `_sys_` CF. Collections are scoped per column family: the pair `(cf, name)` is the unique identity.
- **Index config** — `x\x00{cf}\x00{collection}\x00{field}` stores index metadata.
- **Function config** — `{tag}\x00{cf}\x00{collection}\x00{name}` stores trigger/validator/UDF metadata.
- **Record** — `r\x00{collection}\x00{doc_id}` → encoded `Record` (BSON bytes + optional TTL). Lives in the actual CF, not `_sys_`.
- **Index** — `i\x00{collection}\x00{field}\x00{value_bytes}{doc_id}` → metadata (type byte + optional TTL). Lives in the actual CF. For a **compound** index `{field}` is the component names joined by `\x01` and `{value_bytes}` is the per-component values concatenated, with one type byte per component in the metadata (variable-width length suffixes trail the doc_id); a single-field index is the one-component case of the same layout.
- **Unique index** — `u\x00{collection}\x00{field}\x00{type_byte}{value_bytes}` → owning `doc_id`. Written only for unique indexes, *in addition to* the `i` entry: the doc_id is dropped from the key (a unique value has one owner) so the key doubles as a point-lookup enforcement slot. Lives in the actual CF. See [roadmap](roadmap.md) for the enforcement and slot-ownership design.

### Record Format

Records are stored as a version-tagged byte sequence:

- `[0x00][BSON...]` — no TTL
- `[0x01][8-byte LE i64 millis][BSON...]` — with TTL expiry timestamp

### TTL

TTL filtering is handled inside the engine. The transaction captures `now_millis` at
creation time. `get()`, `scan()`, and `scan_index()` skip expired records
transparently — callers never see expired data. A background sweep thread
(`purge_expired`) deletes expired records and their index entries periodically.

### Index Maintenance

On `put()`, the engine reads the old record (if any), computes an `IndexDiff` between
the old and new index entries, and applies only the changes (deletes for removed
values, inserts for new values). A fast path skips the diff entirely when the old and
new record bytes are identical.

On `delete()`, the engine reads the existing record, generates all its index entries
via `IndexDiff::for_delete`, and removes them.

### IndexEntry

`scan_index()` returns an iterator of `IndexEntry` values. Each entry holds raw key
and metadata bytes with pre-computed offsets for lazy decoding — `doc_id()` and
`value()` are only parsed to `RawBson` when the consumer calls them. In the common
path (non-covered queries), only `doc_id()` is called, avoiding unnecessary work.

### Catalog

The `Catalog` trait provides collection and index lifecycle: `create_collection`,
`drop_collection`, `create_index` (with backfill), `drop_index`, `collection`
handle resolution, and function management (`create_function`, `drop_function`,
`load_functions`). All methods take a `cf` parameter — collections are scoped per
column family, so the pair `(cf, name)` is the unique identity.

