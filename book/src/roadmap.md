# Roadmap

A status index. Each substantial design lives in its own [RFC](./SUMMARY.md#rfcs);
the entries here are a one-line description, a status, and a link to the detail.

## Query Engine

`find` and SQL run through one stack: the Mongo front-end (`slate-query`) and SQL
front-end (`slate-sql`) lower to one shared AST (`slate-ast`), planned by
`slate-planner`, executed by `slate-executor`, with expression evaluation in
`slate-eval` (over the fast `slate-rawbson` field scanner). Two query surfaces, one
planner/executor/evaluator, so they can't drift. The legacy in-crate v1 engine has
been **removed** — this is the only engine; a Mongo filter using an untranslated
operator (`$in`, `$ne`) is a hard error, not a silent fallback.

**Testing the engine.** Correctness is covered by per-crate unit tests plus the
external Cosmos parity harness (`tools/cosmos-parity`), which diffs against the real
emulator. Planned successor: a hermetic golden-replay suite (capture Cosmos results
offline, commit them, replay in-process) so the oracle runs under `cargo test`
without Docker.

## Database Hardening — proposed

Data-trust and operational foundations identified in a "proper embedded database"
survey — guarantees currently delegated wholesale to the backends, untested and
uninstrumented:

- **[Durability & Crash Safety](./rfcs/durability-and-crash-safety.md)** — *done.*
  A `Durability` knob (`Strict`/`Buffered`/`Relaxed`, builder default + per-txn
  override) with a documented per-backend commit guarantee, a kill-during-commit
  crash-test harness, and engine-level integrity `verify()`/`repair()`.
- **[On-Disk Format Versioning](./rfcs/on-disk-format-versioning.md)** — *v1 done.*
  The index-encoding version marker is generalised into a `_sys_` format registry
  (`index_encoding` + `catalog`); the catalog now carries a version with a
  refuse-too-new gate and the typed `UnsupportedFormatVersion` error, so a newer
  binary migrates an older store forward or refuses cleanly — never silently
  mis-reads. The record-blob version is design-reserved (seam in place, the tag
  byte stays the backstop) until a second record format needs it.
- **[Transaction & Concurrency Contract](./rfcs/transaction-concurrency-contract.md)**
  — *done (parts 1–3; savepoints deferred).* The cross-backend guarantee is pinned
  and written down in [Database → Concurrency and the Transaction Contract](./architecture-database.md#concurrency-and-the-transaction-contract):
  snapshot isolation at `begin` + read-your-writes + atomic commit, with a
  commit-time write-write conflict possible on **every** backend so a redb-tested
  app stays correct on RocksDB. Backed by a first-class `DbError::Conflict`
  (RocksDB's optimistic `Busy`/`TryAgain` mapped onto it at the store layer, with
  the seam left for redb/memory) and a `db.transact(|txn| …)` retry helper
  (bounded retries, `wasm32`-safe by default, optional injectable backoff via
  `RetryPolicy`). `delete_range` is named as the one explicitly non-transactional
  operation. **Deferred:** savepoints / nested transactions (their own RFC).
- **[Encryption at Rest](./rfcs/encryption-at-rest.md)** — *v1 done (documentation
  deliverable).* The layer is decided and the guarantee is written down:
  OS / device-level encryption (iOS Data Protection, FileVault/APFS, equivalents) is
  the supported at-rest story, documented with its honest threat model in
  [Architecture → Storage Layer → Encryption at Rest](./architecture-storage.md#encryption-at-rest).
  Slate ships zero crypto code. Value-only encryption is rejected (it leaks indexed
  values, which live in keys). **Deferred:** backend page-level cipher and the
  reserved `KeyProvider` seam — a future design, not a shipped API, to build only if
  a threat model demands more than OS trust.
- **[Observability & Introspection](./rfcs/observability-and-introspection.md)** —
  *A + B + C done; D + disk-size deferred.* Feature-gated `tracing` (the `trace`
  feature, off by default, zero-cost when off), EXPLAIN ANALYZE execution stats
  (the `.analyze(&txn)` terminal / `Executor::execute_analyze`, CLI
  `.explain analyze`), and a `stats()` size/cardinality surface
  (`Database::stats` / `Transaction::collection_stats`, CLI `.stats`). Remaining:
  the slow-query log (thread D) and `disk_size_bytes` backend plumbing (the slot
  is a reserved `Option`, `None` today).
- **[Resource Limits & Safety Valves](./rfcs/resource-limits-and-safety-valves.md)**
  — query deadline, materialization cap (the OOM guard), and document/key size
  limits, so the store can't take down its host.
- **[Logical Export / Import](./rfcs/logical-export-import.md)** — *done (BSON path).*
  Manifest-driven BSON dump+reload for cross-backend migration, seeding, and
  recovery (complements physical `backup()`); see
  [Storage & Durability](#storage--durability) below. JSONL interop export deferred.

## Indexing

- **[Index Key Value/Doc-Id Boundary](./rfcs/index-key-boundary.md)** — *done.*
  Deterministic variable-width string index keys (`u32` value-length suffix); a
  versioned re-index migration.
- **[Index Sargability](./rfcs/index-sargability.md)** — *decided; A/B/C shipped.*
  One `sargable()` recogniser; multikey containment, prefix range, and
  `STRINGEQUALS` → `Eq` all push down to index access.
- **[Unified Numeric Index Key](./rfcs/unified-numeric-index-key.md)** — *done.* All
  numbers project to one order-preserving `f64` key; numeric `Eq` −93% at 10k rows.
  Follow-ups: unique-index numeric collapse *decided* (matches Cosmos; impl pending);
  `into_index_value` micro-opt deferred.
- **[Compound Indexes](./rfcs/compound-indexes.md)** — *done (Phase 1).* Multi-field
  keys, leftmost-prefix rule, compound-unique. Programmatic-only:
  `indexes().create(paths, …)` (unique via `IndexOptions::unique()`; no SQL
  `CREATE INDEX` grammar). Identity = component field names joined by `\x01`; the key concatenates
  per-component sortable values with a trailing `u32` length suffix per string
  component, so single-field is the byte-identical N=1 case. The planner seeks the
  leftmost equality prefix as a conservative superset; the executor rechecks the
  leading equalities + trailing range exactly. Scalar components only — multikey
  (`[]`) components are rejected at creation. Remaining: multikey compound
  components (see [Multikey RFC](./rfcs/multikey-indexes.md)); a SQL `CREATE INDEX`
  surface is not planned.
- **[Covering Index Scans & Engine-Level Recheck](./rfcs/covering-index-scans.md)**
  — *Part A done; Part B phases 1–2 done; phase 3 deferred.* **Part A** (drop the
  redundant residual `Filter`): a planner-only change — the executor's
  `CompoundIndexScan` already rechecks each equality against the index entry, so the
  planner now *consumes* the compound-covered conjuncts instead of re-checking them
  against the fetched document (mirrors single-field `Eq`). Measured **−15–19 %** on
  a compound leading-equality query (`query_compound_eq`). **Part B** (covering scan
  — skip the `KeyLookup` when referenced fields ⊆ index components, synthesizing rows
  from the entry): **phase 1 shipped** — a single-field `IndexScan` gains a
  `covering` flag and a conservative, two-phase planner pass (decide read-only, then
  rebuild only if coverable) drops the fetch when the query reads only the indexed
  field and the pk. Measured **−15 % (1k) / −21 % (10k)** on a covered string
  projection, **−10 % / −27 %** on a covered numeric projection (`query_indexed_eq_proj`,
  `_numeric_proj`), with a covered-≡-materialized differential test. Also covers
  **dotted single-field paths** (`user.id`): an exact-path-match rule (a reference
  covers iff its reconstructed dotted path string-equals the index field or pk —
  parent/extension/sibling bail) plus nested synthesis (`{user: {id: value}}`),
  measured **−7 % (1k) / −15 % (10k)** (`query_indexed_eq_dotted_proj`). **Phase 2
  shipped** — a `CompoundIndexScan` gains a `covering: Option<Vec<String>>` marker
  (carrying the component paths, since its `field` is the opaque joined identity),
  and the same analysis covers a query reading only the index's components and the
  pk, synthesizing each row from the entry's per-component values (merging shared
  dotted prefixes); a `query_compound_covering` before/after harness is in place
  (headline number not yet captured). Phase 3 (covered aggregate) stays designed in
  the RFC, deferred.
- **[Multikey (Array) Indexes](./rfcs/multikey-indexes.md)** — *proposed.* Formalize
  `[]` fan-out; the open work is multikey-unique.
- **[Partial Indexes](./rfcs/partial-indexes.md)** — *proposed.* Index a subset of
  documents via a native filter predicate (a `Pure` function, same model as
  validators — the RFC was refreshed off the deleted Lua runtime).
- **[Spatial Index](./rfcs/spatial-index.md)** — *proposed (design spike).*
  Geohash/S2 candidate-cell scan + recheck for `ST_DISTANCE`/`ST_WITHIN`.
- **[Index Intersection Strategy](./rfcs/index-intersection-strategy.md)** —
  *phase 1 (stats-free skip-merge) shipped; cost-based selection (Door B)
  deferred.* An all-equality `AND` of ≥2 indexed parts now plans as a galloping
  `IndexIntersect` — a zig-zag skip-merge over the streams' shared doc-id order,
  bounded by the *smallest* input — instead of the asymmetric `IndexMerge(And)`
  that read the *larger* side fully and hashed it. Equality index streams are
  doc-id-sorted, so the merge seeks (a seekable `open_index_cursor` with
  galloping `seek`) rather than materialises; any range/prefix/compound part
  keeps the hash `IndexMerge(And)` fold. Invisible to results (asserted vs the
  hash path, a full scan, and the Cosmos golden replay). Cost-based index
  *selection* (a statistics catalog — the whale that fools uniform stats) stays
  an explicit non-goal.
- **[Vector Index & `VECTORDISTANCE`](./rfcs/vector-index.md)** — *function + flat
  index (Phase 1) done; quantization (Phase 2) + ANN (Phase 3) deferred.* The
  `VECTORDISTANCE` scalar (cosine/dotproduct/euclidean) plus a flat (exact,
  brute-force) vector index have shipped: `ORDER BY VECTORDISTANCE(c.field, @q)
  [DESC|ASC] LIMIT k` seeks a per-field index — a `doc_id → packed-f32` keyspace
  (TTL-header expiry-filtered) scanned into a bounded top-k heap, with a `WHERE`
  constraining the candidate set *before* the top-k (exact, no recall loss).
  Created via `indexes().create(path, VectorIndexOptions::float32(dims, metric))`
  (no SQL `CREATE INDEX` grammar yet); the planner only seeks when
  the call's metric matches the index and the `ORDER BY` direction is the metric's
  nearest-first sense, else falls back to a correct full scan. The math is one
  shared definition (`slate-eval::VectorMetric`) the scalar function and the index
  both call, result-validated against an independent MongoDB Atlas exact-kNN oracle
  (the Cosmos emulator can't validate vector search). On-device nearest-neighbour
  for RAG / semantic search. Quantization (float16/int8/binary widths) and ANN
  (IVF/HNSW) deferred to later phases.
- **Full-text indexes** — *proposed.* BM25 full-text (`FULLTEXTCONTAINS`,
  `FULLTEXTSCORE`, `RRF`); see the [SQL Query Surface RFC](./rfcs/sql-query-surface.md).

## Query & Execution

- **[SQL Query Surface](./rfcs/sql-query-surface.md)** — *partially implemented.*
  CosmosDB-style SQL; core `SELECT`/`WHERE`/`GROUP BY`/`HAVING`/`ORDER BY`/`JOIN … IN`,
  aggregates (incl. `ARRAY_AGG`/`COLLECT`), `DOCUMENTID`, and subqueries shipped.
  Vector search shipped (flat index — see Indexing); full-text remains (needs its index).
- **[Collect Node](./rfcs/collect-node.md)** — *proposed.* Make plan materialization
  points explicit; asymmetric `IndexMerge(And)`.
- **Plan Inspection (EXPLAIN / EXPLAIN ANALYZE)** — *done.* `Plan::explain()` /
  the `.explain(&txn)` terminal render the logical operator tree; REPL `.explain`. Runtime
  stats now ship too: the `.analyze(&txn)` terminal (REPL `.explain analyze`)
  renders the same tree annotated with per-node `rows=`/`examined=` counts, via the
  [Observability RFC](./rfcs/observability-and-introspection.md). No SQL `EXPLAIN`
  keyword (kept out of the grammar) — plan inspection stays a shell/library affair.
- **[Raw BSON Robustness](./rfcs/rawbson-robustness.md)** — *done.* A malformed-input
  contract (typed errors at the public boundary; a total, bounds-checked scanner) and
  a differential fuzz against the `bson` crate for the byte scanner.
- **Dynamic Primary Key Path** — *done.* All executor nodes use
  `CollectionHandle::pk_path()`; `pk_path` must be a top-level scalar (dot-paths
  rejected at creation). The TTL path may be a dot-path (read-only).
- **[Decimal128 in Query Evaluation](./rfcs/decimal128-evaluation.md)** — *done.*
  `Decimal128` joins the `f64` number tower for eval/sort/aggregates; stored bytes
  untouched.

## Constraints & Logic

- **[Unique Indexes](./rfcs/unique-indexes.md)** — *done.* Single-field scalar
  uniqueness via a dual `i`/`u` key format; sparse; scalar-only.
- **[User-Defined Logic](./rfcs/user-defined-logic.md)** — *partially implemented.*
  Triggers, validators, and UDFs are all native now (see Native Functions);
  computed fields, custom key extractors, partial-index filters, and transform
  pipelines remain.
- **[Native Functions](./rfcs/native-functions.md)** — *UDFs, validators, and
  triggers done; `slate-vm` removed.* Native Rust hooks behind one model — a
  database-scoped code bag (`register` / `with_udf` / `with_validator` /
  `with_trigger`) plus per-collection durable bindings (`functions().create` /
  `validators().create` / `triggers().create`), resolved at plan build (UDFs) or
  once per query (validators, triggers) and run under `catch_unwind`. The binding
  maps a name to a native function; the bag supplies the code.
  `dangling_bindings()` reports unresolved symbols (tagged `Udf` / `Validator` /
  `Trigger`). **Validators** are `dyn Validator` (`Pure`, a write-path gate
  returning a `Verdict`); **triggers** are `dyn Trigger` (`ReadWrite`, a
  column-family-confined `get`/`put`/`delete` context) — a dangling validator or
  trigger aborts all writes to its collection, fail-safe. With triggers native,
  `slate-vm` (the Lua/JS VM) is deleted: the core links no scripting runtime, so
  the whole stack compiles to `wasm32`. UDFs run in compiled positions
  (`SELECT`/`WHERE`); `ORDER BY`/`UNWIND`/`GROUP BY` (which interpret) are a
  follow-up.

## Public API

- **[Public API Ergonomics — the `Collection` handle](./rfcs/db-api-cleanup.md)** —
  *done; shipped and canonical.* The collection-scoped surface is now a `Collection`
  handle off `db.collection(name)` / `txn.collection(name)` (with a `db.cf(…)` sub-scope)
  on a **build-lazily / run-at-a-terminal** model: `find(f)` is Mongo-chainable, `query(sql)`
  is SQL, reads consume with `.iter_raw`/`.iter::<T>`, writes/commands finish with
  `.execute(&txn)` → `WriteResult`. Sub-handles group the schema surface — `indexes()`
  (one `create(paths, opts)` folding all five `create_*_index` via the sealed `IndexBuild`
  trait), per-kind `triggers()`/`validators()`/`functions()`, and `collections()`. The
  ~40-method `Transaction` god-object is gone: the flat CRUD methods were **deleted** (v2 is
  the one real implementation; consumers, benches, tests, and the uniffi/wasm bindings call
  it directly), leaving only lifecycle + admin on `Transaction`/`Database`. **Deferred:** the
  `begin_read()`/`begin_write()` distinct-types split (still a runtime check) and a db-wide
  purge sweep.

## Storage & Durability

- **Hot Backup** — *done.* Online `Database::backup(path)` behind `BackupStore`
  (RocksDB checkpoint, redb file copy; MemoryStore errors). Restore is offline.
  This is the *physical* same-backend snapshot; the *logical* dump/reload is
  Logical Export / Import below.
- **[Logical Export / Import](./rfcs/logical-export-import.md)** — *done (BSON path).*
  `Database::export` / `import` and CLI `.export` / `.import` write a portable
  dump directory: a versioned `manifest.bson` (collection defs — `pk_path`,
  `ttl_path`, indexes incl. the unique subset) plus one `<cf>.<collection>.bson`
  document-stream per collection. Index *entries* are never dumped — import
  rebuilds them from the records (the "records are source of truth" contract), so
  the *target* backend gets correctly-encoded indexes. That makes it the
  cross-backend migration path (redb→RocksDB round-trip test). Whole-DB and
  per-collection scope; collision modes `Error`/`Overwrite`/`Skip`; BSON canonical
  is lossless (ObjectId/DateTime/Decimal128 preserved). JSONL interop export is
  deferred — `.seed` already ingests JSONL.
- **[MemoryStore Persistence](./rfcs/memorystore-persistence.md)** — *proposed.*
  Write-behind flush of MemoryStore to a durable backend (disk/IndexedDB/S3) without
  making the `Store` trait async.

(Durability guarantees and the cross-backend concurrency contract are tracked under
[Database Hardening](#database-hardening--proposed) above.)

## Change Feeds

- **[Change Detection (Watch Queries)](./rfcs/watch-queries.md)** — *partially implemented.*
  A 2×2 API over one detection core: a **BSON filter** (`db.watch` / `db.stream`) or a
  **SQL `WHERE` filter** (`db.watch_query` / `db.stream_query`), delivered by **callback**
  (push) or a long-lived **subscription cursor** (pull, `WatchStream` with non-blocking
  lag-drop). Matching inserts/updates/deletes are coalesced per pk and delivered once per
  commit, recast against the filter's set boundary (enter → `Insert`, leave → `Delete`,
  stay → `Update`). The reactive result-set fold (and lossless/durable delivery) are
  still to come.

## Bindings & Tooling

- **[WebAssembly Support](./rfcs/webassembly-support.md)** — *partially implemented.*
  The full stack compiles to `wasm32`; MemoryStore + the JS scripting bridge work.
  Platform adapters, browser storage (OPFS/IndexedDB), and `getrandom` entropy remain.
- **Interactive Shell (CLI)** — *done.* `slate-cli` REPL: meta-commands + SQL,
  `.seed` bulk-load, online `.backup`, logical `.export` / `.import`,
  `.explain` / `.explain analyze`, `.stats`, multi-line statements,
  tab-completion, persistent history. Not yet: hook management, a `.plan`/`.ast`
  inspector, multi-statement transactions, and Mongo-style `.find`.
- **Browser Playground** — *shipped.* A client-side `slate-wasm` single-page app
  (create/insert/query/hooks/plans, no backend). See the live
  [Playground](./playground.md).

## Test Coverage

Backlog of targeted hardening tests: **error paths** (malformed queries, missing
collections, invalid filter operators); **encoding edge cases** (negative ints in
index keys, empty strings, special characters in record IDs, very long values); and
**`ClientPool`** connection pooling (checkout/return, behavior under contention,
dropped connections).
