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
  — pin the isolation guarantee across backends, a first-class `DbError::Conflict`
  + a `transact()` retry helper, and the `delete_range` exception.
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
  (`Transaction::explain_analyze` / `Executor::execute_analyze`, CLI
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
  `create_compound_index` / `create_unique_compound_index` (no SQL `CREATE INDEX`
  grammar). Identity = component field names joined by `\x01`; the key concatenates
  per-component sortable values with a trailing `u32` length suffix per string
  component, so single-field is the byte-identical N=1 case. The planner seeks the
  leftmost equality prefix as a conservative superset; the executor rechecks the
  leading equalities + trailing range exactly. Scalar components only — multikey
  (`[]`) components are rejected at creation. Remaining: multikey compound
  components (see [Multikey RFC](./rfcs/multikey-indexes.md)); a SQL `CREATE INDEX`
  surface is not planned.
- **[Covering Index Scans & Engine-Level Recheck](./rfcs/covering-index-scans.md)**
  — *Part A done; Part B phase 1 done; phases 2–3 deferred.* **Part A** (drop the
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
  `_numeric_proj`), with a covered-≡-materialized differential test. Phases 2
  (compound) and 3 (covered aggregate) stay designed in the RFC, deferred.
- **[Multikey (Array) Indexes](./rfcs/multikey-indexes.md)** — *proposed.* Formalize
  `[]` fan-out; the open work is multikey-unique.
- **[Partial Indexes](./rfcs/partial-indexes.md)** — *proposed.* Index a subset of
  documents via a Lua filter predicate.
- **[Spatial Index](./rfcs/spatial-index.md)** — *proposed (design spike).*
  Geohash/S2 candidate-cell scan + recheck for `ST_DISTANCE`/`ST_WITHIN`.
- **[Index Intersection Strategy](./rfcs/index-intersection-strategy.md)** —
  *proposed.* How `IndexMerge` chooses and combines indexes.
- **[Vector Index & `VECTORDISTANCE`](./rfcs/vector-index.md)** — *proposed (design
  spike).* On-device nearest-neighbour search for RAG / semantic search. Flat
  (brute-force) `scan_range` + top-k first — exact, KV-native, filter-friendly,
  Cosmos `VECTORDISTANCE` parity; quantization and ANN deferred to later phases.
- **Full-text indexes** — *proposed.* BM25 full-text (`FULLTEXTCONTAINS`,
  `FULLTEXTSCORE`, `RRF`); see the [SQL Query Surface RFC](./rfcs/sql-query-surface.md).

## Query & Execution

- **[SQL Query Surface](./rfcs/sql-query-surface.md)** — *partially implemented.*
  CosmosDB-style SQL; core `SELECT`/`WHERE`/`GROUP BY`/`HAVING`/`ORDER BY`/`JOIN … IN`,
  aggregates (incl. `ARRAY_AGG`/`COLLECT`), `DOCUMENTID`, and subqueries shipped.
  Full-text and vector remain (each needs its index).
- **[Collect Node](./rfcs/collect-node.md)** — *proposed.* Make plan materialization
  points explicit; asymmetric `IndexMerge(And)`.
- **Plan Inspection (EXPLAIN / EXPLAIN ANALYZE)** — *done.* `Plan::explain()` /
  `Transaction::explain` render the logical operator tree; REPL `.explain`. Runtime
  stats now ship too: `Transaction::explain_analyze` (REPL `.explain analyze`)
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
  Lua/JS triggers + validators shipped; computed fields, custom key extractors,
  partial-index filters, and transform pipelines remain.

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

- **[Change Detection (Watch Queries)](./rfcs/watch-queries.md)** — *proposed.*
  Register a filter; matching inserts/updates/deletes fire a callback at commit.

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
