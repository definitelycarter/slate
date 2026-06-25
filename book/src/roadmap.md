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
- **[On-Disk Format Versioning](./rfcs/on-disk-format-versioning.md)** — generalise
  the index-encoding version+migration precedent to the record blob and catalog, so
  a newer binary migrates an older store forward or refuses cleanly — never silently
  mis-reads.
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
  feature-gated `tracing`, EXPLAIN ANALYZE execution stats, and a `stats()`
  size/cardinality surface.
- **[Resource Limits & Safety Valves](./rfcs/resource-limits-and-safety-valves.md)**
  — query deadline, materialization cap (the OOM guard), and document/key size
  limits, so the store can't take down its host.
- **[Logical Export / Import](./rfcs/logical-export-import.md)** — manifest-driven
  BSON/JSONL dump+reload for cross-backend migration, seeding, and recovery
  (complements physical `backup()`).

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
- **[Compound Indexes](./rfcs/compound-indexes.md)** — *proposed.* Multi-field keys,
  leftmost-prefix rule, compound-unique.
- **[Multikey (Array) Indexes](./rfcs/multikey-indexes.md)** — *proposed.* Formalize
  `[]` fan-out; the open work is multikey-unique.
- **[Partial Indexes](./rfcs/partial-indexes.md)** — *proposed.* Index a subset of
  documents via a Lua filter predicate.
- **[Spatial Index](./rfcs/spatial-index.md)** — *proposed (design spike).*
  Geohash/S2 candidate-cell scan + recheck for `ST_DISTANCE`/`ST_WITHIN`.
- **[Index Intersection Strategy](./rfcs/index-intersection-strategy.md)** —
  *proposed.* How `IndexMerge` chooses and combines indexes.
- **Full-text & vector indexes** — *proposed.* BM25 full-text and `VECTORDISTANCE`;
  see the [SQL Query Surface RFC](./rfcs/sql-query-surface.md).

## Query & Execution

- **[SQL Query Surface](./rfcs/sql-query-surface.md)** — *partially implemented.*
  CosmosDB-style SQL; core `SELECT`/`WHERE`/`GROUP BY`/`HAVING`/`ORDER BY`/`JOIN … IN`,
  aggregates (incl. `ARRAY_AGG`/`COLLECT`), `DOCUMENTID`, and subqueries shipped.
  Full-text and vector remain (each needs its index).
- **[Collect Node](./rfcs/collect-node.md)** — *proposed.* Make plan materialization
  points explicit; asymmetric `IndexMerge(And)`.
- **Plan Inspection (EXPLAIN)** — *done.* `Plan::explain()` / `Transaction::explain`
  render the logical operator tree; REPL `.explain`. No SQL `EXPLAIN` keyword (kept
  out of the grammar). Runtime stats are the
  [Observability RFC](./rfcs/observability-and-introspection.md).
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
  Logical dump/reload is the
  [Logical Export / Import RFC](./rfcs/logical-export-import.md).
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
  `.seed` bulk-load, online `.backup`, multi-line statements, tab-completion,
  persistent history. Not yet: hook management, a `.plan`/`.ast` inspector,
  multi-statement transactions, and Mongo-style `.find`.
- **Browser Playground** — *shipped.* A client-side `slate-wasm` single-page app
  (create/insert/query/hooks/plans, no backend). See the live
  [Playground](./playground.md).

## Test Coverage

Backlog of targeted hardening tests: **error paths** (malformed queries, missing
collections, invalid filter operators); **encoding edge cases** (negative ints in
index keys, empty strings, special characters in record IDs, very long values); and
**`ClientPool`** connection pooling (checkout/return, behavior under contention,
dropped connections).
