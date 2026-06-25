# RFC: Observability & Introspection

> **Status: A + B + C shipped; D + on-disk size deferred.** Surfaced in the
> "proper embedded database" survey. Slate had zero runtime visibility: no
> tracing, no metrics, no execution statistics, no size/cardinality
> introspection. `EXPLAIN` printed the *logical* plan but never said what actually
> happened. This hurt users debugging a slow query and hurt our own perf work —
> the [bench](../benchmarks.md) loop was flying blind on rows-scanned and
> index-hit-rate.
>
> **Shipped:** thread A (feature-gated `tracing`, off by default), thread B
> (EXPLAIN ANALYZE — `Transaction::explain_analyze` / `Executor::execute_analyze`,
> CLI `.explain analyze`), and thread C (`Database::stats()` /
> `Transaction::collection_stats()`, CLI `.stats`). **Remaining:** thread D
> (slow-query log — a trivial composition of A + B, sequenced last) and the
> `disk_size_bytes` per-backend plumbing (the slot is reserved as `Option`, `None`
> today). The per-thread notes below are kept as the historical design record.

## Problem

You can ask Slate what plan it *would* run, but never what a run *did*: how many
rows it scanned versus returned, whether an index was used, where the time went.
You also can't ask the database about itself — its on-disk size, a collection's
document count, an index's cardinality. For an embedded DB this matters twice
over: the host application has no operational window into the store it embeds,
and we have no instrument to point at our own hot paths.

## Current state

- **No instrumentation anywhere.** Grepping the workspace for `tracing`,
  `metrics`, `log::`, `stats`, `profil` returns nothing. There is no logging
  framework wired in.
- **`EXPLAIN` is logical only.** `Plan::explain()` (`slate-planner`) renders the
  operator tree the planner settled on; `Transaction::explain`
  (`crates/slate-db/src/database.rs:365`) and the CLI `.explain` expose it. It
  shows *shape* — index choice, bounds, sort keys — with no row counts, no cost,
  no timing. (The roadmap's `Collect` node would make materialization points
  visible in this same tree — an observability win already noted there.)
- **The CLI times commands** end-to-end and prints execution time per result,
  but that lives in the REPL session layer, not the engine — there are no
  counters underneath it.
- **No size/stat API.** Nothing reports DB size, per-collection doc counts, index
  entry counts, or cardinality at runtime.
- **Precedent exists for a measured limit:** the Lua VM enforces instruction
  limits, so the codebase already tracks *something* per execution — just not
  surfaced.

## Design

Four threads, roughly independent, ordered by leverage.

### A — `tracing` spans, feature-gated and zero-cost when off — *shipped*

Adopt the `tracing` crate behind an off-by-default feature (`trace`). Instrument
the seams: plan lowering, each executor node's open/next, store commit,
index maintenance. When the feature is off it compiles to nothing (wasm-safe,
no dependency in the default build); when on, the host wires its own subscriber.
This is the standard Rust-ecosystem answer and composes with everything else.

**As shipped:** the `trace` feature lives on both `slate-executor` and `slate-db`
(enabling it on `slate-db` cascades to the executor via
`trace = [..., "slate-executor/trace"]`, so one flag turns on the whole query
stack). With it off, `tracing` is not even a dependency: the `trace_span!`/
`trace_event!` call sites — in `slate-executor/src/trace.rs` and
`slate-db/src/trace.rs` — expand to nothing, so the default build carries zero
instrumentation cost and stays wasm-safe. The executor emits a span/event around
`execute` (keyed by plan kind) and the database emits an event on commit. The
host wires its own subscriber; slate bundles no exporter (see Non-goals).

### B — execution statistics (EXPLAIN ANALYZE) — *shipped*

The highest-value thread for both audiences. Collect per-node counters during
execution — rows examined, rows emitted, index-scan vs full-scan, bytes
materialized — and attach them to the result. Two surfaces:

- `Transaction::explain_analyze(cf, collection, sql) -> String` — runs the query
  and renders the *same* tree `explain` prints, annotated with actuals
  (`IndexScan(status) rows=120 examined=120`, `Filter ... rows=8 examined=120`).
- The counters are also available structurally on the cursor for programmatic
  use (the slow-query log in D and the limits in the
  [Resource Limits RFC](./resource-limits-and-safety-valves.md) both consume the
  same "rows examined" counter).

This pairs directly with the planner's existing EXPLAIN and is the instrument the
bench skill actually wants.

**As shipped:** `Transaction::explain_analyze(cf, collection, sql) -> String`
lowers exactly as `query` (same parse/planner/index choice), executes read-only,
collects the rows and drops them, and renders the annotated tree. Programmatic
access is `Executor::execute_analyze(plan) -> (Vec<RawBson>, Rc<PlanStats>)`. The
per-node counters live in `slate_planner::PlanStats`, keyed by pre-order node
index; the renderer (`Plan::explain_analyze(&stats)`) and the executor's analyze
walk advance that index in lock-step. Each node line gains `rows=N` (rows it
emitted) and, for non-source nodes, `examined=M` (its child's emitted count, i.e.
what flowed in) — e.g.

```
Project c.name rows=8 examined=8
  Filter c.age > 21 rows=8 examined=120
    Scan default.users rows=120
```

The instrumentation is on *only* during `execute_analyze`: the normal `execute`
path never builds the counting wrappers, so ordinary queries pay zero per-row
cost. Like `query`, the analyze surface binds no `@parameters` (the plan shape
never depends on a parameter value), so a parameterized query is rejected. The
"bytes materialized" counter named in the original sketch is not yet collected —
the shipped counters are per-node row in/out.

### C — catalog & storage statistics — *shipped (disk size deferred)*

A `stats()` surface answering "how big is this?":

- **Per collection** — document count, index entry counts, approximate
  cardinality per index.
- **Per database** — approximate on-disk size.

Backends differ and some numbers are estimates, which the API must be honest
about: RocksDB exposes SST size / `estimate-num-keys` properties, redb has file
size + table stats, MemoryStore can count exactly. Document counts can be exact
(scan) or approximate (maintained counter); a maintained counter is cheap on the
write path and worth it — surface it as approximate to leave room for lazy
correction. Expose via the public API and a CLI `.stats` command.

**As shipped:** `Transaction::collection_stats(cf, collection) -> CollectionStats`
and `Transaction::stats() -> DatabaseStats`, each mirrored by a
`Database::{collection_stats, stats}` convenience that opens and rolls back its
own read snapshot. `CollectionStats` carries the live `document_count` plus a
`Vec<IndexStats>` (per index: `entry_count` and distinct-value `cardinality`);
`DatabaseStats` rolls these up with `total_documents` and an optional
`disk_size_bytes`. All counts are computed by *scanning* (distinct values deduped
by canonical BSON bytes), so they are **exact** as of the snapshot — no
write-path counter to maintain, no drift — at an O(rows + index entries) cost
that makes this an introspection call, not a hot path. The `approximate` flag is
wired (always `false` today) so a future maintained-counter fast path can flip it
without an API change. CLI: `.stats` (whole database) / `.stats <collection>`.

The maintained-counter design above was *not* taken (the scan keeps the numbers
exact and the write path untouched); it remains the documented option behind the
`approximate` flag. **`disk_size_bytes` is `None` today** — on-disk size is a
backend property (RocksDB SST size, redb file size) that needs per-backend
plumbing through the store trait; the slot is reserved as an `Option` so adding it
later is non-breaking.

### D — slow-query log — *deferred*

A thin layer on A+B: an optional threshold (`with_slow_query_log(Duration)`) that
emits a `tracing` event (with the EXPLAIN-ANALYZE annotation) when a query
exceeds it. Almost free once A and B exist.

**Not yet implemented.** A and B (which it composes) have shipped, so this is now
unblocked; it is sequenced last and remains the open thread of this RFC.

## Recommendation

Sequence **A + B first** (they share the per-node counter plumbing and deliver the
debugging + bench payoff), **C second** (independent, mostly backend-property
plumbing), **D last** (a trivial composition of A+B). No heavy spike needed; a
small one to confirm the per-node counters don't measurably dent the hot path
when the `trace` feature is *off* (they must be zero-cost in the default build)
and are cheap when on.

**Outcome:** A + B + C landed in that order. The zero-cost requirement held — the
analyze counters are built only on the `execute_analyze` path, so the normal
`execute` is unchanged, and the `trace` feature drops `tracing` from the default
build entirely. D and the `disk_size_bytes` plumbing remain (see status banner).

## Non-goals

- **No bundled exporter.** No Prometheus endpoint, no OpenTelemetry pipeline
  baked into `slate-db`. We emit `tracing` spans/events and structured stats;
  the host wires its own subscriber/exporter. Keeping exporters out preserves the
  dependency-light, wasm-safe posture.
- **No cost-based optimizer.** Statistics here are for *observation*, not for
  feeding a cost model — the planner stays rule-based (sargability), as decided
  in the [Index Sargability RFC](./index-sargability.md). Cardinality stats could
  one day inform planning, but that's a separate RFC.
- **No always-on overhead.** Tracing is feature-gated; the default build carries
  no instrumentation cost.
- Cosmos is irrelevant here — query metrics aren't a result the oracle can
  validate (the emulator reports no index-utilization metrics anyway, as the
  sargability work already found).
