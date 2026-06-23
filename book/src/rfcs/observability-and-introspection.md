# RFC: Observability & Introspection

> **Status: proposed.** Surfaced in the "proper embedded database" survey. Slate
> has zero runtime visibility: no tracing, no metrics, no execution statistics,
> no size/cardinality introspection. `EXPLAIN` prints the *logical* plan but
> never says what actually happened. This hurts users debugging a slow query and
> hurts our own perf work — the [bench](../benchmarks.md) loop is flying blind on
> rows-scanned and index-hit-rate.

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

### A — `tracing` spans, feature-gated and zero-cost when off

Adopt the `tracing` crate behind an off-by-default feature (`trace`). Instrument
the seams: plan lowering, each executor node's open/next, store commit,
index maintenance. When the feature is off it compiles to nothing (wasm-safe,
no dependency in the default build); when on, the host wires its own subscriber.
This is the standard Rust-ecosystem answer and composes with everything else.

### B — execution statistics (EXPLAIN ANALYZE)

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

### C — catalog & storage statistics

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

### D — slow-query log

A thin layer on A+B: an optional threshold (`with_slow_query_log(Duration)`) that
emits a `tracing` event (with the EXPLAIN-ANALYZE annotation) when a query
exceeds it. Almost free once A and B exist.

## Recommendation

Sequence **A + B first** (they share the per-node counter plumbing and deliver the
debugging + bench payoff), **C second** (independent, mostly backend-property
plumbing), **D last** (a trivial composition of A+B). No heavy spike needed; a
small one to confirm the per-node counters don't measurably dent the hot path
when the `trace` feature is *off* (they must be zero-cost in the default build)
and are cheap when on.

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
