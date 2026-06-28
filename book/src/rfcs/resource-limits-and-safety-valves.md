# RFC: Resource Limits & Safety Valves

> **Status: A + B done; C + D deferred.** Surfaced in the "proper embedded
> database" survey. The two valves that keep a runaway query from taking down the
> host process have shipped: **A** a cooperative per-query **deadline**
> (`DbError::Timeout`, checked between rows in every source node off the injectable
> clock — wasm-safe) and **B** a **materialization cap** (`DbError::LimitExceeded`)
> on every blocking buffer (`Sort` / `IndexMerge` / `Distinct` / `GroupBy` + the
> vector kNN pre-filter set, checked as it grows). Both are a `DatabaseBuilder`
> default (`with_limits` / `with_deadline` / `with_materialization_cap`) with an
> optional per-query override (`.deadline(..)` / `.materialization_cap(..)` on
> `find` / `query` / `distinct`); see
> [Database → Resource Limits and Safety Valves](../architecture-database.md#resource-limits-and-safety-valves).
> **Deferred:** **C** input-size limits (max document / key / value →
> `DbError::TooLarge`, a write-path change) and **D** a rows-*examined* cap (best
> landed with the [Observability RFC](./observability-and-introspection.md)'s
> shared counter). The original framing follows.

## Problem

Embedding means sharing fate. A server database that mismanages memory degrades
*itself*; an embedded database that does so degrades the *app*. Today nothing
bounds:

- **Time** — a query that plans into a full scan over a huge collection runs to
  completion with no deadline.
- **Memory** — `Sort`, `IndexMerge`, `Distinct`, and `GroupBy` all materialize
  their input into `Vec`s (the roadmap's `Collect` section names these exact
  nodes as the internal-materialization points). Nothing caps how large that
  grows; a `SORT` over a giant collection allocates without limit.
- **Input size** — there is no maximum document, key, or value size enforced on
  the write path, so one absurd document is accepted and then has to be scanned
  by every query that touches the collection.
- **Transaction size** — no bound on how much one transaction may write.

## Current state

- **`DatabaseBuilder` has no limit knobs.** Its options
  (`crates/slate-db/src/database.rs:54-167`) are `with_scripting`, `with_clock`,
  `with_rand`, `with_sweep` — capabilities and injection, not governance.
- **`DbError` has no resource-exhaustion variant**
  (`crates/slate-db/src/error.rs:6-23`) — there is no `Timeout`, no `TooLarge`,
  no `LimitExceeded` to return even if a limit existed.
- **Cursors stream, but callers can drain unboundedly**, and the materializing
  nodes above collect eagerly with no ceiling.
- **One precedent already exists:** the Lua VM enforces an instruction limit on
  script execution — the codebase already accepts "bound the work, fail loudly
  when exceeded" as a pattern. This RFC generalizes that posture to the query
  engine and the write path.
- **An internal key-size constraint exists** (~2 KiB on some index paths per the
  survey) but is not publicly documented or configurable.

## Design

Limits are configured as a `DatabaseBuilder` default with optional per-query
override (on `FindOptions` / the SQL entry points), and each is enforced
cooperatively at an existing loop boundary — Slate's API is synchronous, so a
"timeout" is a *deadline checked between rows*, not a preemptive interrupt. Each
violation gets a distinct `DbError` variant so callers can distinguish "too slow"
from "too big" from "I/O error."

### A — query deadline (cooperative timeout)

A per-query `Duration`; the executor checks an elapsed-time budget at each node's
`next()` (and at scan-iteration granularity for long scans) and returns
`DbError::Timeout` when exceeded. Cooperative checking fits the streaming
executor with negligible overhead (a clock read every N rows). Reuses the
injectable clock already on the builder (`with_clock`), so it's wasm-safe.

### B — materialization cap (the real OOM guard)

A ceiling on bytes (or rows) buffered by a materializing node — `Sort`,
`IndexMerge`, `Distinct`, `GroupBy`. Exceeding it returns
`DbError::LimitExceeded` (or, later, triggers spill-to-disk — the roadmap's
`Collect` node is explicitly named as the future spill boundary, so this cap is
the honest first cut that a spill implementation later relaxes). This is the
single highest-value valve: it closes the unbounded-memory paths the `Collect`
discussion already identified.

### C — input size limits

Configurable maximum document size, key size, and value size, enforced on the
write path, returning `DbError::TooLarge`. Promotes the existing implicit
~2 KiB key constraint to a documented, configurable limit and adds a document
ceiling so one bad insert can't poison every later scan.

### D — rows-examined cap

A bound on rows *examined* (not just returned) per query — the guard against an
accidental full scan over a huge collection. Shares the per-node counter the
[Observability RFC](./observability-and-introspection.md) introduces for
EXPLAIN ANALYZE, so B/D and that RFC's Thread B are the same plumbing viewed two
ways (one reports the counter, one trips on it).

## Recommendation

Ship **A + B first** — the deadline and the materialization cap are the two
actual ways an embedded query takes down its host, and B directly bounds the
nodes the `Collect` roadmap item already flagged. **C** (input limits) is a small,
independent write-path change worth doing next. **D** is best landed *with* the
Observability RFC's counter work, since they share the instrument. New `DbError`
variants (`Timeout`, `LimitExceeded`, `TooLarge`) land alongside A–C.

**Spike:** confirm the cooperative deadline check is noise on the scan hot path
(clock read every N rows, tuned so it doesn't show up on the engine bench), and
prototype the materialization accounting so the byte counter itself isn't a
measurable tax. Mirror the Lua instruction-limit precedent for the failure shape.

## Non-goals

- **No per-tenant quota system, no admission control, no scheduler.** These are
  single-process safety valves, not a multi-tenant resource manager.
- **No preemptive cancellation.** The sync API means cooperative deadlines only;
  a wholly off-CPU call (a blocking backend I/O) won't be interrupted mid-syscall.
- **No spill-to-disk in v1.** The materialization cap *fails* when exceeded;
  spill is a future relaxation gated on the `Collect` node.
- Cosmos has request-unit (RU) budgets, but that's a hosted-billing construct,
  not a guarantee the oracle validates — irrelevant to local limits.
