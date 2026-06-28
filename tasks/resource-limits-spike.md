# Spike: Resource Limits & Safety Valves (A + B)

RFC: `book/src/rfcs/resource-limits-and-safety-valves.md`. This spike de-risks the
two recommended valves before building them out:

- **A — query deadline** (cooperative timeout): a clock read every *N* rows.
- **B — materialization cap**: a row counter on the buffering (`Sort`/`IndexMerge`/
  `Distinct`/`Aggregate`) nodes.

**Gate:** both checks must be ~free on the scan hot path. **Result: PASSED**, with
one load-bearing design constraint (below).

## Method

A throwaway Criterion target (`crates/slate-executor/benches/spike_budget.rs`,
since removed) drove the **real** seeded `Scan` (`bench-internals` fixture, the
same `people` corpus the `nodes` bench uses) and timed open-iterator-plus-drain:

- **A**: `scan_baseline` vs two ways to add the check —
  - `scan_deadline_wrapper`: the guard as a *separate* `.map` adapter wrapping the
    scan iterator (a second `Box<dyn Iterator>` layer).
  - `scan_deadline_inline`: the check folded into the scan's *own* existing `.map`
    closure (one box layer) — what production will do.
  - The guard reads a real `SystemTime` clock every `INTERVAL = 1024` rows and
    compares against a deadline one hour out (`black_box`ed so the comparison
    isn't constant-folded and the clock read can't be elided). It never trips, so
    we time the always-present per-row cost, not the abort path.
- **B**: `buffer_plain` (buffer every scanned row into a `Vec`) vs `buffer_capped`
  (same loop + `n += 1; if n > cap { return Err }`, `cap` large enough never to
  trip, `black_box`ed).

Run: `--warm-up-time 1 --measurement-time 3`, native (Apple Silicon), nothing else
running. Per-row scan cost ≈ 55 ns/row (553 µs / 10 000).

## Numbers

| case (10k rows)                  | mean    | vs baseline     |
|----------------------------------|---------|-----------------|
| `scan_baseline`                  | 553 µs  | —               |
| `scan_deadline_wrapper`          | 598 µs  | **+8.1%** ❌    |
| `scan_deadline_inline`           | 518 µs  | within noise ✓  |
| `buffer_plain`                   | 541 µs  | —               |
| `buffer_capped`                  | 543 µs  | +0.4% (noise) ✓ |

| case (1k rows)                   | mean    | vs baseline     |
|----------------------------------|---------|-----------------|
| `scan_baseline`                  | 56.7 µs | —               |
| `scan_deadline_wrapper`          | 62.1 µs | **+9.5%** ❌    |
| `scan_deadline_inline`           | 53.9 µs | within noise ✓  |
| `buffer_plain`                   | 56.1 µs | —               |
| `buffer_capped`                  | 55.4 µs | noise ✓         |

(`inline`/`capped` land slightly *under* baseline — run-to-run variance is ~3–6%;
the point is they're statistically indistinguishable from baseline.)

## Findings

1. **The cap (B) is free.** A `usize` counter increment + one `> cap` compare per
   buffered row, integrated into the existing buffering loop, does not register
   (+0.4% at 10k, faster at 1k). Ship it as a plain counter in each blocking node.

2. **The deadline (A) is free *only if folded in*.** The check arithmetic itself
   (`n & 1023 == 0` mask + a predictable branch; clock read amortized ~1/1024
   rows ≈ 0.03 ns/row) is noise. The +8–10% in `scan_deadline_wrapper` is **not**
   the check — it's the extra `Box<dyn Iterator>` vtable hop from wrapping the
   source in a second adapter iterator. `scan_deadline_inline`, with the identical
   check folded into the scan's own `.map` closure, is at baseline.

   → **Load-bearing constraint:** the deadline check must ride each source node's
   *existing* per-row closure/loop, never a separate adapter `ValueIter`. All four
   source nodes have a foldable site: `scan` (`iter.map`), `index_scan` /
   `compound_index_scan` / `index_intersect` (`std::iter::from_fn` loops — fold per
   *examined* entry, so a heavily-post-filtered range scan still checks).

3. **`INTERVAL = 1024`** (power of two → a mask, not a modulo). At ~55 ns/row that
   is a clock read roughly every ~56 µs of scan work — tight enough for a
   cooperative deadline, rare enough to vanish. Tunable later; nothing downstream
   depends on the exact value.

4. **Counter starts at 0 so the 0th pulled row is also a check** (`0 & mask == 0`).
   An already-expired deadline trips on the first row, so the Timeout tests need
   only a tiny fixture, not one larger than `INTERVAL`.

## Implementation shape (for slices 3–4)

- **ExecEnv** carries `deadline: Option<Rc<Deadline>>` and
  `materialization_cap: Option<usize>` (both `None` = absent = zero cost). The
  `Deadline` bundles a live clock reader (`Rc<dyn Fn() -> i64>`, mirroring the
  `rand` handle) and the absolute deadline (epoch ms = clock-at-begin + the
  configured `Duration`). The db layer builds it in `Transaction::exec_env`,
  keeping the clock `Arc` on the `Database` exactly as `rand` is kept (no
  slate-engine change; `txn.now_millis()` is a begin-snapshot and can't measure
  elapsed time).
- A small `Ticker` (an `Option<Rc<Deadline>>` + a `u64` counter) with an
  `#[inline] tick(&mut self) -> Result<(), ExecError>` is `move`d / `&mut`-captured
  into each source node's existing closure. `None` → one predictable branch (free);
  `Some` → the masked clock check.
- Each blocking node counts buffered rows and returns `ExecError::LimitExceeded`
  when the count exceeds `materialization_cap`.

## Failure shape (mirrors the removed Lua VM instruction limit)

The Lua VM bounded script work and **failed loudly** when the instruction budget
was exceeded — never silently truncated. Same posture here: exceeding either limit
yields a hard error on the result stream (`ExecError::Timeout` /
`ExecError::LimitExceeded` → `DbError::Timeout` / `DbError::LimitExceeded`), which
aborts the query. No partial result set is returned as if complete. Distinct
variants so callers tell "too slow" from "too big" from an I/O error.

## Scope

A + B only. C (input-size limits, write path) and D (rows-examined cap, shares the
Observability RFC's per-node counter) are noted in the RFC as fast-follows and are
**not** built here.
