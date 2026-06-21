---
name: bench
description: Use when running, adding, or interpreting benchmarks in this repo. Covers the targeted before/after workflow, an affected-bench map by source area, noise thresholds, and the full-sweep release check.
---

# Benchmarks in slate

We benchmark **targeted before/after** by default: capture the affected benches on `main`, make the change, capture them again, compare. Always running the full suite is too slow to be a habit, so it gets reserved for periodic full sweeps and releases.

## 1. What benches exist

| Crate           | Bench harness | What it measures                            |
|-----------------|---------------|---------------------------------------------|
| `slate-db`      | `executor`    | Plan node execution / pipeline cost         |
| `slate-db`      | `planner`     | Plan building cost                          |
| `slate-db`      | `query`       | End-to-end query (parse → plan → execute)   |
| `slate-db`      | `mutation`    | Insert/update/delete paths                  |
| `slate-eval`    | `apply`       | UPDATE apply step (in-place byte edit vs rebuild) |
| `slate-engine`  | `engine`      | KV layer: record encoding, index sync, scan |
| `slate-store`   | `memory`      | Memory backend write/scan                   |
| `slate-store`   | `rocks`       | RocksDB backend write/scan                  |
| `slate-store`   | `redb`        | redb backend write/scan                     |
| `slate-vm`      | `vm`          | Lua/JS hook execution                       |

Run a single harness:

```bash
cargo bench -p <crate> --bench <name>
```

## 2. Affected-bench map

Use this to pick which benches to run. **Widen when uncertain** — running an extra bench is cheaper than missing a regression.

| Code area touched                                          | Run benches                          |
|------------------------------------------------------------|--------------------------------------|
| `slate-db/src/planner/`                                    | `planner`, `query`                   |
| `slate-db/src/executor/`                                   | `executor`, `query`                  |
| `slate-db/src/mutation/`                                   | `mutation`                           |
| `slate-eval/src/apply/`                                    | `apply`                              |
| `slate-db/src/parser/`, `expression/`, `statement/`        | `query` (parse is in the hot path)   |
| `slate-db/src/cursor.rs`, `collection.rs`, `database.rs`   | `executor`, `mutation`, `query`      |
| `slate-engine/src/encoding/`                               | `engine`, `executor`, `mutation`, all `slate-store` |
| `slate-engine/src/kv/`, `index_sync.rs`                    | `engine`, `mutation`, all `slate-store` |
| `slate-store/src/<backend>.rs`                             | that backend's bench + `mutation`    |
| `slate-vm/`, hooks/triggers/validators                     | `vm`, `mutation` (triggers fire on writes) |
| Cross-cutting: error types, traits in `lib.rs`             | run the parent crate's full set      |

If a change touches multiple rows, take the union.

## 3. The before/after workflow

Use criterion's `--save-baseline` so you don't have to keep the `main` checkout around.

```bash
# On main (or a clean base), before the change
git switch main
cargo bench -p <crate> --bench <name> -- --save-baseline before

# Switch to your branch, make the change, then:
cargo bench -p <crate> --bench <name> -- --baseline before
```

Criterion prints a colored diff per benchmark group. The number that matters is the **change in mean time** with the confidence interval.

Capture the diff output (or screenshot it for `roadmap/`) so it's preserved beyond the terminal session — criterion overwrites baselines on the next run.

## 4. Reading the numbers

- **< 5% change**: noise. Re-run once before believing it. Criterion's own variance is 1-3%.
- **5-10%**: real, but only worth surfacing if the bench is on a documented hot path.
- **> 10%**: real and material. Mention in the commit body. If regressing, surface to the user before committing — do not commit a > 10% regression without explicit acknowledgement.
- **Confidence interval crosses zero**: not significant, regardless of mean.

When a number is material (regression *or* improvement > ~10% on a documented bench), include it in the commit body:

```
scan 20k rows: 142ms → 98ms (-31%)
```

Don't pad commit bodies with bench numbers when nothing meaningfully moved.

## 5. README numbers

`README.md` quotes specific bench results. If a change moves any of those numbers materially, update the README **in the same commit** (precedent: `52d64db`). Don't let README drift from reality across commits.

## 6. Adding a new benchmark

1. Add the bench file under the relevant crate's `benches/` directory.
2. Register it in that crate's `Cargo.toml`:

   ```toml
   [[bench]]
   name = "your_bench"
   harness = false
   ```

3. Add a row to the "What benches exist" table above and, if it covers a new area, to the affected-bench map.
4. Run it once on `main` and save the baseline so future before/after runs have something to compare to.

A new bench should measure one specific operation. If you find yourself wanting to add five `bench_*` functions to one file, split it — easier to interpret diffs and easier to skip irrelevant ones.

## 7. Periodic full sweep

Run the full suite:

- Before tagging a release.
- After a large refactor that touched cross-cutting code (encoding, error types, trait signatures).
- Any time the affected-bench map felt ambiguous and you want a sanity check.

```bash
cargo bench
```

This is slow (multiple minutes). Don't put it in a pre-commit hook.

## 8. Failure modes to avoid

- Running `cargo bench` (everything) when only one harness is affected — wastes minutes per commit.
- Trusting a single sub-5% delta — re-run before believing it.
- Forgetting to save the `main` baseline before switching branches, then having to checkout `main`, re-bench, and switch back.
- Updating README numbers in a separate commit from the change that moved them.
- Including bench numbers in commit bodies when they didn't materially move (noise).
