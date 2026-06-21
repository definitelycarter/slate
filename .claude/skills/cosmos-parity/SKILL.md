---
name: cosmos-parity
description: Use after changing Slate's query surface (planner, eval, SQL front-end, functions) and before declaring a query feature done. Runs the Cosmos parity checks — the hermetic golden replays (Azure-Samples corpus + slate's own matrices) and the live-emulator differential — and covers normalization, divergence triage, the known-emulator-bug allowlist, and what the oracle can't cover.
---

# Cosmos parity in slate

Slate's query surface is checked against **real Cosmos DB** behavior. Run a query
on both engines over the same documents, normalize both outputs the same way, and
assert they agree. Use this any time you touch the query path — planner, eval,
parser/SQL grammar, the query front-ends, or scalar/aggregate functions — and
before calling a query feature done.

Depth lives in `tools/cosmos-parity/README.md`; this is the recipe.

## 1. Three modes, two purposes

| Mode | Command | Oracle | Docker | Hermetic |
|------|---------|--------|--------|----------|
| **Corpus golden replay** | `run_samples.py` | committed `result.json` (Azure-Samples) | no | **yes** |
| **Slate-matrix golden replay** | `cargo test -p slate-db --test cosmos_golden` | committed `goldens/` (captured from the emulator) | no | **yes** |
| **Live emulator differential** | `run.py` | the running Cosmos emulator | yes | no |

The two golden replays are the hermetic core — deterministic, no Docker, run
anywhere (and in CI). The corpus replay diffs against Microsoft's authoritative
`result.json`; the slate-matrix replay diffs slate's own curated `queries/*.sql`
matrices against goldens we captured from the emulator (§7) and normalize in Rust
at replay (§8). The live differential is the broad net: it catches divergences the
goldens don't cover (every scalar function, generated subquery matrices,
negative/edge cases over the live oracle), but it depends on a real emulator, so
it's non-hermetic and not for CI gating.

All three are the cross-engine successor to the now-removed in-repo v1↔v2
differential tests — the oracle is real Cosmos instead of the old v1 executor.

## 2. Run the hermetic golden replays (do these first)

Both are Docker-free and safe to gate on. The slate-matrix replay is just `cargo
test`; the corpus replay needs a one-time pinned fetch.

```bash
cargo test -p slate-db --test cosmos_golden    # slate's own curated matrices vs committed goldens/
tools/cosmos-parity/samples_fetch.sh           # one-time: fetch corpus into .samples/ (gitignored)
python3 tools/cosmos-parity/run_samples.py     # Azure-Samples corpus vs its result.json
```

`samples_fetch.sh` fetches the Azure-Samples corpus **pinned to a specific commit
with no git history** — a single shallow snapshot, not a tracking clone — into the
gitignored `.samples/`. The pin is what keeps the baseline reproducible: upstream
can't move the corpus underneath you. The fetch is idempotent (a no-op once
pinned), so it's a safe one-time prerequisite, not a per-run cost.

No emulator needed for either. The corpus runner builds the `parity_samples`
slate-cli example, runs each `.samples/scripts/<name>/query.sql` in-process against
a fresh in-memory DB (seeded from the folder's `seed.json`), normalizes, and diffs
against the folder's authoritative `result.json`. The slate-matrix replay
(`cosmos_golden.rs`) does the same against the committed `goldens/` we captured from
the emulator (§7–§8).

**Current baselines:** corpus replay `111/117 match` — the 6 remaining are all
spatial functions (`ST_AREA`, `ST_DISTANCE`, `ST_INTERSECTS`, `ST_ISVALID`,
`ST_ISVALIDDETAILED`, `ST_WITHIN`), not yet implemented in slate, tracked as a real
gap. Slate-matrix replay `214/216 match` (2 known gaps: `1/0`, `5%0`). If a change
drops either count, you introduced a regression; if it raises one, note the new
number here and in `tools/cosmos-parity/README.md`.

## 3. Run the live emulator differential (when Docker is available)

```bash
python3 tools/cosmos-parity/run.py
```

`run.py` manages the emulator itself: it `docker rm -f`s and recreates the
`slate-cosmos` container each run (the emulator persists to its container layer, so
a restart would keep stale docs), seeds each dataset into its own container, runs
the matrices on both sides, and reports per-dataset pass counts plus every
divergence. Needs Docker + `cargo`; the image is the ARM64-friendly
`mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-latest`. Reach for
this when you've changed a function or grammar the corpus doesn't exercise, or
before a larger query-surface change lands.

## 4. Normalization — applied identically on both sides

A "match" is value-equality after normalizing **both** outputs the same way (see
`numify`/`strip`/`norm` in the runners, and the README's Normalization section):

1. **Strip Cosmos `_`-system fields** (`_rid`, `_etag`, `_ts`, …) from every
   object.
2. **Sort result lists** unless the query has `ORDER BY` (set ordering isn't
   guaranteed; only `ORDER BY` pins it).
3. **Whole-valued floats compare equal to ints.** Slate is BSON-typed, so math
   functions return `Double` (`SQRT(16)` → `4.0`); Cosmos has one JSON number type
   and serializes whole numbers as `4`. Same value, different representation.

These rules are the contract. When you read a divergence, first ask whether it's a
representation difference these rules already cover — if so, the harness is telling
you the rule isn't being applied (a bug in your change or a normalization gap), not
that Cosmos and slate truly disagree.

## 5. Triage a divergence — real gap vs. emulator bug

When the harness flags a divergence, classify it:

- **Real slate gap** (slate errors or returns the wrong value, Cosmos is right):
  this is the actionable case — fix slate, or record it as a known gap (the spatial
  `ST_*` functions are the current example). Reproduce in isolation with the
  in-process example:
  ```bash
  cargo run -q -p slate-cli --example parity_samples -- tools/cosmos-parity/.samples/scripts
  ```
- **Emulator bug** (the oracle is wrong, slate is right): the emulator is *not*
  hosted Cosmos and has known defects. **Do not "fix" slate to match a wrong
  oracle, and do not let a golden pin emulator behavior.** Keep these on an
  xfail/allowlist. Known cases (confirm against hosted Cosmos when possible) — the
  through-line is that the emulator mishandles subquery sources and aggregates:
  - A **multi-value subquery as a JOIN source** (`JOIN j IN (SELECT …)`) returns
    `[]` in the emulator even when rows should match; slate returns the rows.
  - A **`COUNT` scalar-subquery in `WHERE`** raises an internal
    `localCount must be a number` error in the emulator; slate evaluates it.
  - An **aggregate inside a `SELECT VALUE` subquery hoists to the outer query** in
    the emulator (the projection shape shouldn't depend on whether the outer query
    aggregates); slate is self-consistent per-row.
  - A **subquery used as an aggregate source** (`COUNT(1) FROM y IN (SELECT … )`)
    counts `0` in the emulator — it sees the inner subquery's rows as empty; slate
    counts the real rows. Same root cause as the JOIN-source `[]` bug.
  - A **`SUM`/aggregate over an empty (filtered) subquery** returns `0` in the
    emulator but `undefined` in slate, so slate omits the projected field. The
    largest single family (~14 queries in the live run); slate stays consistent
    with its own subquery handling.
- **Benign — both outputs arguably valid.** `ORDER BY` on a *tied* key with
  `OFFSET`/`LIMIT` can page back in a different order across engines (the sort is
  only defined up to ties); large-integer float math (`SQUARE` of an epoch past
  2⁵³) diverges in the low digits (both are f64, rounding differently — a
  number-model edge). Neither is a gap; pin a fuller `ORDER BY` for determinism.

When in doubt, isolate the query, read both outputs after normalization, and check
the divergence list in `tools/cosmos-parity/README.md` — most known ones are
already catalogued there.

## 6. Add a scenario (dataset + query)

A scenario is a dataset file plus a query in a matrix:

1. **Dataset** — add or reuse a `tools/cosmos-parity/datasets/<name>.json` (a JSON
   array of documents; each has an `id` — slate loads it with `pk_path: "id"` to
   match Cosmos's system key). Reuse `products`/`orders`/`families` when they fit.
2. **Query** — append the query to the matching `tools/cosmos-parity/queries/<name>.sql`
   (one query per line; `#` lines are comments). Put error-expected queries in
   `negative.sql` (parity = both engines error) and undefined/JOIN corner cases in
   `edge.sql` (parity = same result).
3. **Wire a new dataset** into the `DATASETS` list in `run.py` if you added one.
4. For broad subquery/function coverage, prefer **generating** queries: edit the
   `SCHEMA`/`VOLUME` knobs in `gen.py` and re-run `python3 tools/cosmos-parity/gen.py`
   — it writes `queries/gen_<dataset>.sql`, which `run.py` auto-includes.

## 7. Capture / refresh goldens

There are two committed golden sources, refreshed independently.

**Corpus goldens** are the `result.json` files — Microsoft's authoritative output,
not committed here but fetched (pinned, gitignored) by `samples_fetch.sh`. To
refresh: **bump the pinned `COMMIT` in `samples_fetch.sh`**, delete `.samples/`,
re-run the fetch, then re-run `run_samples.py` and update the match count in this
skill and the README.

**Slate-matrix goldens** live committed under `tools/cosmos-parity/goldens/<label>/<NNN>.json`
— one per curated `queries/*.sql` query, each `{query, dataset, result | error}`
holding Cosmos's **raw** output (system fields and all; normalization happens once
in Rust at replay, §8). Capture them from the emulator (Docker required for capture
only — replay is hermetic):

```bash
python3 tools/cosmos-parity/capture_goldens.py            # recreate emulator, capture
python3 tools/cosmos-parity/capture_goldens.py --triage   # report only, write nothing
```

The capture is **self-auditing**: it runs slate in-process alongside the emulator
and refuses to silently pin a divergence. Two committed sidecars govern it:

- `goldens/_excluded.json` — emulator-bug families (§5) we refuse to golden;
  pinning a known-wrong oracle is forbidden. Skipped at capture, still exercised by
  `run.py`. (Currently empty — none of those families appear in the curated
  matrices; they live only in the generated `gen_*.sql`, which aren't goldened.)
- `goldens/_known_gaps.json` — queries where the emulator is right but slate
  diverges (a real gap, §5). The correct Cosmos golden *is* committed, but the
  replay treats these as expected divergences (reports, doesn't assert), so the
  suite stays green while the gap stays pinned.

If capture finds a divergence that's on neither list, it prints it as
`UNCLASSIFIED`, writes no golden, and exits non-zero — triage it (emulator bug →
`_excluded.json`; real slate gap → `_known_gaps.json`) and re-run. When refreshing,
re-run the replay (below) and update the match count here and in the README.

Generated matrices (`gen_*.sql`) are **not** goldened — a randomly generated query
has no fixed oracle, so they keep diffing live against the emulator via `run.py`.

## 8. Hermetic golden-replay under `cargo test`

Slate's own matrices replay hermetically (no Docker, no network) in
`crates/slate-db/tests/cosmos_golden.rs`:

```bash
cargo test -p slate-db --test cosmos_golden
```

For each committed golden it loads the dataset into a fresh in-memory slate DB
(`pk_path: "id"`), runs the query through the SQL API, and **normalizes BOTH sides
with the one Rust normalizer in that file** — the single source of truth for the §4
rules (strip `_`-system fields, sort unless `ORDER BY`, whole-floats == ints). The
Python runners' `numify`/`strip`/`norm` mirror it for the live/corpus paths, but
the §4 contract is enforced once, in Rust. **Current baseline: `214/216` curated
queries matched, 2 reported as known slate gaps (`1/0`, `5%0` — Cosmos errors,
slate drops the row), 0 emulator-bug exclusions.** If your change drops below 214 or
adds a failure (a divergence not on a sidecar list), you introduced a regression or
a new triage case; if it raises the match count (e.g. a known gap now matches), the
test prints a nudge to promote it out of `_known_gaps.json` — do so and update the
counts here and in the README.

This is the successor to the removed v1↔v2 differential, folded into `cargo test`.

## 9. Coverage boundary — what the Cosmos oracle can never cover

The emulator covers the query + CRUD surface broadly (aggregates, joins,
`ORDER BY`, paging, range operators, subdocuments, string/math/array operators,
patch, batch/bulk, change feed, TTL). Parity is bounded by what it *doesn't* do —
per the [emulator feature matrix](https://learn.microsoft.com/en-us/azure/cosmos-db/emulator-linux)
(re-check it; the emulator evolves):

- **Stored Procedures, Triggers, and UDFs are ❌ Not planned.** So the Cosmos
  oracle can **never** cover slate's hooks — triggers, validators, and UDFs are
  slate's own Lua extensions with no Cosmos analogue. Those keep their own
  `slate-db` / `slate-vm` tests; don't expect or chase parity for them here.
- **Index choice isn't checkable.** "Create collection with custom index policy"
  and "Update collection" are ⚠️ No-ops, so the emulator ignores index policy
  entirely — only query *results* are comparable, which is exactly what we want
  (results must match regardless of how either engine indexes).
- **Control-plane / metering is out of scope.** Request Units, parallel
  partitioned query, and the collection/database read-*feed* endpoints are ⚠️ Not
  yet implemented; the Offers / Users / Permissions / CEK endpoints are ⚠️ No-ops.
  None affect result parity. (The *change feed* itself is ✅ Supported — it's the
  metadata read-feeds that aren't.)

Parity is a **result-equivalence** check on the query/CRUD surface, nothing more.

## Failure modes to avoid

- Treating an **emulator bug as a slate bug** — changing slate to match a wrong
  oracle. Check the §5 allowlist and the README divergence list first.
- **Pinning a golden to emulator behavior** that's known-wrong — goldens encode
  correct Cosmos semantics, not the emulator's defects.
- Chasing parity for **triggers / validators / UDFs / index choice / RUs** — the
  oracle structurally can't cover them (§9).
- Reading a divergence as a real disagreement when it's just **float-vs-int or
  unsorted output** — that's what normalization (§4) handles.
- Running only `run.py` and skipping the **hermetic** replays — `cargo test -p
  slate-db --test cosmos_golden` (214/216) and `run_samples.py` (111/117) are the
  ones that run without Docker and should stay green.
- **Capturing a golden against an already-seeded emulator** — re-seeding errors on
  duplicate ids. `capture_goldens.py` recreates the emulator by default for a clean
  slate; only pass `--no-recreate` against a freshly recreated container.
- Forgetting to update the **match counts** here and in
  `tools/cosmos-parity/README.md` when your change moves them.
