---
name: cosmos-parity
description: Use after changing Slate's query surface (planner, eval, SQL front-end, functions) and before declaring a query feature done. Runs the Cosmos parity checks — the hermetic corpus replay and the live-emulator differential — and covers normalization, divergence triage, the known-emulator-bug allowlist, and what the oracle can't cover.
---

# Cosmos parity in slate

Slate's query surface is checked against **real Cosmos DB** behavior. Run a query
on both engines over the same documents, normalize both outputs the same way, and
assert they agree. Use this any time you touch the query path — planner, eval,
parser/SQL grammar, the query front-ends, or scalar/aggregate functions — and
before calling a query feature done.

Depth lives in `tools/cosmos-parity/README.md`; this is the recipe.

## 1. Two modes, two purposes

| Mode | Command | Oracle | Docker | Hermetic |
|------|---------|--------|--------|----------|
| **Corpus / golden replay** | `run_samples.py` | committed `result.json` (Azure-Samples) | no | **yes** |
| **Live emulator differential** | `run.py` | the running Cosmos emulator | yes | no |

The corpus replay is the model to grow — deterministic, no Docker, runs anywhere.
The live differential is the broad net: it catches divergences the corpus doesn't
cover (every scalar function, generated subquery matrices, negative/edge cases),
but it depends on a real emulator, so it's non-hermetic and not for CI gating.

Both are the cross-engine successor to the now-removed in-repo v1↔v2 differential
tests — the oracle is real Cosmos instead of the old v1 executor.

## 2. Run the corpus replay (hermetic, do this first)

```bash
tools/cosmos-parity/samples_fetch.sh        # one-time: fetch corpus into .samples/ (gitignored)
python3 tools/cosmos-parity/run_samples.py
```

`samples_fetch.sh` fetches the Azure-Samples corpus **pinned to a specific commit
with no git history** — a single shallow snapshot, not a tracking clone — into the
gitignored `.samples/`. The pin is what keeps the baseline reproducible: upstream
can't move the corpus underneath you. The fetch is idempotent (a no-op once
pinned), so it's a safe one-time prerequisite, not a per-run cost.

No emulator needed — each `.samples/scripts/<name>/` ships an authoritative
`result.json`. The runner builds the `parity_samples` slate-cli example, runs each
folder's `query.sql` in-process against a fresh in-memory DB (seeded from the
folder's `seed.json`), normalizes, and diffs.

**Current baseline: `111/117 match`.** The 6 remaining are all spatial functions
(`ST_AREA`, `ST_DISTANCE`, `ST_INTERSECTS`, `ST_ISVALID`, `ST_ISVALIDDETAILED`,
`ST_WITHIN`) — not yet implemented in slate, tracked as a real gap. If your change
drops the count below 111, you introduced a regression; if it raises the count,
note the new number here and in `tools/cosmos-parity/README.md`.

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

## 7. Capture / refresh a golden

Today the only goldens are the **corpus's own `result.json` files** — Microsoft's
authoritative output, not committed here but fetched (pinned, gitignored) by
`samples_fetch.sh`. To refresh the corpus, **bump the pinned `COMMIT` in
`samples_fetch.sh`**, delete `.samples/`, re-run the fetch, then re-run
`run_samples.py` and update the match count in this skill and the README. Slate's
own matrices (`queries/*.sql`) have **no committed golden**; they diff live against
the emulator via `run.py`.

Capturing goldens for slate's own scenarios is the **planned successor** (§8) — not
yet built. Until it exists, slate-matrix parity is emulator-dependent (`run.py`).

## 8. Planned successor — hermetic golden-replay under `cargo test` (NOT YET BUILT)

The target is to fold slate's own matrices into a hermetic, Docker-free test, the
way the corpus replay already is. Design (document only — no harness exists yet):

- **Capture step** (extend `run.py`): query the emulator, normalize, and commit
  `<scenario>.expected.json` next to the dataset + query.
- **Replay test** (e.g. `crates/slate-db/tests/cosmos_golden.rs`): load dataset +
  query + golden, run slate in-process, **normalize both sides in Rust (one
  implementation)**, and assert equal — so the normalization rules of §4 live in
  one place, not duplicated across Python and Rust.
- **Random/fuzz stays opt-in and out-of-process** (`run.py`): you can't pre-capture
  a golden for a randomly generated query, so generated matrices keep using the
  live emulator.

This is the successor to the removed v1↔v2 differential. When you build it, replace
this section with the real commands.

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
- Running only `run.py` and skipping the **hermetic** `run_samples.py` — the corpus
  replay is the one that runs without Docker and should stay green (111/117).
- Forgetting to update the **match count** here and in
  `tools/cosmos-parity/README.md` when your change moves it.
