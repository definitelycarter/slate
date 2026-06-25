# RFC: Covering Index Scans & Engine-Level Recheck

> **Status: Part A implemented (planner-only); Part B specified, deferred.**
> A query/execution optimization for index access, surfaced while benchmarking
> compound indexes. Part A shipped as a small planner change (below). Part B
> (the covering scan) is a larger, correctness-critical change — fully designed
> here before an implementation pass. The [roadmap](../roadmap.md) tracks status
> at a glance.

## Concept

Two related changes so an index scan does no redundant work *above* the engine:

1. **Leading-equality recheck → drop the residual `Filter` (Part A).** A compound
   index scan's executor node already rechecks each equality-constrained component
   against the index *entry*; the planner was *also* keeping a residual `Filter`
   that re-checked the same predicate against the *fetched document*. Part A drops
   that redundant filter.
2. **Covering scan (Part B).** When every field a query references is a component
   of the chosen index, serve the query from the index entries alone and **skip the
   `KeyLookup`** entirely — never touch the documents.

Both remove cost the single-field `Eq` path already avoids, and that a compound
index — which carries *more* information — should never pay.

## Motivation

A `GROUP BY` over a user's rows, benchmarked on a 52k-doc collection:

```sql
SELECT c.status, COUNT(1) FROM c
WHERE c.user.id = 'user_no_triggers' GROUP BY c.status
```

| index | plan source | latency |
|---|---|---|
| none | `Scan` | 94 ms |
| compound `(user.id, status)` | `CompoundIndexScan [= …] → KeyLookup → Filter user.id` | 7.7 ms |
| single `user.id` | `IndexScan user.id = … → KeyLookup` (no Filter) | **4.4 ms** |

The single-field index is **faster than the compound index** on its own leading
prefix — counterintuitive, since the compound index contains strictly more. Two
inefficiencies explain the gap, and both are visible in the plans:

- The compound plan keeps a residual **`Filter c.user.id = …`** above the
  `KeyLookup`, re-reading `user.id` from each fetched document — even though the
  scan node already rechecked it against the entry. (Part A.)
- The query touches only `c.user.id` (filter) and `c.status` (group key) — **both
  components of the compound index** — yet still does a `KeyLookup` to fetch ~1,000
  whole documents it doesn't need. It is a *covered* query served as an uncovered
  one. (Part B.)

## Part A — drop the redundant residual `Filter` *(implemented)*

### What it actually was

The RFC originally framed Part A as adding an "engine-level recheck" to the
compound scan. Tracing the code showed the recheck **already exists**: the
executor's `CompoundIndexScan` node (`nodes/compound_index_scan.rs`) decodes each
entry's per-component values via `IndexEntry::component_value` and rechecks every
leading equality and the trailing range with the same coercing `compare_bson`
comparator `WHERE` uses — its module docs even say "the recheck is identical to the
residual `Filter` the planner keeps." So the equality was being enforced **twice**:
once on the entry (in the scan node) and again on the fetched document (in the
planner's residual `Filter`).

So Part A is **purely a planner change**: in `sargable.rs::plan_source`, the
conjuncts a compound scan claims are now added to `consumed` (dropped from the
residual) instead of `claimed`-but-retained. This mirrors single-field `Eq`, whose
in-scan recheck likewise consumes its atom. `compound_index_scan` returns exactly
the conjuncts that form the `eq_prefix` + trailing range, and the scan node
rechecks exactly those — a 1:1 correspondence — so consuming them is sound.

- **Scope:** compound indexes only. Single-field `Eq` already rechecked in-engine
  *and* already dropped its residual filter; the asymmetry was compound-only.
- **Cost removed:** a per-row predicate eval over the *materialized* document. The
  `KeyLookup` (the fetch itself) is unchanged — that's Part B.
- **Status:** implemented on `worktree-covering-index-scans` with two planner unit
  tests (`compound_scan_consumes_covered_conjuncts`,
  `compound_scan_keeps_uncovered_conjunct_in_residual`); 36 planner/executor/db
  suites green. **Measured** on a new compound-index leading-equality bench
  (`query_compound_eq` — `WHERE c.status = "active"` over compound
  `(status, contacts_count)`): **−18.7 % at 1k rows (220 → 179 µs), −15.4 % at 10k
  (2.79 → 2.36 ms)** purely from dropping the residual `Filter`. Correctness is
  unaffected (the executor recheck already enforced the equality; A only removes
  the redundant second check), which the end-to-end suites confirm.

## Part B — covering scan: skip the `KeyLookup` *(specified, deferred)*

When the set of fields a query references is a subset of the chosen index's
components (plus `_id`/pk, which the entry carries as its doc_id), the planner
marks the scan **covering** and omits the `KeyLookup`; the executor synthesizes
each row from the index entry instead of fetching the document.

### Measured opportunity

The existing single-field covered-projection bench is the baseline target:

```
find(filter = {status: "active"}, columns = ["status"])   -- status is indexed
```

| `query_indexed_eq_proj` | mean (current: `IndexScan → KeyLookup → Project`) |
|---|---|
| 1 000 docs | **245 µs** |
| 10 000 docs | **2.71 ms** |

Part B removes the per-matched-row document fetch; the win scales with the number
of matched rows.

### B.1 — IR change

Add a covering marker to `Node::IndexScan` and `Node::CompoundIndexScan` carrying
the component field names to synthesize (e.g. `covering: Option<Vec<String>>`,
`None` = today's id-yielding behaviour). Constructed only by the covering pass;
the variant is matched/constructed in ~15 sites (executor dispatch, `explain`,
`index_merge`, `key_lookup`, `sargable`, `lower`, `stats`, tests) — all need the
new field. Alternative: dedicated `Covering*` node variants (fewer construction
sites, but two new exhaustive-match arms and some duplicated scan glue). Settle in
the spike; a field on the existing variant keeps the scan logic unified.

### B.2 — executor synthesis

When covering, the scan yields a synthesized `RawBson::Document` per entry instead
of a bare doc_id, and the planner drops the enclosing `KeyLookup`:

- **Single-field:** `{ <field>: entry.value(), <pk_path>: doc_id }`.
- **Compound:** `{ <f₀>: component_value(0), …, <pk_path>: doc_id }` for each
  covered component, via `IndexEntry::component_value`.

The synthesized document is a drop-in for the fetched one — `Bind` attaches the
alias and `Project`/`Filter`/`Sort` read fields from it unchanged. `pk_path` comes
from the entry's doc_id, so `_id`/pk references stay covered.

### B.3 — coverage analysis (the correctness-critical part)

A planner pass over the lowered plan. **Index-type-agnostic** — the same analysis
serves single-field and compound; only the synthesized field set differs.

**Referenced-field collection.** Collect every access path to the `FROM` alias from
the residual `Filter`, the projection, the sort keys, and (for aggregates) the
group keys and aggregate arguments. Modeled on the existing exhaustive
`slate_ast::Expression::collect_parameters` walker, with an alias-path classifier:

- `<alias>.<field>` (depth-1 member) → references `field`.
- bare `<alias>` (whole row) → **uncoverable**.
- `<alias>.a.b`, `<alias>[i]`, `<alias>.tags[0]` (deeper / indexed) → **uncoverable**
  (the entry holds only the scalar component value; deeper navigation would differ).
- `PathGet` / `MultikeyEq` / `Subquery` touching the alias → **uncoverable**.

**Coverable iff** every referenced field ∈ (index components ∪ `{pk_path}`),
**and** all index components are top-level scalar (no dot-path, no `[]` multikey),
**and** the source is a single index scan (not `IndexMerge`), **and** the plan has
no join/`Unwind` (one alias). Any node or expression shape the pass does not fully
understand → **bail (do not cover)**. The invariant: *a missed cover is a lost
optimization; a wrong cover is wrong results* — so the collector returns
"uncoverable" rather than guess.

### B.4 — the aggregate binding shift

Above a `Node::Aggregate`, the `Project`/`Having` reference the aggregate output
slots (`$key0`, `$agg0`), **not** the alias. The fields actually read from the
document live in the `Aggregate`'s `group_keys` + aggregate args, plus any residual
`Filter` *below* the aggregate. So covered-aggregate analysis must collect refs
from that lower binding context, not from the `Project` above. This binding shift —
not present in plain projections — is why aggregates are a later phase.

### B.5 — phasing

1. **Single-field, non-aggregate.** Exactly the captured baseline
   (`query_indexed_eq_proj`); smallest correctness surface. Ship + measure first.
2. **Compound, non-aggregate.** *Same* coverage analysis; synthesize N components;
   covered set = the component list. A small increment over (1) — the executor
   reads several component values instead of one — **not** a from-scratch effort.
3. **Covered aggregate.** The RFC's headline `GROUP BY` (e.g.
   `SELECT c.status, COUNT(1) … GROUP BY c.status` over compound `(user.id,
   status)`). Adds the B.4 binding-aware collection. Largest win, most analysis.

### B.6 — safety net

A **differential test**: for each covered query, assert its result equals the same
query forced through the materialized plan (covering disabled). Pin the
single-field, compound, and aggregate covered cases *and* the bail cases (whole-row
`SELECT c`, deeper path `c.a.b`, indexed `c.tags[0]`, multikey component, joined
query) — each of which must keep the `KeyLookup`. This is the guard against the
one dangerous failure mode (a too-eager cover returning wrong rows).

## Benefits

- A compound index dominates the single-field index for any leading-prefix query
  (Part A removes the surprise; Part B makes the covered case fetch zero documents).
- Covered aggregates and projections (`GROUP BY` on indexed components, `SELECT`ing
  only indexed fields, `COUNT` with an indexed filter) avoid **all** document
  fetches.
- No new on-disk format — both parts are planner + executor changes over the
  existing entry layout.

## Non-goals

- No "INCLUDE"/payload columns — covering is limited to fields that are already
  index *components*.
- No covering over multikey (`[]`) or dot-path components in v1 — the synthesized
  scalar can't reproduce array-distributing or nested access.
- No streaming/ordered `GROUP BY` (the index returns a fixed leading prefix already
  grouped by the next component) — a real but separate optimization; see
  [Collect Node](./collect-node.md).

## Spike (for the Part B pass)

1. Settle the IR representation (covering field on the existing variants vs new
   `Covering*` nodes).
2. Build the conservative referenced-field collector + its bail rules, with the
   differential test (B.6) as the gate — *before* wiring the executor synthesis.
3. Land phase (1) single-field, re-run `query_indexed_eq_proj` against the saved
   `cov_before` baseline, then extend to (2) compound and (3) aggregate.

## Cosmos, for reference

Covering and recheck-placement are physical-plan choices, invisible in results —
the Cosmos oracle validates the *answers* (unchanged), not the plan, so it guards
correctness here but says nothing about the optimization itself. Related:
[Compound Indexes](./compound-indexes.md), [Index Sargability](./index-sargability.md),
[Unified Numeric Index Key](./unified-numeric-index-key.md).
