# RFC: Covering Index Scans & Engine-Level Recheck

> **Status: proposed.** A query/execution optimization for index access, surfaced
> while benchmarking compound indexes. The [roadmap](../roadmap.md) tracks status
> at a glance.

## Concept

Two related changes so an index scan does no redundant work *above* the engine:

1. **Engine-level leading-equality recheck.** When a (compound) index scan
   constrains a component with exact equality, recheck it *inside the scan* by
   comparing the index entry's component value bytes — the way the single-field
   `Eq` scan already does — instead of leaving a residual `Filter` that re-reads
   the value from the fetched document.
2. **Covering scan.** When every field a query references is a component of the
   chosen index, serve the query from the index entries alone and **skip the
   `KeyLookup`** entirely — never touch the documents.

Both remove cost that the single-field `Eq` path already avoids, and that a
compound index — which carries *more* information — should never pay.

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
  `KeyLookup`, re-reading `user.id` from each fetched document. The single-field
  `Eq` scan rechecks the same equality *inside the engine* (a byte compare on the
  entry) and emits **no** `Filter`.
- The query touches only `c.user.id` (filter) and `c.status` (group key) — **both
  components of the compound index** — yet still does a `KeyLookup` to fetch ~1,000
  whole documents it doesn't need. It is a *covered* query served as an
  uncovered one.

## Current behavior

- **Single-field `Eq`** (`scan_index` with `IndexRange::Eq`) returns an `exact`
  recheck tuple; the engine iterator byte-compares each entry's value/tag against
  the wanted value before yielding. The conservative string-prefix over-read is
  excluded *in the engine*; no residual `Filter` is planned.
- **Compound** (`scan_compound_index`) seeks the leftmost-equality byte prefix as
  a conservative superset, and the planner keeps the constraining predicate as a
  residual `Filter`. With nothing else covering it, that `Filter` runs **after**
  `KeyLookup`, against the materialized document.

## Design

### A. Engine-level recheck for constrained equality components

Give `CompoundIndexScan` the single-field `Eq` treatment: recheck each
equality-constrained component against the entry's per-component value bytes
(already decodable via `IndexEntry::component_value`), inside the scan. The
planner then drops the residual `Filter` for those components. Net: the
leading-equality recheck stops re-reading the document, so a compound scan is
**never slower than the single-field index** on the same leading prefix. Small,
self-contained, and shippable on its own.

### B. Covering scan (skip `KeyLookup`)

When the set of fields a query references — residual filter, group keys,
projection, sort keys — is a subset of the chosen index's components (plus the
`_id`, which the entry carries as the doc_id), the planner marks the scan
**covering** and omits the `KeyLookup`. The executor synthesizes each row from the
index entry's component values rather than the document. Non-covered queries are
unchanged.

### Interactions

- The conservative string-prefix superset recheck still applies — it just reads
  the component value from the **entry**, not the document.
- `_id` / pk is available from the entry's doc_id, so projections of `_id` stay
  covered.
- Fixed-width trailing equality components could skip the recheck entirely (the
  seek is exact); strings still need it.

## Benefits

- A compound index dominates the single-field index for any leading-prefix query
  (removes the surprise above).
- Covered aggregates and projections (`GROUP BY` on a non-leading component,
  `SELECT`ing only indexed fields, `COUNT` with an indexed filter) avoid **all**
  document fetches.
- No new on-disk format — both parts are planner + executor changes over the
  existing entry layout.

## Performance note

Part A removes a per-row, post-fetch `Filter`; Part B removes the per-row document
fetch. For the motivating query both apply: the covered, engine-rechecked plan
fetches zero documents and should land **below** the 4.4 ms single-field number,
not above it. The win scales with rows-per-group; it is negligible when the
leading key is unique (few rows per match).

## Non-goals

- No "INCLUDE"/payload columns — covering is limited to fields that are already
  index *components*.
- No streaming/ordered `GROUP BY` (the index returns a fixed leading prefix
  already grouped by the next component) — a real but separate optimization; see
  [Collect Node](./collect-node.md).

## Spike

Before implementing: confirm the planner can compute "referenced fields ⊆ index
components" from the lowered plan, and that the executor can present a row built
from `IndexEntry` component values to `Filter`/`Aggregate`/`Project` without a
document. Land **A** first (it is the clear correctness-of-cost fix), measure, then **B**.

## Cosmos, for reference

Covering and recheck-placement are physical-plan choices, invisible in results —
the Cosmos oracle validates the *answers* (unchanged), not the plan, so it guards
correctness here but says nothing about the optimization itself. Related:
[Compound Indexes](./compound-indexes.md), [Index Sargability](./index-sargability.md),
[Unified Numeric Index Key](./unified-numeric-index-key.md).
