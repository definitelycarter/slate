# RFC: Covering Index Scans & Engine-Level Recheck

> **Status: Part A implemented; Part B phase 1 (single-field, non-aggregate —
> top-level *and* dotted paths) implemented; phases 2–3 (compound, covered
> aggregate) specified, deferred.**
> A query/execution optimization for index access, surfaced while benchmarking
> compound indexes. Part A shipped as a small planner change (below). Part B (the
> covering scan) is a larger, correctness-critical change; its first, smallest
> phase — covering a single-field index scan — has now shipped, measured at
> **−15% (1k) / −21% (10k)** on a covered string projection and **−10% / −27%**
> on a covered numeric projection, and since extended to **dotted single-field
> paths** (`user.id`, synthesized as `{user: {id: value}}`) under an
> exact-path-match rule — measured at **−7 % (1k) / −15 % (10k)** on a covered
> dotted projection over realistic documents (`query_indexed_eq_dotted_proj`). The
> remaining phases are fully designed here before their implementation pass. The
> [roadmap](../roadmap.md) tracks status at a glance.

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

## Part B — covering scan: skip the `KeyLookup` *(phase 1 implemented, incl. dotted paths)*

When the set of fields a query references is a subset of the chosen index's
components (plus `_id`/pk, which the entry carries as its doc_id), the planner
marks the scan **covering** and omits the `KeyLookup`; the executor synthesizes
each row from the index entry instead of fetching the document.

### What shipped (phase 1: single-field, non-aggregate)

Phase 1 covers exactly the `query_indexed_eq_proj` shape below — a single-field
index scan whose query reads only that field and the pk. Concretely:

- **IR:** `Node::IndexScan` gained a `covering: bool` (not the
  `Option<Vec<String>>` the spike floated — see B.1). The scan already carries
  its `field`, so a bool is the honest representation; compound covering will
  carry the component names because a compound node's `field` is an opaque joined
  identity.
- **Executor** (`index_scan.rs`): a covering scan synthesizes
  `{ <field>: entry.value(), <pk>: doc_id }` per entry (`synthesize_row`) instead
  of a bare doc-id; a dotted `field` nests (`user.id` → `{user: {id: value}}`,
  built by `append_path`); the non-covering path is unchanged.
- **Planner** (`covering.rs`): a conservative post-pass over the lowered
  `Plan::Query`, run at both read entry points (`plan()`'s Query arm and
  `lower()`), **never** on the write path. It is **two-phase**: a read-only
  `is_coverable` walk decides first (no allocation, alias borrowed not cloned),
  and only a coverable plan is rebuilt — so a non-coverable query (the common
  case) keeps its plan untouched and the optimization never taxes the paths it
  doesn't help.
- **Safety:** the analysis returns *uncoverable* the instant it sees any shape it
  doesn't fully understand (an `Env` binding ⇒ join/unwind/aggregate/subquery; a
  bare-alias whole-row read; an `Index`/`PathGet`/`MultikeyEq`/`Subquery` on the
  alias; a multikey `.[]` index field; an `IndexMerge`/compound/multikey source).
  The invariant from B.3 holds: *a missed cover is a lost optimization; a wrong
  cover is wrong results.* A differential test (`covering_index.rs`) pins covered
  ≡ materialized (indexed vs unindexed) and that `EXPLAIN` actually drops the
  `KeyLookup`.
- **Dotted paths (the exact-path-match rule):** an index field may be a dotted
  scalar path (`user.id`), not just top-level. A reference covers iff its
  reconstructed dotted path *string-equals* a component path or the pk — so
  `c.user.id` covers an index on `user.id`, but its **parent** (`c.user`, the
  whole subdoc), an **extension** (`c.user.id.x`), and a **sibling**
  (`c.user.name`) all bail, since the scalar entry carries only `user.id`. A
  multikey `.[]` path still never covers (array fan-out is unreconstructable), and
  a dotted field whose root segment collides with the (top-level) pk bails to
  avoid a duplicate synthesized key.
- **Measured** (back-to-back vs clean `main`, same machine): covered string
  projection **−15% (1k) / −21% (10k)**, covered numeric projection **−10% /
  −27%** — the win scaling with matched-row count, as predicted. Non-covered
  control queries were within the machine's run-to-run drift band (the same
  control swung −0.1%→+6% across runs against a fixed baseline), so no measurable
  regression on the paths the two-phase pass leaves untouched.
- **Measured (dotted)** (`query_indexed_eq_dotted_proj`, an indexed `meta.note`
  nested inside *realistic* documents, vs the same query uncovered on `main`):
  **−7% (1k) / −15% (10k)**, reproduced. Smaller than the top-level win because
  nested synthesis allocates an inner document per row, but a clear net win at
  scale — and, like the others, it scales with matched rows. The win requires a
  *heavy* fetched document to skip: on tiny documents (the synthesized cost ≈ the
  fetch saved) it washes out, which is why the bench nests the indexed path inside
  the realistic corpus, not a minimal one.

Phases 2 (compound) and 3 (covered aggregate) remain as designed below.

### Measured opportunity

The existing single-field covered-projection bench is the baseline target:

```
find(filter = {status: "active"}, columns = ["status"])   -- status is indexed
```

| `query_indexed_eq_proj` | before (`IndexScan → KeyLookup → Project`) | after (covering) |
|---|---|---|
| 1 000 docs | **245 µs** | **~−15%** |
| 10 000 docs | **2.71 ms** | **~−21%** |

Part B removes the per-matched-row document fetch; the win scales with the number
of matched rows.

### B.1 — IR change *(settled: `covering: bool` for single-field)*

Phase 1 added `covering: bool` to `Node::IndexScan` (not the
`Option<Vec<String>>` below): the single-field scan already names its `field`, so
the executor synthesizes `{field, pk}` from a bool alone. The spike's original
sketch — carrying component names — is the right shape for **compound** covering
(phase 2), whose `field` is the opaque joined identity `f1\x01f2`, so
`Node::CompoundIndexScan` will gain its own covering marker then. The original
analysis follows:

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

> **Implemented refinement (single-field, dotted).** The shipped pass loosens the
> "depth-1 member / top-level scalar" rule above to **dotted scalar paths** by
> *exact-path match*: it reconstructs each clean alias-rooted member chain into
> its full dotted path (`c.user.id` → `"user.id"`, mirroring the planner's
> `path_of`) and covers iff that string equals the index field or the pk. This is
> strictly the safe loosening — a parent (`c.user`), an extension (`c.user.id.x`),
> and a sibling (`c.user.name`) each reconstruct to a *different* string and so
> bail, exactly where a wrong cover would return wrong rows. Multikey `.[]` paths
> stay excluded. Compound (phase 2) reuses the same per-component path-matching.

### B.4 — the aggregate binding shift

Above a `Node::Aggregate`, the `Project`/`Having` reference the aggregate output
slots (`$key0`, `$agg0`), **not** the alias. The fields actually read from the
document live in the `Aggregate`'s `group_keys` + aggregate args, plus any residual
`Filter` *below* the aggregate. So covered-aggregate analysis must collect refs
from that lower binding context, not from the `Project` above. This binding shift —
not present in plain projections — is why aggregates are a later phase.

### B.5 — phasing

1. **Single-field, non-aggregate.** ✅ **Shipped** (top-level *and* dotted paths).
   Exactly the captured baseline (`query_indexed_eq_proj`); smallest correctness
   surface. The implemented pass handles only this shape; the coverage analysis
   below was scoped down to a single-field scan (no compound, no `IndexMerge`, no
   multikey) and a single `Alias`-bound source. Dotted scalar paths were added
   under the exact-path-match rule (above), nesting the synthesized value.
2. **Compound, non-aggregate.** *Same* coverage analysis; synthesize N components;
   covered set = the component list. A small increment over (1) — the executor
   reads several component values instead of one — **not** a from-scratch effort.
   Adds a covering marker to `Node::CompoundIndexScan` (carrying component names,
   since its `field` is the opaque joined identity).
3. **Covered aggregate.** The RFC's headline `GROUP BY` (e.g.
   `SELECT c.status, COUNT(1) … GROUP BY c.status` over compound `(user.id,
   status)`). Adds the B.4 binding-aware collection. Largest win, most analysis.

### B.6 — safety net

A **differential test** (shipped as `covering_index.rs`): assert a covered query
returns exactly what the materialized plan returns. There is no runtime
"covering off" toggle, so the differential runs the same query against an
**indexed** collection (covered) and an **unindexed** copy (full-scan,
materialized) and asserts equal results — the only plan difference is the index,
hence the cover. It pins the single-field covered case (find with `columns`, plus
a range predicate) *and* the bail cases (whole-row `SELECT c`, an unindexed
field), and — for a dotted index (`meta.note`) — the covered nested projection
alongside its parent/extension/sibling bails (`c.meta` / `c.meta.note.deeper` /
`c.meta.tag`). It uses `EXPLAIN` to confirm the covered plan actually drops the
`KeyLookup` while the bail cases keep it (so the differential can't pass vacuously
by never covering). The compound and aggregate cases join it as phases 2–3 land. This is the guard against the one dangerous failure mode (a
too-eager cover returning wrong rows).

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
- No covering over multikey (`[]`) components — the synthesized scalar can't
  reproduce array-distributing access. (Dot-path components *are* covered, by
  nesting the synthesized value under an exact-path-match rule; see phase 1.)
- No streaming/ordered `GROUP BY` (the index returns a fixed leading prefix already
  grouped by the next component) — a real but separate optimization; see
  [Collect Node](./collect-node.md).

## Spike (for the Part B pass)

Phase 1 resolved all three for the single-field case; they recur for phases 2–3:

1. ✅ IR representation settled — `covering: bool` on `Node::IndexScan` (the
   single-field scan names its field). Compound will add a component-name marker
   on `Node::CompoundIndexScan` (B.1).
2. ✅ The conservative referenced-field collector + bail rules shipped as
   `covering.rs`, gated by the differential test (B.6) before the executor
   synthesis was trusted. Since extended to dotted scalar paths via exact-path
   match + nested synthesis (`append_path`), with the same differential guard.
3. ✅ Phase (1) single-field landed and was measured against a clean-`main`
   baseline (−15%/−21% string, −10%/−27% numeric covered projections). Dotted
   single-field paths followed (same synthesize-vs-fetch win, structurally). Phases
   (2) compound and (3) aggregate remain.

## Cosmos, for reference

Covering and recheck-placement are physical-plan choices, invisible in results —
the Cosmos oracle validates the *answers* (unchanged), not the plan, so it guards
correctness here but says nothing about the optimization itself. Related:
[Compound Indexes](./compound-indexes.md), [Index Sargability](./index-sargability.md),
[Unified Numeric Index Key](./unified-numeric-index-key.md).
