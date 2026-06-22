# RFC: Index Sargability (predicate pushdown)

**Status:** Draft — seeded from a first-pass audit. The Open Questions are a spike
charter (`tasks/index-sargability-spike.md`); the spike validates and completes this.

## Problem

Several predicate shapes that *could* be answered from a secondary index instead run
as a full `Scan → Filter`. Worse, the recognisers that decide "can this use an
index?" are added one function at a time (`as_atom`, `as_mongo_eq`, `as_multikey_eq`,
the spatial RFC's planned `as_spatial_predicate`), so each new case is a bespoke patch
and the coverage is uneven. We want one **doctrine** for what is sargable, one
**recogniser** that applies it, and a deliberate decision on the cases that need an
index *capability* we don't have (null, function-of-field).

Concrete trigger: `ARRAY_CONTAINS(c.tags.[], 'x')` full-scans even though a multikey
index on `tags.[]` exists and the Mongo `{tags.[]: 'x'}` form already uses it.

## Current state

Sargable today (`slate-planner/src/sargable.rs`):

- `x = lit`, `x <,<=,>,>= lit` → `Eq` / `Range` (`as_atom`)
- `x IN (…)` → `IndexMerge(Or)` of `Eq`; `x BETWEEN a AND b` → `Range` (sugar)
- Mongo `{x: lit}`, the `x = lit OR ARRAY_CONTAINS(x, lit)` idiom (`as_mongo_eq`)
- Mongo `{x.[]: lit}` multikey equality (`as_multikey_eq`)

Every match keeps the original predicate as a **residual `Filter`** — the
`(scan_node, residual)` contract `plan_source` returns — so a pushdown can be a
*conservative superset* and correctness never depends on it.

**Indexes are sparse.** `from_raw_bson_ref` / `from_bson` index only scalar non-null
values (String, Int32/64, Double, DateTime, Boolean, ObjectId); `null`, missing,
arrays, and objects produce **no entry** (a `.[]` multikey index indexes each scalar
*element*; `index_record.rs`: "sparse by construction"). This is the standard
sparse-index choice and is the single most important constraint on this design — it
decides the null family below.

## Audit — first pass (the spike validates and extends this)

| Shape | Sargable? | Lowers to | Status |
|---|---|---|---|
| `x = lit`, `x <,<=,>,>= lit`, `BETWEEN`, `IN` | yes | Eq / Range / Merge(Or) | done |
| Mongo `{x:lit}`, `{x.[]:lit}`, `=…OR ARRAY_CONTAINS` idiom | yes | Eq / multikey | done |
| **`ARRAY_CONTAINS(x.[], lit)`** | should | multikey `Eq` | **gap — increment A** |
| **`ARRAY_CONTAINS_ANY(x.[], …)`** | should | `IndexMerge(Or)` | gap — A |
| **`ARRAY_CONTAINS_ALL(x.[], …)`** | should | `IndexMerge(And)` | gap — A |
| **`STARTSWITH(x,'pre')`, `LIKE 'pre%'`, anchored `^pre` regex** | should | `Range [pre, pre⁺)` | **gap — increment B** |
| `STRINGEQUALS(x, lit)` | maybe | `Eq` | gap — C (trivial) |
| `IS_NULL(x)`, `IS_DEFINED(x)`, `NOT IS_DEFINED(x)` | **no** — sparse | — | needs dense/sentinel index (decision) |
| `NOT <sargable>` (negation) | no | anti-scan = full scan | Filter |
| `UPPER(x)=…`, `ABS(x)=…` (function-of-field) | no | breaks key order | needs expression index (future) |
| `CONTAINS(x, sub)` (substring), `field = field`, `ARRAY_LENGTH(x)=n` | no | not a range/point | Filter |
| `ST_DISTANCE` / `ST_WITHIN` | yes (planned) | covering scan | see [Spatial Index](./spatial-index.md) |

## Doctrine

> A predicate is sargable iff it reduces to a contiguous **range or point** (or a
> boolean **merge** of them) over some index's key order, with the indexed field
> appearing **raw** on one side and a **constant** on the other — plus the
> **multikey-containment** special case (an `Eq` over a `.[]` element index). A
> *function* is sargable only when it is provably equivalent to such a range/point:
> `STARTSWITH`→range, `ARRAY_CONTAINS`→multikey-`Eq`, `STRINGEQUALS`→`Eq`. Negation, a
> function *wrapping* the indexed field, substrings, and field-to-field comparisons
> are not. Every pushdown retains the original predicate as a residual recheck, so the
> index is **invisible to results** — it changes speed, never which rows return.

## Unified recogniser

Replace the ad-hoc `as_*` helpers with one extensible entry point —
`sargable(pred, indexes) -> Option<IndexAccess>` — dispatching on predicate shape and
returning an `IndexAccess` built from the shared primitives (`Eq`, `Range`,
`Merge{And,Or}`, `MultikeyEq`). Each recognised shape is one arm with its own
guardrails; the default is `None` → Filter. Adding a sargable function becomes "add an
arm," not "thread a new path through the planner." `plan_source` consumes `IndexAccess`
exactly as it consumes today's atoms, so the residual/recheck plumbing is untouched.
(The spatial RFC's `as_spatial_predicate` becomes one such arm.)

## Increments

- **A — multikey containment.** `ARRAY_CONTAINS` / `_ANY` / `_ALL` → the existing
  multikey machinery. Already scoped: branch `feat/array-contains-index`.
- **B — prefix range.** `STARTSWITH` / `LIKE 'pre%'` / anchored literal-prefix
  `REGEXMATCH` → a string range scan. The high-frequency one after A.
- **C — `STRINGEQUALS` → `Eq`.** Trivial once the recogniser exists.
- **Recogniser refactor** underpins all three and should land first (or with A, which
  gives it a real second case to prove its shape).

## The sparse-vs-dense null question

`IS_NULL`, `IS_DEFINED`, `NOT IS_DEFINED` cannot use a sparse index — the rows they
select aren't in it. Making them index-backed means indexing `null` (and a sentinel
for "present, non-scalar"?), i.e. **dense** indexes: more storage, a new encoding arm,
and semantics calls (does a `tags.[]` index store anything for an *empty* array? a
missing field?). Lean: **keep sparse**, `IS_NULL` / `IS_DEFINED` stay Filters — unless
the spike finds a workload that needs them. Decide explicitly.

## Cosmos, for reference only

Cosmos has no `EXPLAIN` statement; it exposes **query metrics** (index hit ratio,
`Retrieved` / `Output Document Count`, index lookup time) via response diagnostics. The
`tools/cosmos-parity` harness captures *results* but not these metrics today. As
elsewhere, Cosmos is a *validation oracle, not a spec*: because a pushdown is invisible
to results, there is nothing for the oracle to validate about the *mechanism* beyond
"same rows as a full scan" — which the recheck guarantees. The metrics are useful only
as a cross-check on *which predicates Cosmos itself bothers to index* (does it index
`STARTSWITH`? `ARRAY_CONTAINS`?), to sanity-check the audit's "should" column. The
spike evaluates whether wiring those metrics into the harness is worth it.

## Open questions — the spike charter

1. **Complete the audit.** Walk the *entire* `functions.md` surface, the Mongo
   operators (`mongo-operators.md`), `JOIN … IN`, and subqueries (`EXISTS` / `ARRAY` /
   scalar). For each: sargable? → which access? → the guardrail that keeps it a
   conservative superset. Promote/demote rows in the table with `file:line` evidence.
2. **JOIN-IN / subquery interplay.** Can `EXISTS(SELECT … FROM e IN c.tags WHERE e =
   v)` be recognised as the same multikey lookup as `ARRAY_CONTAINS`? Should the planner
   *rewrite* one into the other, or recognise both at the sargability layer? Where does
   dedup for an unwound source belong (`index_merge.rs` dedups by doc_id)?
3. **Prefix-range encoding.** The upper bound for `STARTSWITH('pre')` is "`pre` with the
   last byte incremented" on raw UTF-8 — confirm the string range scan handles it
   (multi-byte chars, the all-`0xFF` edge). How is an anchored prefix extracted from a
   `REGEXMATCH` pattern (LIKE desugars to regex *before* the planner)?
4. **The null decision.** Sparse vs dense — recommend one, with the storage/semantics
   cost and any workload that would justify dense.
5. **Function-of-field / expression indexes.** Is there appetite for expression indexes
   (index `UPPER(x)`)? If not, state it as a non-goal so it stops resurfacing.
6. **Cosmos metrics oracle.** Can/should the parity harness capture Cosmos query metrics
   to cross-check the audit? Cost vs value.
7. **Recogniser shape.** Validate the `sargable() -> IndexAccess` design against all
   recognised shapes (including spatial) before the increments build on it.

## Recommendation

Land the **recogniser refactor + increment A** together (A is fully understood, and the
refactor needs a real second case to prove its shape), then **B**, then **C**. Keep
indexes **sparse** (the null family stays Filter) pending a workload that says
otherwise. Treat function-of-field as out of scope absent an expression-index RFC.
