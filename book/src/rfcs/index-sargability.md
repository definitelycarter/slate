# RFC: Index Sargability (predicate pushdown)

> **Status:** decided — the spike (`tasks/index-sargability-spike.md`) audited the
> full function/operator/subquery surface, validated each "should" against
> `file:line`, and resolved every Open Question. The increments below can be built
> from this document with no further design. **No engine code yet.**

## Problem

Several predicate shapes that *could* be answered from a secondary index instead
run as a full `Scan → Filter`. Worse, the recognisers that decide "can this use an
index?" are added one function at a time (`as_atom`, `as_mongo_eq`,
`as_multikey_eq`, the spatial RFC's planned `as_spatial_predicate`), so each new
case is a bespoke patch and the coverage is uneven. We want one **doctrine** for
what is sargable, one **recogniser** that applies it, and a deliberate decision on
the cases that need an index *capability* we don't have (null, function-of-field).

Concrete trigger, confirmed against the engine (`.explain`, `tags.[]` index
present):

```
SELECT VALUE c FROM c WHERE ARRAY_CONTAINS(c.tags, "db")
  Project c
    Filter ARRAY_CONTAINS(c.tags, "db")
      Scan default_cf.things          ← full scan, despite a tags.[] index
```

The Mongo `{ "tags.[]": "db" }` form *does* use that index (it lowers to
`MultikeyEq`); the SQL `ARRAY_CONTAINS` over the same data does not. That
asymmetry is the whole motivation.

## Current state

Sargable today (`crates/slate-planner/src/sargable.rs`), all confirmed by
`.explain`:

- `x = lit`, `x <,<=,>,>= lit` → `Eq` / `Range` (`as_atom`, `sargable.rs:345`;
  range bounds combined in `field_index_scan`, `sargable.rs:159-220`).
- `x IN (…)` → `IndexMerge(Or)` of `Eq`; `x BETWEEN a AND b` → `Range` (SQL sugar,
  desugared before the planner).
- Mongo `{x: lit}` → the `x = lit OR ARRAY_CONTAINS(x, lit)` idiom (`as_mongo_eq`,
  `sargable.rs:374`), recognised as an `Eq` and **consumed** (no residual recheck:
  a scalar index holds no array entries, so the idiom is vacuously true over the
  candidate set — `sargable.rs:104-114`).
- Mongo `{x.[]: lit}` → `MultikeyEq` (`as_multikey_eq`, `sargable.rs:412`) →
  multikey `Eq`, kept as a residual recheck (`sargable.rs:119-125`).

Every match keeps the original predicate as a **residual `Filter`** — the
`(scan_node, residual)` contract `plan_source` returns (`sargable.rs:39`) — so a
pushdown can be a *conservative superset* and correctness never depends on it,
**with one critical exception** (false negatives, below).

**Indexes are sparse.** `BsonValue::from_raw_bson_ref` / `from_bson`
(`crates/slate-engine/src/encoding/bson_value.rs:87-156`) index only scalar
non-null values — ObjectId, String, Int32/64, Double, DateTime, Boolean; every
other type (`null`, Document, Array, Binary, …) hits the `_ => None` arm
(`bson_value.rs:117`, `:154`) and produces **no entry**. A `.[]` multikey index
indexes each *scalar element* (`extract_all` → `collect_from_value`,
`bson_value.rs:360-428`: the array arm at `:401-406` pushes one entry per scalar
element and silently skips non-scalars); an empty array, a missing field, and an
all-object array all produce nothing. This is the standard sparse-index choice and
is the single most important constraint on this design — it decides the null
family below.

**Superset + recheck has one failure mode: false negatives.** The recheck (residual
`Filter`) re-runs the original predicate over the candidate set, so it removes
false positives for free. It can never *add* a row the index dropped. So the
doctrine holds only while the index is a true *superset* — it may over-return, but
must never under-return. Two engine facts bear on this:

1. **The variable-width string boundary is a known, deferred bug.** An `i` key is
   `i\0{collection}\0{field}\0{value_bytes}{doc_id_lp}` with *no delimiter* between
   value and doc_id. Fixed-width values derive their length from the type byte, but
   **strings locate the boundary by guessing** (`split_trailing_doc_id`), which is
   ambiguous and can mis-decode entries — observed: a `status = "active"` index
   scan undercounting a full scan by 3 on a 52k corpus (`book/src/roadmap.md:24-45`).
   An undercount is a **false negative**, which the recheck cannot repair. This
   gates every string-keyed pushdown (increments B and C, and the string-valued
   slice of A).
2. **Multikey scans must de-duplicate doc-ids.** A `.[]` Eq scan emits one entry
   per matching element, so `tags: ["db","db"]` yields the doc-id twice. `KeyLookup`
   fetches one document per incoming id with no dedup (`key_lookup.rs:21-42`), and a
   lone `IndexScan` doesn't dedup either — only `IndexMerge` does, via a HashSet on
   `id_hash` (`index_merge.rs:34`, `:48`, `:72`). So a multikey pushdown that skips
   dedup over-returns *duplicate rows* — a wrong answer the recheck can't fix
   (it filters values, not multiplicities). See [increment A](#a--multikey-containment).

## The doctrine

> A predicate is sargable iff it reduces to a contiguous **range or point** (or a
> boolean **merge** of them) over some index's key order, with the indexed field
> appearing **raw** on one side and a **constant** on the other — plus the
> **multikey-containment** special case (an `Eq` over a `.[]` element index). A
> *function* is sargable only when it is provably equivalent to such a range/point:
> `STARTSWITH`→range, `ARRAY_CONTAINS`→multikey-`Eq`, `STRINGEQUALS`→`Eq`. Negation,
> a function *wrapping* the indexed field, substrings, suffixes, field-to-field
> comparisons, and anything whose truth needs a value the **sparse** index never
> stored (null/undefined/type) are not. Every pushdown retains the original
> predicate as a residual recheck, so the index is **invisible to results** — it
> changes speed, never which rows return — provided the candidate set is a true
> *superset* (never a false negative, never a duplicate).

## The unified recogniser

Replace the ad-hoc `as_*` helpers with one extensible entry point that returns a
declarative **`IndexAccess`** the planner lowers, rather than building `Node`s
inline:

```rust
/// What an index can do for one predicate. Lowered by `plan_source` into the
/// existing IndexScan / IndexMerge / KeyLookup nodes; the residual plumbing is
/// untouched.
enum IndexAccess {
    /// Point or range scan on one scalar index field.
    Scan { field: String, range: IndexScanRange },
    /// A `.[]` element scan. `dedup` ⇒ collapse duplicate doc-ids before
    /// KeyLookup (one index entry per matching element).
    Multikey { field: String, value: Bson },          // always dedups
    /// Boolean combination: And → IndexMerge(And) (intersect), Or →
    /// IndexMerge(Or) (union). Both dedup doc-ids today.
    Merge { op: LogicalOp, parts: Vec<IndexAccess> },
}

/// `None` → not sargable → Filter. Each arm also reports whether the conjunct is
/// *consumed* (provably exact) or *retained* as a residual recheck.
fn sargable(pred: &Expression, alias: &str, meta: &CollectionMeta)
    -> Option<(IndexAccess, Residual)>;
```

`sargable` dispatches on predicate shape — one arm per recognised case, each with
its own guardrails; the default is `None`. Adding a sargable function becomes "add
an arm," not "thread a new path through the planner." The arms map cleanly onto the
existing helpers: `as_atom` → the `Scan` arm, `as_mongo_eq`/`as_multikey_eq` → the
`Multikey`/`Scan` arms, the OR-merge logic in `index_source_for`
(`sargable.rs:225-279`) → the `Merge` arm. **Spatial fits without a new variant:**
`as_spatial_predicate` computes a covering and returns `Merge { Or, [Scan{Range}, …] }`
(one range per cell) — exactly the `IndexMerge(Or)` of per-cell scans the
[Spatial Index RFC](./spatial-index.md) describes. The recogniser shape is
therefore validated against all five recognised families (Eq, Range, Merge,
Multikey, Spatial) before any increment builds on it.

`Residual` distinguishes the two existing behaviours and is decided per-arm, not
globally: a scalar `Eq` on a scalar index is **consumed** (exact); ranges, multikey
Eq, OR-merges, and every typed string scan are **retained** (the index narrows;
the recheck keeps it precise — and is the safety net for the string-boundary
caveat above). This is exactly today's split (`sargable.rs:104-114` consume vs
`:119-147` retain), now named.

## The complete audit

Walked shape-by-shape across the entire surface: every function in
[`functions.md`](../functions.md), every operator in
[`mongo-operators.md`](../mongo-operators.md), `JOIN … IN`, and the three subquery
kinds. Grouped for legibility; the verdict column is the `IndexAccess` (or the
reason it's `None`), and "guardrail" is the condition that keeps the pushdown a
conservative superset.

### Comparisons, logical structure, sugar (done)

| Shape | Sargable? | IndexAccess | Guardrail / evidence |
|---|---|---|---|
| `x = lit` | yes (consumed) | `Scan Eq` | `as_atom` `sargable.rs:345`; numeric Eq is a full-field scan + `compare_bson` so cross-type `40`/`40.0` match (`index_scan.rs:84-100`, `transaction.rs:288-300`) |
| `x <,<=,>,>= lit`, `BETWEEN` | yes (retained) | `Scan Range` | field raw on one side, const on the other; bounds combined per field `sargable.rs:159-220` |
| `x IN (…)` | yes (retained) | `Merge(Or)` of `Eq` | each branch indexable `index_source_for:225` |
| `a AND b`, `a OR b` | structural | conjunct split / `Merge(Or)` | `flatten_and:501`; top-level OR → `Merge(Or)` iff every branch indexable `:53-60` |
| `NOT <sargable>`, `x != lit` | no | — | anti-scan = full scan; `is_comparison` excludes `Neq` `sargable.rs:483` |
| `field = field` | no | — | no constant side; `as_atom` needs one literal operand `:352-358` |

### Functions that become sargable (the increments)

| Function | Sargable? | IndexAccess | Guardrail / evidence |
|---|---|---|---|
| `ARRAY_CONTAINS(x, lit)` | **A** (retained) | `Multikey{x.[], lit}` | `lit` a **scalar** literal (not object/array — the multikey index stores no object elements, `bson_value.rs:401-406`); **no `partial` 3rd arg** (object-subset match is broader than the indexed elements); `array_contains.rs` compares with `compare_values` (numeric-coercing), which the numeric full-field Eq scan already mirrors |
| `ARRAY_CONTAINS_ANY(x, v1, …)` | **A** (retained) | `Merge(Or)` of `Multikey` | all `vᵢ` scalar literals; union of per-value element scans. Undefined-arg 3-valued cases (`array_contains_any.rs`) don't break the superset: literal args are never undefined, and any `undefined` result is excluded at the output boundary anyway, so the union still covers every *true* row |
| `ARRAY_CONTAINS_ALL(x, v1, …)` | **A** (retained) | `Merge(And)` of `Multikey` | all `vᵢ` scalar literals; intersection of per-value element scans; same undefined-arg reasoning as `_ANY` |
| `STARTSWITH(x, 'pre')` | **B** (retained) | `Scan Range [pre, pre⁺)` | exactly 2 args — the optional `ignoreCase` 3rd arg makes the match case-insensitive (`str_match`, `functions/mod.rs:392-405`), which a case-sensitive index can't bound; `'pre'` a non-empty string literal. See [prefix-range proof](#b--prefix-range) |
| `LIKE 'pre%'` (anchored `^pre…` regex) | **B** (retained) | `Scan Range [pre, pre⁺)` | LIKE desugars to `REGEXMATCH(x, "^pre.*$")` *before* the planner (`parser.rs:741`, confirmed: `LIKE "al%"` → `REGEXMATCH(c.name,"^al.*$")`); sargable iff an anchored literal prefix is extractable and no `(?i)`. See [prefix extraction](#extracting-a-prefix-from-the-regex-form) |
| `STRINGEQUALS(x, lit)` | **C** (retained) | `Scan Eq` | exactly 2 args (no `ignoreCase`); `stringequals.rs` is plain `==`. Trivial once the recogniser exists |

### Functions that stay `Filter` (with the reason)

| Family | Why not sargable |
|---|---|
| `IS_NULL(x)`, `IS_DEFINED(x)`, `NOT IS_DEFINED(x)`, `IS_ARRAY/BOOL/NUMBER/STRING/OBJECT/…` | the rows they select aren't in a **sparse** index (`is_null.rs`/`is_defined.rs` are exact, but `null`/missing/type leave no entry). See [the null decision](#the-sparse-vs-dense-null-question) |
| `CONTAINS(x, sub)` (substring), `ENDSWITH(x, suf)` (suffix), general `REGEXMATCH` | a substring/suffix/arbitrary pattern is not a contiguous prefix range on a forward index (`contains.rs`, `endswith.rs`, `regexmatch.rs`) |
| `UPPER/LOWER/LEFT/RIGHT/SUBSTRING/TRIM/REPLACE/CONCAT/…`, all Math/Integer/bitwise (`ABS`, `CEILING`, `INTADD`, …), all Date/time (`DATETIMEADD`, `DATETIMEPART`, `TICKSTODATETIME`, …) | a function *wrapping* the field breaks key order — `f(x) = c` is not a range on `x`. See [expression indexes](#function-of-field--expression-indexes) (explicit non-goal). Note a *bare* DateTime field still ranges normally; only the conversion functions are opaque |
| `ARRAY_LENGTH(x)=n`, `ARRAY_SLICE/CONCAT`, `SETINTERSECT/UNION` | a derived scalar/array, not a point/range on the stored field |
| `CHOOSE`, `IIF`, `OBJECTTOARRAY`, `TOSTRING`, `STRINGTO*`, `LPUSH/RPUSH/POP` | computed values / write-side helpers, never an indexed field on one side |
| `ST_DISTANCE`, `ST_WITHIN`, `ST_INTERSECTS` | sargable, but via a **spatial covering**, not this recogniser's scalar arms — see the [Spatial Index RFC](./spatial-index.md) |
| `COUNT/SUM/AVG/MIN/MAX` | aggregates over a group, not row predicates |

### Mongo operators

The find front-end translates only a subset; the rest are **hard errors**, so they
never reach the planner (`crates/slate-query/src/translate.rs`).

| Operator | AST it emits | Sargable? |
|---|---|---|
| `{x: v}` / `$eq` (scalar) | `x = v OR ARRAY_CONTAINS(x, v)` (`eq_or_contains:187`) | yes — `Scan Eq`, consumed |
| `{x: null}` | adds a `NOT IS_DEFINED(x)` disjunct (`:187-213`) | no — the null disjunct is non-sargable (sparse) |
| `{"x.[]": v}` | `MultikeyEq` (`multikey_eq:218`) | yes — `Multikey`, retained |
| `$gt/$gte/$lt/$lte` (scalar) | `Binary{cmp, x, v}` (`:242-245`) | yes — `Scan Range` (rejected on `.[]`) |
| `$and`, `$or` | `Binary{And/Or}` (`:72-73`) | structural |
| `$exists` | `IS_DEFINED(x)` / `NOT …` (`translate_exists:302`) | no — sparse |
| `$regex` + `$options` | `REGEXMATCH(x, "(?<opts>)<pat>")` (`translate_regex:261`) | **B** iff anchored literal prefix and no `i` option |
| `$in`, `$nin`, `$ne`, `$not`, `$nor`, `$elemMatch`, `$all`, `$size`, `$type`, `$mod` | — | **unsupported** (`TranslateError::Unsupported`, `:248`) — out of scope until translated. (SQL `IN`/`BETWEEN` are supported and sargable; Mongo `$in` is roadmap.) |

### Subqueries and `JOIN … IN`

`EXISTS (…)`, `ARRAY (…)`, and scalar `(SELECT …)` (`SubqueryKind`,
`slate-ast/src/lib.rs:108-123`) lower to a correlated `Node::Subquery` over a
`Node::CurrentRow` leaf (`lower.rs:406` + `:415-427`). Their `FROM e IN c.arr`
source is an **in-document array unwind** (`Node::Unwind` over `CurrentRow`,
`lower.rs:175-205`), and — decisively — the inner `WHERE` is stored as `residual`
and **never passed to `plan_source`** (contrast `lower.rs:144`, the top-level
container `FROM` that *does* call it). `JOIN … IN` is the same `Node::Unwind`
(`lower.rs:276-311`). So:

| Shape | Sargable? | Why |
|---|---|---|
| `JOIN t IN c.arr WHERE t = v` | no | `c.arr` is the *row's in-memory array*, evaluated per-row by `Unwind`; no secondary index is consulted |
| `EXISTS(SELECT … FROM e IN c.arr WHERE e = v)` | no (as written) | same in-document unwind; the inner `e = v` never reaches sargability |
| `ARRAY(…)`, scalar `(SELECT …)` | no | correlated apply; per-row array iteration |

This answers the interplay question (below): the inner array of a subquery and a
collection multikey index are **different objects**, so they do not converge at the
recogniser. An optional *rewrite* is the only bridge.

## A — multikey containment

`ARRAY_CONTAINS` / `_ANY` / `_ALL` over a `.[]`-indexed path. The element index
already exists and already serves the Mongo `MultikeyEq` form; increment A reaches
it from SQL. Two design points the audit surfaced:

**Index naming.** SQL `ARRAY_CONTAINS(c.tags, 'x')` carries the field path `tags`
(no `[]`), whereas the multikey index is registered as `tags.[]` and the Mongo
form carries `tags.[]` verbatim. The recogniser's `ARRAY_CONTAINS` arm must derive
the index name `tags.[]` from the path `tags` (check `meta.indexes` for
`"{path}.[]"`); the Mongo arm matches verbatim. They converge on the same
`Multikey{ field: "tags.[]", value }` afterwards.

**Dedup is part of A, not a follow-up.** As noted above, a single `.[]` Eq scan can
yield a doc-id once per matching element, and nothing downstream of a lone
`IndexScan → KeyLookup` de-duplicates (`key_lookup.rs:21-42`). So
`IndexAccess::Multikey` must lower to a doc-id-deduped id stream before `KeyLookup`.
The cheapest correct lowering reuses the `IndexMerge` HashSet dedup
(`index_merge.rs:34`): route even a *single* multikey scan through a degenerate
merge (or a small `DistinctById` over the id stream). `_ANY`/`_ALL` already lower
to `Merge(Or)`/`Merge(And)`, which dedup by construction — so the only new dedup
need is the single-`ARRAY_CONTAINS` case. **This also fixes a latent bug in today's
`MultikeyEq` path**, which has the same lone-scan shape and would return
`tags:["db","db"]` twice; increment A should land with a test for duplicate
elements.

**Numeric coercion is already handled.** `ARRAY_CONTAINS(arr, 7)` matches `7`,
`7.0`, and a stored `Int32`/`Int64`/`Double` alike (`compare_values`), and the
three numeric types encode to *different* sortable keys
(`bson_value.rs:19-47`). A tight `Eq` probe would miss cross-type numerics — but
the executor already turns a numeric `Eq` into a *full-field scan + `compare_bson`
post-filter* precisely for this (`index_scan.rs:84-100`, `transaction.rs:288-300`),
so `Multikey{value: Int64(7)}` over the element index sweeps every numeric element
and the recheck keeps it exact. No new work; just don't assume a point probe for
numbers.

**Size.** Small-to-moderate. The recogniser arms + index-name derivation + the
single-scan dedup wrapper. No encoding change. This is the increment that gives the
recogniser refactor its second real case, so land them together.

## B — prefix range

`STARTSWITH(x, 'pre')` and `LIKE 'pre%'` → a half-open string range
`[pre, pre⁺)` on the `x` index, with the original predicate retained as the recheck.
Today both full-scan (confirmed by `.explain`).

### The prefix-range proof

String index keys store **raw UTF-8 value bytes** with no length prefix, no
terminator, and no type tag in the key (`bson_value.rs:93-96`); the `Range` scan
byte-compares each entry's full `value_bytes` against the bound's bytes
(`transaction.rs:362-386`, bound bytes from `BsonValue::from_bson(v).bytes`,
`:307`/`:311`). On that substrate:

1. **String-prefix ⟺ byte-prefix.** UTF-8 is order-preserving and a string prefix
   is exactly a byte prefix, so "value starts with `P`" ⟺ "`value_bytes` starts
   with `bytes(P)`".
2. **Lower bound (inclusive `P`).** Every value starting with `P` is
   `bytes(P)` ++ suffix ≥ `bytes(P)`, so an inclusive lower bound at `bytes(P)`
   admits them all (and `"pre"` itself: its `value_bytes` *equals* the bound).
3. **Upper bound `pre⁺` = `bytes(P)` with the final byte + 1, exclusive.** For any
   true match, byte `k-1` equals `P`'s final byte, which is strictly less than
   `+1` — so every true match sorts *before* `pre⁺`. **No true match is dropped.**
4. **The carry edge is vacuous.** "Increment the final byte" only fails if that
   byte is `0xFF` — but **no UTF-8 byte is ever `0xFF`** (lead bytes ≤ `0xF4`,
   continuation bytes ≤ `0xBF`). So the increment never carries; the all-`0xFF`
   hazard cannot arise for a valid string literal. The only degenerate case is the
   **empty prefix** (`P = ""`), which has no byte to increment and means "every
   string" — lower bound only, the recheck does the rest.

The pushdown is therefore *exact* on a correct string index, not merely a superset
— but we retain the recheck regardless, because of the string-boundary caveat
(§Current state). **B is gated on that boundary fix:** it is a false-negative bug,
which the recheck cannot mask. B should land after (or together with) the
deterministic variable-width boundary (roadmap: store the value length, or
length-suffix the doc_id).

**Representation.** `pre⁺` may not be valid UTF-8 (incrementing a `0xBF`
continuation byte yields `0xC0`, an invalid lead byte), so the planner cannot
always express it as `Bson::String` for the existing `IndexScanRange::Range`. Add a
dedicated `IndexScanRange::StringPrefix(String)` that the *executor* lowers to the
byte-level lower/upper bounds (it already byte-compares, so this is a few lines and
keeps the planner from fabricating non-UTF-8 BSON). This is the only new
representation B needs.

### Extracting a prefix from the regex form

LIKE is gone before the planner — `x LIKE 'pre%'` is `REGEXMATCH(x, "^pre.*$")`
(`parser.rs:741` `like_to_regex`; `%`→`.*`, `_`→`.`, literals regex-escaped via
`push_regex_literal:796`, anchored `^…$`, **no `(?i)`** — LIKE is case-sensitive).
A `REGEXMATCH`-prefix arm walks the pattern: require a leading `^`, then accumulate
a literal run, stopping at the first regex metacharacter. The run terminates at
`.* `/`.` /`[`/`(`/`|`/`+`/`?`/`{`/`$`; an *escaped* metacharacter (`\.`, `\(`)
contributes its literal char and continues. Disqualify outright on a leading `(?i)`
(case-insensitive ⇒ unsound on a case-sensitive index) or if the run is empty
(`^.*…`, a non-anchored pattern). The extracted run is the prefix `P` → the same
`StringPrefix` range.

**Recommendation:** make `STARTSWITH(x, lit)` the **primary** B recogniser — it is
explicit, two-token, and unambiguous — and treat regex-prefix extraction as a
**secondary** arm for the LIKE path. Both feed the identical `StringPrefix` range,
so the regex arm is purely a front-end convenience, not a separate mechanism.

## C — `STRINGEQUALS` → `Eq`

`STRINGEQUALS(x, lit)` (2-arg) is plain string equality (`stringequals.rs`), so it
lowers to `Scan Eq(String)` — trivial once the recogniser exists. Guardrail: no
`ignoreCase` 3rd arg. Like B, it rides the string index path and so inherits the
boundary caveat (retained recheck; gated on the boundary fix for soundness). The
3-arg case-insensitive form stays a `Filter`.

## The sparse-vs-dense null question

**Decision: keep indexes sparse. `IS_NULL` / `IS_DEFINED` / `$exists` stay
`Filter`s.**

`IS_NULL(x)` is true only for stored JSON `null`; `IS_DEFINED(x)` is true for any
present value; `NOT IS_DEFINED(x)` for a missing field. All three select rows a
sparse index *has no entry for* (`bson_value.rs:117`/`:154` → `None` for null;
missing fields and empty arrays produce nothing). The functions themselves are
exact (`is_null.rs`, `is_defined.rs`); the gap is purely that the index doesn't
record absence.

Making them index-backed means a **dense** index: a `null` sentinel, and — to
answer `IS_DEFINED` over non-scalar values — a "present but non-scalar" sentinel,
with semantics calls at every edge (does a `tags.[]` index store anything for an
empty array? a missing field? `null` elements?). That is a new encoding arm, more
storage and write amplification on *every* document, and a re-index migration —
all to accelerate predicates that are typically low-selectivity (most documents
*have* the field, so `IS_DEFINED` returns most of the collection and a full scan is
near-optimal anyway). Cosmos, as an oracle, doesn't force our hand here: it tells
us which *rows* match `IS_NULL`, not how to index it, and the pushdown is invisible
to results regardless. **Keep sparse** unless a concrete workload shows a
high-selectivity `IS_NULL`/`NOT IS_DEFINED` hot path; revisit then with a dense
*partial* index (index only the null/absent rows) rather than densifying every
entry. The roadmap's Partial Indexes work is the natural vehicle if that day comes.

## Function-of-field / expression indexes

**Decision: explicit non-goal for this RFC.** `UPPER(x) = 'A'`, `ABS(x) = 5`,
`DATETIMEPART('yyyy', x) = 2024`, `ARRAY_LENGTH(x) = 3` all wrap the field in a
computation that destroys the index's key order, so no scalar index on `x` can
serve them. The general fix is an **expression index** (index the computed
`UPPER(x)` and recognise the same expression in the predicate), which is its own
feature: a new index kind, an expression-equality check in the planner, and index
maintenance that evaluates the expression on write. It composes with the
recogniser (an expression index would add a `Scan` arm keyed by the matched
expression) but needs nothing from increments A–C. Stated here so it stops
resurfacing: **out of scope absent a dedicated expression-index RFC.** (The
roadmap's "custom index key extractors" hook is the closest existing seam.)

## JOIN-IN / subquery interplay

Three sub-questions from the charter, decided:

**Should `EXISTS(… FROM e IN c.arr WHERE e = v)` and `ARRAY_CONTAINS(c.arr, v)`
converge on one recogniser?** **No.** They operate on different objects: the
subquery iterates the *row's in-memory array* (`Unwind` over `CurrentRow`,
`lower.rs:175-205`), while `ARRAY_CONTAINS` (under increment A) consults the
*collection's multikey secondary index*. The subquery's inner `WHERE` never reaches
`plan_source`, so there is nothing at the sargability layer to unify.

**Should the planner rewrite one into the other?** **Optionally, later — not in
A–C.** A top-level, uncorrelated-shape `EXISTS(SELECT VALUE e FROM e IN c.arr WHERE
e = <literal>)` *is* semantically `ARRAY_CONTAINS(c.arr, <literal>)`, and an
algebraic pre-planner rewrite could route it into increment A's recogniser and pick
up the multikey index. But it is a narrow pattern (anchored on `e = literal`, no
other correlation), it needs A's recogniser to exist first, and the safe rewrite
conditions (the subquery is `EXISTS`, the array is a direct member path, the body
is a single element-equality) want their own careful proof. Document it as a future
optimisation; build A first.

**Where does dedup for an unwound multikey source belong?** For the *index* path,
at the multikey-scan → `KeyLookup` boundary, reusing the `IndexMerge` HashSet dedup
(`index_merge.rs:34/48/72`) — see [increment A](#a--multikey-containment). For the
*subquery/JOIN* path it does **not** arise: `Unwind` deliberately emits one row per
element (`unwind.rs`), and Cosmos semantics keep those as distinct rows
(`SELECT … JOIN t IN c.tags` yields one row per tag), so there is nothing to dedup
there. The two paths never share a dedup site because they never share a source.

## Cosmos metrics oracle

**Decision: no — do not wire Cosmos query metrics into `tools/cosmos-parity`.**

Two independent reasons:

1. **The emulator can't report them.** The parity harness runs against the Cosmos
   *Linux emulator* via `cosmoshell.sh`, capturing only the `items` array
   (`tools/cosmos-parity/run.py`). The emulator treats index policy as a **no-op**
   and implements no RU metering or index-utilization diagnostics
   (`.claude/skills/cosmos-parity/SKILL.md`), so there are no index-hit-ratio /
   retrieved-vs-output counts to capture. Getting real index metrics would require
   *live Azure Cosmos*, which the harness deliberately doesn't use.
2. **There's nothing for the oracle to validate.** Because a pushdown is invisible
   to results, the oracle's only job is "same rows as a full scan" — which the
   recheck already guarantees. Cosmos is a *validation oracle, not a spec*: it
   confirms *which rows* match a predicate, never *how* an engine should index it.
   Metrics would at best sanity-check our audit's "should index?" column against
   Cosmos's choices — a one-time curiosity, not a regression gate, and not worth
   ~1–2 days of plumbing against an emulator that can't supply the numbers anyway.

If we ever want that cross-check, the cheaper path is a handful of manual queries
against a live Azure account while designing an increment — not a permanent harness
feature.

## Increment sequencing

1. **Recogniser refactor + A (multikey containment), together.** The refactor needs
   a real second case to prove the `IndexAccess` shape, and A supplies it. A is
   fully understood (already scoped on `feat/array-contains-index`), needs no
   encoding change, and its dedup requirement also closes a latent `MultikeyEq` bug.
   *Size: small–moderate.*
2. **The variable-width string-boundary fix** (roadmap §"Index Key Value/Doc-Id
   Boundary") is a **prerequisite for B and C**, because string pushdowns can
   false-negative and the recheck can't repair that. It is an encoding change with a
   re-index migration, so it is its own change — but B/C must not ship before it.
   *Size: moderate (encoding + migration).*
3. **B (prefix range).** `STARTSWITH` primary + LIKE-regex-prefix secondary, on a
   new `IndexScanRange::StringPrefix`. The highest-frequency win after A. *Size:
   small–moderate (one range variant + two recogniser arms + the executor byte-bound
   lowering).*
4. **C (`STRINGEQUALS` → `Eq`).** Trivial once the recogniser and the string path
   are in place. *Size: tiny.*

## Recommendation

Land the **recogniser refactor + increment A** first (A is fully understood and
gives the refactor its proving case, and its dedup work fixes a latent multikey
bug). Sequence **B and C after the variable-width string-boundary fix**, since their
soundness depends on it (false negatives, which the recheck cannot mask). Keep
indexes **sparse** — the null family stays `Filter` — pending a workload that
justifies a dense *partial* index. Treat **function-of-field / expression indexes**
as out of scope absent a dedicated RFC. Do **not** add Cosmos metrics capture: the
emulator can't supply them and the pushdown is invisible to results, so there is
nothing to validate. Leave **EXISTS-over-array → ARRAY_CONTAINS** as a documented
future rewrite, not part of A–C.
