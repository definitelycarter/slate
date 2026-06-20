# SQL Support

Slate's SQL surface (`Transaction::query`) targets the **CosmosDB SQL** dialect.
This page tracks coverage against the Cosmos function/keyword/clause set so we
can work through it incrementally.

**Legend:** `[x]` implemented · `[ ]` planned · ⭐ high value / architecturally
fun · 🚧 needs new infrastructure (deferred).

**How we work through it:** one commit per item (or per small batch of
functions). Each scalar function is a new file in `slate-eval/functions/` whose
tests mirror the worked example on its Cosmos doc page
(`learn.microsoft.com/en-us/cosmos-db/query/<name>`), so we track the spec, not
our own guesses. Keywords/clauses touch the parser → AST → planner.

**Type semantics:** type-identity tests (`IS_STRING`, …) are strict by BSON
type; `IS_INTEGER` is a value/range test (Cosmos); comparison coerces numerics
by value. Math functions follow Cosmos's all-`double` numeric model, so they
return a `Double` (`CEILING(0)` → `0.0`) — `ABS` is the exception, preserving
the input's integer type. See `slate-eval/functions/mod.rs`.

## Current surface

`SELECT VALUE <expr> | * | <expr> [AS k], …  FROM <alias>  [JOIN <a> IN <arr>]*
[WHERE …] [GROUP BY …] [ORDER BY … [ASC|DESC]] [OFFSET n] [LIMIT n]`

Done: `FROM`, `WHERE`, `ORDER BY`, `OFFSET`/`LIMIT`, `JOIN … IN`, `SELECT VALUE`,
`SELECT *`, tabular `SELECT a, b [AS c]`, full scalar expressions, object/array
literals, `IN`/`NOT IN`, `BETWEEN`/`NOT BETWEEN`, aggregate functions
(`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`), `GROUP BY`.

---

## Tiers (suggested order)

- **Tier 0** — scalar functions: pure `slate-eval/functions.rs` adds, no
  parser/planner changes. Knock out in batches.
- **Tier 1** — expression sugar (`IN`, `BETWEEN`, `LIKE`, `IIF`, `@params`):
  small parser + lowering work; `IN`/`BETWEEN` are sargable.
- **Tier 2** — aggregates without `GROUP BY` (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`),
  `SELECT DISTINCT`, `TOP`: a new `Aggregate` executor node.
- **Tier 3** — `GROUP BY` + per-group aggregates: AST clause + hash-aggregation
  node.
- **Tier 4** — subqueries (correlated array subqueries first; then `EXISTS` /
  `IN (subquery)`): depends on Tier 2/3.

---

## Scalar functions

### Math (Tier 0)

- [x] `ABS`
- [x] `CEILING`  [x] `FLOOR`  [x] `ROUND`  [x] `TRUNC`  [x] `SIGN`
- [x] `SQRT`  [x] `SQUARE`  [x] `POWER`  [x] `EXP`  [x] `LOG`  [x] `LOG10`
- [x] `PI`  [ ] `RAND` *(non-deterministic — needs the txn's RNG/clock)*
- [ ] `NUMBERBIN` *(round to a multiple)*
- [ ] Trig: `SIN` `COS` `TAN` `COT` `ASIN` `ACOS` `ATAN` `ATN2` `DEGREES` `RADIANS`
- [ ] Integer ops: `INTADD` `INTSUB` `INTMUL` `INTDIV` `INTMOD` *(i64-typed)*
- [ ] Bitwise: `INTBITAND` `INTBITOR` `INTBITXOR` `INTBITNOT` `INTBITLEFTSHIFT` `INTBITRIGHTSHIFT`

### String (Tier 0)

- [x] `CONCAT`  [x] `CONTAINS`  [x] `STARTSWITH`  [x] `LENGTH`  [x] `LOWER`  [x] `UPPER`  [x] `REGEXMATCH`
- [x] `ENDSWITH`  [x] `INDEX_OF`  [x] `SUBSTRING`  [x] `LEFT`  [x] `RIGHT`
- [x] `TRIM`  [x] `LTRIM`  [x] `RTRIM`  [x] `REPLACE`  [x] `REPLICATE`  [x] `REVERSE`
- [x] `STRINGEQUALS`  [ ] `STRINGJOIN`  [ ] `STRINGSPLIT`  [ ] `TOSTRING`
- [ ] Parsing: `STRINGTONUMBER` `STRINGTOBOOLEAN` `STRINGTONULL` `STRINGTOARRAY` `STRINGTOOBJECT`

The string predicates `STARTSWITH`/`ENDSWITH`/`CONTAINS`/`STRINGEQUALS` accept an
optional trailing `true` for a case-insensitive comparison; `INDEX_OF`,
`SUBSTRING`, `LEFT`, and `RIGHT` index by character (not byte).

### Array (Tier 0)

- [x] `ARRAY_CONTAINS`  [x] `ARRAY_LENGTH`
- [x] `ARRAY_CONCAT`  [x] `ARRAY_SLICE`  [x] `ARRAY_CONTAINS_ALL`  [x] `ARRAY_CONTAINS_ANY`
- [x] `CHOOSE`  [x] `SETINTERSECT`  [x] `SETUNION`  [x] `OBJECTTOARRAY`

### Type checking (Tier 0)

- [x] `IS_DEFINED`  [x] `IS_NULL`
- [x] `IS_STRING`  [x] `IS_NUMBER`  [x] `IS_BOOL`  [x] `IS_ARRAY`  [x] `IS_OBJECT`  [x] `IS_PRIMITIVE`
- [x] `IS_INTEGER`  [x] `IS_FINITE_NUMBER`

### Conditional (Tier 1)

- [x] `IIF(cond, a, b)`  *(only the boolean `true` takes the true branch)*  [ ] `??` coalesce operator

### Date & time (Tier 0–1, own batch)

Needs an ISO-8601 ⇄ BSON `DateTime` story; the txn already captures `now_millis`.
- [ ] `GETCURRENTDATETIME` / `…STATIC`  [ ] `GETCURRENTTIMESTAMP` / `…STATIC`  [ ] `GETCURRENTTICKS` / `…STATIC`
- [ ] `DATETIMEADD`  [ ] `DATETIMEDIFF`  [ ] `DATETIMEPART`  [ ] `DATETIMEBIN`  [ ] `DATETIMEFROMPARTS`
- [ ] `DATETIMETOTIMESTAMP`  [ ] `DATETIMETOTICKS`  [ ] `TIMESTAMPTODATETIME`  [ ] `TICKSTODATETIME`

### Item (Tier 1)

- [ ] `DOCUMENTID` *(returns the configured pk value — slate pk path is dynamic)*

---

## Keywords (Tier 1 unless noted)

- [x] `IN (a, b, …)`  ⭐ *(desugars to OR-of-equalities → sargable `IndexMerge(Or)`; `NOT IN` supported)*
- [x] `BETWEEN x AND y`  ⭐ *(desugars to `>= x AND <= y`, inclusive → sargable range; `NOT BETWEEN` supported)*
- [x] `LIKE <pattern> [ESCAPE c]` *(desugars to a safely-escaped, anchored `REGEXMATCH`; `NOT LIKE` supported)*
- [ ] `DISTINCT` (`SELECT DISTINCT …`) *(maps onto the existing `Distinct` node — Tier 2)*
- [ ] `TOP N` *(Cosmos alias for `LIMIT`)*

---

## Clauses

- [x] `FROM`  [x] `WHERE`  [x] `ORDER BY`  [x] `OFFSET … LIMIT`  [x] `SELECT`
- [x] `GROUP BY <expr>, …` — one row per distinct group; `SELECT` and `ORDER BY`
  reference group keys and/or aggregates (`ORDER BY` sorts the group rows, after
  aggregation). An ungrouped non-aggregate column — or `SELECT *` — is rejected,
  matching Cosmos.
- [ ] Subquery  *(Tier 4 — correlated array subqueries first)*

---

## Aggregation functions (Tier 2)

Both whole-result and per-group (`GROUP BY`) aggregation run through the blocking
`Aggregate` executor node.
- [x] `COUNT`  [x] `SUM`  [x] `AVG`  [x] `MIN`  [x] `MAX`

Semantics (Cosmos): `COUNT(expr)` counts defined values (`COUNT(1)` counts all),
empty → `0`. `SUM`/`AVG` skip undefined but a non-numeric *defined* value poisons
the result to undefined; empty → undefined. `MIN`/`MAX` use the shared total
order, preserve the winning value's type, and have no poison rule. `COUNT` →
`Int64`, `SUM`/`AVG` → `Double` (the all-double convention), `MIN`/`MAX` → the
actual value.

---

## Cross-cutting gaps

- [x] **`@params` end-to-end** ⭐ — `query_with_params(cf, collection, sql, params)`
  threads a params document (keys are the bare names, no `@`) through the
  executor into expression evaluation; a referenced-but-unsupplied parameter is
  a hard error (matching Cosmos), not silently undefined.
- [ ] **Numeric literal typing** — SQL integer literals are `Int64`; the index
  path now compares cross-type (correct), but selectivity is a known follow-up
  (canonical numeric index encoding).

---

## Deferred — need new infrastructure 🚧

These require subsystems slate doesn't have yet; out of scope for the SQL pass.

- **Full-text search**: `FULLTEXTCONTAINS` / `…ALL` / `…ANY`, `FULLTEXTSCORE`,
  `RRF`, `ORDER BY RANK` — need a full-text index + BM25 scoring.
- **Spatial**: `ST_AREA` `ST_DISTANCE` `ST_INTERSECTS` `ST_ISVALID`
  `ST_ISVALIDDETAILED` `ST_WITHIN` — need GeoJSON + a spatial index.
- **Vector**: `VECTORDISTANCE` — needs a vector index.
