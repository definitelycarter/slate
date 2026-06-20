# SQL Support

Slate's SQL surface (`Transaction::query`) targets the **CosmosDB SQL** dialect.
This page tracks coverage against the Cosmos function/keyword/clause set so we
can work through it incrementally.

**Legend:** `[x]` implemented · `[ ]` planned · ⭐ high value / architecturally
fun · 🚧 needs new infrastructure (deferred).

**How we work through it:** one commit per item (or per small batch of
functions), each to Cosmos semantics with tests. Scalar functions are pure adds
to `slate-eval/functions.rs`; keywords/clauses touch the parser → AST → planner.

## Current surface

`SELECT VALUE <expr> | * | <expr> [AS k], …  FROM <alias>  [JOIN <a> IN <arr>]*
[WHERE …] [ORDER BY … [ASC|DESC]] [OFFSET n] [LIMIT n]`

Done: `FROM`, `WHERE`, `ORDER BY`, `OFFSET`/`LIMIT`, `JOIN … IN`, `SELECT VALUE`,
`SELECT *`, tabular `SELECT a, b [AS c]`, full scalar expressions, object/array
literals.

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
- [ ] `CEILING`  [ ] `FLOOR`  [ ] `ROUND`  [ ] `TRUNC`  [ ] `SIGN`
- [ ] `SQRT`  [ ] `SQUARE`  [ ] `POWER`  [ ] `EXP`  [ ] `LOG`  [ ] `LOG10`
- [ ] `PI`  [ ] `RAND` *(non-deterministic — needs the txn's RNG/clock)*
- [ ] `NUMBERBIN` *(round to a multiple)*
- [ ] Trig: `SIN` `COS` `TAN` `COT` `ASIN` `ACOS` `ATAN` `ATN2` `DEGREES` `RADIANS`
- [ ] Integer ops: `INTADD` `INTSUB` `INTMUL` `INTDIV` `INTMOD` *(i64-typed)*
- [ ] Bitwise: `INTBITAND` `INTBITOR` `INTBITXOR` `INTBITNOT` `INTBITLEFTSHIFT` `INTBITRIGHTSHIFT`

### String (Tier 0)

- [x] `CONCAT`  [x] `CONTAINS`  [x] `STARTSWITH`  [x] `LENGTH`  [x] `LOWER`  [x] `UPPER`  [x] `REGEXMATCH`
- [ ] `ENDSWITH`  [ ] `INDEX_OF`  [ ] `SUBSTRING`  [ ] `LEFT`  [ ] `RIGHT`
- [ ] `TRIM`  [ ] `LTRIM`  [ ] `RTRIM`  [ ] `REPLACE`  [ ] `REPLICATE`  [ ] `REVERSE`
- [ ] `STRINGEQUALS`  [ ] `STRINGJOIN`  [ ] `STRINGSPLIT`  [ ] `TOSTRING`
- [ ] Parsing: `STRINGTONUMBER` `STRINGTOBOOLEAN` `STRINGTONULL` `STRINGTOARRAY` `STRINGTOOBJECT`

### Array (Tier 0)

- [x] `ARRAY_CONTAINS`  [x] `ARRAY_LENGTH`
- [ ] `ARRAY_CONCAT`  [ ] `ARRAY_SLICE`  [ ] `ARRAY_CONTAINS_ALL`  [ ] `ARRAY_CONTAINS_ANY`
- [ ] `CHOOSE`  [ ] `SETINTERSECT`  [ ] `SETUNION`  [ ] `OBJECTTOARRAY`

### Type checking (Tier 0)

- [x] `IS_DEFINED`  [x] `IS_NULL`
- [ ] `IS_STRING`  [ ] `IS_NUMBER`  [ ] `IS_BOOL`  [ ] `IS_ARRAY`  [ ] `IS_OBJECT`  [ ] `IS_PRIMITIVE`
- [ ] `IS_INTEGER`  [ ] `IS_FINITE_NUMBER`

### Conditional (Tier 1)

- [ ] `IIF(cond, a, b)`  ⭐ *(also expose `??` coalesce operator)*

### Date & time (Tier 0–1, own batch)

Needs an ISO-8601 ⇄ BSON `DateTime` story; the txn already captures `now_millis`.
- [ ] `GETCURRENTDATETIME` / `…STATIC`  [ ] `GETCURRENTTIMESTAMP` / `…STATIC`  [ ] `GETCURRENTTICKS` / `…STATIC`
- [ ] `DATETIMEADD`  [ ] `DATETIMEDIFF`  [ ] `DATETIMEPART`  [ ] `DATETIMEBIN`  [ ] `DATETIMEFROMPARTS`
- [ ] `DATETIMETOTIMESTAMP`  [ ] `DATETIMETOTICKS`  [ ] `TIMESTAMPTODATETIME`  [ ] `TICKSTODATETIME`

### Item (Tier 1)

- [ ] `DOCUMENTID` *(returns the configured pk value — slate pk path is dynamic)*

---

## Keywords (Tier 1 unless noted)

- [ ] `IN (a, b, …)`  ⭐ *(sargable → `IndexMerge(Or)`)*
- [ ] `BETWEEN x AND y`  ⭐ *(sargable → range)*
- [ ] `LIKE <pattern>` *(lower to regex)*
- [ ] `DISTINCT` (`SELECT DISTINCT …`) *(maps onto the existing `Distinct` node — Tier 2)*
- [ ] `TOP N` *(Cosmos alias for `LIMIT`)*

---

## Clauses

- [x] `FROM`  [x] `WHERE`  [x] `ORDER BY`  [x] `OFFSET … LIMIT`  [x] `SELECT`
- [ ] `GROUP BY` + aggregates  *(Tier 3 — new hash-aggregation node)*
- [ ] Subquery  *(Tier 4 — correlated array subqueries first)*

---

## Aggregation functions (Tier 2)

Need an `Aggregate` executor node (whole-result first, then per-group with
`GROUP BY`).
- [ ] `COUNT`  [ ] `SUM`  [ ] `AVG`  [ ] `MIN`  [ ] `MAX`

---

## Cross-cutting gaps

- [ ] **`@params` end-to-end** ⭐ — the AST already has `Parameter`; thread a
  params document through `slate-executor` and add a `query_with_params` API.
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
