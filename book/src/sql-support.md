# SQL Reference

Slate's SQL surface (`collection.query(sql)`) implements the **CosmosDB SQL** dialect.
This page covers the grammar, keywords, and clauses; for the built-in scalar and
aggregate functions, see the [Function Reference](./functions.md). For the query
model and plan shapes, see [Querying](./querying.md).

## Grammar

```
SELECT [DISTINCT] [TOP n] VALUE <expr> | * | <expr> [AS k], …
[FROM <alias> [JOIN <a> IN <arr>]*]
[WHERE <expr>]
[GROUP BY <expr>, …]
[HAVING <expr>]
[ORDER BY <expr> [ASC|DESC], …]
[OFFSET n] [LIMIT n]
```

The `FROM` clause names only the **row alias**; the container is the collection
the handle names (matching Cosmos, where the container is external to the query
text). `WHERE` is the full scalar grammar (operators, function calls,
object/array literals) plus the `IN` / `BETWEEN` / `LIKE` predicate forms below.
`@name` placeholders are supplied via the `.params(params)` stage —
`query(sql).params(doc! { ... })`.

## Type semantics

- **Type-identity tests** (`IS_STRING`, …) are strict by BSON type; `IS_INTEGER`
  is a value/range test (Cosmos semantics); comparison coerces numerics by value.
- **Number model:** every number is a 64-bit double, so math functions return a
  `Double` (`CEILING(0)` → `0.0`) — `ABS` is the exception, preserving the input's
  integer type, and the `INT*`/`INC` functions return integers. A stored
  `Decimal128` reads as its double value, so it is a number everywhere:
  comparison, `SUM`/`AVG`, and `IS_NUMBER` all treat it as one. See the
  [Function Reference](./functions.md) for the per-function detail.
- A **type mismatch yields `undefined`**, which is omitted from a projection and
  excludes a row from a `WHERE` (rather than erroring).

## Keywords

- `IN (a, b, …)` — desugars to an OR of equalities → sargable `IndexMerge(Or)`; `NOT IN` supported.
- `BETWEEN x AND y` — desugars to `>= x AND <= y` (inclusive) → sargable range; `NOT BETWEEN` supported.
- `LIKE <pattern> [ESCAPE c]` — desugars to a safely-escaped, anchored `REGEXMATCH` (the wildcards `%`/`_` and `[…]`/`[^…]` sets become regex constructs; every other character is escaped to a literal, so a pattern is never a regex-injection vector); `NOT LIKE` supported.
- `DISTINCT` (`SELECT DISTINCT …`) — dedups whole projected rows (an array value counts as one value; no Mongo-style flattening).
- `TOP n` — a result cap; follows `DISTINCT`, mutually exclusive with `OFFSET`/`LIMIT`.

## Clauses

- **`SELECT`** — three projection forms: `VALUE <expr>` (one bare value per row), `*` (the whole document), and tabular `<expr> [AS k], …` (a document of the selected columns). Tabular keys follow Cosmos: the last path segment of a member access, an explicit `AS`, or a positional `$1`/`$2`/… for an unnamed computed column; two columns resolving to the same key are a parse error.
- **`FROM` is optional** (matching Cosmos): a FROM-less query (`SELECT VALUE 1`, `SELECT 1 AS a, 2 AS b`) evaluates the `SELECT` once over a single implicit row. Only `SELECT *` requires a `FROM`.
- **`JOIN <a> IN <array-expr>`** — cross-joins each document with the elements of one of its in-document arrays (it does not join another container).
- **`WHERE` / `ORDER BY` / `OFFSET … LIMIT`** — standard; `ORDER BY` takes multiple keys with per-key `ASC`/`DESC`.
- **`GROUP BY <expr>, …`** — one row per distinct group; `SELECT`, `HAVING`, and `ORDER BY` may reference only the group-key expressions or aggregates (`ORDER BY` sorts the resulting group rows). An ungrouped non-aggregate column — or `SELECT *` — is rejected, matching Cosmos.
- **`HAVING <expr>`** — a post-aggregation filter over the group rows (runs after `GROUP BY`, before `ORDER BY`). Unlike `WHERE` (which filters input rows before grouping), `HAVING` is grounded against the group keys and aggregates, exactly like the projection — a bare ungrouped column is rejected. A `HAVING` implies grouping even without a `GROUP BY` (so `SELECT VALUE COUNT(1) FROM c HAVING COUNT(1) > 0` is valid), and an aggregate used only in `HAVING` is still computed.
- **Subqueries** range over an *in-document array* (`FROM x IN <array>`), never another container. Three forms: a scalar `(SELECT …)`, `EXISTS (…)`, and `ARRAY (…)`. They appear in `SELECT`, `WHERE`, and as a `JOIN` source; may be **correlated** (referencing the outer row) or uncorrelated, and may nest to any depth. A subquery whose `FROM` names an outer alias is item-scoped (iterates that single bound value), matching Cosmos.
- **Subroot `FROM <base>.<path> [AS] <alias>`** — scopes iteration to a sub-path of each document; the alias binds to the whole sub-value (no unwinding), dropping documents where the path is undefined. The alias is optional (defaults to the last path segment).

## Aggregation

`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, and `ARRAY_AGG` (synonym `COLLECT`) collapse
the matching rows (or each `GROUP BY` group) to one result row, via the blocking
`Aggregate` node. A query that aggregates always emits one row per group, so
`COUNT` over an empty set is `0` and `ARRAY_AGG` over an empty set is `[]`. See the
[Function Reference](./functions.md#aggregate-functions) for the per-function
skip/poison/type rules.

## Non-deterministic functions

`GETCURRENTTIMESTAMP()` (with the `GETCURRENTDATETIME`/`GETCURRENTTICKS` variants)
and `RAND()` can't be answered from the row data, so they read from sources the
host injects when it opens the database — there is no syscall in the evaluator,
which keeps the query path wasm-clean. They differ in shape:

- **The clock** is a *static* value: captured once per transaction and threaded
  through as the `$now` parameter, so every `GETCURRENT*` call in a query agrees.
  Injected via `DatabaseBuilder::with_clock` (native default: `SystemTime::now()`).
- **`RAND()`** returns a *fresh* `Double` in `[0, 1)` on every call — it takes a
  *callable*, not a value — so two `RAND()`s in one query need not agree. Injected
  via `DatabaseBuilder::with_rand` (native default: a seeded PRNG behind the
  `runtime` feature; absent any source, `RAND()` is undefined). `RAND()` is a
  deliberate non-Cosmos extension, so it is not part of the parity corpus.

## Not yet supported

The parser accepts any `IDENT(...)` as a function call, so unimplemented functions
surface as an eval-time "unknown function" error rather than a parse error. Notable
gaps (tracked in the [Roadmap](./roadmap.md)):

- **Full-text search** — `FULLTEXTCONTAINS`/`…ALL`/`…ANY`, `FULLTEXTSCORE`, `RRF`, `ORDER BY RANK`: needs a full-text index + BM25 scoring.
- **Vector** — `VECTORDISTANCE`: needs a vector index.
- **Spatial index** — the `ST_*` functions are implemented (see the [Function Reference](./functions.md#spatial)); a spatial *index* is future work.
