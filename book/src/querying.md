# Querying

## Overview

Queries are executed as a two-tier plan tree. `slate-planner` analyzes filter conditions and available indexes to build an execution plan (a tree of `Node`s); `slate-executor` runs it lazily — records that fail a filter are never fully deserialized (see [Architecture](architecture.md)).

```
Value tier:      Limit (skip/take on the result stream)
                   ↑
                 Project (SELECT VALUE — identity for find; builds RawDocumentBuf)
                   ↑
                 Sort (lazy field access on raw bytes)
                   ↑
                 Filter (residual WHERE — lazy field access on raw bytes)
                   ↑
                 [Bind / Unwind]  (joins only; a join-free find binds one alias inline)
                   ↑
                 KeyLookup (fetch raw BSON bytes for an ID stream)
                   ↑
ID tier:         Scan / IndexScan / IndexMerge  (Scan yields documents; index nodes yield IDs)
```

For mutation plans, extra nodes wrap the pipeline:

```
Plan::Trigger { action: "inserted" }        ← after-trigger (sees NEW doc)
  └── Plan::Insert { collection }
        └── Node::Trigger { action: "inserting" }  ← before-trigger (sees source doc)
              └── Node::Validate { validators }     ← gate: pass or reject
                    └── Node::Values([...docs])
```

**ID tier** — `Scan` streams documents; `IndexScan`/`IndexMerge` produce record IDs without touching document bytes, and `KeyLookup` fetches the documents for an ID stream.
**Value tier** — everything above `KeyLookup` operates on `Option<RawBson>` values, constructing `&RawDocument` views to access individual fields lazily (via the `slate-rawbson` scanner) with no full deserialization. `Project` builds `RawDocumentBuf` output using `append()` for selective field copying — no `bson::Document` materialization in the pipeline. `find()` returns a `Cursor` whose `.iter::<T>()` deserializes each document into `T`, or `.iter_raw()` yields `RawDocumentBuf` directly with no deserialization. For distinct queries, the pipeline emits a single `RawBson::Array` — Sort and Limit handle arrays natively by sorting/slicing elements in-place.

## Query Model (find)

A `find` is a BSON filter document plus options:

```rust
FindOptions {
    sort: Vec<Sort>,              // ORDER BY
    skip: Option<usize>,          // OFFSET
    take: Option<usize>,          // LIMIT
    columns: Option<Vec<String>>, // projected columns
}
```

The filter is a Mongo-style `$`-operator document — `$and`, `$or`, `$eq`/`$gt`/`$gte`/`$lt`/`$lte`, `$regex`, `$exists`, and the implicit `{field: value}` equality (which matches a scalar *or* an array containing the value). `slate-query` translates it into the shared AST (`slate_ast::ScalarExpr`), which the planner lowers — the *same* AST a SQL query produces, so both surfaces share one planner, executor, and evaluator.

## SQL Queries

`Transaction::query(cf, collection, sql)` runs a CosmosDB-style SQL query and returns a [`Cursor`]:

```
SELECT VALUE <expr>                         -- one value per row
  | *                                       -- the whole document
  | <expr> [AS <key>], ...                  -- a document of projected fields
FROM <alias>
[JOIN <alias> IN <array-expr>]*
[WHERE <expr>]
[GROUP BY <expr>, ...]
[ORDER BY <expr> [ASC|DESC], ...]
[OFFSET <n>] [LIMIT <n>]
```

The `FROM` clause names only the row alias; the container is the `(cf, collection)` passed to `query()` (matching Cosmos, where the container is external to the query text). SQL is read-only and always runs on the v2 engine.

These run live against the playground's `products` and `families` collections —
edit and run them (see the [Playground](./playground.md) for the data):

```slate-sql
SELECT c.name, c.price FROM products c WHERE c.price > 100 ORDER BY c.price DESC
```

```slate-sql
SELECT c.category, COUNT(1) AS n, AVG(c.price) AS avgPrice
FROM products c GROUP BY c.category
```

```slate-sql
SELECT f.lastName, ch.firstName, ch.grade FROM families f JOIN ch IN f.children
```

Supply values for `@name` placeholders with `query_with_params(cf, collection, sql, params)`, where `params` serializes to a document keyed by the bare parameter names (no leading `@`) — e.g. `WHERE c.age > @minAge` with `doc! { "minAge": 21 }`. A referenced parameter with no supplied value is a hard error (matching Cosmos), which catches a misspelled or forgotten name; the plain `query` API supplies no parameters, so any `@name` there is rejected too. Parameters are visible everywhere an expression is evaluated (`WHERE`, projections, `ORDER BY`, `JOIN … IN`).

The `WHERE` expression is the full scalar grammar plus three predicate forms: `<expr> IN (a, b, …)`, `<expr> BETWEEN <lo> AND <hi>`, and `<expr> LIKE '<pattern>' [ESCAPE '<c>']` (each negatable with `NOT`). They are pure sugar — `IN` desugars to an OR of equalities and `BETWEEN` to an inclusive `>= lo AND <= hi`, so both reuse the planner's sargable paths unchanged (an `IN` over an indexed field becomes an `IndexMerge(Or)` and a `BETWEEN` a range `IndexScan`; see [Plan Scenarios](#plan-scenarios)). `LIKE` desugars to an anchored `RegexMatch`: the SQL wildcards `%` (any run) and `_` (any single character) and `[…]`/`[^…]` sets become regex constructs, while every other character — including regex metacharacters — is escaped to a literal, so a pattern is never a regex-injection vector. A `LIKE 'pre%'` with a literal anchored prefix is itself sargable: it plans as a `[pre, pre⁺)` prefix-range `IndexScan` over a string index (the same path `STARTSWITH(x, 'pre')` takes), with the full pattern kept as a recheck.

There are three projection forms, all matching Cosmos semantics:

```rust
// VALUE — one bare value per row (scalar, document, or array):
//   SELECT VALUE c.name  ->  "ada", "alan", ...
for name in txn.query(cf, "people", "SELECT VALUE c.name FROM c")?
    .iter_values::<String>()? { /* ... */ }

// *  — the whole document:
for doc in txn.query(cf, "people", "SELECT * FROM c")?.iter::<Document>()? { /* ... */ }

// tabular — a document of the selected columns:
//   SELECT c.name, c.age  ->  { "name": "ada", "age": 36 }
for doc in txn.query(cf, "people", "SELECT c.name, c.age FROM c")?
    .iter::<Document>()? { /* ... */ }
```

**Tabular projection keys** follow Cosmos: the last path segment of a member access (`c.address.city` → `"city"`), an explicit `AS <key>`, or a positional `$1`, `$2`, … for an unnamed computed column (`c.age + 1` → `"$1"`). Two columns that resolve to the **same key** are a parse error (use `AS` to disambiguate) — slate rejects the collision rather than silently dropping a value.

Two deliberate differences from a Mongo `find` projection:
- **No auto primary key.** `find(columns: [...])` prepends `_id`; SQL returns *exactly* the selected columns. Ask for the pk explicitly (`SELECT c._id, c.name`) or use `SELECT *`.
- **Member values are not trimmed.** `SELECT c.address` yields `{ "address": <the whole sub-document> }`, whereas `find(columns: ["address.city"])` builds a trimmed, nested `{ address: { city } }`.

**Aggregates and `GROUP BY`.** `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` in a `SELECT` collapse the matching rows to one result row — `SELECT VALUE COUNT(1) FROM c` → `[3]`, `SELECT MAX(c.age) AS oldest FROM c` → `[{ "oldest": 44 }]`. `COUNT` over an empty set is `0`; `SUM`/`AVG`/`MIN`/`MAX` over no values are undefined. With `GROUP BY <expr>, …` the rows collapse to one per distinct group instead — `SELECT c.kind, COUNT(c.tags) AS n FROM c GROUP BY c.kind` yields a row per `kind`. `SELECT` and `ORDER BY` may reference only the group-key expressions or aggregates (`ORDER BY` sorts the resulting group rows); an ungrouped non-aggregate column is rejected, as in Cosmos.

**Subqueries.** A subquery ranges over an *in-document array* (`FROM x IN <array>`), never another container, so it's a per-row sub-pipeline rather than a second scan. Three forms are supported: a scalar `(SELECT …)` (its single value, or undefined), `EXISTS (…)` (a boolean), and `ARRAY (…)` (the rows collected into an array). They appear in `SELECT` and `WHERE`, may be **correlated** (referencing the outer row, e.g. `(SELECT VALUE COUNT(1) FROM t IN c.tags)`) or uncorrelated (a literal source), and may nest to any depth — e.g. `WHERE EXISTS (SELECT VALUE t FROM t IN c.tags WHERE t.key = "fabric")`. (A subquery as a `JOIN` source isn't supported yet.)

### Numbers and comparison

slate evaluates all numbers — `Int32`, `Int64`, `Double`, and `Decimal128` (`$numberDecimal`) — in one `f64` tower, matching Cosmos's single JSON number model. A stored decimal is therefore fully queryable: `SELECT c.price`, `WHERE c.price > 100`, `ORDER BY c.price`, and aggregates all work. The contract:

- **The stored value is preserved.** A projected decimal comes back as the original `Decimal128`, and `find` round-trips it byte-for-byte — only *computed* results are ever `f64`.
- **`SUM`/`AVG` return a double; `MIN`/`MAX` return the original `Decimal128`.**
- **Comparison and sort use the `f64` value** — exact for realistic magnitudes, diverging only beyond ~15–17 significant digits.

Comparison is **per-domain**: numbers compare to numbers, strings to strings, dates to dates; values from different domains are not order-comparable, so a `WHERE` over them matches nothing. One gotcha follows from that: a stored `$date` is a real BSON `DateTime`, so `WHERE c.when > "2024-01-01"` compares a date against a *string* and returns no rows — compare dates to dates. (Cosmos stores dates as ISO strings, which is why the string form appears to work there.)

### Iterating results

A `Cursor` — from `find` or `query` — exposes:

| Accessor | Yields | Use for |
|---|---|---|
| `iter::<T>()` | `T` per **document** | `find`; `SELECT *`; tabular `SELECT a, b`; document-shaped `SELECT VALUE` |
| `iter_raw()` | `RawDocumentBuf` per document | zero-copy document access |
| `iter_values::<T>()` | `T` per **value** | SQL scalar projections (`SELECT VALUE c.name`) |
| `iter_raw_values()` | `RawBson` per value | zero-copy scalar / document / array access |
| `drain()` | row count | counting without materializing |

`iter`/`iter_raw` error on a non-document value; `iter_values`/`iter_raw_values` accept any value.

## Index Configuration

Indexes are created per collection via `create_index(cf, collection, field)`. The order of indexed fields in the collection's handle determines **priority** — when multiple indexed fields appear in an AND group, the first one in the list wins.

```rust
// indexes: ["user_id", "status"]
// user_id is priority 1, status is priority 2
```

### Unique Indexes

A unique index additionally enforces that no two live documents share the same value for a field. Create one with `create_unique_index(cf, collection, field)`; an insert or update that lands on a value already held by another document fails with `UniqueViolation { index, value, existing_id }`.

A unique index keeps its regular value-first `i` entry, so it serves index scans, range scans, and covered projections exactly like any other index. It additionally writes a point-lookup `u` entry — keyed by the value alone (`u\0{collection}\0{field}\0{type}{value}`) with the owning `_id` stored in the entry value. Enforcement is a point read on that `u` key before each write, backed by the store's write-write conflict detection for concurrent writers.

**Scalar values only (for now).** Uniqueness is defined for single scalar values: a non-multikey path resolves to at most one value per document, so the value → `_id` mapping is one-to-one and the doc_id-less `u` key is unambiguous. A unique index on a multikey (`[]`) path is rejected — array fields fan out to multiple values per document, which implies cross-element uniqueness semantics we have not yet committed to. Compound unique indexes ship — see [Compound Indexes](#compound-indexes) below; multikey unique indexes are tracked in the [roadmap](roadmap.md).

**Sparse.** A document that lacks the field (or holds a non-scalar there) produces no `u` entry and is unconstrained — any number of documents may omit a unique field.

**Expired documents keep their slot.** A unique value owned by a document that has expired via TTL but not yet been purged still blocks new inserts of that value. This is conservative — a unique index never silently accepts a duplicate — and the slot is reclaimed when the dead document is purged.

### Compound Indexes

A compound index spans **multiple fields in order**. Create one with `create_compound_index(cf, collection, fields)` (and `create_unique_compound_index(…)` to constrain the *combination* of values). Like all indexes, these are programmatic — there is no SQL `CREATE INDEX`.

```rust
// compound index on (status, created_at)
txn.create_compound_index(DEFAULT_CF, "orders", &["status".into(), "created_at".into()])?;
```

The planner applies the **leftmost-prefix rule**: an index on `(a, b, c)` serves any query that constrains a leftmost prefix of its fields, with at most a trailing range on the last constrained field. A query that skips the leading field falls back to a `Scan`.

```text
index on (status, created_at):
  WHERE status = 'open' AND created_at >= @t   → CompoundIndexScan (equality + trailing range)
  WHERE status = 'open'                        → CompoundIndexScan (leading prefix only)
  WHERE created_at >= @t                       → Scan (skips the leading field)
```

The engine seeks the leading-equality prefix as a conservative superset (a string leading value can over-read its byte prefix); the executor then rechecks each leading equality and the trailing range exactly, so results match a full scan.

**Compound-unique** enforces uniqueness of the whole tuple: two documents may share `status` or `created_at` individually, but not the same `(status, created_at)` pair. Backfill on creation fails with `UniqueViolation` if existing data already holds a duplicate combination.

**Scalar components only (Phase 1).** Every component must resolve to a single scalar; a multikey (`[]`) component is rejected at creation, since an array component would fan one document across a cross-product of keys a leftmost-prefix seek can't address. Multikey compound indexes are tracked in the [roadmap](roadmap.md). A single-field index is exactly the one-component case of the same encoding.


## Plan Scenarios

The planner's source selection — when a filter becomes an `IndexScan`, an `IndexMerge`, or falls back to a `Scan` — is catalogued with 20 worked examples (plus a full pipeline and limit placement) in **[Plan Scenarios](./plan-scenarios.md)**.

To see the plan a specific query lowers to, use `.explain <query>` in the `slate-cli` shell (or `Transaction::explain(cf, collection, sql)` in the library). It prints the chosen plan as an indented operator tree — the same lowering `query` uses, so the tree reflects what would actually run — without executing it. There is no SQL `EXPLAIN` keyword; plan inspection is a shell/library affair.

## Distinct Queries

Distinct queries find the unique values of a single field. v2 builds a `Scan`-based pipeline and adds `Project` → `Distinct` to extract and deduplicate the values.

### Query Model

```rust
DistinctQuery {
    field: String,                     // The field to collect unique values from
    filter: Option<RawDocumentBuf>,    // Optional BSON filter document (same as find)
    sort: Option<SortDirection>,       // Optional sort on the distinct values
    skip: Option<usize>,              // Skip first N unique values
    take: Option<usize>,              // Take N unique values
}
```

### Pipeline

```
Scan → [Filter] → Project(field) → Distinct → [Sort] → [Limit]
```

`Project` extracts the target field (distributing over arrays when the path reaches into an array of sub-documents); `Distinct` deduplicates with a `HashSet` over the raw BSON bytes and emits a single `RawBson::Array` of the unique values. Unlike `find()` — one item per document — `Distinct` collapses the whole result set into one array value.

### Plan Trees

**Distinct (no filter, no sort):**

```
Distinct
  └── Project(status)
        └── Scan
```

**Distinct (with filter):**

```
Distinct
  └── Project(status)
        └── Filter(score > 50)
              └── Scan
```

The filter is translated exactly as for `find()`, but distinct does **not** push it into an index — its source is always a full `Scan`.

**Distinct (with sort):**

```
Sort(value ASC)
  └── Distinct
        └── Project(status)
              └── Scan
```

Sort sits above Distinct and orders the distinct values themselves (the values are bound as the row, so the sort key is the bare value). It handles the `RawBson::Array` natively.

**Distinct (with sort and limit):**

```
Limit(skip: 1, take: 2)
  └── Sort(value ASC)
        └── Distinct
              └── Project(status)
                    └── Scan
```

Limit sits above Sort. It detects the single `RawBson::Array` item and slices its elements with skip/take — no per-record iteration needed.

### How Sort and Limit Handle Arrays

Sort detects when its input is a single `RawBson::Array` item (the output shape of Distinct) and switches to array-sorting mode:

1. Unpacks the array into individual `RawBson` elements
2. Sorts elements using the same comparison infrastructure as document sorting:
   - **Scalar arrays** (strings, numbers): direct `raw_compare_field_values` comparison
   - **Document arrays**: extracts the sort field from each document via `raw_get_path`, then compares
3. Rebuilds a sorted `RawBson::Array` and re-emits it as a single item

Limit uses the same pattern — it peeks the first item from its source. If it's a `RawBson::Array`, it slices elements with `skip/take` and rebuilds a `RawArrayBuf`. Otherwise it chains the peeked item back and applies lazy `skip().take()` on the record stream.

Both Sort and Limit are general-purpose nodes — they don't need to know whether their input came from Distinct or a regular find pipeline.

### Return Type

`distinct()` returns `bson::RawBson` — specifically `RawBson::Array(RawArrayBuf)`. No materialization to `bson::Bson` happens at the database boundary. The raw array is serialized directly when sent over the wire.

### Deduplication

Distinct hashes the raw BSON bytes of each value to deduplicate, avoiding materialization. Null values are skipped. A dotted path that reaches into an array of sub-documents distributes over the elements (the `Project` step emits each value the path resolves to), so distinct over an array field or a path like `items.sku` collects the element-level values.

## Subqueries

A subquery ranges over an **in-document array** (`FROM x IN <array>`) — never another container — so it isn't a second `Scan`. It's a *per-outer-row sub-pipeline*, modeled by two plan nodes that keep the single execution engine:

- **`Subquery { slot, kind, subplan, source }`** — a *correlated apply*. It has one real input (`source`) and a `subplan` it runs as a subroutine. For each row from `source` it runs `subplan`, reduces the subplan's rows by `kind` (`Scalar` → first value or undefined; `Exists` → a bool; `Array` → the values collected into an array), and emits the row extended with `{slot: value}`. So `subplan` is **not** a second input streaming tuples up — it's a function the node calls per row.
- **`CurrentRow`** — the subplan's leaf, in place of `Scan`. It yields the single outer row the apply is currently processing (carrying the accumulated environment), which the subplan's `Unwind` reads to resolve correlated fields.

The planner extracts each subquery out of the surrounding expression into a `$subN` slot (the same mechanism `Aggregate` uses for `$aggN`), so the surrounding `Project`/`Filter` just reads a slot and the evaluators never see a raw subquery.

### Runtime — yes, we pass the row in

For each outer row, the `Subquery` node:

1. **feeds the row in** as `CurrentRow` (this is the correlation — the subplan's array source like `c.tags` resolves against it),
2. runs `subplan` to completion (its own pull pipeline),
3. **reduces** the result rows by `kind` to a single value,
4. **attaches** that value to the row under `slot` and yields it up.

It runs eagerly (a blocking node) so the result stream borrows only the transaction. Mentally: the subplan is a compiled `fn(row) -> value`; **correlated** subqueries read the fed-in row, **uncorrelated** ones ignore it (and so could be hoisted — computed once). Data flows *down* (the row, into `CurrentRow`) then *up* (the reduced value, out to the slot); never sideways between siblings.

### Plan Trees

**Scalar — `SELECT VALUE (SELECT VALUE COUNT(1) FROM t IN c.tags) FROM c`** (count each doc's tags):

```
Project($sub0)
  └── Subquery(slot=$sub0, kind=Scalar)
        ├── subplan:
        │     Project($agg0)
        │       └── Aggregate(COUNT(1) → $agg0)
        │             └── Unwind(t, c.tags)
        │                   └── CurrentRow          ← the outer row {c}
        └── source:
              Bind(c)
                └── Scan
```

The aggregate `COUNT(1)` lives *inside* the subplan as an ordinary `Aggregate` node — empty `c.tags` yields no `Unwind` rows, which the aggregate turns into `0`.

**EXISTS — `… WHERE EXISTS (SELECT VALUE t FROM t IN c.tags WHERE t.key = "fabric")`** (the result is just a bool the `Filter` tests):

```
Project(c.name)
  └── Filter($sub0)
        └── Subquery(slot=$sub0, kind=Exists)
              ├── subplan:
              │     Project(t)
              │       └── Filter(t.key = "fabric")
              │             └── Unwind(t, c.tags)
              │                   └── CurrentRow
              └── source:
                    Bind(c)
                      └── Scan
```

**ARRAY — `SELECT VALUE ARRAY(SELECT VALUE t FROM t IN c.tags) FROM c`** (collect the rows into an array):

```
Project($sub0)
  └── Subquery(slot=$sub0, kind=Array)
        ├── subplan:
        │     Project(t)
        │       └── Unwind(t, c.tags)
        │             └── CurrentRow
        └── source:
              Bind(c)
                └── Scan
```

**Uncorrelated — `… (SELECT VALUE COUNT(1) FROM x IN [10, 20, 30]) …`**: identical shape, but the subplan's `Unwind` source is a literal array rather than `c.tags`, so it ignores `CurrentRow` and produces the same value (`3`) for every outer row.

**Nested** — a subquery's `subplan` is a full `Node` tree, so it can contain another `Subquery`, and `CurrentRow` carries the accumulated environment (`{c, g}`) down. `… (SELECT VALUE COUNT(1) FROM g IN c.groups WHERE EXISTS (SELECT VALUE i FROM i IN g.items WHERE i = "x"))`:

```
Project($sub0)
  └── Subquery(slot=$sub0, kind=Scalar)              ← outer apply: feeds {c}
        ├── subplan:
        │     Project($agg0)
        │       └── Aggregate(COUNT(1) → $agg0)
        │             └── Filter($sub0)              ← inner subplan's own slot
        │                   └── Subquery(slot=$sub0, kind=Exists)  ← inner apply: feeds {c, g}
        │                         ├── subplan:
        │                         │     Project(i)
        │                         │       └── Filter(i = "x")
        │                         │             └── Unwind(i, g.items)
        │                         │                   └── CurrentRow   ← {c, g}
        │                         └── source:
        │                               Unwind(g, c.groups)
        │                                 └── CurrentRow               ← {c}
        └── source:
              Bind(c)
                └── Scan
```

Nothing here is subquery-specific except the two new nodes — `Filter`, `Aggregate`, and `Unwind` are the same nodes used everywhere else, which is what lets subqueries nest to any depth.

## Dot-Notation Paths

Filters, sorts, and projections support nested field access via dot notation:

```
filter: address.city = "Austin"
sort: address.zip ASC
columns: ["name", "address.city"]
```

Path resolution scans raw BSON bytes directly (via the `slate-rawbson` field scanner) — it walks `address` then `city` without deserializing the document.

A dotted projection column rebuilds the nested shape: `columns: ["address.city"]` projects `{ address: { city } }`, not a flat `"address.city"` key. Columns sharing a prefix merge under one sub-object (matching Mongo).


## Performance Characteristics

**Zero deserialization** is the core optimization — the entire pipeline stays in raw bytes. The cost model:

| Scenario | Cost |
|----------|------|
| Record passes filter + included in result | Selective field copying (projection via `RawDocumentBuf::append`) or full raw copy |
| Record passes filter + excluded by Limit (with sort) | Sort key access only (raw bytes) |
| Record passes filter + excluded by Limit (no sort) | Zero — never touched |
| Record fails filter | Filter field access only (raw bytes) |
| No filter, no projection | Full raw copy (all fields) |

**Index acceleration** reduces the number of records entering the raw tier:

| Access pattern | Records entering raw tier |
|----------------|--------------------------|
| `Scan` (no index) | All records |
| `IndexScan` (single index) | Records matching the indexed condition |
| `IndexMerge(Or)` | Union of records from each index |

**Combined effect:** A query with an indexed filter that rejects 90% of candidates and a non-indexed filter that rejects another 50% of the remaining — only 5% of records pay the cost of selective field copying into the result set.
