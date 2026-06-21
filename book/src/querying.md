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

The `WHERE` expression is the full scalar grammar plus three predicate forms: `<expr> IN (a, b, …)`, `<expr> BETWEEN <lo> AND <hi>`, and `<expr> LIKE '<pattern>' [ESCAPE '<c>']` (each negatable with `NOT`). They are pure sugar — `IN` desugars to an OR of equalities and `BETWEEN` to an inclusive `>= lo AND <= hi`, so both reuse the planner's sargable paths unchanged (an `IN` over an indexed field becomes an `IndexMerge(Or)` and a `BETWEEN` a range `IndexScan`; see [Plan Scenarios](#plan-scenarios)). `LIKE` desugars to an anchored `RegexMatch`: the SQL wildcards `%` (any run) and `_` (any single character) and `[…]`/`[^…]` sets become regex constructs, while every other character — including regex metacharacters — is escaped to a literal, so a pattern is never a regex-injection vector.

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

**Scalar values only (for now).** Uniqueness is defined for single scalar values: a non-multikey path resolves to at most one value per document, so the value → `_id` mapping is one-to-one and the doc_id-less `u` key is unambiguous. A unique index on a multikey (`[]`) path is rejected — array fields fan out to multiple values per document, which implies cross-element uniqueness semantics we have not yet committed to. Compound and multikey unique indexes are tracked in the [roadmap](roadmap.md).

**Sparse.** A document that lacks the field (or holds a non-scalar there) produces no `u` entry and is unconstrained — any number of documents may omit a unique field.

**Expired documents keep their slot.** A unique value owned by a document that has expired via TTL but not yet been purged still blocks new inserts of that value. This is conservative — a unique index never silently accepts a duplicate — and the slot is reclaimed when the dead document is purged.

## Plan Scenarios

The following scenarios show how the planner chooses a scan source (sargability and index selection) for different filter combinations. All examples assume:

```
indexed_fields: ["user_id", "status"]
```

> **Reading these trees.** Every `find` plan is wrapped in a top-level `Project(c)` (the identity projection) over a single inline-bound alias; both are omitted below to focus on source selection. `KeyLookup` fetches documents for an ID stream produced by an `IndexScan`/`IndexMerge`. v2 uses **every** applicable index — multiple indexed equalities intersect via `IndexMerge(And)` rather than the planner picking one — and a sargable equality that fully covers its index is *consumed* (no residual recheck).

---

### 1. No Filter

**Query:** `find({})`

```
Scan
```

Full scan — reads every document. No filter node needed.

---

### 2. Single Indexed Eq

**Query:** `find({ status = "active" })`

```
KeyLookup
  └── IndexScan(status = "active")
```

The `Eq` on an indexed field becomes an `IndexScan`; `KeyLookup` fetches the matched documents. The equality is *consumed* — a scalar index returns only documents whose `status` equals `"active"`, so no residual recheck is needed.

---

### 3. Single Non-Indexed Condition

**Query:** `find({ score > 50 })`

```
Filter(score > 50)
  └── Scan
```

No index available — full scan with a filter. Each document is read as raw bytes and `score > 50` is evaluated lazily; documents that fail are never deserialized.

---

### 4. AND — Indexed + Non-Indexed

**Query:** `find({ status = "active" AND score > 50 })`

```
Filter(score > 50)
  └── KeyLookup
        └── IndexScan(status = "active")
```

The `Eq` on `status` becomes an `IndexScan` (consumed). `score > 50` can't use an index, so it stays a residual `Filter`. The index narrows the candidate set; the filter evaluates the rest lazily.

---

### 5. AND — Multiple Indexed Fields → IndexMerge(And)

**Query:** `find({ status = "active" AND user_id = "abc" })`

```
KeyLookup
  └── IndexMerge(And)
        ├── IndexScan(status = "active")
        └── IndexScan(user_id = "abc")
```

Both fields are indexed, so v2 scans **both** indexes and intersects their ID sets with `IndexMerge(And)` — a smaller candidate set than either index alone. Both equalities are consumed, so there's no residual filter. (Unlike a pick-one-index strategy, v2 applies every index that helps.)

---

### 6. AND — Multiple Indexed + Non-Indexed

**Query:** `find({ user_id = "abc" AND status = "active" AND score > 50 AND name ~ "alice" })`

```
Filter(score > 50 AND REGEXMATCH(name, "alice"))
  └── KeyLookup
        └── IndexMerge(And)
              ├── IndexScan(user_id = "abc")
              └── IndexScan(status = "active")
```

Both indexed equalities push into the `IndexMerge(And)` and are consumed. The non-indexable conditions — `score > 50` and the regex on `name` — remain as the residual filter, evaluated lazily on the intersected candidates.

---

### 7. OR — Both Branches Indexed

**Query:** `find({ user_id = "abc" OR status = "active" })`

```
Filter(user_id = "abc" OR status = "active")
  └── KeyLookup
        └── IndexMerge(Or)
              ├── IndexScan(user_id = "abc")
              └── IndexScan(status = "active")
```

Every OR branch is an indexed equality, so the planner builds an `IndexMerge(Or)` — a union of the two index scans' ID sets. The full OR is kept as a residual recheck (not consumed): an index union can over-return, so the filter re-confirms each candidate.

---

### 8. OR — One Branch Not Indexed

**Query:** `find({ status = "active" OR name = "test" })`

```
Filter(status = "active" OR name = "test")
  └── Scan
```

`name` is not indexed. If **any** OR branch lacks an indexed equality, the whole OR falls back to a full `Scan` — a disjunction needs every branch to contribute, so one unindexed branch forces scanning everything.

---

### 9. OR — Same Field, Multiple Values (IN-style)

**Query:** `find({ user_id = 1 OR user_id = 2 })`

```
Filter(user_id = 1 OR user_id = 2)
  └── KeyLookup
        └── IndexMerge(Or)
              ├── IndexScan(user_id = 1)
              └── IndexScan(user_id = 2)
```

Effectively an `IN` query. Each value gets its own `IndexScan`, unioned via `IndexMerge(Or)`. Common for multi-tenant access control — "show records for user 1 or user 2." (Each `{user_id: v}` lowers to the implicit-equality idiom `user_id = v OR ARRAY_CONTAINS(user_id, v)`; the planner recognizes the whole idiom as one indexed equality, so the union still forms.)

---

### 10. OR — Three Values (Left-Associative Fold)

**Query:** `find({ user_id = 1 OR user_id = 2 OR user_id = 3 })`

```
Filter(user_id = 1 OR user_id = 2 OR user_id = 3)
  └── KeyLookup
        └── IndexMerge(Or)
              ├── IndexMerge(Or)
              │     ├── IndexScan(user_id = 1)
              │     └── IndexScan(user_id = 2)
              └── IndexScan(user_id = 3)
```

Multiple OR branches fold into a left-associative tree: `((1 Or 2) Or 3)`. Each `IndexMerge(Or)` unions its children's ID sets.

---

### 11. AND with Nested OR — Intersect the Eq with the OR-Merge

**Query:** `find({ user_id = "abc" AND (status = "active" OR status = "archived") })`

```
Filter(status = "active" OR status = "archived")
  └── KeyLookup
        └── IndexMerge(And)
              ├── IndexScan(user_id = "abc")
              └── IndexMerge(Or)
                    ├── IndexScan(status = "active")
                    └── IndexScan(status = "archived")
```

Both halves are indexable: the `user_id` equality is consumed as an `IndexScan`, and the fully-indexable OR sub-group becomes an `IndexMerge(Or)`. v2 intersects them with `IndexMerge(And)`, narrowing to documents that satisfy both. The OR sub-group is kept as a residual recheck (an index union can over-return).

---

### 12. AND with Nested OR — No Other Indexed Conjunct

**Query:** `find({ (status = "active" OR status = "archived") AND score > 50 })`

Indexed fields: `["status"]`

```
Filter((status = "active" OR status = "archived") AND score > 50)
  └── KeyLookup
        └── IndexMerge(Or)
              ├── IndexScan(status = "active")
              └── IndexScan(status = "archived")
```

The fully-indexable OR sub-group becomes an `IndexMerge(Or)` (kept as a recheck). `score > 50` isn't indexable, so the residual filter carries both the OR recheck and `score > 50`, evaluated lazily on the narrowed candidates.

---

### 13. OR with Nested ANDs — Falls Back to Scan (known gap)

**Query:** `find({ (user_id = "abc" AND status = "active") OR (user_id = "xyz" AND status = "pending") })`

```
Filter((user_id = "abc" AND status = "active") OR (user_id = "xyz" AND status = "pending"))
  └── Scan
```

Each OR branch is itself a multi-field **AND**. The OR-branch planner recognizes indexed atoms and single-field equalities, but does not yet plan a conjunction branch (pick a branch's best index), so the disjunction can't be indexed and falls back to a `Scan`. **This is a known gap** — see [Roadmap](roadmap.md). The intended plan unions a per-branch `IndexScan` (e.g. on `user_id`) via `IndexMerge(Or)`.

---

### 14. OR with Partial Index Per Branch — Falls Back to Scan (known gap)

**Query:** `find({ (user_id = "abc" AND score > 50) OR status = "active" })`

```
Filter((user_id = "abc" AND score > 50) OR status = "active")
  └── Scan
```

The second branch (`status = "active"`) is indexable on its own, but the first branch is an AND (`user_id = "abc" AND score > 50`) — the same conjunction-branch gap as scenario 13 — so the whole OR scans. Once OR branches can be planned as conjunctions, this becomes an `IndexMerge(Or)` of the `user_id` and `status` scans.

---

### 15. OR with Unindexed Branch — Fallback to Scan

**Query:** `find({ (user_id = "abc" OR status = "active") OR (count > 5 OR name = "foo") })`

```
Filter((user_id = "abc" OR status = "active") OR (count > 5 OR name = "foo"))
  └── Scan
```

The branch `(count > 5 OR name = "foo")` has no indexed equality. Any unindexed OR branch poisons the whole disjunction, so the query falls back to `Scan` with the full predicate as the residual filter.

---

### 16. Complex — Multi-Level Nesting

**Query:** `find({ (user_id = 1 OR user_id = 2 OR user_id = 3) AND status = "active" AND (count > 5 OR name = "foo") })`

```
Filter((user_id = 1 OR user_id = 2 OR user_id = 3) AND (count > 5 OR name = "foo"))
  └── KeyLookup
        └── IndexMerge(And)
              ├── IndexScan(status = "active")
              └── IndexMerge(Or)
                    ├── IndexMerge(Or)
                    │     ├── IndexScan(user_id = 1)
                    │     └── IndexScan(user_id = 2)
                    └── IndexScan(user_id = 3)
```

The AND group has three children:
1. `(user_id = 1 OR user_id = 2 OR user_id = 3)` — fully-indexable OR sub-group → `IndexMerge(Or)`
2. `status = "active"` — indexed equality → `IndexScan` (consumed)
3. `(count > 5 OR name = "foo")` — not indexable → residual

v2 intersects every indexable source: the `status` scan and the `user_id` `IndexMerge(Or)` are combined with `IndexMerge(And)`. The unindexable `(count > 5 OR name = "foo")` and the `user_id` OR recheck remain the residual filter.

If `status` were **not** indexed, the `user_id` OR sub-group is the only indexable source, so it becomes the whole source and everything else is residual:

```
Filter((user_id = 1 OR ...) AND status = "active" AND (count > 5 OR name = "foo"))
  └── KeyLookup
        └── IndexMerge(Or)
              ├── IndexMerge(Or)
              │     ├── IndexScan(user_id = 1)
              │     └── IndexScan(user_id = 2)
              └── IndexScan(user_id = 3)
```

---

### 17. Fully Unindexed

**Query:** `find({ score > 50 AND name ~ "alice" })`

```
Filter(score > 50 AND REGEXMATCH(name, "alice"))
  └── Scan
```

No indexed fields, no equality — full scan. Both conditions are evaluated lazily on raw bytes; rejected documents are never fully deserialized.

---

### 18. Projection Over an Index Scan

**Query:** `find({ status = "active", columns: ["status"] })`

```
Project([_id, status])
  └── KeyLookup
        └── IndexScan(status = "active")
```

`IndexScan` yields document IDs, `KeyLookup` fetches the documents, and `Project` builds `{ _id, status }` from them.

> **Covered-index projection is future work in v2.** Even when every projected column is the indexed field, v2 still fetches the document via `KeyLookup` — it does not yet serve a projection directly from the index entry's value. The deferred optimization (and the approaches to extend it to multiple columns — composite indexes, or secondary index lookups) is tracked in the [Roadmap](roadmap.md).

---

### 19. Array Element Matching

**Query:** `find({ tags = "renewal_due" })` (where `tags` is an array like `["active", "renewal_due"]`)

```
Filter(tags = "renewal_due" OR ARRAY_CONTAINS(tags, "renewal_due"))
  └── Scan
```

The Mongo `{tags: v}` form lowers to the implicit-equality idiom `tags = v OR ARRAY_CONTAINS(tags, v)`: it matches when `tags` equals `v` *or* is an array containing `v`. The evaluator iterates array elements, delegating each to the shared scalar comparison (so cross-type coercion works within elements) — matching MongoDB's behavior without `$elemMatch`.

For an **explicit** multikey path (`tags.[]`, `items.[].sku`), the front-end emits a `MultikeyEq` the planner can match to a `.[]` index. Sorting on array fields has no meaningful scalar ordering and is unsupported.

---

## Full Pipeline Example

**Query:** `find({ filter: status = "active" AND score > 50, sort: score DESC, skip: 10, take: 5, columns: ["name", "score"] })`

```
Limit(skip: 10, take: 5)
  └── Project({_id, name, score})
        └── Sort(score DESC)
              └── Filter(score > 50)
                    └── KeyLookup
                          └── IndexScan(status = "active")
```

Execution flow (data flows bottom-to-top):

1. **IndexScan** — iterates the `status = "active"` index, yields document IDs (the equality is consumed — no `status` recheck)
2. **KeyLookup** — batch-fetches raw BSON bytes via `multi_get`
3. **Filter** — evaluates `score > 50` by reading just the `score` field from raw bytes; rejected documents are skipped with zero deserialization cost
4. **Sort** — collects survivors, reads `score` from raw bytes, sorts in memory
5. **Project** — copies `name` and `score` (plus `_id`) into a `RawDocumentBuf` via `append()`
6. **Limit** — skips 10, takes 5

`Project` sits below `Limit`, so v2 projects the sorted rows and then limits. Documents that fail the filter at step 3 never reach steps 4–6.

## Limit Placement

For document streams (find queries), `Limit` is lazy `skip()` + `take()` on the iterator; for the `RawBson::Array` of a distinct query, it slices array elements directly. It always sits at the top of the pipeline (above `Project`); whether work is saved depends on whether a `Sort` is present.

### Scenario A: Limit with Sort

**Query:** `find({ status = "active", sort: score DESC, take: 200 })`

```
Limit(take: 200)
  └── Project(c)
        └── Sort(score DESC)
              └── KeyLookup
                    └── IndexScan(status = "active")
```

Limit can't take before Sort orders the rows, so all matching documents enter Sort and Limit takes 200 from the sorted result. Cost: O(n log n) on the full matched set. (`Project(c)` is the identity projection for `find`.)

### Scenario B: Limit without Sort

**Query:** `find({ status = "active", take: 200 })`

```
Limit(take: 200)
  └── Project(c)
        └── KeyLookup
              └── IndexScan(status = "active")
```

No Sort: the stream stops after 200 documents pass through. Remaining index entries and raw bytes are never touched — much cheaper than Scenario A.

### Scenario C: Limit without Sort, with Filter

**Query:** `find({ score > 50, take: 200 })`

```
Limit(take: 200)
  └── Project(c)
        └── Filter(score > 50)
              └── Scan
```

Documents stream through Filter one at a time; Limit stops after 200 pass. Failures don't count toward the limit — the scan continues until 200 qualify (or the collection is exhausted).

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

## Mutation Pipeline — Triggers and Validators

Mutations (insert, update, replace, delete, upsert) can have **triggers** and **validators** attached to a collection. These are resolved at plan time from a `HookSnapshot` — a frozen view of all registered hooks captured when the transaction begins. The planner wraps the mutation pipeline with typed nodes; the executor just runs them.

### Node Types

**`Node::Validate`** — a gate. Runs each validator function against the document. If any validator returns `{ ok: false, reason: "..." }`, the mutation is rejected with an error. On success, the document passes through unchanged. Validators are pure functions — they receive `{ doc: <the document> }` and have no access to the database.

**`Node::Trigger`** — a tap. Fires each trigger function as a side effect, passing the document through unchanged. Triggers receive `{ action: "<event>", doc: <the document> }` and have read-write access to the database via a scoped context (`ctx.get`, `ctx.put`, `ctx.delete`).

**`Plan::Trigger`** — identical behavior to `Node::Trigger`, but wraps an entire `Plan` instead of a `Node`. Used for after-mutation triggers that need to see the result of the mutation.

### Lifecycle Events

Each mutation type fires before/after trigger pairs:

| Mutation | Before | After |
|----------|--------|-------|
| Insert | `inserting` | `inserted` |
| Update | `updating` | `updated` |
| Replace | `updating` | `updated` |
| Delete | `deleting` | `deleted` |

Before-triggers see the **source** document (old state for updates, new doc for inserts). After-triggers see the **result** document (the doc as written to storage).

### Plan Trees

**Insert with validators and triggers:**

```
Plan::Trigger { action: "inserted", hooks }
  └── Plan::Insert { collection }
        └── Node::Trigger { action: "inserting", hooks }
              └── Node::Validate { validators }
                    └── Node::Values([...docs])
```

**Update with triggers (no validation — mutation applies to existing docs):**

```
Plan::Trigger { action: "updated", hooks }
  └── Plan::Update { collection, mutation }
        └── Node::Trigger { action: "updating", hooks }
              └── Node::Validate { validators }
                    └── Filter → KeyLookup → IndexScan   (or Filter → Scan)
```

**Delete (no validation — nothing being written):**

```
Plan::Trigger { action: "deleted", hooks }
  └── Plan::Delete { collection }
        └── Node::Trigger { action: "deleting", hooks }
              └── Filter → Scan
```

The delete node yields full documents (not `None`) so the after-trigger can see what was deleted.

**Upsert — special case:**

Upsert's trigger actions are runtime-conditional: `inserting`/`inserted` for new docs, `updating`/`updated` for existing docs. Because the action depends on whether the document already exists, triggers are fired internally by the upsert node rather than as separate plan wrappers. Validators still apply as a `Node::Validate` in the source pipeline.

### Hook Resolution

Hooks are not discovered at execution time. The planner resolves them from the `HookSnapshot`:

1. `HookSnapshot` is captured at `begin()` time via `ArcSwap` (lock-free load)
2. Planner calls `snapshot.validators_for(cf, collection)` and `snapshot.triggers_for(cf, collection)`
3. If hooks exist, the planner wraps the pipeline with the appropriate nodes
4. If no hooks exist, no wrapper nodes are added — zero overhead for collections without scripts

When a transaction commits after modifying hooks (`register_trigger`, `register_validator`, `drop_trigger`, etc.), the `HookRegistry` is swapped with a fresh snapshot so subsequent transactions see the updated hooks.

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
