# Roadmap

## ~~Streaming Mutations~~ (Done)

Streaming mutation pipelines are fully implemented. All mutation operations (`delete_one`,
`delete_many`, `update_one`, `update_many`, `replace_one`) go through composable pipeline
nodes (`DeleteIndex → Delete`, `InsertIndex → Update → DeleteIndex`, etc.) via the
`Statement` enum and unified `planner::plan()` entry point. The executor streams one record
at a time through the pipeline with `O(1)` memory.

---

## Index Range Scans

### Problem

`IndexScan` currently only supports exact-value lookups (`Eq`) or full column scans. Range
predicates (`Lt`, `Gt`, `Lte`, `Gte`) on indexed fields fall back to a full `Scan` with a
residual `Filter`, which reads every record in the collection.

This matters for:

- **TTL purge**: the `purge_expired_inner` function manually walks the `ttl` index prefix
  and stops at `now`. With range scan support, this becomes a simple
  `Statement::Delete { filter: ttl < now }` that the planner routes through
  `IndexScan(ttl, Asc, upper_bound=now)`.
- **General range queries**: `find({ age: { $gt: 18 } })` on an indexed `age` field should
  use the index instead of scanning all records.

### Approach

Add optional `lower_bound` / `upper_bound` fields to `IndexScan`:

```rust
IndexScan {
    collection: String,
    column: String,
    value: Option<bson::Bson>,       // existing: exact Eq match
    range: Option<IndexRange>,       // new: range bounds
    direction: SortDirection,
    limit: Option<usize>,
    complete_groups: bool,
}

struct IndexRange {
    lower: Option<(bson::Bson, bool)>,  // (value, inclusive)
    upper: Option<(bson::Bson, bool)>,  // (value, inclusive)
}
```

The executor walks the index prefix and uses the sortable encoded bytes to skip entries
outside the range bounds, stopping early when the upper bound is exceeded (ascending) or
lower bound is exceeded (descending).

The planner pushes range predicates (`Lt`, `Gt`, `Lte`, `Gte`) on indexed fields into
`IndexScan.range` instead of leaving them as residual filters.

### Unlocks

- **TTL purge through planner**: `purge_expired_inner` becomes a `Statement::Delete`
  with `filter: ttl < DateTime(now)`, eliminating 50 lines of manual index walking.
- **TTL as a regular index**: once range scans work, the implicit `ttl` index can be
  declared as a normal index in `CollectionConfig`, removing all special-cased TTL
  handling from the executor and database.
- **Efficient range queries**: any indexed field benefits from range predicates.

---

## Route Inserts Through Planner

### Problem

`insert_one` / `insert_many` bypass the planner and executor entirely, doing manual
store operations (ID generation, duplicate key check, record write, index writes, TTL writes).
This duplicates logic that the executor already handles via `InsertIndex` and `Values`.

### Approach

Add `PlanNode::InsertRecord` (writes the record, complementing `PlanNode::Delete`) and
`Statement::Insert`. The pipeline becomes:

```
InsertIndex → InsertRecord → Values
```

ID generation and duplicate-key checking stay in `database.rs` (pre-processing before
building the `Statement`), or move into a `PlanNode::CheckDuplicate` node.

---

## CoverProject (Index-Covered Queries)

### Problem

When a query's projection only requests fields that exist in an index, the executor still
fetches the full document from the record store via `ReadRecord`. This is wasteful — the
index already contains enough data to satisfy the query without touching the main store.

For example, `find({ status: 'active' }, { projection: { status: 1 } })` on a collection
with a `status` index currently runs:

```
Projection [status]
  Filter [status = 'active']
    ReadRecord              ← unnecessary store fetch
      IndexScan [status]
```

The `ReadRecord` fetches the full document just so `Projection` can extract `status` — a
value that was already available in the index key.

### Background

`IndexScan` returns bare `_id` strings (`RawBson::String`), and `ReadRecord` uses that id
to fetch the full document from the store. This is the correct default path — most queries
need fields beyond what the index covers.

Previously, the planner had an `index_covered` optimization that skipped `ReadRecord` when
the index covered the projection. This was removed because `IndexScan` no longer carries
field values (it returns only the record id). `CoverProject` restores this optimization
with a cleaner architecture.

### Approach

Add a `PlanNode::CoverProject` that replaces `ReadRecord → Projection` when the planner
detects all projected fields exist in the index:

```rust
PlanNode::CoverProject {
    input: Box<PlanNode>,       // IndexScan
    column: String,             // indexed field name
    columns: Vec<String>,       // projected field names (subset of index)
}
```

The pipeline becomes:

```
CoverProject [status]       ← constructs doc from index key data
  Filter [status = 'active']
    IndexScan [status]       ← returns bare _id string
```

`CoverProject` would re-scan the index key for the covered fields, or the planner could
teach `IndexScan` to return structured key data (id + field value) when a `CoverProject`
follows it. The simplest approach: `CoverProject` takes the `_id` from `IndexScan`, looks
up the index key to extract the field value, and constructs a minimal
`RawDocumentBuf { _id, field: value }`.

### Planner detection

The planner already knows the projected columns and which fields are indexed. Detection
logic:

1. Query uses an `IndexScan` on field `F`
2. Projection requests only `_id` and/or `F`
3. Replace `Projection → ReadRecord` with `CoverProject`

For compound indexes (future), extend to check that all projected fields are covered by
the index key.

### Benefits

- **Skip store fetches entirely** for index-covered queries
- **Large wins on selective indexed queries** — the `status='active' projection [status]`
  benchmark regressed from ~8ms to ~35ms when the old covered path was removed; this
  restores that performance
- **Clean separation** — `IndexScan` stays simple (bare ids), `CoverProject` handles
  document construction from index data

---

## Collect Node (Plan Materialization Barrier)

### Concept

A `Collect` plan node that drains its child stream and emits the result as a single
materialized batch. This makes materialization points explicit in the plan tree instead
of hidden inside executor implementations.

```
Sort                    IndexMerge(And)
  Collect                 Collect            ← materialized into a set
    Scan                    IndexScan(status)
                          IndexScan(user_id) ← streamed, probed against set
```

### Motivation

Today, `Sort` and `IndexMerge` secretly materialize their inputs internally. The plan
tree shows `Sort(Scan)` which looks like a streaming pipeline, but the executor collects
everything into a `Vec` before sorting. With `Collect` as an explicit node, the plan
tree is honest about where memory grows.

### Current nodes that collect internally

- **`IndexMerge`** — collects both `lhs` and `rhs` into `Vec`s for set intersection/union
- **`Sort`** — collects all records into a `Vec` to sort in memory
- **`Distinct`** — iterates source eagerly to build a dedup set

### Asymmetric IndexMerge (And)

Postgres-style optimization: for `And`, collect only one side into a hash set, then
stream the other side and probe against it. This avoids materializing both sides.
For `Or`, both sides still need collection (full union).

### Benefits

- **Plan legibility** — materialization is visible in the plan tree
- **Reusable** — any node needing a materialized input wraps its child in `Collect`
- **EXPLAIN** — `Collect` nodes tell the user where memory grows
- **Future stages** — natural boundary for spill-to-disk, distributed execution, caching

### Performance note

This is primarily a **composability win**, not a performance win. The same work happens
either way. The real performance opportunity is the asymmetric `IndexMerge(And)` path,
which is an algorithm change that could be implemented with or without `Collect` as a
plan node.

---

## Document Walker (Single-Pass Field Extraction)

### Problem

Multiple executor nodes need to read fields from BSON documents by dot-notation paths:
Filter calls `raw_get_path` per predicate field, Sort calls it per sort key per comparison
(n*log(n) times), Projection iterates the doc and checks a key map, Distinct walks paths
to extract values. Each node walks the document independently, and some walk it repeatedly
(Sort re-walks on every comparison).

### Idea

A single-pass document walker that yields `(path, scalar_value)` pairs, guided by a
`FieldTree` to skip irrelevant subtrees:

```rust
fn walk_fields<'a>(
    doc: &'a RawDocument,
    tree: &HashMap<&str, FieldTree>,
) -> Vec<(&'a str, RawBsonRef<'a>)>
```

For `{ _id: "1", status: "active", address: { city: "Austin", zip: 78701 } }` with
tree `{ "status": Leaf, "address": Branch({ "city": Leaf }) }`, yields:

```
("status", String("active"))
("address.city", String("Austin"))
```

Each node consumes the extracted fields differently:
- **Projection**: write matched fields to output doc (preserving nesting structure)
- **Filter**: check values against predicate operators
- **Sort**: pre-extract sort keys once per doc, compare extracted keys during sort
- **Distinct**: grab the target field value for dedup

### Benefits

- **One walk per document** instead of N walks for N fields
- **FieldTree-guided pruning** — skip entire subtrees that no node cares about
- **Reuses `FieldTree`** already built for projection
- **Eliminates per-comparison path walks** in Sort (already done via pre-extraction,
  but this generalizes it)

### Considerations

- Projection needs to preserve document structure (nested docs, arrays), not just flat
  scalars — the walker needs to emit structural values too
- Filter needs to handle array element matching (`tags = 'foo'` matches if any element
  equals `'foo'`), which is more complex than scalar extraction
- May be better as a visitor/callback pattern than a collected vec, to avoid allocation

---

## Route Upserts/Merges Through Planner

### Problem

`upsert_many` and `merge_many` do per-document conditional logic (check if exists, then
branch to insert or update) with manual store operations. This is the hardest operation
to route through the planner because of the per-document branching.

### Approach

Options to explore:
- Keep the per-document conditional logic in `database.rs`, calling `plan()` for each branch
- Introduce a `PlanNode::Upsert` that handles the exists-check + branch internally
- Defer until inserts and range scans are proven stable

---

## Test Coverage

### Error paths

Test behavior for: malformed queries, missing collections, invalid filter operators.

### Encoding edge cases

Negative ints in index keys, empty strings, special characters in record IDs, very long values.

### ClientPool

Test connection pooling: checkout/return, behavior under contention, handling of dropped connections.
