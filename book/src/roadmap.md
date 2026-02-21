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
