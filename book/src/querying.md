# Querying

## Overview

Queries are executed as a three-tier plan tree. The planner analyzes filter conditions and available indexes to build an optimal execution plan, then the executor runs it lazily — records that fail a filter are never fully deserialized.

```
Document tier:   Projection (materialize raw → bson::Document)
                   ↑
                 Limit (skip/take)
                   ↑
                 Sort (lazy field access on raw bytes)
                   ↑
Raw tier:        Filter (lazy field access on raw bytes)
                   ↑
                 ReadRecord (fetch raw BSON bytes)
                   ↑
ID tier:         Scan / IndexScan / IndexMerge (produce record IDs)
```

**ID tier** — produces record IDs without touching document bytes.
**Raw tier** — operates on `RawDocumentBuf` (raw BSON bytes), accessing individual fields lazily. No full deserialization.
**Document tier** — `Projection` is the single materialization point, selectively converting only the requested columns to `bson::Document`.

## Query Model

```rust
Query {
    filter: Option<FilterGroup>,      // WHERE clause
    sort: Vec<Sort>,                   // ORDER BY
    skip: Option<usize>,              // OFFSET
    take: Option<usize>,              // LIMIT
    columns: Option<Vec<String>>,     // SELECT columns (projection)
}
```

Filters use a recursive group structure with AND/OR logical operators:

```rust
FilterGroup {
    logical: LogicalOp,               // And | Or
    children: Vec<FilterNode>,        // Condition | nested Group
}
```

Operators: `Eq`, `Gt`, `Gte`, `Lt`, `Lte`, `IsNull`, `IContains`, `IStartsWith`, `IEndsWith`.

## Index Configuration

Indexes are defined per datasource in the field definitions. The order of indexed fields in the `CollectionConfig` determines **priority** — when multiple indexed fields appear in an AND group, the first one in the list wins.

```rust
// indexes: ["user_id", "status"]
// user_id is priority 1, status is priority 2
```

## Plan Scenarios

The following scenarios show how the planner builds execution plans for different filter combinations. All examples assume:

```
indexed_fields: ["user_id", "status"]
```

---

### 1. No Filter

**Query:** `find({})`

```
ReadRecord
  └── Scan
```

Full scan — reads every record. No filter node needed.

---

### 2. Single Indexed Eq

**Query:** `find({ filter: status = "active" })`

```
ReadRecord
  └── IndexScan(status = "active")
```

The `Eq` condition on an indexed field becomes an `IndexScan`. Since it's the only condition, there's no residual filter — every record from the index matches.

---

### 3. Single Non-Indexed Condition

**Query:** `find({ filter: score > 50 })`

```
Filter(score > 50)
  └── ReadRecord
        └── Scan
```

No index available — full scan with a filter. Every record is fetched as raw bytes, and `score > 50` is evaluated lazily on the raw BSON. Records that fail are never deserialized.

---

### 4. AND — Indexed + Non-Indexed

**Query:** `find({ filter: status = "active" AND score > 50 })`

```
Filter(score > 50)
  └── ReadRecord
        └── IndexScan(status = "active")
```

The `Eq` on `status` becomes an `IndexScan`. The `score > 50` condition can't use an index (not `Eq`, and `score` isn't indexed anyway), so it becomes a residual `Filter`. The index narrows the candidate set; the filter evaluates the rest lazily.

---

### 5. AND — Priority Selection

**Query:** `find({ filter: status = "active" AND user_id = "abc" })`

```
Filter(status = "active")
  └── ReadRecord
        └── IndexScan(user_id = "abc")
```

Both fields are indexed, but only one `IndexScan` is chosen. The planner iterates `indexed_fields` in order — `user_id` comes before `status`, so `user_id` wins regardless of the filter's ordering. `status = "active"` becomes a residual filter.

This is priority-based selection: the `indexed_fields` order determines which index is preferred, giving you control over which index is used when multiple are available.

---

### 6. AND — Multiple Indexed + Non-Indexed

**Query:** `find({ filter: user_id = "abc" AND status = "active" AND score > 50 AND name IContains "alice" })`

```
Filter(status = "active" AND score > 50 AND name IContains "alice")
  └── ReadRecord
        └── IndexScan(user_id = "abc")
```

Only one index is used. `user_id` has the highest priority, so it becomes the `IndexScan`. Everything else — even the indexed `status = "active"` — becomes residual. The index narrows the set to records with `user_id = "abc"`, then the filter evaluates the remaining three conditions lazily on raw bytes.

---

### 7. OR — Both Branches Indexed

**Query:** `find({ filter: user_id = "abc" OR status = "active" })`

```
Filter(user_id = "abc" OR status = "active")
  └── ReadRecord
        └── IndexMerge(Or)
              ├── IndexScan(user_id = "abc")
              └── IndexScan(status = "active")
```

Every OR branch has an indexed `Eq`, so the planner builds an `IndexMerge(Or)` — a union of ID sets from both index scans. The full OR is kept as a residual filter for rechecking, because each `IndexScan` may over-fetch (an index only guarantees the indexed field matches, not the full predicate of a nested group).

---

### 8. OR — One Branch Not Indexed

**Query:** `find({ filter: status = "active" OR name = "test" })`

```
Filter(status = "active" OR name = "test")
  └── ReadRecord
        └── Scan
```

`name` is not indexed. If **any** OR branch lacks an indexed `Eq` condition, the entire OR falls back to a full `Scan`. This is because an OR requires *all* branches to produce results — you can't skip a branch. A single unindexed branch means you need to scan everything anyway.

---

### 9. OR — Same Field, Multiple Values (IN-style)

**Query:** `find({ filter: user_id = 1 OR user_id = 2 })`

```
Filter(user_id = 1 OR user_id = 2)
  └── ReadRecord
        └── IndexMerge(Or)
              ├── IndexScan(user_id = 1)
              └── IndexScan(user_id = 2)
```

This is effectively an `IN` query. Each value gets its own `IndexScan`, and the results are unioned via `IndexMerge(Or)`. This pattern is common for multi-tenant access control — e.g., "show records for user 1 or user 2."

---

### 10. OR — Three Values (Left-Associative Fold)

**Query:** `find({ filter: user_id = 1 OR user_id = 2 OR user_id = 3 })`

```
Filter(user_id = 1 OR user_id = 2 OR user_id = 3)
  └── ReadRecord
        └── IndexMerge(Or)
              ├── IndexMerge(Or)
              │     ├── IndexScan(user_id = 1)
              │     └── IndexScan(user_id = 2)
              └── IndexScan(user_id = 3)
```

Multiple OR branches fold into a left-associative binary tree: `((1 Or 2) Or 3)`. Each `IndexMerge(Or)` unions the ID sets from its children.

---

### 11. AND with Nested OR — Direct Eq Takes Priority

**Query:** `find({ filter: user_id = "abc" AND (status = "active" OR status = "archived") })`

```
Filter(status = "active" OR status = "archived")
  └── ReadRecord
        └── IndexScan(user_id = "abc")
```

The planner's first pass looks for direct `Eq` conditions on indexed fields. `user_id = "abc"` is a direct `Eq` on the highest-priority indexed field, so it becomes the `IndexScan`. The OR sub-group, even though it's fully indexable, becomes a residual filter.

This is the expected behavior — `user_id` is higher priority, so we use its index. The OR filter then evaluates cheaply on the smaller candidate set.

---

### 12. AND with Nested OR — OR Becomes IndexMerge When No Direct Eq

**Query:** `find({ filter: (status = "active" OR status = "archived") AND score > 50 })`

Indexed fields: `["status"]` (no `user_id` in this example)

```
Filter(score > 50)
  └── ReadRecord
        └── IndexMerge(Or)
              ├── IndexScan(status = "active")
              └── IndexScan(status = "archived")
```

No direct `Eq` condition on an indexed field exists in the AND group. The planner's second pass checks OR sub-groups — `(status = "active" OR status = "archived")` is fully indexable, so it becomes an `IndexMerge(Or)`. The `score > 50` condition becomes the residual filter.

---

### 13. OR with Nested ANDs

**Query:** `find({ filter: (user_id = "abc" AND status = "active") OR (user_id = "xyz" AND status = "pending") })`

```
Filter((user_id = "abc" AND status = "active") OR (user_id = "xyz" AND status = "pending"))
  └── ReadRecord
        └── IndexMerge(Or)
              ├── IndexScan(user_id = "abc")
              └── IndexScan(user_id = "xyz")
```

Each AND branch is planned independently. Within each branch, `user_id` (highest priority) becomes the `IndexScan`. The two branches are combined with `IndexMerge(Or)`. The full OR predicate is the residual filter — it rechecks both the indexed and non-indexed conditions per branch.

---

### 14. OR with Partial Index Per Branch

**Query:** `find({ filter: (user_id = "abc" AND score > 50) OR status = "active" })`

```
Filter((user_id = "abc" AND score > 50) OR status = "active")
  └── ReadRecord
        └── IndexMerge(Or)
              ├── IndexScan(user_id = "abc")
              └── IndexScan(status = "active")
```

Each OR branch has at least one indexed `Eq`. The first branch picks `user_id` via AND priority selection; the second branch uses `status` directly. Both produce `IndexScan` nodes combined with `IndexMerge(Or)`. The full predicate is the residual recheck — `score > 50` is evaluated lazily for records from the `user_id` branch.

---

### 15. OR with Unindexed Branch — Fallback to Scan

**Query:** `find({ filter: (user_id = "abc" OR status = "active") OR (count > 5 OR name = "foo") })`

```
Filter((user_id = "abc" OR status = "active") OR (count > 5 OR name = "foo"))
  └── ReadRecord
        └── Scan
```

The second OR branch `(count > 5 OR name = "foo")` has zero indexed `Eq` conditions. Since any unindexed OR branch poisons the entire OR, the whole query falls back to `Scan`. The complete predicate becomes a residual filter.

---

### 16. Complex — Multi-Level Nesting

**Query:** `find({ filter: (user_id = 1 OR user_id = 2 OR user_id = 3) AND status = "active" AND (count > 5 OR name = "foo") })`

```
Filter((user_id = 1 OR user_id = 2 OR user_id = 3) AND (count > 5 OR name = "foo"))
  └── ReadRecord
        └── IndexScan(status = "active")
```

The AND group has three children:
1. `(user_id = 1 OR user_id = 2 OR user_id = 3)` — fully indexable OR sub-group
2. `status = "active"` — direct `Eq` on an indexed field
3. `(count > 5 OR name = "foo")` — not indexable

The planner's first pass checks for **direct** `Eq` conditions in indexed field priority order. `user_id` is highest priority but isn't a direct condition — it's inside an OR group. `status = "active"` is a direct `Eq`, so it wins. The OR sub-group and the unindexable `(count > 5 OR name = "foo")` both become residual filters.

The second pass (which checks for fully-indexable OR sub-groups) is only reached if no direct `Eq` is found. If `status` were not indexed, the second pass would pick the OR sub-group and produce:

```
Filter(status = "active" AND (count > 5 OR name = "foo"))
  └── ReadRecord
        └── IndexMerge(Or)
              ├── IndexMerge(Or)
              │     ├── IndexScan(user_id = 1)
              │     └── IndexScan(user_id = 2)
              └── IndexScan(user_id = 3)
```

---

### 17. Fully Unindexed

**Query:** `find({ filter: score > 50 AND name IContains "alice" })`

```
Filter(score > 50 AND name IContains "alice")
  └── ReadRecord
        └── Scan
```

No indexed fields, no `Eq` operators — full scan. All conditions are evaluated lazily on raw bytes. Even without index acceleration, lazy materialization means rejected records are never fully deserialized.

---

## Full Pipeline Example

**Query:** `find({ filter: status = "active" AND score > 50, sort: score DESC, skip: 10, take: 5, columns: ["name", "score"] })`

```
Projection(name, score)
  └── Limit(skip: 10, take: 5)
        └── Sort(score DESC)
              └── Filter(score > 50)
                    └── ReadRecord
                          └── IndexScan(status = "active")
```

Execution flow:

1. **IndexScan** — iterates the `status = "active"` index, yields record IDs
2. **ReadRecord** — batch-fetches raw BSON bytes via `multi_get`
3. **Filter** — evaluates `score > 50` by accessing just the `score` field from raw bytes. Rejected records are skipped with zero deserialization cost
4. **Sort** — collects surviving records, accesses `score` from raw bytes for comparison, sorts in memory
5. **Limit** — skips the first 10 records, takes the next 5
6. **Projection** — materializes only `name` and `score` from raw bytes into a `bson::Document`, inserts `_id`

Records that fail the filter at step 3 never reach steps 4-6. Only the final 5 records at step 6 pay the cost of full (selective) deserialization.

## Dot-Notation Paths

Filters, sorts, and projections support nested field access via dot notation:

```
filter: address.city = "Austin"
sort: address.zip ASC
columns: ["name", "address.city"]
```

Path resolution works on raw BSON bytes — `RawDocumentBuf::get_document("address")` retrieves the nested document, then `.get("city")` retrieves the field. No full deserialization needed.

For projections with dot-notation, the top-level key is included in materialization (e.g., `address.city` includes the entire `address` object), then `apply_projection` trims nested documents to only the requested sub-paths.

## Performance Characteristics

**Lazy materialization** is the core optimization. The cost model:

| Scenario | Deserialization cost |
|----------|---------------------|
| Record passes filter + included in result | Full (or selective with projection) |
| Record passes filter + excluded by Limit | Sort key access only (raw bytes) |
| Record fails filter | Filter field access only (raw bytes) |
| No filter, no projection | Full deserialization |

**Index acceleration** reduces the number of records entering the raw tier:

| Access pattern | Records entering raw tier |
|----------------|--------------------------|
| `Scan` (no index) | All records |
| `IndexScan` (single index) | Records matching the indexed condition |
| `IndexMerge(Or)` | Union of records from each index |

**Combined effect:** A query with an indexed filter that rejects 90% of candidates and a non-indexed filter that rejects another 50% of the remaining — only 5% of records are fully deserialized.
