# Querying

## Overview

Queries are executed as a two-tier plan tree. The planner analyzes filter conditions and available indexes to build an optimal execution plan, then the executor runs it lazily — records that fail a filter are never fully deserialized.

```
Raw tier:        Projection (selective field copying via RawDocumentBuf::append)
                   ↑
                 Limit (skip/take on record stream or RawBson::Array)
                   ↑
                 Sort (lazy field access on raw bytes)
                   ↑
                 Filter (lazy field access on raw bytes)
                   ↑
                 ReadRecord (fetch raw BSON bytes)
                   ↑
ID tier:         Scan / IndexScan / IndexMerge (produce record IDs)
```

**ID tier** — produces record IDs without touching document bytes.
**Raw tier** — everything above ReadRecord operates on `RawValue<'a>` — a Cow-like enum over `RawBsonRef<'a>` (borrowed) and `RawBson` (owned). For documents, constructs `&RawDocument` views to access individual fields lazily. No full deserialization. MemoryStore preserves zero-copy via `RawValue::Borrowed`, RocksDB uses `RawValue::Owned`. For index-covered queries, `RawValue` carries scalar values (String, Int, etc.) directly from the index — no document fetch needed. Projection builds `RawDocumentBuf` output using `append()` for selective field copying — no `bson::Document` materialization in the pipeline. `find()` returns `Vec<RawDocumentBuf>` directly. For distinct queries, the pipeline emits a single `RawBson::Array` — Sort and Limit handle arrays natively by sorting/slicing elements in-place.

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

### 18. Index-Covered Projection

**Query:** `find({ filter: status = "active", columns: ["status"] })`

```
Projection(status)
  └── IndexScan(status = "active")
```

The projection only requests `status`, which is the indexed field used by the `IndexScan`. Since the `IndexScan` already carries the matched value (`"active"`) as a `RawValue`, the planner skips `ReadRecord` entirely — no document bytes are fetched from storage. `Projection` constructs `{ _id, status: "active" }` directly from the index value.

Each index entry stores the BSON element type byte in its value (e.g. `0x10` for Int32, `0x12` for Int64). When the query's value type differs from the stored type (e.g. query sends `Int64(100)` but the document stored `Int32(100)`), the executor coerces the emitted value to match the stored type. This ensures correct type round-tripping through index-covered projections.

This optimization applies when **all** of these conditions are met:
1. The ID tier is an `IndexScan` with an `Eq` value (not an ordered scan)
2. No residual filter exists (the index fully satisfies the WHERE clause)
3. No sort is requested
4. Every projected column matches the indexed field

When any condition isn't met, `ReadRecord` is included as normal:

**Query:** `find({ filter: status = "active", columns: ["status", "name"] })`

```
Projection(status, name)
  └── ReadRecord
        └── IndexScan(status = "active")
```

Here `name` isn't available from the index, so the full document must be fetched.

#### Limitation: Multi-Column Coverage

The optimization is single-field only — the indexed column must be the *only* projected column (besides `_id`). Even if all projected columns are individually indexed, `ReadRecord` is still required:

**Query:** `find({ filter: status = "active", columns: ["user_id", "status"] })` (both indexed)

```
Projection(user_id, status)
  └── ReadRecord
        └── IndexScan(status = "active")
```

The `IndexScan` on `status` carries the value `"active"`, but has no way to provide `user_id` — that lives in a separate index. To cover this case without `ReadRecord`, two possible future approaches:

1. **Composite indexes** — a single index on `(status, user_id)` that stores both values per key
2. **Secondary index lookups** — after getting IDs from the primary `IndexScan`, look up each ID in the `user_id` index to retrieve its value

---

### 19. Array Element Matching

**Query:** `find({ filter: tags = "renewal_due" })` (where `tags` is an array field like `["active", "renewal_due"]`)

```
Filter(tags = "renewal_due")
  └── ReadRecord
        └── Scan
```

When a filter field resolves to a `RawBsonRef::Array`, the executor iterates the array elements and returns true if **any** element satisfies the operator. This is implicit — no special syntax needed, matching MongoDB's behavior without `$elemMatch`.

Applies to all scalar comparison operators: `Eq`, `Gt`, `Gte`, `Lt`, `Lte`. Each element delegates to the existing scalar comparison functions, so cross-type coercion (String→Int, Double↔Int, etc.) works automatically within array elements.

**Not supported:** sorting on array fields has no meaningful scalar ordering and is left unsupported. Filtering on nested array paths (e.g. `items.[].sku = "A1"`) is handled separately by the multi-key path resolution in `get_path_values`.

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
6. **Projection** — copies only `name` and `score` from raw bytes into a `RawDocumentBuf` via `append()`, inserts `_id`

Records that fail the filter at step 3 never reach steps 4-6. Only the final 5 records at step 6 pay the cost of selective field copying.

## Limit Placement

Limit operates in the raw tier. For record streams (find queries), it's lazy `skip()` + `take()` on the iterator. For `RawBson::Array` values (distinct queries), it slices the array elements directly. Where it sits in the plan tree depends on whether Sort is present.

### Scenario A: Limit with Sort

**Query:** `find({ filter: status = "active", sort: score DESC, take: 200 })`

```
Projection
  └── Limit(take: 200)
        └── Sort(score DESC)
              └── Filter(status = "active")
                    └── ReadRecord
                          └── IndexScan(status = "active")
```

Limit must stay above Sort — you can't take before sorting. All ~50k matching records enter Sort. Limit takes 200 from the sorted result. Cost: O(n log n) sort on the full set.

### Scenario B: Limit without Sort

**Query:** `find({ filter: status = "active", take: 200 })`

```
Projection
  └── Limit(take: 200)
        └── ReadRecord
              └── IndexScan(status = "active")
```

No Sort means Limit sits directly above ReadRecord. The iterator stops after 200 records pass through. Remaining index entries and raw bytes are never touched. Much cheaper than Scenario A.

### Scenario C: Limit without Sort, with Filter

**Query:** `find({ filter: score > 50, take: 200 })`

```
Projection
  └── Limit(take: 200)
        └── Filter(score > 50)
              └── ReadRecord
                    └── Scan
```

Records stream through Filter one at a time. Limit stops after 200 pass. Records that fail the filter don't count toward the limit — the scan continues until 200 qualifying records are found (or the collection is exhausted).

## Distinct Queries

Distinct queries find unique values for a single field. The planner builds a plan tree that reuses the same filter/index infrastructure as `find()`, then adds `Projection` → `Distinct` to extract and deduplicate values.

### Query Model

```rust
DistinctQuery {
    field: String,                     // The field to collect unique values from
    filter: Option<FilterGroup>,       // Optional WHERE clause (same as find)
    sort: Option<SortDirection>,       // Optional sort on the distinct values
    skip: Option<usize>,              // Skip first N unique values
    take: Option<usize>,              // Take N unique values
}
```

### Pipeline

```
ID tier:     Scan / IndexScan / IndexMerge
               ↑
Raw tier:    ReadRecord → Filter → Projection → Distinct → Sort → Limit
```

Distinct receives projected documents from Projection, extracts the target field, deduplicates with a `HashSet<u64>` (hashing raw BSON bytes), and emits a single `RawBson::Array` containing all unique values. This is fundamentally different from `find()`, which emits one item per record — Distinct collapses the entire result set into one array value.

### Plan Trees

**Distinct (no filter, no sort):**

```
Distinct(status)
  └── Projection([status])
        └── ReadRecord
              └── Scan
```

**Distinct (with filter):**

```
Distinct(status)
  └── Projection([status])
        └── Filter(score > 50)
              └── ReadRecord
                    └── IndexScan(user_id = "abc")
```

Filter planning is identical to `find()` — the same index priority rules, AND/OR handling, and residual filter logic apply.

**Distinct (with sort):**

```
Sort(status ASC)
  └── Distinct(status)
        └── Projection([status])
              └── ReadRecord
                    └── Scan
```

Sort sits above Distinct. The planner passes the sort field and direction to `Sort`, which handles the `RawBson::Array` natively.

**Distinct (with sort and limit):**

```
Limit(skip: 1, take: 2)
  └── Sort(status ASC)
        └── Distinct(status)
              └── Projection([status])
                    └── ReadRecord
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

Distinct uses `HashSet<u64>` with a hash of the raw BSON bytes (`hash_raw`). This avoids materializing values for comparison — the raw byte representation is hashed directly. Null values are skipped. Nested fields are supported via `raw_walk_path`, which recursively walks dot-notation paths and invokes a callback for each matching value (handling both scalar fields and array elements).

## Dot-Notation Paths

Filters, sorts, and projections support nested field access via dot notation:

```
filter: address.city = "Austin"
sort: address.zip ASC
columns: ["name", "address.city"]
```

Path resolution works on raw BSON bytes — `RawDocument::get_document("address")` retrieves the nested document, then `.get("city")` retrieves the field. No full deserialization needed.

For projections with dot-notation, the top-level key is included in materialization (e.g., `address.city` includes the entire `address` object), then `apply_projection` trims nested documents to only the requested sub-paths.

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
