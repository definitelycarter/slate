# Roadmap

## ~~Streaming Mutations~~ (Done)

Streaming mutation pipelines are fully implemented. All mutation operations (`delete_one`,
`delete_many`, `update_one`, `update_many`, `replace_one`, `upsert_many`, `merge_many`)
go through composable pipeline nodes (`DeleteIndex → Delete`, `InsertIndex → Update →
DeleteIndex`, `InsertIndex → Upsert → Values`, etc.) via the `Statement` enum and unified
`planner::plan()` entry point. The executor streams one record at a time through the
pipeline with `O(1)` memory.

---

## ~~Index Range Scans~~ (Done)

`IndexScan` now supports range predicates (`Gt`, `Gte`, `Lt`, `Lte`) on indexed fields
via the `IndexFilter` enum, replacing the old `value: Option<Bson>` field:

```rust
enum IndexFilter {
    Eq(Bson),
    Gt(Bson), Gte(Bson), Lt(Bson), Lte(Bson),
    Range { lower: IndexBound, upper: IndexBound },
}
```

The planner detects range conditions on indexed fields and pushes them into `IndexScan`
with priority order: Eq > OR sub-group > Range. Dual bounds on the same field (e.g.
`score >= 50 AND score <= 90`) are merged into a single `Range` filter, consuming both
AND children. Non-indexed range predicates remain as residual filters.

The executor does byte-level comparison against pre-encoded bounds with early termination:
ascending scans skip below the lower bound and stop at the upper bound; descending scans
skip above the upper bound and stop at the lower bound. Range bounds are boxed behind
`Option<Box<RangeBounds>>` to keep the Eq/None closure at zero overhead (8 bytes vs 56
bytes inlined).

### Unlocks

- **TTL purge through planner**: `purge_expired_inner` can become a `Statement::Delete`
  with `filter: ttl < DateTime(now)`, eliminating manual index walking.
- **Efficient range queries**: any indexed field benefits from range predicates.

---

## ~~Route Inserts Through Planner~~ (Done)

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

## ~~Index-Covered Projection~~ (Done)

When a query projects only `_id` and/or the indexed field, and uses an equality match
with no residual filter, `IndexScan` now operates in **covered mode** — it yields finished
`{ _id, field: value }` documents directly, bypassing `ReadRecord`, `Filter`, and
`Projection` entirely.

```
IndexScan [status, covered=true]    ← yields { _id, status } docs directly
```

vs the standard path:

```
Projection [status]
  Filter [status = 'active']
    ReadRecord
      IndexScan [status]            ← yields bare _id strings
```

The implementation lives in `IndexScan` itself via a `covered: bool` flag on `PlanNode::IndexScan`.
When covered, `IndexScan` coerces the query value back to its stored BSON type using the
type byte from the index entry (e.g. the query may use `Int64(100)` but the document stored
`Int32(100)` — the type byte ensures fidelity). The coerced value is converted to `RawBson`
once and reused across all matching records.

The non-covered path is unchanged — bare `String` IDs with zero overhead.

**Benchmarks**: ~77% faster on indexed equality + projection queries. No regression on
non-covered queries.

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

## ~~Document Walker (Single-Pass Field Extraction)~~ (Done)

`FieldTree` + `walk()` in `executor/field_tree.rs` implements the tree-guided document
walker. Built once from dot-notation paths, reused across all documents with zero
per-document allocations.

`FieldTree::from_paths()` builds a tree from a list of dot-notation paths:
```text
["foo.bar.baz", "foo.bar.bux", "name"] →
{ "foo": Branch({ "bar": Branch({ "baz": Leaf, "bux": Leaf }) }), "name": Leaf }
```

`walk(doc, tree, visitor)` walks a `&RawDocument` once, calling the visitor for each
matching field. Expands arrays automatically — Leaf arrays call the visitor per element,
Branch arrays recurse into document elements. Skips irrelevant subtrees entirely.

Used by 5 of 7 executor nodes:
- **Projection** — selective field copying via `FieldTree` key matching
- **InsertIndex** / **DeleteIndex** — extract indexed field values for index key writes/deletes
- **Distinct** — extract field values for dedup
- **Upsert** — delete old index entries before replace/merge

Sort and Filter still use the per-field `raw_get_path()` approach. Migrating them is a
potential optimization but not critical — Sort already pre-extracts keys once per record,
and Filter short-circuits on the first failing predicate

---

## ~~Route Upserts/Merges Through Planner~~ (Done)

Upsert and merge operations now go through the planner and executor pipeline. Two new
`Statement` variants (`UpsertMany`, `MergeMany`) route through `planner::plan()`, which
builds a streaming pipeline:

```
InsertIndex → Upsert → Values
```

The `PlanNode::Upsert` node handles the full per-document lifecycle in a single streaming
pass:

1. **Extract `_id`** from the input doc (or generate a UUID if missing)
2. **Look up existing doc** via `txn.get()` on the record key
3. **If exists**: delete old index entries via `FieldTree::walk()`, then either replace
   the full document (`UpsertMode::Replace`) or merge fields via
   `RawDocumentBuf::append_ref()` (`UpsertMode::Merge`)
4. **If new**: normalize the document (ensure `_id` is first field) and insert
5. **Emit the written doc** downstream so `InsertIndex` can write new index entries

`UpsertMode` controls the update behavior:
- **`Replace`** — full document swap (like MongoDB's `replaceOne` with `upsert: true`)
- **`Merge`** — partial update, unchanged fields are zero-copy via `append_ref()` (like
  MongoDB's `$set`-style merge). Returns `None` (no-op) when all fields already match,
  avoiding unnecessary writes

Old index cleanup uses `FieldTree::walk()` (zero per-document allocations) instead of the
previous `raw_get_path_values()` approach that allocated a `String` per invocation. TTL
index entries are cleaned up when the old value is a `DateTime`; non-DateTime TTL values
are skipped (no delete needed since they were never indexed).

---

## Test Coverage

### Error paths

Test behavior for: malformed queries, missing collections, invalid filter operators.

### Encoding edge cases

Negative ints in index keys, empty strings, special characters in record IDs, very long values.

### ClientPool

Test connection pooling: checkout/return, behavior under contention, handling of dropped connections.
