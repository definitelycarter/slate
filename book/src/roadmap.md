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

- ~~**TTL purge through planner**~~: Done — see Engine/Client Split below.
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

## ~~Engine/Client Split + TTL Purge via Planner~~ (Done)

Split the monolithic `database.rs` into a layered architecture:

- **`Engine<S>`** (`engine.rs`) — core engine holding `store` + `catalog`. Provides
  `begin()` → `Transaction` with all query/mutation/index logic. No background threads.
- **`Transaction`** (renamed from `DatabaseTransaction`) — all planner/executor operations.
- **`Database<S>`** (`database.rs`) — thin embedded client wrapping `Arc<Engine<S>>`.
  Adds TTL sweep thread, delegates everything else to the engine.

The sweep thread holds its own `Arc<Engine<S>>` and calls `engine.purge_expired()`
directly — no circular ownership. `purge_expired` is now a single `delete_many(ttl < now)`
through the planner pipeline, replacing ~50 lines of manual index walking, byte comparison,
and record deletion.

"ttl" is auto-added as an index on every collection via `ensure_indexes`, so the planner
produces `IndexScan(Lt(now))` with early termination. Special TTL handling removed from
`InsertIndex`/`DeleteIndex` — TTL is now a regular indexed field.

`DatabaseTransaction` is re-exported as a type alias for backwards compatibility.

---

## ~~TTL Read Filtering~~ (Done)

Expired documents are now immediately invisible to all reads and mutations, without
waiting for the background sweep. TTL is stored in a **record envelope** — a fixed-offset
prefix on every stored value — enabling O(1) expiry checks (~0.5ns/doc) with zero BSON
parsing.

### On-Disk Format

Every record value is wrapped in a thin envelope before storage:

```
No TTL:   [0x00][BSON bytes...]
Has TTL:  [0x01][8-byte LE i64 millis][BSON bytes...]
```

The tag byte is extensible — future tags (0x02, 0x03...) can add more fixed-size metadata
fields (e.g. `created_at`, `updated_at`) before the BSON payload. The `ttl` field is kept
in the BSON document as well (duplicate data) to avoid stripping/injecting it on reads.

### Envelope Helpers (`encoding.rs`)

- `encode_record(bson_bytes)` — scans BSON for `ttl` DateTime field at write time (~12ns),
  prepends `[0x01][millis]` or `[0x00]`
- `decode_record(data)` — reads tag byte, returns `(Option<ttl_millis>, bson_slice)`
- `is_record_expired(data, now_millis)` — O(1): read tag byte, if `0x01` compare 8-byte millis
- `record_bson_offset(data)` — returns byte offset where BSON starts (avoids borrow issues in scan)

### Performance

| Approach | With TTL | Without TTL |
|---|---|---|
| `RawDocument::get` (full BSON parse) | 57.4ns | 48.0ns |
| Byte walker (scan raw BSON fields) | 12.3ns | 10.1ns |
| **Record envelope (current)** | **0.5ns** | **0.4ns** |

The byte walker (~12ns) is still used at write time inside `encode_record` to extract the
TTL millis from the BSON payload. This cost is negligible relative to the `txn.put` overhead.

### Where Checks Happen

TTL checks happen inline at the two data-yielding executor nodes:

- **`Scan`** — calls `is_record_expired` on raw bytes before any BSON parsing
- **`ReadRecord`** — calls `get_record_if_alive` (wraps `txn.get()` + TTL check)

These two nodes never appear together in the same pipeline, so every document is checked
exactly once. The `Executor` struct carries `now_millis` — normal queries use
`DateTime::now()`, while `purge_expired` uses `i64::MIN` to disable filtering so the
delete pipeline can read expired docs.

- `find`, `count`, `distinct` — expired docs filtered at Scan/ReadRecord
- `find_by_id` — envelope TTL check before deserialization
- `update`, `replace`, `delete` — expired docs filtered at source nodes
- `upsert`/`merge` — expired existing docs treated as non-existent (old indexes cleaned, insert path taken)
- `purge_expired` — uses `Statement::FlushExpired` with TTL index for efficient scanning; TTL filtering disabled via `Executor::new_no_ttl_filter`

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
previous `raw_get_path_values()` approach that allocated a `String` per invocation.

---

## Test Coverage

### Error paths

Test behavior for: malformed queries, missing collections, invalid filter operators.

### Encoding edge cases

Negative ints in index keys, empty strings, special characters in record IDs, very long values.

### ClientPool

Test connection pooling: checkout/return, behavior under contention, handling of dropped connections.
