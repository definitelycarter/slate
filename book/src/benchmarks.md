# Benchmarks

Snapshot from `slate-bench` — run on Apple Silicon (M-series) in release mode. Each record has 8 fields (6 required, 2 nullable). Two dataset sizes: 10k and 100k records per user, 3 users each.

All benchmarks use the **embedded MemoryStore** backend — no persistence layer, no TCP overhead. This isolates query engine performance from storage-specific noise (RocksDB compaction, block cache warmup, etc.), making benchmark-to-benchmark comparisons reliable.

Storage model: records are stored as `d:{_id}` → raw BSON bytes via `bson::to_vec`. Queries use a three-tier plan tree: the **ID tier** (Scan, IndexScan, IndexMerge) produces record IDs without touching document bytes. `ReadRecord` fetches raw BSON bytes by ID but does **not** deserialize them. The **raw tier** (Filter, Sort, Limit) operates on `RawValue<'a>` — a Cow-like enum over `RawBsonRef<'a>` (borrowed, zero-copy from MemoryStore snapshots) and `RawBson` (owned). Filter and Sort construct `&RawDocument` views on demand to access individual fields without allocation. Records that fail a filter are never cloned or deserialized. Finally, **Projection** is the single materialization point where raw bytes → `bson::Document`, selectively deserializing only the requested columns. For index-covered queries (where all projected columns are available from the index), `ReadRecord` is skipped entirely — `Projection` constructs documents directly from `RawValue` scalars carried by `IndexScan`. For full scans without a filter, `ReadRecord` optimizes the `Scan` input into a single-pass iteration.

## Record Schema

| Field | Type | Indexed | Notes |
|-------|------|---------|-------|
| name | String | No | Random name |
| status | String | **Yes** | Random: "active" or "rejected" (~50/50) |
| contacts_count | Int | **Yes** | Random 0–100 |
| product_recommendation1 | String | No | Random: ProductA / ProductB / ProductC |
| product_recommendation2 | String | No | Random: ProductX / ProductY / ProductZ |
| product_recommendation3 | String | No | Random: Widget1 / Widget2 / Widget3 |
| last_contacted_at | Date | No | ~30% null |
| notes | String | No | ~50% null |

## 10k Records

Results from a single collection (10,000 records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 1,000 records | ~25ms | ~0.0025ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~10ms | 10,000 | Single key per record |
| status = 'active' (indexed) | ~6ms | ~5,000 | IndexScan → lazy filter |
| status = 'active' projection [status] (index-covered) | ~1.2ms | ~5,000 | IndexScan → Projection, no ReadRecord |
| product_recommendation1 = 'ProductA' | ~4.4ms | ~3,300 | Scan + lazy filter, ~67% rejected without deserializing |
| status + rec1 + rec2 (AND) | ~2.4ms | ~550 | IndexScan + lazy filter, ~89% rejected |
| status='active' + sort + skip/take(50) | ~6ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~6ms | ~5,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~11ms | ~5,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~0.45ms | 200 | Lazy ID tier — Limit stops after 200 |
| status='active' + sort + take(200) | ~6.2ms | 200 | IndexScan → Sort → Limit |
| sort contacts_count + take(200) | ~0.05ms | 200 | Indexed sort — walks 200 index entries, no Sort node |
| sort contacts_count (no take) | ~20ms | 10,000 | Full sort — no optimization without Limit |
| sort [contacts_count DESC, name ASC] + take(200) | ~1.9ms | 200 | Indexed sort — group-aware limit, sub-sort ~1 group |
| sort [status DESC, contacts_count ASC] + take(200) | ~9.6ms | 200 | Indexed sort — low cardinality, large group |
| sort [name ASC, contacts_count DESC] + take(200) | ~1.3ms | 200 | Not indexed — full Sort, Limit stops early |
| last_contacted_at is null (~30%) | ~4.4ms | ~3,000 | Scan + lazy filter, ~70% rejected |
| notes is null (~50%) | ~6.2ms | ~5,000 | Scan + lazy filter |
| last_contacted_at is not null (~70%) | ~8.7ms | ~7,000 | Scan + lazy filter |
| status='active' AND notes is null | ~4.2ms | ~2,500 | IndexScan + lazy filter |
| 1,000 point lookups (find_by_id) | ~1.3ms | 1,000 | Direct key access |
| projection (name, status only) | ~6.2ms | 10,000 | Selective materialization at Projection |

### Distinct

| Query | Time | Values Returned | Notes |
|-------|------|-----------------|-------|
| distinct: status (indexed) | ~1.2ms | 2 | Full scan, low cardinality |
| distinct: status (indexed) + sort asc | ~1.2ms | 2 | Sort overhead negligible at 2 values |
| distinct: product_recommendation1 (not indexed) | ~1.7ms | 3 | Full scan, low cardinality |
| distinct: contacts_count (indexed, ~100 values) | ~1.6ms | 100 | Full scan, medium cardinality |
| distinct: contacts_count (indexed) + sort asc | ~1.6ms | 100 | Sort overhead negligible at 100 values |
| distinct: rec1 with status='active' filter | ~1.9ms | 3 | IndexScan narrows to ~50% of records |
| distinct: last_contacted_at (high cardinality) | ~21ms | ~7,000 | Linear dedup O(n*k), ~70% of records have value |

## 100k Records

Results from a single collection (100,000 records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~285ms | ~0.0029ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~96ms | 100,000 | Single key per record |
| status = 'active' (indexed) | ~62ms | ~50,000 | IndexScan → lazy filter |
| status = 'active' projection [status] (index-covered) | ~12ms | ~50,000 | IndexScan → Projection, no ReadRecord |
| product_recommendation1 = 'ProductA' | ~45ms | ~33,000 | Scan + lazy filter, ~67% rejected without deserializing |
| status + rec1 + rec2 (AND) | ~26ms | ~5,500 | IndexScan + lazy filter, ~89% rejected |
| status='active' + sort + skip/take(50) | ~64ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~61ms | ~50,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~114ms | ~50,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~2.5ms | 200 | Lazy ID tier — Limit stops after 200 |
| status='active' + sort + take(200) | ~64ms | 200 | IndexScan → Sort → Limit |
| sort contacts_count + take(200) | ~0.06ms | 200 | Indexed sort — walks 200 index entries, no Sort node |
| sort contacts_count (no take) | ~213ms | 100,000 | Full sort — no optimization without Limit |
| sort [contacts_count DESC, name ASC] + take(200) | ~22ms | 200 | Indexed sort — group-aware limit, sub-sort ~1k records |
| sort [status DESC, contacts_count ASC] + take(200) | ~95ms | 200 | Indexed sort — low cardinality (~50k/group) |
| sort [name ASC, contacts_count DESC] + take(200) | ~12ms | 200 | Not indexed — full Sort, Limit stops early |
| last_contacted_at is null (~30%) | ~44ms | ~30,000 | Scan + lazy filter, ~70% rejected |
| notes is null (~50%) | ~62ms | ~50,000 | Scan + lazy filter |
| last_contacted_at is not null (~70%) | ~86ms | ~70,000 | Scan + lazy filter |
| status='active' AND notes is null | ~44ms | ~25,000 | IndexScan + lazy filter |
| 1,000 point lookups (find_by_id) | ~1.9ms | 1,000 | Direct key access |
| projection (name, status only) | ~63ms | 100,000 | Selective materialization at Projection |

### Distinct

| Query | Time | Values Returned | Notes |
|-------|------|-----------------|-------|
| distinct: status (indexed) | ~13ms | 2 | Full scan, low cardinality |
| distinct: status (indexed) + sort asc | ~13ms | 2 | Sort overhead negligible at 2 values |
| distinct: product_recommendation1 (not indexed) | ~17ms | 3 | Full scan, low cardinality |
| distinct: contacts_count (indexed, ~100 values) | ~16ms | 100 | Full scan, medium cardinality |
| distinct: contacts_count (indexed) + sort asc | ~16ms | 100 | Sort overhead negligible at 100 values |
| distinct: rec1 with status='active' filter | ~20ms | 3 | IndexScan narrows to ~50% of records |
| distinct: last_contacted_at (high cardinality) | ~2,000ms | ~70,000 | Linear dedup O(n*k) — quadratic blowup at high cardinality |

### Key Observations

- **Zero-copy reads + lazy materialization**: The raw tier uses `RawValue<'a>` — `RawValue::Borrowed(RawBsonRef<'a>)` for zero-copy access to MemoryStore snapshots, `RawValue::Owned(RawBson)` for owned data. Filter and Sort construct `&RawDocument` views on demand to access individual fields without allocation. Records that fail a filter are never cloned or deserialized. Projection is the single materialization point, selectively converting only the requested columns from raw bytes to `bson::Document`. Full materialization uses `RawDocument::try_into()` (direct raw iteration) rather than `bson::from_slice` (serde visitor pattern), avoiding the overhead of extended JSON key matching on every field.
- **Index-covered projection**: When the projection only requests columns available from the `IndexScan` Eq value, `ReadRecord` is skipped entirely. The `IndexScan` carries the matched value as `RawValue::Owned(RawBson)`, and `Projection` constructs `{ _id, field: value }` directly — no document fetch, no deserialization. At 100k records: ~12ms vs ~62ms for the same filter with a full select (5x faster).
- **Single key per record**: All columns packed into one value. Reads are a single `get()` or scan iteration — no per-column key overhead.
- **Dot-notation field access**: Filters, sorts, and projections support dot-notation paths (e.g. `"address.city"`). Nested path resolution works directly on `&RawDocument` via `get_document()` chaining.
- **Lazy ID tier**: `Scan` yields `(id, bytes)` lazily — data is never discarded and re-fetched. `IndexScan` is a lazy `from_fn` iterator that pulls index entries on demand and stops at the limit (or group boundary for `complete_groups`). `ReadRecord` fetches bytes via `txn.get()`. When Limit is present without Sort, the iterator stops early — `take(200)` on 100k records: ~2.5ms.
- **Indexed sort (single field)**: When a query sorts on a single indexed field and has a Limit, the planner eliminates the Sort node entirely and replaces Scan with an ordered IndexScan. The limit is pushed into the IndexScan so it stops after `skip + take` index entries. `sort contacts_count + take(200)` on 100k records: ~0.06ms.
- **Indexed sort (multi-field)**: When `sort[0]` is indexed and a Limit is present, the planner replaces Scan with an ordered IndexScan using `complete_groups: true` and pushes `skip + take` as the limit. IndexScan reads at least that many index entries, then continues until the current value group is complete — ensuring correct sub-sorting by Sort on the reduced record set. High-cardinality first field (`contacts_count`, ~1k/group): ~22ms — **90% faster** than full Sort (~213ms). Low-cardinality (`status`, ~50k/group): ~95ms — **55% faster**.
- **Sort overhead**: When indexed sort doesn't apply, Sort accesses sort keys lazily from raw bytes but must hold all records in memory via `into_owned()`. Only records that survive the filter pay this cost. This is the dominant cost for sorted queries.
- **Point lookups**: ~1.3ms (10k) to ~1.9ms (100k) for 1,000 lookups — direct key access via `find_by_id`.
- **Near-linear scaling**: Most queries scale ~10x from 10k → 100k (10x data), showing minimal overhead from larger datasets.

## Scaling: 10k → 100k

| Query | 10k | 100k | Factor |
|-------|-----|------|--------|
| Full scan | 10ms | 96ms | 9.6x |
| IndexScan (status) | 6ms | 62ms | 10.3x |
| Index-covered projection | 1.2ms | 12ms | 10.0x |
| AND filter (3 conditions) | 2.4ms | 26ms | 10.8x |
| Scan + filter (ProductA) | 4.4ms | 45ms | 10.2x |
| Sort (full, ~50%) | 11ms | 114ms | 10.4x |
| take(200) no sort | 0.45ms | 2.5ms | 5.6x |
| sort + take(200) indexed | 0.05ms | 0.06ms | 1.2x |
| sort [high card, name] + take(200) | 1.9ms | 22ms | 11.6x |
| sort [low card, count] + take(200) | 9.6ms | 95ms | 9.9x |
| 1,000 point lookups | 1.3ms | 1.9ms | 1.5x |
| Projection (2 cols) | 6.2ms | 63ms | 10.2x |
| Distinct (status, 2 values) | 1.2ms | 13ms | 10.8x |
| Distinct (contacts_count, ~100 values) | 1.6ms | 16ms | 10.0x |
| Distinct (last_contacted_at, ~7k/70k values) | 21ms | 2,000ms | 95.2x |

Queries scale ~10x for 10x data — near-linear. Point lookups are nearly constant regardless of dataset size. Distinct on high-cardinality fields scales quadratically (~95x for 10x data) due to linear dedup with `Vec<Bson>` + `contains()` — O(n*k) where k approaches n.

## Concurrency

| Test | Result |
|------|--------|
| 2 writers + 4 readers (10k, concurrent) | ~55ms, all complete successfully |
| 2 writers + 4 readers (100k, concurrent) | ~196ms, all complete successfully |

Writers are serialized (global write lock per store). Readers use snapshot isolation via lazy CF snapshots — reads never block on writes.

## Query Planner

The query planner builds a three-tier plan tree:

```
Projection(Limit(Sort(Filter(ReadRecord(IndexScan | IndexMerge | Scan)))))
```

For index-covered queries, `ReadRecord` is omitted:

```
Projection(IndexScan)
```

**ID tier** (produces record IDs lazily, no document bytes touched):
- **Scan**: Iterates all data keys, yields `(id, Some(bytes))` — data is kept, not discarded.
- **IndexScan**: A lazy `from_fn` iterator over index keys, yielding `(id, Some(RawValue))` for Eq lookups (carrying the matched value) or `(id, None)` for ordered column scans. `direction` controls forward/reverse. Optional `limit` caps index entries read (pushed down from Limit). `complete_groups: true` reads past the limit to finish the last value group — ensures correct sub-sorting for multi-field sort queries.
- **IndexMerge**: Binary combiner with `lhs`/`rhs` children and a `LogicalOp`. `Or` unions ID sets (for OR queries where every branch has an indexed Eq). `And` intersects (supported but not currently emitted).

**Raw tier** (operates on `RawValue<'a>` — no deserialization):
- **ReadRecord**: The boundary between ID and raw tiers. For `Scan` inputs, data flows through lazily (bytes already available). For `IndexScan`/`IndexMerge`, collects IDs (releasing the scan borrow), then fetches each record lazily via `txn.get()`. Yields `(id, RawValue<'a>)` tuples. MemoryStore returns `RawValue::Borrowed` (zero-copy from the snapshot), RocksDB returns `RawValue::Owned`. Skipped entirely for index-covered queries.
- **Filter**: Extracts `&RawDocument` via `RawValue::as_document()` and evaluates predicates by accessing individual fields lazily. Records that fail are never deserialized or cloned. AND/OR short-circuit. Supports dot-notation paths.
- **Sort**: Uses `RawValue::as_document()` to access sort keys lazily via `&RawDocument` views. Sorts in memory. Eliminated entirely when the planner detects a single indexed sort field + Limit. For multi-field sort where `sort[0]` is indexed, Sort operates on a reduced record set — IndexScan with `complete_groups` feeds only the records needed for correct ordering.
- **Limit**: Generic `skip()` + `take()` via `apply_limit` on any raw record iterator.

**Document tier** (materialization):
- **Projection**: For document values, constructs a borrowed `&RawDocument` view and selectively deserializes only the projected columns via `raw.iter()` + column filtering. For scalar `RawValue` from index-covered queries, constructs `{ _id, field: value }` directly without any raw byte parsing. For dot-notation paths, trims nested documents to only requested sub-paths. When no Projection node exists, full materialization uses `RawDocument::try_into()` — direct raw iteration that converts each `RawBsonRef` to an owned `Bson` value without serde overhead.

**Index selection**: For AND groups, the planner picks the highest-priority indexed field (ordered by `CollectionConfig.indexes`) with an `Eq` condition. For OR groups, if every branch has at least one indexed `Eq`, the planner builds an `IndexMerge(Or)` tree with a residual `Filter` for recheck; if any branch lacks an indexed condition, the entire OR falls back to `Scan`.

Index keys use the format `i:{field}\x00{value_bytes}\x00{_id}` and are maintained on insert/update/delete for indexed fields.
