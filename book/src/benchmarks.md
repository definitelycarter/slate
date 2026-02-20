# Benchmarks

Snapshot from `slate-bench` — run on Apple Silicon (M-series) in release mode. Each record has 8 fields (6 required, 2 nullable). Two dataset sizes: 10k and 100k records per user, 3 users each.

All benchmarks use the **embedded MemoryStore** backend — no persistence layer, no TCP overhead. This isolates query engine performance from storage-specific noise (RocksDB compaction, block cache warmup, etc.), making benchmark-to-benchmark comparisons reliable. For raw storage throughput across all three backends (MemoryStore, RocksDB, redb), see `slate-store-bench`.

Storage model: records are stored as `d:{_id}` → raw BSON bytes via `bson::to_vec`. Queries use a three-tier plan tree: the **ID tier** (Scan, IndexScan, IndexMerge) produces record IDs without touching document bytes. `ReadRecord` fetches raw BSON bytes by ID but does **not** deserialize them. The **raw tier** (Filter, Sort, Limit, Distinct) operates on `RawValue<'a>` — a Cow-like enum over `RawBsonRef<'a>` (borrowed, zero-copy from MemoryStore snapshots) and `RawBson` (owned). Filter and Sort construct `&RawDocument` views on demand to access individual fields without allocation. Records that fail a filter are never cloned or deserialized. **Projection** builds `RawDocumentBuf` output using `append()` — selective field copying without full materialization. `find()` returns `Vec<RawDocumentBuf>` directly. For index-covered queries (where all projected columns are available from the index), `ReadRecord` is skipped entirely — `Projection` constructs documents directly from `RawValue` scalars carried by `IndexScan`. For full scans without a filter, `ReadRecord` optimizes the `Scan` input into a single-pass iteration.

## Record Schema

| Field | Type | Indexed | Notes |
|-------|------|---------|-------|
| name | String | No | Random name |
| status | String | **Yes** | Random: "active" or "rejected" (~50/50) |
| contacts_count | Int | **Yes** | Random 0–100 |
| product_recommendation1 | String | No | Random: ProductA / ProductB / ProductC |
| product_recommendation2 | String | No | Random: ProductX / ProductY / ProductZ |
| product_recommendation3 | String | No | Random: Widget1 / Widget2 / Widget3 |
| tags | Array(String) | No | 2-4 random tags from 5 values |
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
| Full scan (no filter) | ~1.3ms | 10,000 | Single key per record |
| status = 'active' (indexed) | ~1.8ms | ~5,000 | IndexScan → lazy filter |
| status = 'active' projection [status] (index-covered) | ~0.8ms | ~5,000 | IndexScan → Projection, no ReadRecord |
| product_recommendation1 = 'ProductA' | ~1.6ms | ~3,300 | Scan + lazy filter, ~67% rejected without deserializing |
| status + rec1 + rec2 (AND) | ~2ms | ~550 | IndexScan + lazy filter, ~89% rejected |
| status='active' + sort + skip/take(50) | ~6.3ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~1.7ms | ~5,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~6.6ms | ~5,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~0.35ms | 200 | Lazy ID tier — Limit stops after 200 |
| status='active' + sort + take(200) | ~6.3ms | 200 | IndexScan → Sort → Limit |
| sort contacts_count + take(200) | ~0.17ms | 200 | Indexed sort — walks 200 index entries, no Sort node |
| sort contacts_count (no take) | ~11.6ms | 10,000 | Full sort — no optimization without Limit |
| sort [contacts_count DESC, name ASC] + take(200) | ~0.27ms | 200 | Indexed sort — group-aware limit, sub-sort ~1 group |
| sort [status DESC, contacts_count ASC] + take(200) | ~9.6ms | 200 | Indexed sort — low cardinality, large group |
| sort [name ASC, contacts_count DESC] + take(200) | ~1.3ms | 200 | Not indexed — full Sort, Limit stops early |
| last_contacted_at is null (~30%) | ~2ms | ~3,000 | Scan + lazy filter, ~70% rejected |
| notes is null (~50%) | ~2.2ms | ~5,000 | Scan + lazy filter |
| last_contacted_at is not null (~70%) | ~2.4ms | ~7,000 | Scan + lazy filter |
| status='active' AND notes is null | ~2.2ms | ~2,500 | IndexScan + lazy filter |
| 1,000 point lookups (find_by_id) | ~1.2ms | 1,000 | Direct key access |
| projection (name, status only) | ~4.5ms | 10,000 | Selective field copying via RawDocumentBuf::append() |
| tags = 'renewal_due' (array element match) | ~3.3ms | ~4,800 | Scan + element-wise filter, iterates array per record |

### Distinct

| Query | Time | Values Returned | Notes |
|-------|------|-----------------|-------|
| distinct: status (indexed) | ~5ms | 2 | HashSet dedup, low cardinality |
| distinct: status (indexed) + sort asc | ~5ms | 2 | Sort on RawBson::Array, negligible at 2 values |
| distinct: product_recommendation1 (not indexed) | ~5ms | 3 | HashSet dedup, low cardinality |
| distinct: contacts_count (indexed, ~100 values) | ~4.9ms | 100 | HashSet dedup, medium cardinality |
| distinct: contacts_count (indexed) + sort asc | ~4.7ms | 100 | Sort on RawBson::Array, negligible at 100 values |
| distinct: rec1 with status='active' filter | ~3.6ms | 3 | IndexScan narrows to ~50% of records |
| distinct: last_contacted_at (high cardinality) | ~4.9ms | ~7,000 | HashSet O(1) dedup, ~70% of records have value |

### Upsert / Merge

1,000 records per operation, run after 10k records already inserted.

| Operation | Time | Per Record | Notes |
|-----------|------|------------|-------|
| upsert_many: 1,000 new (insert path) | ~3.6ms | ~0.0036ms | Insert + index write per record |
| upsert_many: 1,000 existing (replace path) | ~5.5ms | ~0.0055ms | Delete old indexes + replace doc + reindex |
| merge_many: 1,000 existing (merge path) | ~3.7ms | ~0.0037ms | Raw merge via `RawDocumentBuf::append_ref()` — unchanged fields zero-copy |
| merge_many: 1,000 new (insert path) | ~3.8ms | ~0.0038ms | Falls back to insert when `_id` not found |
| merge_many: 1,000 mixed (50/50) | ~4.3ms | ~0.0043ms | Mix of insert and merge paths |

## 100k Records

Results from a single collection (100,000 records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~285ms | ~0.0029ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~14ms | 100,000 | Single key per record |
| status = 'active' (indexed) | ~19ms | ~50,000 | IndexScan → lazy filter |
| status = 'active' projection [status] (index-covered) | ~8ms | ~50,000 | IndexScan → Projection, no ReadRecord |
| product_recommendation1 = 'ProductA' | ~16ms | ~33,000 | Scan + lazy filter, ~67% rejected without deserializing |
| status + rec1 + rec2 (AND) | ~21ms | ~5,500 | IndexScan + lazy filter, ~89% rejected |
| status='active' + sort + skip/take(50) | ~68ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~19ms | ~50,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~74ms | ~50,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~3ms | 200 | Lazy ID tier — Limit stops after 200 |
| status='active' + sort + take(200) | ~66ms | 200 | IndexScan → Sort → Limit |
| sort contacts_count + take(200) | ~0.3ms | 200 | Indexed sort — walks 200 index entries, no Sort node |
| sort contacts_count (no take) | ~134ms | 100,000 | Full sort — no optimization without Limit |
| sort [contacts_count DESC, name ASC] + take(200) | ~1.2ms | 200 | Indexed sort — group-aware limit, sub-sort ~1 group |
| sort [status DESC, contacts_count ASC] + take(200) | ~98ms | 200 | Indexed sort — low cardinality (~50k/group) |
| sort [name ASC, contacts_count DESC] + take(200) | ~13ms | 200 | Not indexed — full Sort, Limit stops early |
| last_contacted_at is null (~30%) | ~20ms | ~30,000 | Scan + lazy filter, ~70% rejected |
| notes is null (~50%) | ~23ms | ~50,000 | Scan + lazy filter |
| last_contacted_at is not null (~70%) | ~25ms | ~70,000 | Scan + lazy filter |
| status='active' AND notes is null | ~24ms | ~25,000 | IndexScan + lazy filter |
| 1,000 point lookups (find_by_id) | ~1.9ms | 1,000 | Direct key access |
| projection (name, status only) | ~45ms | 100,000 | Selective field copying via RawDocumentBuf::append() |
| tags = 'renewal_due' (array element match) | ~32ms | ~48,000 | Scan + element-wise filter, iterates array per record |

### Distinct

| Query | Time | Values Returned | Notes |
|-------|------|-----------------|-------|
| distinct: status (indexed) | ~49ms | 2 | HashSet dedup, Projection overhead dominates |
| distinct: status (indexed) + sort asc | ~49ms | 2 | Sort on RawBson::Array, negligible at 2 values |
| distinct: product_recommendation1 (not indexed) | ~48ms | 3 | HashSet dedup, Projection overhead dominates |
| distinct: contacts_count (indexed, ~100 values) | ~48ms | 100 | HashSet dedup, medium cardinality |
| distinct: contacts_count (indexed) + sort asc | ~48ms | 100 | Sort on RawBson::Array, negligible at 100 values |
| distinct: rec1 with status='active' filter | ~38ms | 3 | IndexScan narrows to ~50% of records |
| distinct: last_contacted_at (high cardinality) | ~51ms | ~70,000 | HashSet O(1) dedup — linear scaling vs previous quadratic |

### Upsert / Merge

1,000 records per operation, run after 100k records already inserted.

| Operation | Time | Per Record | Notes |
|-----------|------|------------|-------|
| upsert_many: 1,000 new (insert path) | ~4.2ms | ~0.0042ms | Insert + index write per record |
| upsert_many: 1,000 existing (replace path) | ~6.2ms | ~0.0062ms | Delete old indexes + replace doc + reindex |
| merge_many: 1,000 existing (merge path) | ~3.8ms | ~0.0038ms | Raw merge via `RawDocumentBuf::append_ref()` — unchanged fields zero-copy |
| merge_many: 1,000 new (insert path) | ~4.3ms | ~0.0043ms | Falls back to insert when `_id` not found |
| merge_many: 1,000 mixed (50/50) | ~6.2ms | ~0.0062ms | Mix of insert and merge paths |

### Key Observations

- **Zero-copy reads + lazy materialization**: The raw tier uses `RawValue<'a>` — `RawValue::Borrowed(RawBsonRef<'a>)` for zero-copy access to MemoryStore snapshots, `RawValue::Owned(RawBson)` for owned data. Filter and Sort construct `&RawDocument` views on demand to access individual fields without allocation. Records that fail a filter are never cloned or deserialized. Projection builds `RawDocumentBuf` output using `append()` for selective field copying — no full `bson::Document` materialization in the pipeline. `find()` returns `Vec<RawDocumentBuf>` directly.
- **Index-covered projection**: When the projection only requests columns available from the `IndexScan` Eq value, `ReadRecord` is skipped entirely. The `IndexScan` carries the matched value as `RawValue::Owned(RawBson)`, and `Projection` constructs `{ _id, field: value }` directly — no document fetch, no deserialization. At 100k records: ~8ms vs ~19ms for the same filter with a full select.
- **Single key per record**: All columns packed into one value. Reads are a single `get()` or scan iteration — no per-column key overhead.
- **Dot-notation field access**: Filters, sorts, and projections support dot-notation paths (e.g. `"address.city"`). Nested path resolution works directly on `&RawDocument` via `get_document()` chaining.
- **Lazy ID tier**: `Scan` yields `(id, bytes)` lazily — data is never discarded and re-fetched. `IndexScan` is a lazy `from_fn` iterator that pulls index entries on demand and stops at the limit (or group boundary for `complete_groups`). `ReadRecord` fetches bytes via `txn.get()`. When Limit is present without Sort, the iterator stops early — `take(200)` on 100k records: ~3ms.
- **Indexed sort (single field)**: When a query sorts on a single indexed field and has a Limit, the planner eliminates the Sort node entirely and replaces Scan with an ordered IndexScan. The limit is pushed into the IndexScan so it stops after `skip + take` index entries. `sort contacts_count + take(200)` on 100k records: ~0.3ms.
- **Indexed sort (multi-field)**: When `sort[0]` is indexed and a Limit is present, the planner replaces Scan with an ordered IndexScan using `complete_groups: true` and pushes `skip + take` as the limit. IndexScan reads at least that many index entries, then continues until the current value group is complete — ensuring correct sub-sorting by Sort on the reduced record set. High-cardinality first field (`contacts_count`, ~1k/group): ~1.2ms. Low-cardinality (`status`, ~50k/group): ~97ms.
- **Sort overhead**: When indexed sort doesn't apply, Sort accesses sort keys lazily from raw bytes but must hold all records in memory via `into_owned()`. Only records that survive the filter pay this cost. This is the dominant cost for sorted queries.
- **Distinct dedup**: Uses `HashSet<u64>` with manual hashing of `RawBsonRef` variants for O(1) dedup. Unique values collected into `RawArrayBuf`. Zero `bson::Bson` allocation in the distinct path. `raw_walk_path` recursively walks document paths, descending into arrays of sub-documents without intermediate `Vec` allocation.
- **Point lookups**: ~1.3ms (10k) to ~1.8ms (100k) for 1,000 lookups — direct key access via `find_by_id`.
- **Array element matching**: Filtering on array fields (e.g. `tags = "renewal_due"`) iterates array elements per record — any element match satisfies the filter. At 100k records with 2-4 tags each: ~32ms for ~48k matches. Scales linearly and inherits all cross-type coercion.
- **Raw merge (`merge_many`)**: Partial updates use `RawDocumentBuf::append_ref()` for unchanged fields — raw bytes are copied directly without parsing or allocation. Only updated fields go through `Bson` → `RawBson` conversion. At 100k records, merging 1,000 existing records costs ~3.8ms (~0.0038ms/record) — 39% faster than upsert replace (~6.2ms) which must rewrite the entire document and all indexes.
- **Near-linear scaling**: Most queries scale ~10x from 10k → 100k (10x data), showing minimal overhead from larger datasets.

## Scaling: 10k → 100k

| Query | 10k | 100k | Factor |
|-------|-----|------|--------|
| Full scan | 1.3ms | 14ms | 10.8x |
| IndexScan (status) | 1.8ms | 19ms | 10.6x |
| Index-covered projection | 0.8ms | 8ms | 10.0x |
| AND filter (3 conditions) | 2ms | 21ms | 10.5x |
| Scan + filter (ProductA) | 1.6ms | 16ms | 10.0x |
| Sort (full, ~50%) | 6.6ms | 74ms | 11.2x |
| take(200) no sort | 0.35ms | 3ms | 8.6x |
| sort + take(200) indexed | 0.17ms | 0.3ms | 1.8x |
| sort [high card, name] + take(200) | 0.27ms | 1.2ms | 4.4x |
| sort [low card, count] + take(200) | 9.6ms | 98ms | 10.2x |
| 1,000 point lookups | 1.2ms | 1.9ms | 1.6x |
| Projection (2 cols) | 4.5ms | 45ms | 10.0x |
| Array element match (tags) | 3.3ms | 32ms | 9.7x |
| Distinct (status, 2 values) | 5ms | 49ms | 9.8x |
| Distinct (contacts_count, ~100 values) | 4.9ms | 48ms | 9.8x |
| Distinct (last_contacted_at, ~7k/70k values) | 4.9ms | 51ms | 10.4x |
| upsert_many: 1k insert | 3.6ms | 4.2ms | 1.2x |
| upsert_many: 1k replace | 5.5ms | 6.2ms | 1.1x |
| merge_many: 1k merge | 3.7ms | 3.8ms | 1.0x |
| merge_many: 1k insert | 3.8ms | 4.3ms | 1.1x |

Queries scale ~10x for 10x data — near-linear. Point lookups are nearly constant regardless of dataset size. Distinct now scales linearly across all cardinalities thanks to `HashSet<u64>` O(1) dedup (previously quadratic at high cardinality: ~95x for 10x data). Write operations (upsert/merge) scale ~1.0–1.2x — nearly constant since they operate on a fixed batch of 1,000 records regardless of dataset size. The slight increase at 100k reflects deeper tree traversal in the underlying OrdMap.

## Concurrency

| Test | Result |
|------|--------|
| 2 writers + 4 readers (10k, concurrent) | ~63ms, all complete successfully |
| 2 writers + 4 readers (100k, concurrent) | ~325ms, all complete successfully |

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

For distinct queries:

```
Distinct(Projection(Filter(ReadRecord(Scan | IndexScan))))
```

**ID tier** (produces record IDs lazily, no document bytes touched):
- **Scan**: Iterates all data keys, yields `(id, Some(bytes))` — data is kept, not discarded.
- **IndexScan**: A lazy `from_fn` iterator over index keys, yielding `(id, Some(RawValue))` for Eq lookups (carrying the matched value) or `(id, None)` for ordered column scans. `direction` controls forward/reverse. Optional `limit` caps index entries read (pushed down from Limit). `complete_groups: true` reads past the limit to finish the last value group — ensures correct sub-sorting for multi-field sort queries.
- **IndexMerge**: Binary combiner with `lhs`/`rhs` children and a `LogicalOp`. `Or` unions ID sets (for OR queries where every branch has an indexed Eq). `And` intersects (supported but not currently emitted).

**Raw tier** (operates on `RawValue<'a>` — no deserialization):
- **ReadRecord**: The boundary between ID and raw tiers. For `Scan` inputs, data flows through lazily (bytes already available). For `IndexScan`/`IndexMerge`, collects IDs (releasing the scan borrow), then fetches each record lazily via `txn.get()`. Yields `(id, RawValue<'a>)` tuples. MemoryStore returns `RawValue::Borrowed` (zero-copy from the snapshot), RocksDB returns `RawValue::Owned`. Skipped entirely for index-covered queries.
- **Filter**: Extracts `&RawDocument` via `RawValue::as_document()` and evaluates predicates by accessing individual fields lazily. Records that fail are never deserialized or cloned. AND/OR short-circuit. Supports dot-notation paths.
- **Sort**: For documents, uses `RawValue::as_document()` to access sort keys lazily via `&RawDocument` views and sorts in memory. For `RawBson::Array` values (from Distinct), Sort unpacks the array, sorts elements in-place (direct scalar comparison or document field extraction), and re-emits as a sorted `RawBson::Array`. Eliminated entirely when the planner detects a single indexed sort field + Limit. For multi-field sort where `sort[0]` is indexed, Sort operates on a reduced record set — IndexScan with `complete_groups` feeds only the records needed for correct ordering.
- **Limit**: Peeks the first item — if it's a `RawBson::Array` (from Distinct), slices elements with `skip/take` and rebuilds a `RawArrayBuf`. Otherwise chains the peeked item back and applies lazy `skip()` + `take()` on the record stream. Preserves lazy streaming for find queries.
- **Distinct**: Extracts field values via `raw_walk_path` (recursive callback-based traversal, zero allocation). Deduplicates with `HashSet<u64>` using manual `hash_raw()` over `RawBsonRef` variants. Collects unique values into `RawArrayBuf` and emits a single `(None, Some(RawValue::Owned(RawBson::Array(buf))))`. `distinct()` returns `RawBson` directly — no per-value conversion to `bson::Bson`.

**Document tier** (materialization):
- **Projection**: Builds `RawDocumentBuf` output using `append()` for selective field copying. Injects `_id` from the record ID. For scalar `RawValue` from index-covered queries, constructs `{ _id, field: value }` directly without any raw byte parsing. `find()` returns `Vec<RawDocumentBuf>` — no `bson::Document` materialization in the pipeline.

**Index selection**: For AND groups, the planner picks the highest-priority indexed field (ordered by `CollectionConfig.indexes`) with an `Eq` condition. For OR groups, if every branch has at least one indexed `Eq`, the planner builds an `IndexMerge(Or)` tree with a residual `Filter` for recheck; if any branch lacks an indexed condition, the entire OR falls back to `Scan`.

Index keys use the format `i:{field}\x00{value_bytes}\x00{_id}` and are maintained on insert/update/delete for indexed fields.
