# Benchmarks

Snapshot from `slate-bench` — run on Apple Silicon (M-series) in release mode. Each record has 8 fields (6 required, 2 nullable). Two dataset sizes: 10k and 100k records per user, 3 users each.

Storage model: each collection is a RocksDB column family. Records are stored as `d:{_id}` → raw BSON bytes via `bson::to_vec`. Queries use a three-tier plan tree: the **ID tier** (Scan, IndexScan, IndexMerge) produces record IDs without touching document bytes. `ReadRecord` fetches raw BSON bytes by ID but does **not** deserialize them. The **raw tier** (Filter, Sort, Limit) operates on `RawDocumentBuf` — accessing individual fields lazily without full deserialization. Records that fail a filter are never deserialized. Finally, **Projection** is the single materialization point where `RawDocumentBuf` → `bson::Document`, selectively deserializing only the requested columns. For full scans without a filter, `ReadRecord` optimizes the `Scan` input into a single-pass iteration.

## Record Schema

| Field | Type | Indexed | Notes |
|-------|------|---------|-------|
| name | String | No | Random name |
| status | String | **Yes** | Random: "active" or "rejected" (~50/50) |
| contacts_count | Int | No | Random 0–100 |
| product_recommendation1 | String | No | Random: ProductA / ProductB / ProductC |
| product_recommendation2 | String | No | Random: ProductX / ProductY / ProductZ |
| product_recommendation3 | String | No | Random: Widget1 / Widget2 / Widget3 |
| last_contacted_at | Date | No | ~30% null |
| notes | String | No | ~50% null |

## Embedded (RocksStore) — 10k Records

Results from a single collection (10,000 records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 1,000 records | ~43ms | ~0.0043ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~11ms | 10,000 | Single key per record |
| status = 'active' (indexed) | ~8ms | ~5,000 | IndexScan → lazy filter |
| product_recommendation1 = 'ProductA' | ~5.8ms | ~3,300 | Scan + lazy filter, ~67% rejected without deserializing |
| status + rec1 + rec2 (AND) | ~4.4ms | ~550 | IndexScan + lazy filter, ~89% rejected |
| status='active' + sort + skip/take(50) | ~8.5ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~7.9ms | ~5,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~12.6ms | ~5,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~3.5ms | 200 | IndexScan, only 200 records materialized |
| status='active' + sort + take(200) | ~8.7ms | 200 | IndexScan → Sort → Limit |
| last_contacted_at is null (~30%) | ~5.7ms | ~3,000 | Scan + lazy filter, ~70% rejected |
| notes is null (~50%) | ~7.4ms | ~5,000 | Scan + lazy filter |
| last_contacted_at is not null (~70%) | ~10ms | ~7,000 | Scan + lazy filter |
| status='active' AND notes is null | ~6.2ms | ~2,500 | IndexScan + lazy filter |
| 1,000 point lookups (find_by_id) | ~1.8ms | 1,000 | Direct key access |
| projection (name, status only) | ~8ms | 10,000 | Selective materialization at Projection |

## Embedded (RocksStore) — 100k Records

Results from a single collection (100,000 records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~490ms | ~0.0049ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~111ms | 100,000 | Single key per record |
| status = 'active' (indexed) | ~79ms | ~50,000 | IndexScan → lazy filter |
| product_recommendation1 = 'ProductA' | ~55ms | ~33,300 | Scan + lazy filter, ~67% rejected without deserializing |
| status + rec1 + rec2 (AND) | ~44ms | ~5,500 | IndexScan + lazy filter, ~89% rejected |
| status='active' + sort + skip/take(50) | ~86ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~77ms | ~50,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~132ms | ~50,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~33ms | 200 | IndexScan, only 200 records materialized |
| status='active' + sort + take(200) | ~86ms | 200 | IndexScan → Sort → Limit |
| last_contacted_at is null (~30%) | ~53ms | ~30,000 | Scan + lazy filter, ~70% rejected |
| notes is null (~50%) | ~72ms | ~50,000 | Scan + lazy filter |
| last_contacted_at is not null (~70%) | ~98ms | ~70,000 | Scan + lazy filter |
| status='active' AND notes is null | ~61ms | ~25,000 | IndexScan + lazy filter |
| 1,000 point lookups (find_by_id) | ~2.4ms | 1,000 | Direct key access |
| projection (name, status only) | ~82ms | 100,000 | Selective materialization at Projection |

### Key Observations

- **Lazy materialization**: `ReadRecord` fetches raw BSON bytes but does not deserialize them. Filter and Sort access individual fields via `RawDocumentBuf::get()` — a linear scan of the byte buffer that returns typed references without allocation. Records that fail a filter are never deserialized. Projection is the single materialization point, selectively converting only the requested columns from raw bytes to `bson::Document`. This yields 30–80% improvements on filtered queries compared to eager deserialization.
- **Single key per record**: All columns packed into one value. Reads are a single `get()` or scan iteration — no per-column key overhead.
- **Dot-notation field access**: Filters, sorts, and projections support dot-notation paths (e.g. `"address.city"`). Nested path resolution works directly on `RawDocumentBuf` via `get_document()` chaining.
- **IndexScan → multi_get**: Index lookups collect record IDs, then batch-fetch raw bytes via `multi_get`. Filter evaluates lazily on the raw bytes.
- **Sort overhead**: Sort accesses sort keys lazily from raw bytes, but must hold all records in memory. This is the dominant cost for sorted queries.
- **take() without sort**: When no sort is required, `take(N)` benefits dramatically from lazy materialization — only N records are deserialized regardless of how many match the filter. 100k dataset, `take(200)`: ~33ms vs ~82ms with eager deserialization.
- **Point lookups**: ~1.7ms (10k) to ~2.4ms (100k) for 1,000 lookups — direct key access via `find_by_id`.
- **Near-linear scaling**: Most queries scale ~10x from 10k → 100k (10x data), showing minimal overhead from larger datasets.

## TCP (MessagePack over Localhost)

Same workload as embedded (100k records), accessed through the TCP server on localhost. Wire protocol uses MessagePack (rmp-serde) with length-prefixed framing.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~625ms | ~0.0063ms |

### Queries

| Query | Time | Records Returned |
|-------|------|------------------|
| Full scan (no filter) | ~255ms | 100,000 |
| status = 'active' (indexed) | ~154ms | ~50,000 |
| status + rec1 + rec2 (AND) | ~55ms | ~5,500 |
| status='active' (no sort) | ~152ms | ~50,000 |
| status='active' + sort contacts_count | ~209ms | ~50,000 |
| status='active' + take(200) (no sort) | ~36ms | 200 |
| status='active' + sort + take(200) | ~87ms | 200 |
| status='active' + sort + skip/take(50) | ~85ms | 50 |
| 1,000 point lookups (find_by_id) | ~40ms | 1,000 |

### Embedded vs TCP Overhead

| Query | Embedded | TCP | Overhead |
|-------|----------|-----|----------|
| Full scan (100k records) | 111ms | 255ms | +130% |
| IndexScan filter (50k records) | 79ms | 154ms | +95% |
| IndexScan + narrow (5.5k records) | 44ms | 55ms | +25% |
| IndexScan + sort + take(200) | 86ms | 87ms | +1% |
| 1,000 point lookups | 2.4ms | 40ms | Network round-trips |

**Takeaway**: TCP overhead scales with the number of records serialized over the wire. For paginated queries (sort + take), overhead is modest. Point lookups show the per-request round-trip cost (~0.04ms each).

### Why MessagePack over BSON for the Wire Format

We benchmarked BSON as the wire format (replacing MessagePack for TCP serialization) and found it significantly slower for bulk data transfer:

| Query (TCP, 100k) | MessagePack | BSON | Difference |
|---|---|---|---|
| Bulk insert | ~643ms | ~879ms | +37% slower |
| Full scan (100k records) | ~259ms | ~470ms | +81% slower |
| Indexed filter (~50k records) | ~156ms | ~270ms | +73% slower |
| Sorted (~50k records) | ~206ms | ~316ms | +53% slower |
| Small result sets (take 200) | ~99ms | ~91ms | ~flat |
| Point lookups (1000) | ~38ms | ~43ms | ~flat |

BSON's overhead is per-document and compounds on large result sets. The `bson::Document` type also cannot be serialized directly as a top-level BSON value when wrapped in an enum — it requires an intermediate `bson::to_bson()` → wrapper document → `bson::to_vec()` round-trip, adding allocation overhead.

MessagePack is more compact for structured enum data and handles Rust enums natively via serde. The sweet spot: **BSON for storage** (where `RawDocumentBuf` enables selective field reads without full deserialization) and **MessagePack for the wire** (where compact serialization of complete documents matters most).

## Scaling: 10k → 100k

| Query | 10k | 100k | Factor |
|-------|-----|------|--------|
| Full scan | 11ms | 111ms | 10.1x |
| IndexScan (status) | 8ms | 79ms | 9.9x |
| AND filter (3 conditions) | 4.1ms | 44ms | 10.7x |
| Scan + filter (ProductA) | 5.6ms | 55ms | 9.8x |
| Sort (full, ~50%) | 11.8ms | 132ms | 11.2x |
| take(200) no sort | 3.1ms | 33ms | 10.6x |
| 1,000 point lookups | 1.7ms | 2.4ms | 1.4x |
| Projection (2 cols) | 7.6ms | 82ms | 10.8x |

Queries scale ~10x for 10x data — near-linear. Point lookups are nearly constant regardless of dataset size.

## Concurrency

| Test | Result |
|------|--------|
| 2 writers + 4 readers (10k, concurrent) | ~75-100ms, all complete successfully |
| 2 writers + 4 readers (100k, concurrent) | ~600-630ms, all complete successfully |

Writers are serialized (global write lock per store). Readers use snapshot isolation via lazy CF snapshots — reads never block on writes.

## RocksStore vs MemoryStore (DB-Level)

Same workload as embedded benchmarks, run against both storage backends. 100k records per user.

### Bulk Insert — 100k Records

| Backend | Time | Per Record |
|---------|------|------------|
| RocksStore | ~477ms | ~0.0048ms |
| MemoryStore | ~213ms | ~0.0021ms |
| **Speedup** | **2.2x** | |

### Queries — 100k Records

| Query | RocksStore | MemoryStore | Speedup |
|-------|-----------|-------------|---------|
| Full scan (no filter) | 111ms | 101ms | 1.1x |
| status = 'active' (indexed) | 79ms | 61ms | 1.3x |
| product_recommendation1 = 'ProductA' | 55ms | 49ms | 1.1x |
| status + rec1 + rec2 (AND) | 44ms | 26ms | 1.7x |
| status='active' + sort + skip/take | 86ms | 63ms | 1.4x |
| status='active' (no sort) | 77ms | 59ms | 1.3x |
| status='active' + sort | 132ms | 114ms | 1.2x |
| 1,000 point lookups (find_by_id) | 2.4ms | 2.1ms | 1.1x |
| projection (name, status only) | 82ms | 64ms | 1.3x |

### Key Observations

- **Writes are 2.2x faster** — MemoryStore has no WAL, no fsync, no LSM compaction. Lazy CF snapshots and dirty-only commits minimize overhead further.
- **Indexed queries are 1.3x faster** — index scans hit the store heavily; in-memory B-tree lookups beat RocksDB's block cache.
- **Full scans are ~equal** — the bottleneck is BSON deserialization, which is identical for both backends. Storage read time is a small fraction.
- **Concurrent workloads benefit from lock-free reads** — MemoryStore uses `ArcSwap` for atomic snapshot reads, avoiding RocksDB's internal locking overhead.

### MemoryStore Transaction Model

- **Serialized writers**: Write transactions hold a global `Mutex` for their full duration. Only one writer at a time — no conflict errors possible.
- **Lazy CF snapshots**: Column families are snapshotted on first access, not all at `begin()`. Reduces overhead for transactions that only touch a few collections.
- **Dirty tracking**: Only modified CFs are committed back to the store on `commit()`. Unmodified CFs are discarded.
- **Lock-free readers**: Read-only transactions use `ArcSwap::load_full()` for atomic snapshot reads with no lock contention.

### Store-Level Stress Test (`slate-store-bench`)

Raw key-value performance at 500k records with 10 KB values (~4.7 GB total data). Tests verify zero data loss and zero corruption.

| Operation | MemoryStore | RocksStore | Speedup |
|-----------|------------|-----------|---------|
| Write 500k records | 4,455ms (112k rec/s) | 5,507ms (91k rec/s) | 1.2x |
| Read all 500k | 278ms (1.8M rec/s) | 1,861ms (269k rec/s) | **6.7x** |
| Scan prefix (full) | 147ms | 1,000ms | **6.8x** |

**Integrity checks passed:** rollback, snapshot isolation, delete_range, concurrent stress (4 readers + 1 writer).

The 6-7x raw read speedup narrows to 1.1-1.3x at the DB level because DB queries pay for raw BSON field access and filter evaluation on every record — costs that dominate the storage read time. With lazy materialization, only records that pass the filter are fully deserialized.

## Query Planner

The query planner builds a three-tier plan tree:

```
Projection(Limit(Sort(Filter(ReadRecord(IndexScan | IndexMerge | Scan)))))
```

**ID tier** (produces record IDs, no document bytes touched):
- **Scan**: Iterates all data keys in a collection, yields record IDs.
- **IndexScan**: Scans index keys for a specific `column=value`, yields matching record IDs.
- **IndexMerge**: Binary combiner with `lhs`/`rhs` children and a `LogicalOp`. `Or` unions ID sets (for OR queries where every branch has an indexed Eq). `And` intersects (supported but not currently emitted).

**Raw tier** (operates on `RawDocumentBuf` — no deserialization):
- **ReadRecord**: Fetches raw BSON bytes by ID via `multi_get`. For `Scan` inputs, optimizes into a single-pass iteration. Does **not** deserialize — yields `(id, RawDocumentBuf)` tuples.
- **Filter**: Evaluates predicates by accessing individual fields lazily via `RawDocumentBuf::get()`. Records that fail are never deserialized. AND/OR short-circuit. Supports dot-notation paths.
- **Sort**: Accesses sort keys lazily from raw bytes, sorts in memory.
- **Limit**: `skip()` + `take()` on the raw record stream.

**Document tier** (materialization):
- **Projection**: The single point where `RawDocumentBuf` → `bson::Document`. Selectively deserializes only the projected columns via `raw.iter()` + column filtering. For dot-notation paths, trims nested documents to only requested sub-paths. When no Projection node exists, full `to_document()` is used as a fallback.

**Index selection**: For AND groups, the planner picks the highest-priority indexed field (ordered by `CollectionConfig.indexes`) with an `Eq` condition. For OR groups, if every branch has at least one indexed `Eq`, the planner builds an `IndexMerge(Or)` tree with a residual `Filter` for recheck; if any branch lacks an indexed condition, the entire OR falls back to `Scan`.

Index keys use the format `i:{field}\x00{value_bytes}\x00{_id}` and are maintained on insert/update/delete for indexed fields.
