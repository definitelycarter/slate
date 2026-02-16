# Benchmarks

Snapshot from `slate-bench` — run on Apple Silicon (M-series) in release mode. Each record has 8 fields (6 required, 2 nullable). Two dataset sizes: 10k and 100k records per user, 3 users each.

Storage model: each record is stored as a single key (`d:{partition}:{record_id}` → raw BSON bytes via `bson::to_vec`). Records are stored as `bson::Document` values serialized directly to BSON binary format. Queries use a plan tree (Scan/IndexScan → Filter → Sort → Limit → Projection) with selective materialization — the planner computes the required columns (projection + filter + sort) and scan nodes read only those top-level fields from `RawDocumentBuf`, avoiding full document deserialization when a projection is specified.

All queries use key partitioning — records are stored with prefixed keys (`{partition}:{id}`) and queries scan only the relevant partition via `scan_prefix`.

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

Results from a single user partition (10,000 records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 1,000 records | ~47ms | ~0.0047ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~11ms | 10,000 | Single key per record |
| status = 'active' (indexed) | ~8ms | ~5,000 | IndexScan → multi_get |
| product_recommendation1 = 'ProductA' | ~10.5ms | ~3,300 | Full scan + Filter |
| status + rec1 + rec2 (AND) | ~8ms | ~550 | IndexScan + filter short-circuit |
| status='active' + sort + skip/take(50) | ~10ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~8ms | ~5,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~10ms | ~5,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~8ms | 200 | IndexScan + early stop |
| status='active' + sort + take(200) | ~10ms | 200 | IndexScan → Sort → Limit |
| last_contacted_at is null (~30%) | ~10ms | ~3,000 | Full scan + Filter |
| notes is null (~50%) | ~11ms | ~5,000 | Full scan + Filter |
| last_contacted_at is not null (~70%) | ~11ms | ~7,000 | Full scan + Filter |
| status='active' AND notes is null | ~8ms | ~2,500 | IndexScan on status, Filter on notes |
| 1,000 point lookups (get_by_id) | ~2.6ms | 1,000 | Single key access |
| projection (name, status only) | ~7.3ms | 10,000 | Selective RawDocumentBuf read → Projection |

## Embedded (RocksStore) — 100k Records

Results from a single user partition (100,000 records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~535ms | ~0.0054ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~114ms | 100,000 | Single key per record |
| status = 'active' (indexed) | ~88ms | ~50,000 | IndexScan → multi_get |
| product_recommendation1 = 'ProductA' | ~108ms | ~33,300 | Full scan + Filter |
| status + rec1 + rec2 (AND) | ~91ms | ~5,500 | IndexScan + filter short-circuit |
| status='active' + sort + skip/take(50) | ~124ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~88ms | ~50,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~124ms | ~50,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~88ms | 200 | IndexScan + early stop |
| status='active' + sort + take(200) | ~124ms | 200 | IndexScan → Sort → Limit |
| last_contacted_at is null (~30%) | ~106ms | ~30,000 | Full scan + Filter |
| notes is null (~50%) | ~110ms | ~50,000 | Full scan + Filter |
| last_contacted_at is not null (~70%) | ~116ms | ~70,000 | Full scan + Filter |
| status='active' AND notes is null | ~91ms | ~25,000 | IndexScan on status, Filter on notes |
| 1,000 point lookups (get_by_id) | ~3.4ms | 1,000 | Single key access |
| projection (name, status only) | ~73ms | 100,000 | Selective RawDocumentBuf read → Projection |

### Key Observations

- **Single key per record**: All columns packed into one value. Reads are a single `get()` or scan iteration — no per-column key overhead.
- **Selective materialization**: When a projection is specified, the planner computes the required columns (projection + filter + sort) and scan nodes read only those top-level fields from `RawDocumentBuf`. This avoids full document deserialization and yields a ~40% improvement on projection queries compared to eager materialization.
- **Dot-notation field access**: Filters, sorts, and projections support dot-notation paths (e.g. `"address.city"`). The scan materializes top-level keys; nested trimming happens in the Projection node.
- **IndexScan → multi_get**: Index lookups collect record IDs, then batch-fetch full records via `multi_get`.
- **Sort overhead**: Sort materializes the entire filtered set (~50k records) and sorts in memory. This is the dominant cost for sorted queries.
- **Point lookups**: ~2.6µs (10k) to ~3.4µs (100k) per lookup — single RocksDB key access.
- **Near-linear scaling**: Most queries scale ~10x from 10k → 100k (10x data), showing minimal overhead from larger datasets.

## TCP (MessagePack over Localhost)

Same workload as embedded (100k records), accessed through the TCP server on localhost. Wire protocol uses MessagePack (rmp-serde) with length-prefixed framing.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~668ms | ~0.0067ms |

### Queries

| Query | Time | Records Returned |
|-------|------|------------------|
| Full scan (no filter) | ~267ms | 100,000 |
| status = 'active' (indexed) | ~167ms | ~50,000 |
| status + rec1 + rec2 (AND) | ~112ms | ~5,500 |
| status='active' (no sort) | ~163ms | ~50,000 |
| status='active' + sort contacts_count | ~207ms | ~50,000 |
| status='active' + take(200) (no sort) | ~104ms | 200 |
| status='active' + sort + take(200) | ~149ms | 200 |
| status='active' + sort + skip/take(50) | ~149ms | 50 |
| 1,000 point lookups (get_by_id) | ~39ms | 1,000 |

### Embedded vs TCP Overhead

| Query | Embedded | TCP | Overhead |
|-------|----------|-----|----------|
| Full scan (100k records) | 114ms | 267ms | +134% |
| IndexScan filter (50k records) | 88ms | 167ms | +90% |
| IndexScan + narrow (5.5k records) | 91ms | 112ms | +23% |
| IndexScan + sort + take(200) | 124ms | 149ms | +20% |
| 1,000 point lookups | 3.4ms | 39ms | Network round-trips |

**Takeaway**: TCP overhead scales with the number of records serialized over the wire. For paginated queries (sort + take), overhead is modest. Point lookups show the per-request round-trip cost (~0.04ms each).

### Why MessagePack over BSON for the Wire Format

We benchmarked BSON as the wire format (replacing MessagePack for TCP serialization) and found it significantly slower for bulk data transfer:

| Query (TCP, 100k) | MessagePack | BSON | Difference |
|---|---|---|---|
| Bulk insert | ~668ms | ~879ms | +32% slower |
| Full scan (100k records) | ~267ms | ~470ms | +76% slower |
| Indexed filter (~50k records) | ~167ms | ~270ms | +62% slower |
| Sorted (~50k records) | ~207ms | ~316ms | +53% slower |
| Small result sets (take 200) | ~104ms | ~91ms | ~flat |
| Point lookups (1000) | ~39ms | ~43ms | ~flat |

BSON's overhead is per-document and compounds on large result sets. The `bson::Document` type also cannot be serialized directly as a top-level BSON value when wrapped in an enum — it requires an intermediate `bson::to_bson()` → wrapper document → `bson::to_vec()` round-trip, adding allocation overhead.

MessagePack is more compact for structured enum data and handles Rust enums natively via serde. The sweet spot: **BSON for storage** (where `RawDocumentBuf` enables selective field reads without full deserialization) and **MessagePack for the wire** (where compact serialization of complete documents matters most).

## Scaling: 10k → 100k

| Query | 10k | 100k | Factor |
|-------|-----|------|--------|
| Full scan | 11ms | 114ms | 10.4x |
| IndexScan (status) | 8ms | 88ms | 11.0x |
| AND filter (3 conditions) | 8ms | 91ms | 11.4x |
| Sort (full, ~50%) | 10ms | 124ms | 12.4x |
| 1,000 point lookups | 2.6ms | 3.4ms | 1.3x |
| Projection (2 cols) | 7.3ms | 73ms | 10.0x |

Scan-heavy queries scale ~10x for 10x data — near-linear. Sort remains slightly super-linear due to in-memory materialization costs. Point lookups are nearly constant regardless of dataset size.

## Concurrency

| Test | Result |
|------|--------|
| 2 writers + 4 readers (concurrent) | ~90-100ms, all complete successfully |
| Write conflict detection | RocksDB OptimisticTransactionDB correctly detects conflicts: 1 succeeds, 1 gets a conflict error |

Write conflict recovery/retry is left to the application layer.

## RocksStore vs MemoryStore (DB-Level)

Same workload as embedded benchmarks, run against both storage backends. 100k records per user.

### Bulk Insert — 100k Records

| Backend | Time | Per Record |
|---------|------|------------|
| RocksStore | ~535ms | ~0.0054ms |
| MemoryStore | ~265ms | ~0.0027ms |
| **Speedup** | **2.0x** | |

### Queries — 100k Records

| Query | RocksStore | MemoryStore | Speedup |
|-------|-----------|-------------|---------|
| Full scan (no filter) | 114ms | 108ms | 1.1x |
| status = 'active' (indexed) | 88ms | 68ms | 1.3x |
| product_recommendation1 = 'ProductA' | 108ms | 100ms | 1.1x |
| status + rec1 + rec2 (AND) | 91ms | 69ms | 1.3x |
| status='active' + sort + skip/take | 124ms | 103ms | 1.2x |
| status='active' (no sort) | 88ms | 67ms | 1.3x |
| status='active' + sort | 124ms | 103ms | 1.2x |
| 1,000 point lookups | 3.4ms | 2.7ms | 1.3x |
| projection (name, status only) | 73ms | 63ms | 1.2x |

### Queries — 500k Records

At higher scale, the gap widens — especially for indexed queries and writes.

| Query | RocksStore | MemoryStore | Speedup |
|-------|-----------|-------------|---------|
| Bulk insert 500k | 2,954ms | 1,513ms | **2.0x** |
| Full scan | 653ms | 518ms | 1.3x |
| status = 'active' (indexed) | 627ms | 336ms | **1.9x** |
| status + rec1 + rec2 (AND) | 611ms | 350ms | **1.7x** |
| status='active' (no sort) | 608ms | 335ms | **1.8x** |
| status='active' + sort | 931ms | 658ms | 1.4x |
| 1,000 point lookups | 5.1ms | 3.4ms | 1.5x |
| Concurrency (2w + 4r) | 4,775ms | 2,176ms | **2.2x** |

### Key Observations

- **Writes are 2x faster** — MemoryStore has no WAL, no fsync, no LSM compaction. Batch write times stay flat (~30ms/10k) while RocksDB creeps from 49ms to 66ms as data grows.
- **Indexed queries are 1.4-1.9x faster** — index scans hit the store heavily; in-memory B-tree lookups beat RocksDB's block cache.
- **Full scans are modest (1.1-1.3x)** — the bottleneck is BSON deserialization, which is identical for both backends. Storage read time is a small fraction.
- **Concurrent workloads are 2.2x faster** — MemoryStore's lock-free reads via `ArcSwap` avoid RocksDB's internal locking overhead.

### Store-Level Stress Test (`slate-store-bench`)

Raw key-value performance at 500k records with 10 KB values (~4.7 GB total data). Tests verify zero data loss and zero corruption.

| Operation | MemoryStore | RocksStore | Speedup |
|-----------|------------|-----------|---------|
| Write 500k records | 4,455ms (112k rec/s) | 5,507ms (91k rec/s) | 1.2x |
| Read all 500k | 278ms (1.8M rec/s) | 1,861ms (269k rec/s) | **6.7x** |
| Scan prefix (full) | 147ms | 1,000ms | **6.8x** |

**Integrity checks passed:** rollback, snapshot isolation, delete_range, concurrent stress (4 readers + 1 writer).

The 6-7x raw read speedup narrows to 1.1-1.9x at the DB level because DB queries pay for BSON deserialization and filter evaluation on every record — costs that dominate the storage read time.

## Query Planner

The query planner builds a plan tree from the query and datasource schema:

```
Projection(Limit(Sort(Filter(Scan { columns }))))
```

- **Scan / IndexScan**: Accept an optional `columns` list computed by the planner (union of projection + filter + sort columns, mapped to top-level keys). When present, only those fields are materialized from `RawDocumentBuf`; otherwise full document deserialization.
- **IndexScan**: Used when the filter has an `Eq` condition on an indexed column in a top-level AND group. Collects matching record IDs from index keys, batch-fetches records via `multi_get`.
- **Filter**: Eager evaluation on materialized records. AND/OR short-circuit on conditions. Supports dot-notation paths (e.g. `"address.city"`) via `get_path()`.
- **Sort**: Materializes all records, sorts in memory. Supports dot-notation sort keys.
- **Limit**: `skip()` + `take()` on the record stream.
- **Projection**: Strips unneeded columns. For dot-notation paths (e.g. `"address.city"`), trims nested documents to only requested sub-paths.

Index keys use the format `i:{column}\x00{value_bytes}\x00{record_id}` and are maintained on writes for fields marked `indexed: true`.
