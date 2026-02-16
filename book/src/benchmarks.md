# Benchmarks

Snapshot from `slate-bench` — run on Apple Silicon (M-series) in release mode. Each record has 8 fields (6 required, 2 nullable). Two dataset sizes: 10k and 100k records per user, 3 users each.

Storage model: each collection is a RocksDB column family. Records are stored as `d:{_id}` → raw BSON bytes via `bson::to_vec`. Queries use a plan tree (Scan/IndexScan → Filter → Sort → Limit → Projection) with selective materialization — the planner computes the required columns (projection + filter + sort) and scan nodes read only those top-level fields from `RawDocumentBuf`, avoiding full document deserialization when a projection is specified.

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
| 10 batches of 1,000 records | ~43ms | ~0.0043ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~11ms | 10,000 | Single key per record |
| status = 'active' (indexed) | ~8ms | ~5,000 | IndexScan → multi_get |
| product_recommendation1 = 'ProductA' | ~10.5ms | ~3,300 | Full scan + Filter |
| status + rec1 + rec2 (AND) | ~8ms | ~550 | IndexScan + filter short-circuit |
| status='active' + sort + skip/take(50) | ~9.5ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~7.5ms | ~5,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~9.5ms | ~5,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~7.5ms | 200 | IndexScan + early stop |
| status='active' + sort + take(200) | ~9.5ms | 200 | IndexScan → Sort → Limit |
| last_contacted_at is null (~30%) | ~10.5ms | ~3,000 | Full scan + Filter |
| notes is null (~50%) | ~11ms | ~5,000 | Full scan + Filter |
| last_contacted_at is not null (~70%) | ~11ms | ~7,000 | Full scan + Filter |
| status='active' AND notes is null | ~8ms | ~2,500 | IndexScan on status, Filter on notes |
| 1,000 point lookups (find_by_id) | ~1.8ms | 1,000 | Direct key access |
| projection (name, status only) | ~7.5ms | 10,000 | Selective RawDocumentBuf read → Projection |

## Embedded (RocksStore) — 100k Records

Results from a single user partition (100,000 records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~490ms | ~0.0049ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~115ms | 100,000 | Single key per record |
| status = 'active' (indexed) | ~86ms | ~50,000 | IndexScan → multi_get |
| product_recommendation1 = 'ProductA' | ~109ms | ~33,300 | Full scan + Filter |
| status + rec1 + rec2 (AND) | ~86ms | ~5,500 | IndexScan + filter short-circuit |
| status='active' + sort + skip/take(50) | ~119ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~83ms | ~50,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~121ms | ~50,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~83ms | 200 | IndexScan + early stop |
| status='active' + sort + take(200) | ~120ms | 200 | IndexScan → Sort → Limit |
| last_contacted_at is null (~30%) | ~107ms | ~30,000 | Full scan + Filter |
| notes is null (~50%) | ~111ms | ~50,000 | Full scan + Filter |
| last_contacted_at is not null (~70%) | ~115ms | ~70,000 | Full scan + Filter |
| status='active' AND notes is null | ~86ms | ~25,000 | IndexScan on status, Filter on notes |
| 1,000 point lookups (find_by_id) | ~2.5ms | 1,000 | Direct key access |
| projection (name, status only) | ~74ms | 100,000 | Selective RawDocumentBuf read → Projection |

### Key Observations

- **Single key per record**: All columns packed into one value. Reads are a single `get()` or scan iteration — no per-column key overhead.
- **Selective materialization**: When a projection is specified, the planner computes the required columns (projection + filter + sort) and scan nodes read only those top-level fields from `RawDocumentBuf`. This avoids full document deserialization and yields a ~40% improvement on projection queries compared to eager materialization.
- **Dot-notation field access**: Filters, sorts, and projections support dot-notation paths (e.g. `"address.city"`). The scan materializes top-level keys; nested trimming happens in the Projection node.
- **IndexScan → multi_get**: Index lookups collect record IDs, then batch-fetch full records via `multi_get`.
- **Sort overhead**: Sort materializes the entire filtered set (~50k records) and sorts in memory. This is the dominant cost for sorted queries.
- **Point lookups**: ~1.8ms (10k) to ~2.5ms (100k) for 1,000 lookups — direct key access via `find_by_id`.
- **Near-linear scaling**: Most queries scale ~10x from 10k → 100k (10x data), showing minimal overhead from larger datasets.

## TCP (MessagePack over Localhost)

Same workload as embedded (100k records), accessed through the TCP server on localhost. Wire protocol uses MessagePack (rmp-serde) with length-prefixed framing.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~643ms | ~0.0064ms |

### Queries

| Query | Time | Records Returned |
|-------|------|------------------|
| Full scan (no filter) | ~259ms | 100,000 |
| status = 'active' (indexed) | ~156ms | ~50,000 |
| status + rec1 + rec2 (AND) | ~108ms | ~5,500 |
| status='active' (no sort) | ~155ms | ~50,000 |
| status='active' + sort contacts_count | ~206ms | ~50,000 |
| status='active' + take(200) (no sort) | ~99ms | 200 |
| status='active' + sort + take(200) | ~144ms | 200 |
| status='active' + sort + skip/take(50) | ~146ms | 50 |
| 1,000 point lookups (find_by_id) | ~38ms | 1,000 |

### Embedded vs TCP Overhead

| Query | Embedded | TCP | Overhead |
|-------|----------|-----|----------|
| Full scan (100k records) | 115ms | 259ms | +125% |
| IndexScan filter (50k records) | 86ms | 156ms | +81% |
| IndexScan + narrow (5.5k records) | 86ms | 108ms | +26% |
| IndexScan + sort + take(200) | 120ms | 144ms | +20% |
| 1,000 point lookups | 2.5ms | 38ms | Network round-trips |

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
| Full scan | 11ms | 115ms | 10.5x |
| IndexScan (status) | 8ms | 86ms | 10.8x |
| AND filter (3 conditions) | 8ms | 86ms | 10.8x |
| Sort (full, ~50%) | 9.5ms | 121ms | 12.7x |
| 1,000 point lookups | 1.8ms | 2.5ms | 1.4x |
| Projection (2 cols) | 7.5ms | 74ms | 9.9x |

Scan-heavy queries scale ~10x for 10x data — near-linear. Sort remains slightly super-linear due to in-memory materialization costs. Point lookups are nearly constant regardless of dataset size.

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
| RocksStore | ~490ms | ~0.0049ms |
| MemoryStore | ~221ms | ~0.0022ms |
| **Speedup** | **2.2x** | |

### Queries — 100k Records

| Query | RocksStore | MemoryStore | Speedup |
|-------|-----------|-------------|---------|
| Full scan (no filter) | 115ms | 110ms | 1.0x |
| status = 'active' (indexed) | 86ms | 65ms | 1.3x |
| product_recommendation1 = 'ProductA' | 109ms | 102ms | 1.1x |
| status + rec1 + rec2 (AND) | 86ms | 66ms | 1.3x |
| status='active' + sort + skip/take | 119ms | 104ms | 1.1x |
| status='active' (no sort) | 83ms | 65ms | 1.3x |
| status='active' + sort | 121ms | 105ms | 1.2x |
| 1,000 point lookups (find_by_id) | 2.5ms | 2.0ms | 1.2x |
| projection (name, status only) | 74ms | 66ms | 1.1x |

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

The 6-7x raw read speedup narrows to 1.1-1.3x at the DB level because DB queries pay for BSON deserialization and filter evaluation on every record — costs that dominate the storage read time.

## Query Planner

The query planner builds a plan tree from the query and indexed fields list:

```
Projection(Limit(Sort(Filter(Scan { columns }))))
```

- **Scan / IndexScan**: Accept an optional `columns` list computed by the planner (union of projection + filter + sort columns, mapped to top-level keys). When present, only those fields are materialized from `RawDocumentBuf`; otherwise full document deserialization.
- **IndexScan**: Used when the filter has an `Eq` condition on an indexed field in a top-level AND group. Collects matching record IDs from index keys, batch-fetches records via `multi_get`.
- **Filter**: Eager evaluation on materialized records. AND/OR short-circuit on conditions. Supports dot-notation paths (e.g. `"address.city"`) via `get_path()`.
- **Sort**: Materializes all records, sorts in memory. Supports dot-notation sort keys.
- **Limit**: `skip()` + `take()` on the record stream.
- **Projection**: Strips unneeded columns. For dot-notation paths (e.g. `"address.city"`), trims nested documents to only requested sub-paths.

Index keys use the format `i:{field}\x00{value_bytes}\x00{_id}` and are maintained on insert/update/delete for indexed fields.
