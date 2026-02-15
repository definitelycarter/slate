# Benchmarks

Snapshot from `slate-bench` — run on Apple Silicon (M-series) in release mode. Each record has 8 fields (6 required, 2 nullable). Two dataset sizes: 10k and 100k records per user, 3 users each.

Storage model: each record is stored as a single key (`d:{partition}:{record_id}` → `RecordValue { expire_at, cells: HashMap<column, StoredCell { value, expire_at }> }`). Queries use a plan tree (Scan/IndexScan → Filter → Sort → Limit → Projection) with eager record materialization — each record is fully deserialized on read.

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

## Embedded — 10k Records

Results from a single user partition (10,000 records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 1,000 records | ~50ms | ~0.005ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~11ms | 10,000 | Single key per record |
| status = 'active' (indexed) | ~9ms | ~5,000 | IndexScan → multi_get |
| product_recommendation1 = 'ProductA' | ~10ms | ~3,300 | Full scan + Filter |
| status + rec1 + rec2 (AND) | ~8ms | ~550 | IndexScan + filter short-circuit |
| status='active' + sort + skip/take(50) | ~10ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~8ms | ~5,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~10ms | ~5,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~8ms | 200 | IndexScan + early stop |
| status='active' + sort + take(200) | ~10ms | 200 | IndexScan → Sort → Limit |
| last_contacted_at is null (~30%) | ~10ms | ~3,000 | Full scan + Filter |
| notes is null (~50%) | ~10ms | ~5,100 | Full scan + Filter |
| last_contacted_at is not null (~70%) | ~11ms | ~7,000 | Full scan + Filter |
| status='active' AND notes is null | ~8ms | ~2,500 | IndexScan on status, Filter on notes |
| 1,000 point lookups (get_by_id) | ~2.5ms | 1,000 | Single key access |
| projection (name, status only) | ~12ms | 10,000 | Full scan → Projection |

## Embedded — 100k Records

Results from a single user partition (100,000 records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~555ms | ~0.0055ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~111ms | 100,000 | Single key per record |
| status = 'active' (indexed) | ~90ms | ~50,000 | IndexScan → multi_get |
| product_recommendation1 = 'ProductA' | ~104ms | ~33,300 | Full scan + Filter |
| status + rec1 + rec2 (AND) | ~91ms | ~5,500 | IndexScan + filter short-circuit |
| status='active' + sort + skip/take(50) | ~137ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~90ms | ~50,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~140ms | ~50,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~91ms | 200 | IndexScan + early stop |
| status='active' + sort + take(200) | ~140ms | 200 | IndexScan → Sort → Limit |
| last_contacted_at is null (~30%) | ~103ms | ~30,000 | Full scan + Filter |
| notes is null (~50%) | ~105ms | ~50,000 | Full scan + Filter |
| last_contacted_at is not null (~70%) | ~112ms | ~70,000 | Full scan + Filter |
| status='active' AND notes is null | ~93ms | ~25,000 | IndexScan on status, Filter on notes |
| 1,000 point lookups (get_by_id) | ~3.3ms | 1,000 | Single key access |
| projection (name, status only) | ~123ms | 100,000 | Full scan → Projection |

### Key Observations

- **Single key per record**: All columns packed into one value. Reads are a single `get()` or scan iteration — no per-column key overhead.
- **Eager materialization**: Records are fully deserialized on read. Filter, Sort, and Projection operate on complete in-memory records.
- **IndexScan → multi_get**: Index lookups collect record IDs, then batch-fetch full records via `multi_get`.
- **Sort overhead**: Sort materializes the entire filtered set (~50k records) and sorts in memory. This is the dominant cost for sorted queries.
- **Point lookups**: ~2.5µs (10k) to ~3.3µs (100k) per lookup — single RocksDB key access.
- **Near-linear scaling**: Most queries scale ~10x from 10k → 100k (10x data), showing minimal overhead from larger datasets.

## TCP (MessagePack over Localhost)

Same workload as embedded (100k records), accessed through the TCP server on localhost. Wire protocol uses MessagePack (rmp-serde) for cross-language compatibility.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~635ms | ~0.0063ms |

### Queries

| Query | Time | Records Returned |
|-------|------|------------------|
| Full scan (no filter) | ~182ms | 100,000 |
| status = 'active' (indexed) | ~125ms | ~50,000 |
| status + rec1 + rec2 (AND) | ~95ms | ~5,500 |
| status='active' (no sort) | ~125ms | ~50,000 |
| status='active' + sort contacts_count | ~177ms | ~50,000 |
| status='active' + take(200) (no sort) | ~91ms | 200 |
| status='active' + sort + take(200) | ~139ms | 200 |
| status='active' + sort + skip/take(50) | ~139ms | 50 |
| 1,000 point lookups (get_by_id) | ~39ms | 1,000 |

### Embedded vs TCP Overhead

| Query | Embedded | TCP | Overhead |
|-------|----------|-----|----------|
| Full scan (100k records) | 111ms | 182ms | +64% |
| IndexScan filter (50k records) | 90ms | 125ms | +39% |
| IndexScan + narrow (5.5k records) | 91ms | 95ms | +4% |
| IndexScan + sort + take(200) | 140ms | 139ms | ~0% |
| 1,000 point lookups | 3.3ms | 39ms | Network round-trips |

**Takeaway**: TCP overhead scales with the number of records serialized over the wire. For paginated queries (sort + take), overhead is negligible. Point lookups show the per-request round-trip cost (~0.04ms each).

## Scaling: 10k → 100k

| Query | 10k | 100k | Factor |
|-------|-----|------|--------|
| Full scan | 11ms | 111ms | 10.1x |
| IndexScan (status) | 9ms | 90ms | 10.0x |
| AND filter (3 conditions) | 8ms | 91ms | 11.4x |
| Sort (full, ~50%) | 10ms | 140ms | 14.0x |
| 1,000 point lookups | 2.5ms | 3.3ms | 1.3x |
| Projection (2 cols) | 12ms | 123ms | 10.3x |

Scan-heavy queries scale ~10x for 10x data — near-linear. Sort remains slightly super-linear due to in-memory materialization costs. Point lookups are nearly constant regardless of dataset size.

## Concurrency

| Test | Result |
|------|--------|
| 2 writers + 4 readers (concurrent) | ~90-100ms, all complete successfully |
| Write conflict detection | RocksDB OptimisticTransactionDB correctly detects conflicts: 1 succeeds, 1 gets a conflict error |

Write conflict recovery/retry is left to the application layer.

## Query Planner

The query planner builds a plan tree from the query and datasource schema:

```
Projection(Limit(Sort(Filter(IndexScan))))
```

- **IndexScan**: Used when the filter has an `Eq` condition on an indexed column in a top-level AND group. Collects matching record IDs from index keys, batch-fetches full records via `multi_get`.
- **Scan**: Full partition scan. Iterates one key per record, deserializes complete record.
- **Filter**: Eager evaluation on complete records. AND/OR short-circuit on conditions.
- **Sort**: Materializes all records, sorts in memory.
- **Limit**: `skip()` + `take()` on the record stream.
- **Projection**: Strips unneeded columns from complete records via `retain()`.

Index keys use the format `i:{column}\x00{value_bytes}\x00{record_id}` and are maintained on writes for fields marked `indexed: true`.
