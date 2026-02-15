# Benchmarks

Snapshot from `slate-bench` — run on Apple Silicon (M-series) in release mode. Each record has 8 fields (6 required, 2 nullable) stored as individual cells in wide-column format. Two dataset sizes: 10k and 100k records per user, 3 users each.

Storage model: each record is stored as N cell keys (`d:{record_id}\x00{column}` → `{value, timestamp, expire_at}`), one per column (last write wins, no versioning). Queries use a plan tree (Scan/IndexScan → Filter → Sort → Limit → Projection) executed with lazy iterators.

All queries use key partitioning — records are stored with prefixed keys (`{user}:{datasource}:{id}`) and queries scan only the relevant partition via `scan_prefix`.

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
| 10 batches of 1,000 records | ~150ms | ~0.015ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~19ms | 10,000 | Single-pass scan, all cells inline |
| status = 'active' (indexed) | ~18ms | ~5,000 | IndexScan + read remaining cells |
| product_recommendation1 = 'ProductA' | ~18ms | ~3,350 | Full scan + Filter |
| status + rec1 + rec2 (AND) | ~11ms | ~580 | IndexScan + lazy filter short-circuit |
| status='active' + sort + skip/take(50) | ~10ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~18ms | ~5,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~27ms | ~5,000 | IndexScan → Sort (materializes) |
| status='active' + take(200) (no sort) | ~0.7ms | 200 | **Lazy short-circuit** |
| status='active' + sort + take(200) | ~11ms | 200 | IndexScan → Sort → Limit |
| last_contacted_at is null (~30%) | ~20ms | ~3,000 | Full scan + Filter |
| notes is null (~50%) | ~22ms | ~5,000 | Full scan + Filter |
| last_contacted_at is not null (~70%) | ~21ms | ~7,000 | Full scan + Filter |
| status='active' AND notes is null | ~14ms | ~2,400 | IndexScan on status, Filter on notes |
| 1,000 point lookups (get_by_id) | ~4.5ms | 1,000 | Direct key access |
| projection (name, status only) | ~20ms | 10,000 | Full scan → Projection |

## Embedded — 100k Records

Results from a single user partition (100,000 records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~2,000ms | ~0.020ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~234ms | 100,000 | Single-pass scan, all cells inline |
| status = 'active' (indexed) | ~261ms | ~50,000 | IndexScan + read remaining cells |
| product_recommendation1 = 'ProductA' | ~220ms | ~33,000 | Full scan + Filter |
| status + rec1 + rec2 (AND) | ~159ms | ~5,500 | IndexScan + lazy filter short-circuit |
| status='active' + sort + skip/take(50) | ~162ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~256ms | ~50,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~407ms | ~50,000 | IndexScan → Sort (materializes) |
| status='active' + take(200) (no sort) | ~1.1ms | 200 | **Lazy short-circuit** |
| status='active' + sort + take(200) | ~154ms | 200 | IndexScan → Sort → Limit |
| last_contacted_at is null (~30%) | ~253ms | ~30,000 | Full scan + Filter |
| notes is null (~50%) | ~276ms | ~50,000 | Full scan + Filter |
| last_contacted_at is not null (~70%) | ~262ms | ~70,000 | Full scan + Filter |
| status='active' AND notes is null | ~208ms | ~25,000 | IndexScan on status, Filter on notes |
| 1,000 point lookups (get_by_id) | ~6.3ms | 1,000 | Direct key access |
| projection (name, status only) | ~244ms | 100,000 | Full scan → Projection |

### Key Observations

- **No column versioning**: Timestamp lives in the value, not the key. Each column has exactly one key — reads use exact `get()` instead of `scan_prefix` per column. This enables `multi_get` batching.
- **Lazy iterators**: `take(200)` without sort runs in ~1.1ms — the iterator short-circuits after 200 matches without scanning the full dataset.
- **Lazy filter evaluation**: AND filters read columns on demand and short-circuit. For `status + rec1 + rec2`, if `rec1` fails, `rec2` is never read from disk.
- **multi_get batching**: Sort, Projection, and `get_by_id` with explicit columns use RocksDB `multi_get_cf()` to batch-read columns in a single call instead of per-column `get()` loops.
- **IndexScan + Sort + Limit**: ~154ms for sort + take(200) at 100k. IndexScan yields lightweight records, Sort batch-reads sort columns via `multi_get`, Limit takes 200, then remaining columns are read.
- **Sort overhead**: Sort materializes the entire filtered set (~50k records). Sort columns are batch-read via `multi_get`.
- **Full scan cost**: ~19ms at 10k, ~234ms at 100k. Each record has ~8-9 cell keys to iterate and deserialize.
- **Point lookups**: ~4.5µs (10k) to ~6.3µs (100k) per lookup via RocksDB direct key access.
- **Near-linear scaling**: Most queries scale ~10-13x from 10k → 100k (10x data), showing minimal overhead from larger datasets.

## TCP (Bincode over Localhost)

Same workload as embedded, accessed through the TCP server on localhost.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~2,080ms | ~0.021ms |

### Queries

| Query | Time | Records Returned |
|-------|------|------------------|
| Full scan (no filter) | ~305ms | 100,000 |
| status = 'active' (indexed) | ~300ms | ~50,000 |
| status + rec1 + rec2 (AND) | ~163ms | ~5,500 |
| status='active' (no sort) | ~291ms | ~50,000 |
| status='active' + sort contacts_count | ~443ms | ~50,000 |
| status='active' + take(200) (no sort) | ~1.4ms | 200 |
| status='active' + sort + take(200) | ~157ms | 200 |
| status='active' + sort + skip/take(50) | ~166ms | 50 |
| 1,000 point lookups (get_by_id) | ~44ms | 1,000 |

### Embedded vs TCP Overhead

| Query | Embedded | TCP | Overhead |
|-------|----------|-----|----------|
| Full scan (100k records) | 234ms | 305ms | +30% |
| IndexScan filter (50k records) | 261ms | 300ms | +15% |
| IndexScan + narrow (5.5k records) | 159ms | 163ms | +3% |
| Filter + take(200), no sort | 1.1ms | 1.4ms | +27% |
| IndexScan + sort + take(200) | 154ms | 157ms | ~2% |
| 1,000 point lookups | 6.3ms | 44ms | Network round-trips |

**Takeaway**: TCP overhead scales with the number of records serialized over the wire. For paginated queries (sort + take), overhead is negligible. Point lookups show the per-request round-trip cost (~0.04ms each).

## Scaling: 10k → 100k

| Query | 10k | 100k | Factor |
|-------|-----|------|--------|
| Full scan | 19ms | 234ms | 12.3x |
| IndexScan (status) | 18ms | 261ms | 14.5x |
| AND filter (3 conditions) | 11ms | 159ms | 14.5x |
| Sort + take(200) | 11ms | 154ms | 14.0x |
| Sort (full, ~50%) | 27ms | 407ms | 15.1x |
| take(200), no sort | 0.7ms | 1.1ms | 1.6x |
| 1,000 point lookups | 4.5ms | 6.3ms | 1.4x |
| Projection (2 cols) | 20ms | 244ms | 12.2x |

Scan-heavy queries scale ~12-15x for 10x data — slightly super-linear due to larger RocksDB SST files and reduced cache hit rates. Lazy short-circuit (`take(200)`) and point lookups are nearly constant regardless of dataset size.

## Concurrency

| Test | Result |
|------|--------|
| 2 writers + 4 readers (concurrent) | ~96ms, all complete successfully |
| Write conflict detection | RocksDB OptimisticTransactionDB correctly detects conflicts: 1 succeeds, 1 gets a conflict error |

Write conflict recovery/retry is left to the application layer.

## Query Planner

The query planner builds a plan tree from the query and datasource schema:

```
Projection(Limit(Sort(Filter(IndexScan))))
```

- **IndexScan**: Used when the filter has an `Eq` condition on an indexed column in a top-level AND group. Yields lightweight records (only the indexed column pre-populated).
- **Scan**: Full partition scan. Yields complete records with all cells.
- **Filter**: Lazy evaluation — reads columns on demand during predicate evaluation. AND short-circuits skip reading columns for conditions after the first failure.
- **Sort**: Materializes all records, batch-reads sort columns via `multi_get`, sorts in memory.
- **Limit**: `skip()` + `take()` — lazy, short-circuits the upstream iterator.
- **Projection**: Batch-reads projected columns via `multi_get`, strips unneeded ones.

Index keys use the format `i:{column}\x00{value_bytes}\x00{record_id}` and are maintained on writes for fields marked `indexed: true`.
