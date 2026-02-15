# Benchmarks

Snapshot from `slate-bench` — 100,000 records per user, 3 users, run on Apple Silicon (M-series) in release mode. Each record has 8 fields (6 required, 2 nullable). Total dataset: 300,000 records.

All queries use key partitioning — records are stored with prefixed keys (`{user}:{datasource}:{id}`) and queries scan only the relevant partition via `scan_prefix`. This means queries never touch data outside their partition, even in a shared database.

## Record Schema

| Field | Type | Notes |
|-------|------|-------|
| name | String | Random name |
| status | String | Random: "active" or "rejected" (~50/50) |
| contacts_count | Int | Random 0–1000 |
| product_recommendation1 | String | Random: ProductA / ProductB / ProductC |
| product_recommendation2 | String | Random: ProductA / ProductB / ProductC |
| product_recommendation3 | String | Random: ProductA / ProductB / ProductC |
| last_contacted_at | Date | ~30% null |
| notes | String | ~50% null |

## Embedded (Direct)

Results from a single user partition (100k records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~230ms | ~0.0023ms |

### Queries

| Query | Time | Records Returned |
|-------|------|------------------|
| Full scan (no filter) | ~83ms | 100,000 |
| status = 'active' | ~83ms | ~50,000 |
| product_recommendation1 = 'ProductA' | ~75ms | ~33,000 |
| status + rec1 + rec2 (AND) | ~71ms | ~5,500 |
| status='active' + sort + skip/take(50) | ~132ms | 50 |
| status='active' (no sort) | ~77ms | ~50,000 |
| status='active' + sort contacts_count | ~126ms | ~50,000 |
| status='active' + take(200) (no sort) | ~68ms | 200 |
| status='active' + sort + take(200) | ~127ms | 200 |
| last_contacted_at is null | ~72ms | ~30,000 |
| notes is null | ~74ms | ~50,000 |
| last_contacted_at is not null | ~80ms | ~70,000 |
| status='active' AND notes is null | ~73ms | ~25,000 |
| 1,000 point lookups (get_by_id) | ~2ms | 1,000 |

### Key Observations

- **Sort overhead**: ~45ms on top of filter time. Sort requires materializing the filtered set in memory.
- **take(200) without sort**: Saves ~10ms by short-circuiting the scan after 200 matches.
- **take(200) with sort**: No improvement — the full filtered set must be sorted before taking.
- **Point lookups**: ~2µs per lookup via RocksDB direct key access (longer composite keys vs flat keys).
- **Null queries**: No performance difference compared to value filters.

## Multi-Prefix Queries

Queries across multiple user partitions in a shared database (300k total records, 100k per user). This simulates a manager viewing data across multiple users.

| Query | Time | Records Returned |
|-------|------|------------------|
| 1 prefix, no filter | ~97ms | 100,000 |
| 2 prefixes, no filter | ~176ms | 200,000 |
| 3 prefixes, no filter | ~265ms | 300,000 |
| 3 prefixes, status='active' | ~295ms | ~150,000 |
| 3 prefixes, status='active' + sort + take(200) | ~497ms | 200 |

### Key Observations

- **Linear scaling**: Time scales linearly with the number of prefixes — each prefix is an independent `scan_prefix` call, results are merged via `MultiIter`.
- **Per-record cost is constant**: ~0.0009ms/record regardless of how many prefixes are queried.
- **Partition isolation works**: A single-prefix scan in a 300k-record shared DB takes ~97ms (same as a single-user DB), confirming that `scan_prefix` skips unrelated partitions entirely.

## TCP (Bincode over Localhost)

Same workload as embedded, accessed through the TCP server on localhost.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~321ms | ~0.0032ms |

### Queries

| Query | Time | Records Returned |
|-------|------|------------------|
| Full scan (no filter) | ~154ms | 100,000 |
| status = 'active' | ~133ms | ~50,000 |
| status + rec1 + rec2 (AND) | ~85ms | ~5,500 |
| status='active' (no sort) | ~133ms | ~50,000 |
| status='active' + sort contacts_count | ~182ms | ~50,000 |
| status='active' + take(200) (no sort) | ~75ms | 200 |
| status='active' + sort + take(200) | ~136ms | 200 |
| status='active' + sort + skip/take(50) | ~138ms | 50 |
| 1,000 point lookups (get_by_id) | ~39ms | 1,000 |

### Embedded vs TCP Overhead

| Query | Embedded | TCP | Overhead |
|-------|----------|-----|----------|
| Full scan (100k records) | 83ms | 154ms | +86% (serialization dominant) |
| Filter (50k records) | 83ms | 133ms | +60% |
| Filter + narrow (5.5k records) | 71ms | 85ms | +20% |
| Filter + take(200), no sort | 68ms | 75ms | +10% |
| Filter + sort + take(200) | 127ms | 136ms | +7% |
| 1,000 point lookups | 2ms | 39ms | Network round-trips |

**Takeaway**: For paginated queries (the common case in a UI), TCP adds negligible overhead (~7–10ms). The cost scales with the number of records serialized over the wire, not the number scanned. Point lookups show the per-request round-trip cost (~0.04ms each).

## Concurrency

| Test | Result |
|------|--------|
| 2 writers + 4 readers (concurrent) | ~535ms, all complete successfully |
| Write conflict detection | RocksDB OptimisticTransactionDB correctly detects conflicts: 1 succeeds, 1 gets a conflict error |

Write conflict recovery/retry is left to the application layer.

## Impact of Key Partitioning

Key partitioning adds composite keys (`prefix:id`) instead of flat keys. This has a small cost:

| Metric | Flat Keys | Partitioned Keys | Delta |
|--------|-----------|-------------------|-------|
| Bulk insert (100k) | ~194ms | ~230ms | +19% |
| Point lookups (1k) | ~0.8ms | ~2ms | +150% |
| Filtered query (50k results) | ~80ms | ~83ms | +4% |

The insert and point lookup overhead comes from longer key strings. Query overhead is minimal since `scan_prefix` uses RocksDB's sorted key ordering to seek directly to the partition — it doesn't scan unrelated data. The tradeoff is worth it: without partitioning, every query scans the entire database regardless of which user's data you need.
