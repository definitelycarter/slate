# Benchmarks

Snapshot from `slate-bench` — 100,000 records per user, 3 users, run on Apple Silicon (M-series) in release mode. Each record has 8 fields (6 required, 2 nullable). Total dataset: 300,000 records.

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

Results from a single user run (100k records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~194ms | ~0.002ms |

### Queries

| Query | Time | Records Returned |
|-------|------|------------------|
| Full scan (no filter) | ~74ms | 100,000 |
| status = 'active' | ~80ms | ~50,000 |
| product_recommendation1 = 'ProductA' | ~78ms | ~33,000 |
| status + rec1 + rec2 (AND) | ~74ms | ~5,500 |
| status='active' + sort + skip/take(50) | ~125ms | 50 |
| status='active' (no sort) | ~80ms | ~50,000 |
| status='active' + sort contacts_count | ~127ms | ~50,000 |
| status='active' + take(200) (no sort) | ~69ms | 200 |
| status='active' + sort + take(200) | ~127ms | 200 |
| last_contacted_at is null | ~74ms | ~30,000 |
| notes is null | ~75ms | ~50,000 |
| last_contacted_at is not null | ~84ms | ~70,000 |
| status='active' AND notes is null | ~75ms | ~25,000 |
| 1,000 point lookups (get_by_id) | ~0.8ms | 1,000 |

### Key Observations

- **Sort overhead**: ~45ms on top of filter time. Sort requires materializing the filtered set in memory.
- **take(200) without sort**: Saves ~10ms by short-circuiting the scan after 200 matches.
- **take(200) with sort**: No improvement — the full filtered set must be sorted before taking.
- **Point lookups**: Sub-microsecond per lookup via RocksDB direct key access.
- **Null queries**: No performance difference compared to value filters.

## TCP (Bincode over Localhost)

Same workload as embedded, accessed through the TCP server on localhost.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~283ms | ~0.003ms |

### Queries

| Query | Time | Records Returned |
|-------|------|------------------|
| Full scan (no filter) | ~147ms | 100,000 |
| status = 'active' | ~123ms | ~50,000 |
| status + rec1 + rec2 (AND) | ~81ms | ~5,500 |
| status='active' (no sort) | ~124ms | ~50,000 |
| status='active' + sort contacts_count | ~171ms | ~50,000 |
| status='active' + take(200) (no sort) | ~73ms | 200 |
| status='active' + sort + take(200) | ~131ms | 200 |
| status='active' + sort + skip/take(50) | ~130ms | 50 |
| 1,000 point lookups (get_by_id) | ~38ms | 1,000 |

### Embedded vs TCP Overhead

| Query | Embedded | TCP | Overhead |
|-------|----------|-----|----------|
| Full scan (100k records) | 74ms | 148ms | +100% (serialization dominant) |
| Filter (50k records) | 80ms | 123ms | +54% |
| Filter + narrow (5.5k records) | 74ms | 81ms | +9% |
| Filter + take(200), no sort | 69ms | 73ms | +6% |
| Filter + sort + take(200) | 127ms | 131ms | +3% |
| 1,000 point lookups | 0.8ms | 38ms | Network round-trips |

**Takeaway**: For paginated queries (the common case in a UI), TCP adds negligible overhead (~3–6ms). The cost scales with the number of records serialized over the wire, not the number scanned. Point lookups show the per-request round-trip cost (~0.04ms each).

## Concurrency

| Test | Result |
|------|--------|
| 2 writers + 4 readers (concurrent) | ~530ms, all complete successfully |
| Write conflict detection | RocksDB OptimisticTransactionDB correctly detects conflicts: 1 succeeds, 1 gets a conflict error |

Write conflict recovery/retry is left to the application layer.
