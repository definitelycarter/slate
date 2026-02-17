# Benchmarks

Snapshot from `slate-bench` — run on Apple Silicon (M-series) in release mode. Each record has 8 fields (6 required, 2 nullable). Two dataset sizes: 10k and 100k records per user, 3 users each.

Storage model: each collection is a RocksDB column family. Records are stored as `d:{_id}` → raw BSON bytes via `bson::to_vec`. Queries use a three-tier plan tree: the **ID tier** (Scan, IndexScan, IndexMerge) produces record IDs without touching document bytes. `ReadRecord` fetches raw BSON bytes by ID but does **not** deserialize them. The **raw tier** (Filter, Sort, Limit) operates on `Cow<[u8]>` bytes — constructing borrowed `&RawDocument` views on demand to access individual fields without allocation. The store trait returns `Cow<'_, [u8]>`: MemoryStore returns `Cow::Borrowed` (zero-copy from its snapshot), RocksDB returns `Cow::Owned`. Records that fail a filter are never cloned or deserialized. Finally, **Projection** is the single materialization point where raw bytes → `bson::Document`, selectively deserializing only the requested columns. For full scans without a filter, `ReadRecord` optimizes the `Scan` input into a single-pass iteration.

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
| 10 batches of 1,000 records | ~41ms | ~0.0041ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~13ms | 10,000 | Single key per record |
| status = 'active' (indexed) | ~8.3ms | ~5,000 | IndexScan → lazy filter |
| product_recommendation1 = 'ProductA' | ~6ms | ~3,300 | Scan + lazy filter, ~67% rejected without deserializing |
| status + rec1 + rec2 (AND) | ~4.1ms | ~550 | IndexScan + lazy filter, ~89% rejected |
| status='active' + sort + skip/take(50) | ~7.9ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~8.2ms | ~5,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~13ms | ~5,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~3.2ms | 200 | IndexScan, only 200 records materialized |
| status='active' + sort + take(200) | ~7.8ms | 200 | IndexScan → Sort → Limit |
| last_contacted_at is null (~30%) | ~5.5ms | ~3,000 | Scan + lazy filter, ~70% rejected |
| notes is null (~50%) | ~8ms | ~5,000 | Scan + lazy filter |
| last_contacted_at is not null (~70%) | ~11ms | ~7,000 | Scan + lazy filter |
| status='active' AND notes is null | ~6.2ms | ~2,500 | IndexScan + lazy filter |
| 1,000 point lookups (find_by_id) | ~1.7ms | 1,000 | Direct key access |
| projection (name, status only) | ~7.2ms | 10,000 | Selective materialization at Projection |

## Embedded (RocksStore) — 100k Records

Results from a single collection (100,000 records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~476ms | ~0.0048ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~123ms | 100,000 | Single key per record |
| status = 'active' (indexed) | ~86ms | ~50,000 | IndexScan → lazy filter |
| product_recommendation1 = 'ProductA' | ~60ms | ~33,300 | Scan + lazy filter, ~67% rejected without deserializing |
| status + rec1 + rec2 (AND) | ~44ms | ~5,500 | IndexScan + lazy filter, ~89% rejected |
| status='active' + sort + skip/take(50) | ~83ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~85ms | ~50,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~140ms | ~50,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~32ms | 200 | IndexScan, only 200 records materialized |
| status='active' + sort + take(200) | ~83ms | 200 | IndexScan → Sort → Limit |
| last_contacted_at is null (~30%) | ~56ms | ~30,000 | Scan + lazy filter, ~70% rejected |
| notes is null (~50%) | ~80ms | ~50,000 | Scan + lazy filter |
| last_contacted_at is not null (~70%) | ~110ms | ~70,000 | Scan + lazy filter |
| status='active' AND notes is null | ~65ms | ~25,000 | IndexScan + lazy filter |
| 1,000 point lookups (find_by_id) | ~2.4ms | 1,000 | Direct key access |
| projection (name, status only) | ~82ms | 100,000 | Selective materialization at Projection |

### Key Observations

- **Zero-copy reads + lazy materialization**: The store trait returns `Cow<'_, [u8]>` — MemoryStore returns borrowed references into its snapshot (zero allocation), RocksDB returns owned bytes. The raw tier constructs `&RawDocument` views on demand to access individual fields without allocation. Records that fail a filter are never cloned or deserialized. Projection is the single materialization point, selectively converting only the requested columns from raw bytes to `bson::Document`. Full materialization uses `RawDocument::try_into()` (direct raw iteration) rather than `bson::from_slice` (serde visitor pattern), avoiding the overhead of extended JSON key matching on every field.
- **Single key per record**: All columns packed into one value. Reads are a single `get()` or scan iteration — no per-column key overhead.
- **Dot-notation field access**: Filters, sorts, and projections support dot-notation paths (e.g. `"address.city"`). Nested path resolution works directly on `&RawDocument` via `get_document()` chaining.
- **IndexScan → multi_get**: Index lookups collect record IDs, then batch-fetch raw bytes via `multi_get`. Filter evaluates lazily on the raw bytes.
- **Sort overhead**: Sort accesses sort keys lazily from raw bytes, but must hold all records in memory via `into_owned()`. Only records that survive the filter pay this cost. This is the dominant cost for sorted queries.
- **take() without sort**: When no sort is required, `take(N)` benefits dramatically from lazy materialization — only N records are deserialized regardless of how many match the filter. 100k dataset, `take(200)`: ~32ms vs ~82ms with eager deserialization.
- **Point lookups**: ~1.7ms (10k) to ~2.4ms (100k) for 1,000 lookups — direct key access via `find_by_id`.
- **Near-linear scaling**: Most queries scale ~10x from 10k → 100k (10x data), showing minimal overhead from larger datasets.

## TCP (MessagePack over Localhost)

Same workload as embedded (100k records), accessed through the TCP server on localhost. Wire protocol uses MessagePack (rmp-serde) with length-prefixed framing.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~622ms | ~0.0062ms |

### Queries

| Query | Time | Records Returned |
|-------|------|------------------|
| Full scan (no filter) | ~245ms | 100,000 |
| status = 'active' (indexed) | ~147ms | ~50,000 |
| status + rec1 + rec2 (AND) | ~53ms | ~5,500 |
| status='active' (no sort) | ~146ms | ~50,000 |
| status='active' + sort contacts_count | ~205ms | ~50,000 |
| status='active' + take(200) (no sort) | ~36ms | 200 |
| status='active' + sort + take(200) | ~88ms | 200 |
| status='active' + sort + skip/take(50) | ~86ms | 50 |
| 1,000 point lookups (find_by_id) | ~40ms | 1,000 |

### Embedded vs TCP Overhead

| Query | Embedded | TCP | Overhead |
|-------|----------|-----|----------|
| Full scan (100k records) | 123ms | 245ms | +99% |
| IndexScan filter (50k records) | 86ms | 147ms | +71% |
| IndexScan + narrow (5.5k records) | 44ms | 53ms | +20% |
| IndexScan + sort + take(200) | 83ms | 88ms | +6% |
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

MessagePack is more compact for structured enum data and handles Rust enums natively via serde. The sweet spot: **BSON for storage** (where `&RawDocument` views enable selective field reads without full deserialization) and **MessagePack for the wire** (where compact serialization of complete documents matters most).

## Scaling: 10k → 100k

| Query | 10k | 100k | Factor |
|-------|-----|------|--------|
| Full scan | 13ms | 123ms | 9.5x |
| IndexScan (status) | 8.3ms | 86ms | 10.4x |
| AND filter (3 conditions) | 4.1ms | 44ms | 10.7x |
| Scan + filter (ProductA) | 6ms | 60ms | 10.0x |
| Sort (full, ~50%) | 13ms | 140ms | 10.8x |
| take(200) no sort | 3.2ms | 32ms | 10.0x |
| 1,000 point lookups | 1.7ms | 2.4ms | 1.4x |
| Projection (2 cols) | 7.2ms | 82ms | 11.4x |

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
| RocksStore | ~476ms | ~0.0048ms |
| MemoryStore | ~212ms | ~0.0021ms |
| **Speedup** | **2.2x** | |

### Queries — 100k Records

| Query | RocksStore | MemoryStore | Speedup |
|-------|-----------|-------------|---------|
| Full scan (no filter) | 123ms | 112ms | 1.1x |
| status = 'active' (indexed) | 86ms | 65ms | 1.3x |
| product_recommendation1 = 'ProductA' | 60ms | 50ms | 1.2x |
| status + rec1 + rec2 (AND) | 44ms | 24ms | 1.8x |
| status='active' + sort + skip/take | 83ms | 63ms | 1.3x |
| status='active' (no sort) | 85ms | 65ms | 1.3x |
| status='active' + sort | 140ms | 122ms | 1.1x |
| 1,000 point lookups (find_by_id) | 2.4ms | 1.9ms | 1.3x |
| projection (name, status only) | 82ms | 60ms | 1.4x |

### Key Observations

- **Writes are 2.2x faster** — MemoryStore has no WAL, no fsync, no LSM compaction. Lazy CF snapshots and dirty-only commits minimize overhead further.
- **Indexed queries are 1.3x faster** — index scans hit the store heavily; in-memory B-tree lookups beat RocksDB's block cache. AND filters see up to 1.8x speedup because MemoryStore's zero-copy `Cow::Borrowed` avoids cloning bytes for rejected records.
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

**Raw tier** (operates on `Cow<[u8]>` bytes + `&RawDocument` views — no deserialization):
- **ReadRecord**: Fetches raw BSON bytes by ID via `multi_get`. For `Scan` inputs, optimizes into a single-pass iteration. Does **not** deserialize — yields `(id, Cow<[u8]>)` tuples. MemoryStore returns `Cow::Borrowed` (zero-copy from the snapshot), RocksDB returns `Cow::Owned`.
- **Filter**: Constructs a borrowed `&RawDocument` view over the `Cow` bytes (zero allocation) and evaluates predicates by accessing individual fields lazily. Records that fail are never deserialized or cloned. AND/OR short-circuit. Supports dot-notation paths.
- **Sort**: Calls `into_owned()` only on records that survived the filter, then accesses sort keys lazily via `&RawDocument` views. Sorts in memory.
- **Limit**: `skip()` + `take()` on the raw record stream.

**Document tier** (materialization):
- **Projection**: Constructs a borrowed `&RawDocument` view and selectively deserializes only the projected columns via `raw.iter()` + column filtering. For dot-notation paths, trims nested documents to only requested sub-paths. When no Projection node exists, full materialization uses `RawDocument::try_into()` — direct raw iteration that converts each `RawBsonRef` to an owned `Bson` value without serde overhead.

**Index selection**: For AND groups, the planner picks the highest-priority indexed field (ordered by `CollectionConfig.indexes`) with an `Eq` condition. For OR groups, if every branch has at least one indexed `Eq`, the planner builds an `IndexMerge(Or)` tree with a residual `Filter` for recheck; if any branch lacks an indexed condition, the entire OR falls back to `Scan`.

Index keys use the format `i:{field}\x00{value_bytes}\x00{_id}` and are maintained on insert/update/delete for indexed fields.
