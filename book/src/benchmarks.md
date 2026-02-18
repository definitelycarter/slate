# Benchmarks

Snapshot from `slate-bench` — run on Apple Silicon (M-series) in release mode. Each record has 8 fields (6 required, 2 nullable). Two dataset sizes: 10k and 100k records per user, 3 users each.

Storage model: each collection is a RocksDB column family. Records are stored as `d:{_id}` → raw BSON bytes via `bson::to_vec`. Queries use a three-tier plan tree: the **ID tier** (Scan, IndexScan, IndexMerge) produces record IDs without touching document bytes. `ReadRecord` fetches raw BSON bytes by ID but does **not** deserialize them. The **raw tier** (Filter, Sort, Limit) operates on `Cow<[u8]>` bytes — constructing borrowed `&RawDocument` views on demand to access individual fields without allocation. The store trait returns `Cow<'_, [u8]>`: MemoryStore returns `Cow::Borrowed` (zero-copy from its snapshot), RocksDB returns `Cow::Owned`. Records that fail a filter are never cloned or deserialized. Finally, **Projection** is the single materialization point where raw bytes → `bson::Document`, selectively deserializing only the requested columns. For full scans without a filter, `ReadRecord` optimizes the `Scan` input into a single-pass iteration.

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

## Embedded (RocksStore) — 10k Records

Results from a single collection (10,000 records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 1,000 records | ~64ms | ~0.0064ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~11ms | 10,000 | Single key per record |
| status = 'active' (indexed) | ~8ms | ~5,000 | IndexScan → lazy filter |
| product_recommendation1 = 'ProductA' | ~6ms | ~3,300 | Scan + lazy filter, ~67% rejected without deserializing |
| status + rec1 + rec2 (AND) | ~4ms | ~550 | IndexScan + lazy filter, ~89% rejected |
| status='active' + sort + skip/take(50) | ~8ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~8ms | ~5,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~13ms | ~5,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~1ms | 200 | Lazy ID tier — Limit stops after 200 |
| status='active' + sort + take(200) | ~8ms | 200 | IndexScan → Sort → Limit |
| sort contacts_count + take(200) | ~0.14ms | 200 | Indexed sort — walks 200 index entries, no Sort node |
| sort contacts_count (no take) | ~21ms | 10,000 | Full sort — no optimization without Limit |
| sort [contacts_count DESC, name ASC] + take(200) | ~6ms | 200 | Indexed sort — group-aware limit, sub-sort ~1 group |
| sort [status DESC, contacts_count ASC] + take(200) | ~12ms | 200 | Indexed sort — low cardinality, large group |
| sort [name ASC, contacts_count DESC] + take(200) | ~2.3ms | 200 | Not indexed — full Sort, Limit stops early |
| last_contacted_at is null (~30%) | ~5ms | ~3,000 | Scan + lazy filter, ~70% rejected |
| notes is null (~50%) | ~7ms | ~5,000 | Scan + lazy filter |
| last_contacted_at is not null (~70%) | ~10ms | ~7,000 | Scan + lazy filter |
| status='active' AND notes is null | ~6ms | ~2,500 | IndexScan + lazy filter |
| 1,000 point lookups (find_by_id) | ~1.8ms | 1,000 | Direct key access |
| projection (name, status only) | ~8ms | 10,000 | Selective materialization at Projection |

## Embedded (RocksStore) — 100k Records

Results from a single collection (100,000 records). Times are consistent across all 3 users.

### Bulk Insert

| Operation | Time | Per Record |
|-----------|------|------------|
| 10 batches of 10,000 records | ~760ms | ~0.0076ms |

### Queries

| Query | Time | Records Returned | Notes |
|-------|------|------------------|-------|
| Full scan (no filter) | ~113ms | 100,000 | Single key per record |
| status = 'active' (indexed) | ~85ms | ~50,000 | IndexScan → lazy filter |
| product_recommendation1 = 'ProductA' | ~57ms | ~33,000 | Scan + lazy filter, ~67% rejected without deserializing |
| status + rec1 + rec2 (AND) | ~46ms | ~5,500 | IndexScan + lazy filter, ~89% rejected |
| status='active' + sort + skip/take(50) | ~88ms | 50 | IndexScan → Sort → Limit |
| status='active' (no sort) | ~84ms | ~50,000 | IndexScan, no sort |
| status='active' + sort contacts_count | ~138ms | ~50,000 | IndexScan → Sort |
| status='active' + take(200) (no sort) | ~7ms | 200 | Lazy ID tier — Limit stops after 200 |
| status='active' + sort + take(200) | ~87ms | 200 | IndexScan → Sort → Limit |
| sort contacts_count + take(200) | ~0.17ms | 200 | Indexed sort — walks 200 index entries, no Sort node |
| sort contacts_count (no take) | ~239ms | 100,000 | Full sort — no optimization without Limit |
| sort [contacts_count DESC, name ASC] + take(200) | ~72ms | 200 | Indexed sort — group-aware limit, sub-sort ~1k records |
| sort [status DESC, contacts_count ASC] + take(200) | ~130ms | 200 | Indexed sort — low cardinality (~50k/group) |
| sort [name ASC, contacts_count DESC] + take(200) | ~22ms | 200 | Not indexed — full Sort, Limit stops early |
| last_contacted_at is null (~30%) | ~56ms | ~30,000 | Scan + lazy filter, ~70% rejected |
| notes is null (~50%) | ~76ms | ~50,000 | Scan + lazy filter |
| last_contacted_at is not null (~70%) | ~103ms | ~70,000 | Scan + lazy filter |
| status='active' AND notes is null | ~65ms | ~25,000 | IndexScan + lazy filter |
| 1,000 point lookups (find_by_id) | ~2.5ms | 1,000 | Direct key access |
| projection (name, status only) | ~76ms | 100,000 | Selective materialization at Projection |

### Key Observations

- **Zero-copy reads + lazy materialization**: The store trait returns `Cow<'_, [u8]>` — MemoryStore returns borrowed references into its snapshot (zero allocation), RocksDB returns owned bytes. The raw tier constructs `&RawDocument` views on demand to access individual fields without allocation. Records that fail a filter are never cloned or deserialized. Projection is the single materialization point, selectively converting only the requested columns from raw bytes to `bson::Document`. Full materialization uses `RawDocument::try_into()` (direct raw iteration) rather than `bson::from_slice` (serde visitor pattern), avoiding the overhead of extended JSON key matching on every field.
- **Single key per record**: All columns packed into one value. Reads are a single `get()` or scan iteration — no per-column key overhead.
- **Dot-notation field access**: Filters, sorts, and projections support dot-notation paths (e.g. `"address.city"`). Nested path resolution works directly on `&RawDocument` via `get_document()` chaining.
- **Lazy ID tier**: `Scan` yields `(id, bytes)` lazily — data is never discarded and re-fetched. `IndexScan` yields `(id, None)` and `ReadRecord` fetches bytes via `txn.get()`. When Limit is present without Sort, the iterator stops early — `take(200)` on 100k records: ~7ms (RocksDB), ~3ms (MemoryStore).
- **Indexed sort (single field)**: When a query sorts on a single indexed field and has a Limit, the planner eliminates the Sort node entirely and replaces Scan with an ordered IndexScan. The limit is pushed into the IndexScan so it stops after `skip + take` index entries. `sort contacts_count + take(200)` on 100k records: ~0.17ms (RocksDB), ~0.08ms (MemoryStore).
- **Indexed sort (multi-field)**: When `sort[0]` is indexed and a Limit is present, the planner replaces Scan with an ordered IndexScan using `complete_groups: true` and pushes `skip + take` as the limit. IndexScan reads at least that many index entries, then continues until the current value group is complete — ensuring correct sub-sorting by Sort on the reduced record set. High-cardinality first field (`contacts_count`, ~1k/group): ~72ms RocksDB, ~23ms MemoryStore — **74-91% faster** than full Sort (~276ms / ~266ms). Low-cardinality (`status`, ~50k/group): ~130ms / ~80ms — **38-61% faster**.
- **Sort overhead**: When indexed sort doesn't apply, Sort accesses sort keys lazily from raw bytes but must hold all records in memory via `into_owned()`. Only records that survive the filter pay this cost. This is the dominant cost for sorted queries.
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
| Full scan (no filter) | ~260ms | 100,000 |
| status = 'active' (indexed) | ~160ms | ~50,000 |
| status + rec1 + rec2 (AND) | ~56ms | ~5,500 |
| status='active' (no sort) | ~158ms | ~50,000 |
| status='active' + sort contacts_count | ~210ms | ~50,000 |
| status='active' + take(200) (no sort) | ~8ms | 200 |
| status='active' + sort + take(200) | ~89ms | 200 |
| status='active' + sort + skip/take(50) | ~90ms | 50 |
| 1,000 point lookups (find_by_id) | ~40ms | 1,000 |

### Embedded vs TCP Overhead

| Query | Embedded | TCP | Overhead |
|-------|----------|-----|----------|
| Full scan (100k records) | 113ms | 260ms | +130% |
| IndexScan filter (50k records) | 85ms | 160ms | +88% |
| IndexScan + narrow (5.5k records) | 46ms | 56ms | +22% |
| IndexScan + sort + take(200) | 87ms | 89ms | +2% |
| 1,000 point lookups | 2.5ms | 40ms | Network round-trips |

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
| Full scan | 11ms | 113ms | 10.3x |
| IndexScan (status) | 8ms | 85ms | 10.6x |
| AND filter (3 conditions) | 4ms | 46ms | 11.5x |
| Scan + filter (ProductA) | 6ms | 57ms | 9.5x |
| Sort (full, ~50%) | 13ms | 138ms | 10.6x |
| take(200) no sort | 1ms | 7ms | 7.0x |
| sort + take(200) indexed | 0.14ms | 0.17ms | 1.2x |
| sort [high card, name] + take(200) | 6ms | 72ms | 12x |
| sort [low card, count] + take(200) | 12ms | 130ms | 10.8x |
| 1,000 point lookups | 1.8ms | 2.5ms | 1.4x |
| Projection (2 cols) | 8ms | 76ms | 9.5x |

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
| RocksStore | ~760ms | ~0.0076ms |
| MemoryStore | ~212ms | ~0.0021ms |
| **Speedup** | **3.6x** | |

### Queries — 100k Records

| Query | RocksStore | MemoryStore | Speedup |
|-------|-----------|-------------|---------|
| Full scan (no filter) | 113ms | 101ms | 1.1x |
| status = 'active' (indexed) | 85ms | 60ms | 1.4x |
| product_recommendation1 = 'ProductA' | 57ms | 46ms | 1.2x |
| status + rec1 + rec2 (AND) | 46ms | 24ms | 1.9x |
| status='active' + sort + skip/take | 88ms | 68ms | 1.3x |
| status='active' (no sort) | 84ms | 60ms | 1.4x |
| status='active' + sort | 138ms | 120ms | 1.2x |
| status='active' + take(200) no sort | 7ms | 3ms | 2.3x |
| sort contacts_count + take(200) | 0.17ms | 0.08ms | 2.1x |
| sort [contacts_count, name] + take(200) | 72ms | 23ms | 3.1x |
| sort [status, contacts_count] + take(200) | 130ms | 80ms | 1.6x |
| 1,000 point lookups (find_by_id) | 2.5ms | 2.0ms | 1.3x |
| projection (name, status only) | 76ms | 63ms | 1.2x |

### Key Observations

- **Writes are 3.6x faster** — MemoryStore has no WAL, no fsync, no LSM compaction. Lazy CF snapshots and dirty-only commits minimize overhead further. The gap widened with the addition of `contacts_count` as a second index (more index key writes per record).
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

**ID tier** (produces record IDs lazily, no document bytes touched):
- **Scan**: Iterates all data keys, yields `(id, Some(bytes))` — data is kept, not discarded.
- **IndexScan**: Scans index keys, yields `(id, None)`. Supports `value: Some(v)` for Eq lookups, `value: None` for ordered column scans. `direction` controls forward/reverse. Optional `limit` caps index entries read (pushed down from Limit). `complete_groups: true` reads past the limit to finish the last value group — ensures correct sub-sorting for multi-field sort queries.
- **IndexMerge**: Binary combiner with `lhs`/`rhs` children and a `LogicalOp`. `Or` unions ID sets (for OR queries where every branch has an indexed Eq). `And` intersects (supported but not currently emitted).

**Raw tier** (operates on `Cow<[u8]>` bytes + `&RawDocument` views — no deserialization):
- **ReadRecord**: The boundary between ID and raw tiers. For `Scan` inputs, data flows through lazily (bytes already available). For `IndexScan`/`IndexMerge`, collects IDs (releasing the scan borrow), then fetches each record lazily via `txn.get()`. Yields `(id, Cow<[u8]>)` tuples. MemoryStore returns `Cow::Borrowed` (zero-copy from the snapshot), RocksDB returns `Cow::Owned`.
- **Filter**: Constructs a borrowed `&RawDocument` view over the `Cow` bytes (zero allocation) and evaluates predicates by accessing individual fields lazily. Records that fail are never deserialized or cloned. AND/OR short-circuit. Supports dot-notation paths.
- **Sort**: Calls `into_owned()` only on records that survived the filter, then accesses sort keys lazily via `&RawDocument` views. Sorts in memory. Eliminated entirely when the planner detects a single indexed sort field + Limit. For multi-field sort where `sort[0]` is indexed, Sort operates on a reduced record set — IndexScan with `complete_groups` feeds only the records needed for correct ordering.
- **Limit**: Generic `skip()` + `take()` via `apply_limit` on any raw record iterator.

**Document tier** (materialization):
- **Projection**: Constructs a borrowed `&RawDocument` view and selectively deserializes only the projected columns via `raw.iter()` + column filtering. For dot-notation paths, trims nested documents to only requested sub-paths. When no Projection node exists, full materialization uses `RawDocument::try_into()` — direct raw iteration that converts each `RawBsonRef` to an owned `Bson` value without serde overhead.

**Index selection**: For AND groups, the planner picks the highest-priority indexed field (ordered by `CollectionConfig.indexes`) with an `Eq` condition. For OR groups, if every branch has at least one indexed `Eq`, the planner builds an `IndexMerge(Or)` tree with a residual `Filter` for recheck; if any branch lacks an indexed condition, the entire OR falls back to `Scan`.

Index keys use the format `i:{field}\x00{value_bytes}\x00{_id}` and are maintained on insert/update/delete for indexed fields.
