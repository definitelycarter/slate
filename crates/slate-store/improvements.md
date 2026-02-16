# slate-store / slate-db Performance Improvements

Remaining optimization opportunities, ranked by estimated impact.

## Tier 1: High Impact

### 1. Push filters into scan (avoid full deserialization)

**File:** `crates/slate-db/src/executor.rs` — `execute_scan()`, `materialize_doc()`

Currently every scanned record is fully materialized into `bson::Document` before the `Filter` node checks a single field. If filtering `status = 'active'` and only 50% match, 100% of documents are deserialized just to throw away half.

**Fix:** Evaluate filters directly on `RawDocumentBuf` without full materialization. Only materialize documents that pass the filter. This avoids heap-allocating `bson::Bson` values for records that get discarded.

**Impact:** 20-35% for filtered full scans on wide documents.

### 2. Stream index scans instead of collecting

**File:** `crates/slate-db/src/executor.rs` — `execute_index_scan()`

Index scan collects ALL matching record IDs into a Vec, builds another Vec of keys, multi_gets everything, then deserializes all records. For `find_one` on an indexed field with 50k matches, all 50k are fetched before the `Limit` node takes 1.

**Fix:** Fetch records in batches (e.g. 64 at a time). Pull 64 IDs from the index iterator, multi_get that batch, yield results. If `Limit { take: 1 }` sits above, only 1 batch is ever fetched.

**Complication:** `&mut self` transaction means you can't hold `scan_prefix` iterator alive while calling `multi_get`. Need to collect each batch of IDs first, drop the iterator borrow, multi_get, then re-seek.

**Impact:** 15-25% for index scans with limits or selective filters.

### 3. Copy-on-write snapshots in MemoryStore

**File:** `crates/slate-store/src/memory/transaction.rs` — `Snapshot`, `ensure_cf()`

Every `ensure_cf()` clones the full OrdMap on first CF access. Read-only transactions clone data they never mutate.

**Fix:** Store `Arc<ColumnFamily>` reference for reads. Only clone into a mutable copy on first write (`ensure_cf_mut`). Read-only transactions never clone.

**Impact:** 10-25% of transaction overhead, especially for read-heavy workloads.

## Tier 2: Medium Impact

### 4. Stack-allocated keys in encoding

**File:** `crates/slate-db/src/encoding.rs`

`record_key()`, `index_key()`, `index_scan_prefix()` each allocate a fresh `Vec<u8>`. For 500k records that's millions of small heap allocations.

**Fix:** Use `heapless::Vec<u8, 256>` or accept a reusable `&mut Vec<u8>` buffer.

**Impact:** 5-15%.

### 5. MemoryTransaction put_batch repeated CF lookup

**File:** `crates/slate-store/src/memory/transaction.rs` — `put_batch()`

Calls `ensure_cf_mut(cf)` per entry instead of looking up once and reusing.

**Fix:** Look up CF once, iterate entries against the same reference.

**Impact:** 5-10% for batch operations.

### 6. Reduce document clones in updates

**File:** `crates/slate-db/src/database.rs` — `merge_update()`

Deserializes document, clones indexed field values, clones update values in, re-serializes. 4-5 copies.

**Fix:** Skip clone if update value equals existing. Track indexed values without cloning where possible.

**Impact:** 10-15% for update-heavy workloads.

### 7. Case-insensitive operators allocate strings

**File:** `crates/slate-db/src/exec.rs` — `IContains`, `IStartsWith`, `IEndsWith`

`to_lowercase()` allocates two new Strings per record evaluated.

**Fix:** Byte-wise `eq_ignore_ascii_case` comparison, zero allocation.

**Impact:** 5-10% for case-insensitive query workloads.

## Tier 3: Minor Wins

- **Projection:** Build minimal doc instead of removing fields from full doc
- **RwLock on MemoryStore CFs:** Swap to `DashMap` for lock-free concurrent reads
- **RocksDB scan_prefix:** Compute exclusive end-prefix to avoid redundant `starts_with` check
- **Duplicate encode_value calls:** Cache encoded index values across scan + key construction
