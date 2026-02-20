# slate-store / slate-db Performance Improvements

Remaining optimization opportunities, ranked by estimated impact.

## Tier 1: High Impact

### 1. Stack-allocated keys in encoding

**File:** `crates/slate-db/src/encoding.rs`

`record_key()`, `index_key()`, `index_scan_prefix()` each allocate a fresh `Vec<u8>`. For 500k records that's millions of small heap allocations.

**Fix:** Use `heapless::Vec<u8, 256>` or accept a reusable `&mut Vec<u8>` buffer.

**Impact:** 5-15%.

### 2. Reduce document clones in updates

**File:** `crates/slate-db/src/database.rs` — `merge_update()`

Deserializes document, clones indexed field values, clones update values in, re-serializes. 4-5 copies.

**Fix:** Skip clone if update value equals existing. Track indexed values without cloning where possible.

**Impact:** 10-15% for update-heavy workloads.

### 3. Case-insensitive operators allocate strings

**File:** `crates/slate-db/src/exec.rs` — `IContains`, `IStartsWith`, `IEndsWith`

`to_lowercase()` allocates two new Strings per record evaluated.

**Fix:** Byte-wise `eq_ignore_ascii_case` comparison, zero allocation.

**Impact:** 5-10% for case-insensitive query workloads.

## Tier 2: Minor Wins

- **RwLock on MemoryStore CFs:** Swap to `DashMap` for lock-free concurrent reads
- **RocksDB scan_prefix:** Compute exclusive end-prefix to avoid redundant `starts_with` check
- **Duplicate encode_value calls:** Cache encoded index values across scan + key construction
