# TODO

## Indexed Sort

Currently, `Sort` materializes the entire filtered set and batch-reads sort columns via `multi_get` before sorting in memory. For `status='active' + sort contacts_count + take(200)`, this means reading ~50k sort column values just to return 200 rows.

If the sort field is indexed, we can walk the index in sorted order instead of materializing. Sort + take(200) becomes an index range scan that stops after 200 qualifying rows — O(k) instead of O(n).

This would turn queries like `sort + take(200)` from ~154ms to something closer to the lazy `take(200)` without sort (~1ms range).

## Count/Limit Pushdown on IndexScan

For queries like `status='active' take(200)` without sort, the IndexScan currently yields all matching record IDs from the index (~50k), then Limit takes 200. The IndexScan could accept an optional limit and stop walking the index after enough matches, avoiding the full index traversal.

This is less impactful than indexed sort (the lazy iterator already short-circuits at ~1ms), but it would reduce unnecessary index I/O.

## Single Key Per Record

Currently each record with 8 columns = 8 RocksDB keys. At 100k records that's 800k keys, which causes key bloat (SST index overhead, key comparisons, poor cache locality) and likely explains the ~15x scaling for 10x data.

Pack all columns into a single value per record:

```
d:{record_id} → { col1: {value, expire_at}, col2: {value, expire_at}, ... }
```

- Each column still carries its own `expire_at` for per-column TTL — expired columns are filtered on read
- Drop `timestamp` from storage entirely — no versioning, last write wins, `expire_at` is all we need
- 100k keys instead of 800k for 100k records
- Scans iterate one key per record instead of grouping ~8 keys per record
- `multi_get` and `read_cells_batch` become unnecessary — one `get()` returns the whole record
- Partial updates: read-modify-write (read record, update changed columns, write back)

Trade-off: partial updates require a read-modify-write cycle instead of writing individual column keys. Acceptable for batch-oriented writes.

## MessagePack for Wire Protocol

Replace bincode with MessagePack (rmp-serde) for the TCP wire protocol. Bincode is Rust-specific with no stable spec — if we ever want non-Rust clients it's a dead end. MessagePack is cross-language, self-describing, and only slightly larger.

Keep storage format separate — can evaluate MessagePack for storage too (self-describing format helps with debugging/migration tooling) but the hot path is deserializing during scans, so benchmark first.
