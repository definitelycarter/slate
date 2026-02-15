# TODO

## Indexed Sort

Currently, `Sort` materializes the entire filtered set and batch-reads sort columns via `multi_get` before sorting in memory. For `status='active' + sort contacts_count + take(200)`, this means reading ~50k sort column values just to return 200 rows.

If the sort field is indexed, we can walk the index in sorted order instead of materializing. Sort + take(200) becomes an index range scan that stops after 200 qualifying rows — O(k) instead of O(n).

This would turn queries like `sort + take(200)` from ~154ms to something closer to the lazy `take(200)` without sort (~1ms range).

## Count/Limit Pushdown on IndexScan

For queries like `status='active' take(200)` without sort, the IndexScan currently yields all matching record IDs from the index (~50k), then Limit takes 200. The IndexScan could accept an optional limit and stop walking the index after enough matches, avoiding the full index traversal.

This is less impactful than indexed sort (the lazy iterator already short-circuits at ~1ms), but it would reduce unnecessary index I/O.

## ~~Single Key Per Record~~ ✅

Done. Each record is now a single RocksDB key (`d:{partition}:{record_id}` → `RecordValue`). Timestamp dropped, scans 2-3x faster, writes 3.6x faster, scaling is near-linear (10x for 10x data).

## ~~MessagePack for Wire Protocol~~ ✅

Done. TCP wire protocol uses MessagePack (rmp-serde). Storage stays bincode. Performance is equivalent for paginated queries, ~10-16% overhead on large result set serialization — acceptable trade-off for cross-language compatibility.

## No Index on TTL Fields

Reject field configs that have both `indexed: true` and `ttl_seconds: Some(...)` at `save_datasource` time. An expired column would leave stale index entries pointing to logically-gone values — either requiring background cleanup or read-time filtering that defeats the purpose of the index. Fail loud on write rather than dealing with it at query time.
