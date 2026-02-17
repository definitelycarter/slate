# TODO

## Indexed Sort

Currently, `Sort` materializes the entire filtered set in memory before sorting. For `status='active' + sort contacts_count + take(200)`, this means reading and sorting ~50k raw records just to return 200 rows.

If the sort field is indexed, we can walk the index in sorted order instead of materializing. Sort + take(200) becomes an index range scan that stops after 200 qualifying rows — O(k) instead of O(n).

This would turn queries like `sort + take(200)` from ~86ms to something closer to `take(200)` without sort (~33ms range).

## Count/Limit Pushdown on IndexScan

For queries like `status='active' take(200)` without sort, the IndexScan currently yields all matching record IDs from the index (~50k), then Limit takes 200. The IndexScan could accept an optional limit and stop walking the index after enough matches, avoiding the full index traversal.

## ~~Single Key Per Record~~ ✅

Done. Each record is now a single RocksDB key (`d:{record_id}` → raw BSON bytes). Each collection is a separate column family, providing isolation without key prefixes.

## ~~MessagePack for Wire Protocol~~ ✅

Done. TCP wire protocol uses MessagePack (rmp-serde). Storage uses raw BSON (`bson::to_vec`). Performance is equivalent for paginated queries — acceptable trade-off for cross-language compatibility.
