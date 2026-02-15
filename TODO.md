# TODO

## Indexed Sort

Currently, `Sort` materializes the entire filtered set and batch-reads sort columns via `multi_get` before sorting in memory. For `status='active' + sort contacts_count + take(200)`, this means reading ~50k sort column values just to return 200 rows.

If the sort field is indexed, we can walk the index in sorted order instead of materializing. Sort + take(200) becomes an index range scan that stops after 200 qualifying rows — O(k) instead of O(n).

This would turn queries like `sort + take(200)` from ~154ms to something closer to the lazy `take(200)` without sort (~1ms range).

## Count/Limit Pushdown on IndexScan

For queries like `status='active' take(200)` without sort, the IndexScan currently yields all matching record IDs from the index (~50k), then Limit takes 200. The IndexScan could accept an optional limit and stop walking the index after enough matches, avoiding the full index traversal.

This is less impactful than indexed sort (the lazy iterator already short-circuits at ~1ms), but it would reduce unnecessary index I/O.
