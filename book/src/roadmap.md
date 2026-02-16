# Roadmap

## Indexed Sort

Currently, `Sort` materializes the entire filtered set before sorting in memory. For `status='active' + sort contacts_count + take(200)`, this means deserializing ~50k records just to return 200 rows.

If the sort field is indexed, we can walk the index in sorted order instead of materializing. Sort + take(200) becomes an index range scan that stops after 200 qualifying rows — O(k) instead of O(n).

This would turn queries like `sort + take(200)` from ~140ms to something closer to the ~10ms range.

## Count/Limit Pushdown on IndexScan

For queries like `status='active' take(200)` without sort, the IndexScan currently yields all matching record IDs from the index (~50k), then Limit takes 200. The IndexScan could accept an optional limit and stop walking the index after enough matches, avoiding the full index traversal.

This is less impactful than indexed sort, but it would reduce unnecessary index I/O.

## No Index on TTL Fields

Reject field configs that have both `indexed: true` and `ttl_seconds: Some(...)` at `save_datasource` time. An expired column would leave stale index entries pointing to logically-gone values — either requiring background cleanup or read-time filtering that defeats the purpose of the index. Fail loud on write rather than dealing with it at query time.
