# RFC: Compound Indexes

> **Status: proposed.** Extracted from the roadmap; the [roadmap](../roadmap.md)
> tracks status at a glance.

## Problem

Today, each index covers a single field. A query like `{ "status": "active", "created_at": { "$gt": "2024-01-01" } }` uses one index for `status` (Eq scan) and either a full scan or a second index for `created_at`, merged via `IndexMerge(And)`. This works but requires materializing one side into a hash set for the probe — two index scans plus a set intersection.

## Design

A compound index covers multiple fields in a defined order. The key layout extends naturally:

```
i\0{collection}\0{field1}+{field2}\0{value1_bytes}{value2_bytes}{doc_id_lp}
```

The planner recognizes when a compound index satisfies multiple predicates in a single scan. For the query above, a compound index on `["status", "created_at"]` produces a prefix scan on `status = "active"` followed by a range filter on `created_at` — one index walk, no merge.

## Key prefix rules

Compound indexes follow the leftmost prefix rule (same as MongoDB, MySQL):

- Index on `["a", "b", "c"]` can satisfy queries on `{a}`, `{a, b}`, or `{a, b, c}`
- Cannot satisfy `{b}` or `{b, c}` alone — the leading field must be present
- Range predicates on a field terminate prefix usage — fields after the range use in-memory filtering

## Work

- Extend `CollectionConfig.indexes` to accept `Vec<String>` per index (single field is `vec!["field"]`)
- Update key encoding to concatenate multiple value bytes with length prefixes
- Extend `IndexDiff` to compute entries for multi-field keys
- Planner: score compound indexes by how many query predicates they cover
- Backfill: `create_index` with a compound spec re-indexes existing documents

## Interaction with partial indexes

Compound indexes can be combined with Lua-based [partial index](./partial-indexes.md) filters — e.g. a compound index on `["status", "priority"]` that only indexes documents where `is_archived == false`.

## Interaction with unique indexes

A compound index can also be unique, enforcing that no two documents share the same *combination* of values (e.g. unique on `(org_id, email)` — the same email is allowed across different orgs). This composes with the existing single-field `u` keyspace (see [Unique Indexes](./unique-indexes.md)) by concatenating the per-field sortable values into one `u` key, exactly as the `i` key layout above does. The scalar-only and sparse rules carry over per component.
