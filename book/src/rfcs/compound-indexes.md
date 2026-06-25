# RFC: Compound Indexes

> **Status: shipped (Phase 1).** Programmatic-only — `Database::create_compound_index`
> and `Database::create_unique_compound_index`. Scalar components only; multikey
> (`[]`) components are rejected at creation. The [roadmap](../roadmap.md) tracks
> status at a glance.

## What shipped

Phase 1 is the scalar-component case of the design below:

- **API.** Compound indexes are created programmatically:
  `create_compound_index(cf, collection, &fields)` and the unique variant
  `create_unique_compound_index(cf, collection, &fields)`. There is **no** SQL
  `CREATE INDEX` grammar — indexes are created through the database API. A
  single-element `fields` is byte-identical to `create_index` (the N=1 case).
- **Identity & key layout.** A compound index is identified by its component
  field names joined with `\x01` (`status\x01created_at`), so the whole key
  machinery — prefix scans, `IndexField`, `drop_index`, purge — treats it as one
  opaque field segment. The key concatenates the per-component sortable value
  bytes, then the length-prefixed doc_id, then one trailing `u32` value-length
  suffix per *variable-width* (string) component in field order. Fixed-width
  components recover their length from their type byte (carried in the metadata),
  so they need no suffix. Because the suffixes sit *after* the doc_id, the
  seek-relevant prefix is just the concatenated value bytes: a leading-field
  equality seek is a plain byte prefix. Numerics use the unified order-preserving
  `f64` key per component, exactly as single-field indexes do.
- **Planner.** `best_compound_scan` picks the compound index whose leftmost
  prefix covers the most predicates, lowering to a `CompoundIndexScan` source
  node bounded by a `CompoundScanRange` (an equality prefix plus an optional
  trailing equality/range on the next component). The engine seeks over the
  leftmost equality prefix as a conservative superset; the executor keeps a
  residual recheck of the leading equalities + the trailing range so the result
  is exact (variable-width leading components can over-read, and components past
  the tail are unconstrained).
- **Compound-unique.** Enforces uniqueness of the *combination* of component
  values across `fields` (e.g. unique on `(org_id, email)` allows the same email
  across different orgs). Composes with the existing `u` keyspace by
  concatenating the per-component sortable values into one `u` key, exactly as
  the `i` key does. Scalar-only and sparse rules carry over per component.

### Remaining

- **Multikey components.** Phase 1 rejects `[]` components at creation; compound
  multikey fan-out is future work (see the [Multikey RFC](./multikey-indexes.md)).
- **SQL `CREATE INDEX` grammar.** Indexes stay programmatic; a SQL surface for
  index creation is not planned for now.

---

## Original design

> The sections below are the original proposal, kept as the historical record of
> *why* the feature looks the way it does. One detail changed in implementation:
> the component-name separator is `\x01` (not the `+` sketched below), chosen so it
> can't collide with a real field-name byte.

### Problem

Today, each index covers a single field. A query like `{ "status": "active", "created_at": { "$gt": "2024-01-01" } }` uses one index for `status` (Eq scan) and either a full scan or a second index for `created_at`, merged via `IndexMerge(And)`. This works but requires materializing one side into a hash set for the probe — two index scans plus a set intersection.

### Design

A compound index covers multiple fields in a defined order. The key layout extends naturally (as implemented, the `+` below is `\x01` and string components carry a trailing `u32` length suffix after the doc_id — see "What shipped"):

```
i\0{collection}\0{field1}+{field2}\0{value1_bytes}{value2_bytes}{doc_id_lp}
```

The planner recognizes when a compound index satisfies multiple predicates in a single scan. For the query above, a compound index on `["status", "created_at"]` produces a prefix scan on `status = "active"` followed by a range filter on `created_at` — one index walk, no merge.

### Key prefix rules

Compound indexes follow the leftmost prefix rule (same as MongoDB, MySQL):

- Index on `["a", "b", "c"]` can satisfy queries on `{a}`, `{a, b}`, or `{a, b, c}`
- Cannot satisfy `{b}` or `{b, c}` alone — the leading field must be present
- Range predicates on a field terminate prefix usage — fields after the range use in-memory filtering

### Work

- Extend `CollectionConfig.indexes` to accept `Vec<String>` per index (single field is `vec!["field"]`)
- Update key encoding to concatenate multiple value bytes with length prefixes
- Extend `IndexDiff` to compute entries for multi-field keys
- Planner: score compound indexes by how many query predicates they cover
- Backfill: `create_index` with a compound spec re-indexes existing documents

### Interaction with partial indexes

Compound indexes can be combined with Lua-based [partial index](./partial-indexes.md) filters — e.g. a compound index on `["status", "priority"]` that only indexes documents where `is_archived == false`.

### Interaction with unique indexes

A compound index can also be unique, enforcing that no two documents share the same *combination* of values (e.g. unique on `(org_id, email)` — the same email is allowed across different orgs). This composes with the existing single-field `u` keyspace (see [Unique Indexes](./unique-indexes.md)) by concatenating the per-field sortable values into one `u` key, exactly as the `i` key layout above does. The scalar-only and sparse rules carry over per component.
