# RFC: Unique Indexes

> **Status: implemented.** Extracted from the roadmap; the [roadmap](../roadmap.md)
> tracks status at a glance.

Single-field unique indexes enforce that no two live documents share the same value for a field. Created via `create_unique_index(cf, collection, field)` (or `create_index_with_options` with `IndexOptions { unique: true }`); violations surface as `UniqueViolation { index, value, existing_id }`.

## Design: dual index format

A unique index is still a normal indexed path — it keeps its value-first `i` entry, so every existing read path (index scans, range scans, covered projections) works unchanged. It additionally writes a second, point-lookup entry under a distinct `u` tag:

```
i (scan):     i\0{collection}\0{field}\0{value_bytes}{doc_id_lp}   value: [type][ttl?]
u (enforce):  u\0{collection}\0{field}\0{type_byte}{value_bytes}   value: {doc_id_lp}
```

The `u` key omits the doc_id (a unique value has at most one owner) and folds the BSON type byte into the key so distinct types never alias onto a single slot. The owning `_id` lives in the entry value, so the same `u` entry doubles as a point-lookup that returns the document id directly (see the planner follow-up).

## Enforcement

Two cooperating mechanisms:

- **In-snapshot check** — before writing a `u` entry, `apply_index_changes` point-reads the `u` key. A slot owned by a different document is a `UniqueViolation`; a slot already owned by the same document is idempotent. This catches duplicates already visible to the transaction (committed, or written earlier in the same transaction — e.g. an `insert_many` with internal duplicates).
- **Store conflict detection** — two *concurrent* transactions inserting the same value write the same `u` key. On RocksDB (`OptimisticTransactionDB`) this collides as a write-write conflict at commit and one fails. MemoryStore and redb serialize writers (single global write lock / single-writer), so the second writer simply observes the first's committed slot. All three backends are sound — by conflict detection or by serialization.

## Why scalar-only is sound

Uniqueness is defined here for single scalar values. A non-multikey path resolves to **at most one** value per document (`extract_all` on a path without `[]` yields zero or one scalar), so the value → `_id` mapping is one-to-one and the doc_id-less `u` key is unambiguous. Sparse falls out for free: an absent or non-scalar field produces no `u` entry, so any number of documents may omit a unique field. Multikey (`[]`) paths are rejected at creation — array fan-out would imply cross-element uniqueness (no two documents may share *any* element), a semantic deferred to the multikey follow-up.

## Slot-stealing safety (why blind `u` deletes are correct)

Invariant: **`u(x)` exists ⟺ exactly one document holds value `x`, and only that document's own mutation/delete/purge ever removes `u(x)`.**

It holds because every `u` *write* is enforced (a put never overwrites an occupied slot — even an expired one) and every blind `u` *delete* is keyed by the deleting document's own current value (computed from its freshly-read record). No other document can interpose on a slot, so a blind delete only ever removes a slot the acting document owns. The earlier slot-stealing hazard — an expired document's purge deleting a slot another document had stolen — cannot arise, because "block even when expired" makes the steal itself impossible.

## Expired slots (deferred refinement)

A unique value owned by a document that has expired via TTL but not yet been purged continues to block new inserts of that value. This is conservative — never a false accept — and the slot is reclaimed by purge. The alternative (treat an expired slot as free) was deferred: it reintroduces the slot-stealing problem above and requires ownership-checked deletes. "Block until purge" was chosen as the simpler, sound first cut.

## Follow-ups

- **Planner point-get** — equality on a unique field has at most one match. The planner can read the `u` key directly (one point-get, doc_id straight from the entry value) instead of a prefix scan over the `i` keyspace, then a single `ReadRecord`. Purely additive; the `i` scan path stays the fallback. An expired owner's `u` entry resolves to a doc_id whose record `ReadRecord` filters out, so the lookup correctly returns no row.
- **Compound unique** — uniqueness over a *combination* of paths (e.g. `(org_id, email)`); see [Compound Indexes](./compound-indexes.md).
- **Multikey unique** — uniqueness across array elements; see [Multikey (Array) Indexes](./multikey-indexes.md).
