# RFC: Partial Indexes

> **Status: proposed.** Extracted from the roadmap; the [roadmap](../roadmap.md)
> tracks status at a glance.

## Concept

Index only a subset of documents in a collection, controlled by a filter predicate. Reduces
index size and write amplification for collections where most queries target a known subset.

```rust
txn.create_index("orders", IndexConfig {
    fields: vec!["customer_id".into()],
    filter: Some("status ~= 'cancelled'"),  // Lua expression
})?;
```

## Integration with Lua hooks

The filter predicate is a Lua expression evaluated against each document on insert/update.
If it returns `false`, the document is skipped during index maintenance — no entry is
written. This reuses the Lua runtime from the [user-defined logic](./user-defined-logic.md)
system.

## Use cases

- Index `orders.customer_id` only for non-cancelled orders
- Index `users.email` only for verified users
- Sparse indexes: skip documents missing the indexed field entirely

## Work

- Extend `IndexConfig` with an optional filter expression (stored in index metadata)
- `IndexDiff` evaluates the filter before generating index entries
- `create_index` backfill respects the filter
- Planner: only consider a partial index when the query's filter is a superset of the
  index filter (the query must logically guarantee all matching documents are in the index)
