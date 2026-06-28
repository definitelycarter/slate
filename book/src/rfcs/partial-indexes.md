# RFC: Partial Indexes

> **Status: proposed.** Extracted from the roadmap; the [roadmap](../roadmap.md)
> tracks status at a glance. Updated post-`slate-vm`: the filter predicate is a
> **native** function (the Lua runtime this RFC originally assumed is gone — see
> [Native Functions](./native-functions.md)).

## Concept

Index only a subset of documents in a collection, controlled by a filter predicate.
Reduces index size and write amplification for collections where most queries target a
known subset. The classic special case is a **sparse** index: skip documents that are
missing the indexed field entirely.

```rust
// Register the predicate once (database-scoped bag of code), then bind it to the
// index by name when you create it.
let db = DatabaseBuilder::new(store)
    .with_index_filter("active_only", |ctx: &IndexFilterCtx| {
        Ok(ctx.doc().get_str("status").map(|s| s != "cancelled").unwrap_or(false))
    })
    .build()?;

txn.collection("orders")
    .indexes()
    .create("customer_id", IndexOptions::default().filter("active_only"))
    .execute(&txn)?;
```

## Integration with native functions

The filter is a **native predicate**, the same model as validators and triggers (see the
[Native Functions RFC](./native-functions.md)): a database-scoped **bag** of code
(`name -> Arc<dyn …>`) plus a **durable per-index binding** (the predicate name, stored in
the index metadata). It is `Pure` — it sees only the candidate document, returns a `bool`,
and runs under `catch_unwind`. That is the exact shape of a `Validator` (`Pure`,
candidate-in / verdict-out), so the open design fork is whether to **reuse the `Validator`
trait** directly or introduce a sibling `IndexPredicate` role-crate (the repo's pattern is
one trait crate per role — no shared `slate-hook`). Reusing `Validator` is the smaller
change; a dedicated trait reads more honestly at the call site (`with_index_filter` vs
`with_validator`). Lean toward a thin dedicated `IndexFilterCtx` newtype over the same
`Pure` machinery so the surface can grow without re-shaping the trait.

The predicate is resolved from the bag **once** (at plan/build, like validators), not per
row. A bound-but-unresolved predicate (code missing from the bag) is a **dangling binding**:
reported by `dangling_bindings()` (tagged `IndexFilter`) and fail-safe — any write that
would maintain that index aborts, rather than silently dropping or mis-including entries.

## Use cases

- Index `orders.customer_id` only for non-cancelled orders.
- Index `users.email` only for verified users.
- **Sparse** indexes: skip documents missing the indexed field (`ctx.doc().get(field).is_some()`),
  reducing index size where the field is optional.

## Work

- Extend `IndexOptions` (`crates/slate-db/src/v2/index.rs`) with an optional
  `filter: Option<String>` — the bound predicate name — stored in the index metadata
  (`IndexMeta`).
- Define the predicate surface: a `Pure` native function over the candidate document
  (reuse `Validator` or add an `IndexPredicate` trait + `IndexFilterCtx`), a database-scoped
  bag, and `DatabaseBuilder::with_index_filter(name, f)` to populate it.
- `IndexDiff` evaluates the bound predicate before generating index entries (insert/update);
  when it returns `false`, no entry is written and any stale entry is removed.
- `indexes().create(...)` **backfill** respects the filter — only matching documents are
  indexed during the initial build.
- Planner: only consider a partial index when the query's filter **logically implies** the
  index filter (the query must guarantee all matching documents are present in the index).
  The conservative first cut is exact predicate-name match between a query-level constraint
  and the index filter; broader implication is a follow-up.
- `dangling_bindings()` surfaces unresolved index-filter bindings (tagged `IndexFilter`),
  fail-safe on the write path.

## Non-goals

- **No expression-language filter in v1** (no `WHERE`-string parsed into a predicate). The
  filter is a registered native function, consistent with validators/triggers; a SQL/Mongo
  filter-string front-end that lowers to a predicate is a possible later sugar, not this RFC.
- **No per-row resolution.** Resolve the predicate once; running it is the only per-document
  cost.
