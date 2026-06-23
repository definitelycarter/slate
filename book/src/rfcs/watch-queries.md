# RFC: Change Detection (Watch Queries)

> **Status: proposed.** Extracted from the roadmap; the [roadmap](../roadmap.md)
> tracks status at a glance.

## Concept

Register a filter against a collection. On every insert, update, or delete, the
written document is evaluated against registered filters. If it matches, the
registered handler fires with the change event.

```rust
let handle = db.watch("users", filter!{ "status": "active" }, |events: &[ChangeEvent]| {
    // events contains all matching changes from this commit
})?;
handle.unwatch(); // or just drop it
```

## Change events

```rust
pub enum ChangeEvent {
    Insert { doc: RawDocumentBuf },
    Update { old: RawDocumentBuf, new: RawDocumentBuf },
    Delete { doc: RawDocumentBuf },
}
```

Providing both old and new on updates lets the consumer diff without re-querying.

## Why it fits an embedded DB

In a client-server database, change streams are a networking concern (oplog tailing,
WebSocket push). In an embedded DB, writes happen in-process — the write path can
evaluate filters synchronously and dispatch to callbacks with zero serialization
overhead.

## Architecture

**Registration table** (in-memory): `handle_id → (cf, collection, filter, callback)`,
indexed by `(cf, collection) → Vec<handle_id>` for fast lookup during writes.
Collections are unique per CF, so both are needed to identify a watch target.

**Write path** — as documents flow through the plan tree, the executor checks
registered watches for the current `(cf, collection)`. Matching docs buffer
`(handle_id, op, old_doc?, new_doc?)` on the transaction. For deletes and updates
the old doc is captured here since it won't survive commit. This is similar to how
`Node::Trigger` and `Node::Validate` tap into the write pipeline — filter evaluation
is a side effect of the write, not a separate scan.

**Commit sequence:**

1. **During writes** — filter evaluates as docs flow through the plan tree. Buffer
   `(handle_id, op)` for matches. For deletes/updates, also buffer the old doc.
2. **Before store commit** — read new docs for buffered IDs from the still-open
   transaction. Assemble `ChangeEvent`s grouped by handle.
3. **Store commit** — actual commit happens.
4. **Emit** — fire callbacks with assembled events per handle.

On rollback the buffer is dropped — no phantom events. Everything reads from the
same transaction, no extra transaction needed.

**Unwatch** — remove from the registration table. Any pending entries for that
handle in in-flight transactions are ignored on flush.

## Filter semantics

For inserts and deletes, the filter checks the document directly. For updates, the
filter matches on **old or new** — this catches documents entering and leaving the
watched set (e.g. a user becoming active or stopping being active).

## Design considerations

- **Filter evaluation** — reuse the existing query filter logic from the planner/executor.
  A registered watch is essentially a compiled filter predicate.
- **Granularity** — fire on insert, update, delete, or any combination. Include both old
  and new document for updates so the handler can diff.
- **Lifecycle** — return a handle that can be dropped to unregister. Watches should not
  prevent collection drops but should be cleaned up gracefully.
- **Threading** — handlers run on the writer's thread by default (synchronous). Async
  dispatch (channel-based) as an option for handlers that shouldn't block writes.
- **Index-aware fast path** — if the watch filter matches an indexed field with an Eq
  predicate, skip evaluation for writes that don't touch that field.
