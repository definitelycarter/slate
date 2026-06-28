# RFC: Change Detection (Watch Queries)

> **Status: IMPLEMENTED — change-event core (2026-06-25, merged + pushed `60f5924`,
> commit `8c776bb`).** Originally expanded from the roadmap stub against the real write
> path (file:line citations throughout), then converged through design discussion. **The
> change-event core shipped essentially as designed** — the 2×2 surface, one detection
> core + one registry, snapshot-at-`begin`, set-transition recasting, per-commit
> coalescing, and the non-blocking lag-drop cursor — validated by 28 tests across the
> executor and db layers. Two things changed between this design and the code, corrected
> throughout below:
>
> - **The surface is the v2 builder, not flat `db.watch(...)` methods.** A watch hangs
>   off the `find`/`query` builders as a terminal:
>   `db.collection(c).find(filter).watch(cb)` / `.stream()` (BSON) and
>   `db.collection(c).query(sql).watch(cb)` / `.stream()` (SQL). The BSON-vs-SQL split is
>   *which builder you start from* (`find` vs `query`), so both expose the same
>   `.watch`/`.stream` terminal names — the `_query`-suffix naming in the original draft
>   was dropped.
> - **What is still deferred (layers on top, none built here):** reactive result-sets
>   (the § Reactive queries fold), shaping projections on the filter, an async
>   `Stream` / blocking `Iterator` delivery, and a durable/replayable feed. See
>   § Implementation notes → "Deferred layers."

## Concept

Register a filter against a collection. On every insert, update, or delete, the
written document is evaluated against registered filters. If it matches, the
registration is notified — once per commit — with the batch of matching changes.

The filter comes in **two front-ends** that lower to the *same* compiled predicate,
mirroring slate's `find`/`query` split: a **BSON filter document** (the Mongo `find`
filter form, via `slate_query::translate_filter`) or a **SQL `SELECT` string** (via the
`slate-sql` parser). Each front-end pairs with **two delivery shapes** — a sync
**callback** (push) or a long-lived **subscription cursor** (pull) — yielding a 2×2
public API. You reach a BSON filter through `find(filter)` and a SQL filter through
`query(sql)`; both builders expose the same `.watch`/`.stream` terminals:

```rust
// Callback (push) — low-latency, in-process (e.g. an IoT reaction on the writer thread)
let sensors = db.collection("sensors");      // or db.cf("telemetry").collection("sensors")
let handle = sensors.find(doc! { "temp": { "$gt": 80 } })
    .watch(|events: &[ChangeEvent]| { /* matching changes this commit, in write order */ })?;
let handle = sensors.query("SELECT * FROM s WHERE s.temp > 80")
    .watch(|events: &[ChangeEvent]| { /* … */ })?;
handle.unwatch(); // or just drop it

// Cursor (pull) — decoupled consumer, drains on its own thread (telemetry, sync, FFI)
let stream = sensors.find(doc! { "temp": { "$gt": 80 } }).stream()?;
let stream = sensors.query("SELECT * FROM s WHERE s.temp > 80").stream()?;
while let Some(batch) = stream.try_next() { /* Vec<ChangeEvent> per commit */ }
// or block on a consumer thread: stream.next_blocking(); check stream.lagged().
```

All four are **thin front-ends over one detection core + one registry**: BSON-vs-SQL only
changes how the filter `Expression` is produced; callback-vs-cursor only changes delivery
(invoke the user's closure at emit, or push the batch into the subscription's bounded
channel). The detection, recasting, coalescing, capture seam, and registry are shared.

The **BSON `watch`/`stream` are filter-only** (no projection — the event carries the whole
document). On the SQL `watch_query`/`stream_query`, only the per-row-meaningful clauses of
the SELECT apply — `WHERE` (the filter) and the identity projection. `ORDER BY` / `LIMIT`
/ `GROUP BY` / aggregates are *set* operations (a `GROUP BY` watch is a live aggregate —
incremental view maintenance, out of scope) and are rejected at registration.

A watch is **ephemeral and in-memory** (it holds a live closure) — unlike triggers
and validators, which are persisted in the catalog. It is registered at runtime via
`db.watch()`, not inside a transaction.

## Change events

```rust
pub enum ChangeEvent {
    Insert { doc: RawDocumentBuf },
    Update { old: RawDocumentBuf, new: RawDocumentBuf },
    Delete { doc: RawDocumentBuf },
}
```

Both `old` and `new` are provided on updates so the consumer can diff without
re-querying. See § "Set-transition semantics" for how a watch re-casts an update at
the boundary of its filtered set (a doc *entering* the set surfaces as `Insert`, one
*leaving* as `Delete`).

## Why it fits an embedded DB

In a client-server database, change streams are a networking concern (oplog
tailing, WebSocket push). In an embedded DB, writes happen in-process — the write
path evaluates filters synchronously and dispatches to callbacks with zero
serialization overhead. And because slate is **single-writer** (MemoryStore guards a
`write_lock: Mutex<()>`; redb's `begin_write()` is inherently exclusive), commits
serialize — callbacks fire one commit at a time, so there is no inter-commit
interleaving to coordinate.

## The write-path seam (corrected)

The original stub claimed watches tap the write path "similar to `Node::Trigger` /
`Node::Validate`." That is **only half right**, and the correction drives the rest of
the design.

`Node::Trigger` / `Node::Validate` are streaming taps — iterator adapters that
`.map()` over a source `ValueIter`, firing a side effect per document and passing the
doc through unchanged (`crates/slate-executor/src/nodes/trigger.rs:16-35`,
`nodes/validate.rs:14-30`). The planner wraps a mutation like so
(`crates/slate-planner/src/planner.rs:41-121`, `wrap_before`/`wrap_after`
`:144-183`):

```
Plan::Trigger{action:"updated"}            (AFTER-trigger — carries the NEW doc)
  └─ Plan::Update{assignments,
       source: Node::Trigger{"updating"}    (BEFORE-trigger — carries the OLD/matched doc)
                 └─ Node::Validate
                      └─ <write_source: Scan→Filter→Project(c)> }
```

So there are two generic tap points, and **neither sees old *and* new at once**:

- **Before** the mutation node: the stream carries the **old/matched** rows.
- **After** the mutation (`Plan::Trigger`): the stream carries the **new** docs.

Old *and* new are simultaneously in hand **only inside the mutation node**, and only
for the ops that have a prior document:

| Op | Old in hand | New in hand | Where |
| --- | --- | --- | --- |
| Update | yes (input row) | yes (computed) | `nodes/mutate.rs:28-41` |
| Replace | yes (input row) | yes (built buf) | `nodes/replace.rs:24-54` |
| Upsert | yes (`txn.get`) | yes (built) | `nodes/upsert.rs:56-74` |
| Delete | yes (input row) | — | `nodes/delete.rs:16-31` |
| Insert | — | yes | `nodes/insert.rs:22-41` |

There is also a lower seam: the engine's `put`/`delete` already read the prior record
for the index diff but **don't surface it** — `crates/slate-engine/src/kv/transaction.rs:421`
(`let old_data = self.txn.get(...)` in `put`) and `:488` (in `delete`). That
`old_data` is the *previously committed* state — exactly right for "did this doc
leave the watched set."

**Consequence:** watch capture must live **inside the mutation nodes** (or be fed by
the engine's `old_data`), not at a generic outer `Node` wrapper. New-doc capture
rides the mutation-node output / after-`Plan::Trigger` position; old-doc capture is
the mutation node's input row (or the engine `old_data`).

## Filter compilation & evaluation

Reuse the existing compiled-predicate machinery wholesale, so a watch can never drift
from `find`/SQL semantics. Both front-ends converge on a single `slate_ast::Expression`
before compilation:

- **SQL** (`watch_query`/`stream_query`): the `SELECT` string is parsed by `slate-sql`
  into a query AST; the `WHERE` clause's `Expression` is extracted (plus the optional
  identity projection from the SELECT list). Non-watchable clauses
  (`ORDER BY`/`GROUP BY`/aggregate/`LIMIT`) are rejected at registration.
- **BSON** (`watch`/`stream`): the `find` filter document is translated by
  `slate_query::translate_filter` straight to the same `Expression` (an empty filter →
  literal `true`). No projection or set-ops to reject — the surface is filter-only.
- Either way the `Expression` compiles **once** (at registration time) into a reusable
  `slate_eval::raweval::Compiled` via `raweval::compile(expr, sole_alias)`
  (`crates/slate-eval/src/raweval.rs:733`), evaluated per row by `eval_compiled`
  (`:884`).
- The `Filter` node is the exact template
  (`crates/slate-executor/src/nodes/filter.rs:25-39`): compile once, then per-row
  `eval_compiled(...).as_bool() == Some(true)` with 3-valued logic (undefined/false
  both drop). Walks raw BSON bytes; zero serialization.

## Per-transaction buffering

The buffer holds `(handle_id, op, old?, new?)` per match until commit. **Borrow
caveat the stub missed:** the executor borrows the *engine* transaction
(`&self.txn`, `crates/slate-db/src/database.rs:80`), not the db `Transaction` that
owns per-txn state like `hooks_dirty: Cell<bool>` (`database.rs:331-339`). So the
buffer can't simply live on the db `Transaction` and be seen by the executor nodes.
Two options:

1. **Thread an `Rc<RefCell<WatchBuffer>>` into the `Executor`** the way `params` /
   `rand` are already threaded (`crates/slate-executor/src/lib.rs:60,65`). Preferred —
   keeps the buffer executor-local and `!Send` (matching the executor).
2. Hang the buffer on the engine transaction. More invasive across the engine trait.

Captured `ChangeEvent`s own their `RawDocumentBuf`s, so they outlive the consumed
transaction fine.

## Commit sequence (revised)

Mutations are applied lazily as the cursor drains (`cursor.rs:124-132`), so by the
time the caller invokes `commit()` every `put`/`delete` has run against the open
transaction. `commit(self)` **consumes** the transaction (`database.rs:1078`), and
there is already prior art for a post-commit side effect: the hook
`registry.swap()` runs *after* `self.txn.commit()` succeeds (`database.rs:1091-1093`).
Watch-emit slots into exactly that spot.

1. **During drain** — filter evaluates as docs flow through the mutation nodes;
   buffer matches (capture new inline; capture old from the input row / `old_data`).
2. **`self.txn.commit()`** (`database.rs:1087`).
3. **Emit** — fire callbacks with the assembled per-handle batches, at the
   `registry.swap` position.

On rollback the buffer drops — no phantom events. (The stub's "read new docs by
buffered id before commit" step is dropped: the mutation node already holds the new
doc, so re-reading is redundant.)

## Registry & handle lifecycle

Model on `HookRegistry` (`crates/slate-db/src/hooks.rs:99-126`), which is an
`ArcSwap<HookSnapshot>` read lock-free and **snapshotted at `begin()`**
(`database.rs:245`) so an in-flight transaction sees a frozen view. Copy that:

- `WatchRegistry` = `ArcSwap<WatchSnapshot>` keyed `(cf, collection) -> Vec<Watch>`,
  each `Watch { handle_id, compiled_filter, callback }`. Lock-free on the hot write
  path; snapshot-at-`begin` makes a mid-transaction `unwatch` well-defined (the txn
  keeps the watches it began with — cleaner than the stub's "ignored on flush").
- Differs from hooks: ephemeral, **not** catalog-backed, registered/removed at
  runtime via `db.watch()` (not on a `Transaction`).
- `WatchHandle { registry: Arc<...>, handle_id }` unregisters on `Drop` and via
  explicit `unwatch()`. It holds no collection lock, so `drop_collection`
  (`database.rs:1070`) proceeds independently; a watch on a dropped collection simply
  goes cold (and is cleaned up opportunistically on drop).

## Callback contract

- **Stored type:** `Box<dyn Fn(&[ChangeEvent]) + Send + Sync>`. `Database<S>` is
  already `Send + Sync` (`database.rs:138`); choosing `Send + Sync` now lets the async
  dispatcher (below) reuse the same registry without re-typing. The single-writer
  guarantee means no *concurrent* invocation, so `Fn` (+ interior mutability) suffices —
  `FnMut` would force a `Mutex`.
- **Batch-per-commit:** one invocation per handle per commit, carrying the full
  ordered batch. A commit with no matches fires **zero** times (no empty batches).
- **Panic isolation:** the commit has already succeeded by emit time, so a panicking
  callback must not poison anything — wrap each invocation in
  `catch_unwind(AssertUnwindSafe(..))`, log, and continue. Document: "callbacks
  should be fast and non-panicking; offload heavy work via a channel."

## Threading & the async option

Sync default runs the callback inline on the writer thread (the `Executor` is `!Send`
by design — `Rc` params, `ValueIter` has no `Send` bound — but the callback runs
*after* commit, off the executor, so this is fine). The optional channel dispatcher
is modeled on the TTL sweep (`crates/slate-db/src/runtime/sweep.rs`,
`Arc<KvEngine<S>>`, `S: Send + Sync + 'static`); events are `Send` (`RawDocumentBuf`
is `Send`), so a `watch_async()` variant can push batches to a worker.

## Ordering relative to triggers / validators

A watch fires on **post-trigger, validated, committed** state: the doc reflects all
trigger mutations (triggers can `ctx.put`, run before *and* after the write —
`planner.rs:144-183`), and only fires for writes that actually persisted (validators
can reject — `nodes/validate.rs`). Concretely: new-doc capture at/after the mutation
node, old-doc capture from the engine's pre-overwrite `old_data` (the previously
committed state).

## Set-transition semantics (update = enter / leave / modify)

The filter is evaluated on **both** old and new. The watch therefore knows
`matched_before` and `matched_after` and re-casts per its own set boundary:

- `!before && after` → **`Insert { new }`** (entered the set)
- `before && !after` → **`Delete { old }`** (left the set)
- `before && after`  → **`Update { old, new }`** (modified, still in set)
- `before == after == false` → not delivered to this watch

This makes the reactive-query layer trivial (apply the event verbatim to a result
set). The raw `matched_before` / `matched_after` booleans are also exposed for
consumers that want the unfiltered truth.

## Reactive queries (layer on top — not implemented here)

A live result set is `events + a client-side fold`: seed with an initial `find`, keep
a `HashMap<pk, doc>`, and apply each per-watch event (`Insert` → add, `Delete` →
remove, `Update` → replace). The set-transition recasting above + the doc's pk
(`handle.pk_path()`) make this sufficient with no extra engine work. Ordered /
aggregated live views (full incremental view maintenance) are explicitly deferred.

## Index-aware fast path

If a watch filter pins an Eq predicate on an indexed field, skip evaluation for
writes that don't touch that field. Note that multikey / index fan-out is **already**
handled below this seam — the executor's write nodes operate on whole documents (one
`RawBson::Document` per affected doc); the per-index-entry expansion happens in
`apply_index_changes` (`transaction.rs:314`), beneath where a watch sits. So a watch
naturally sees **one event per document**, never per index entry.

## Decisions — RESOLVED (2026-06-25)

1. **Filter surface → BSON *and* SQL, mirroring `find`/`query`.** Two front-ends lower
   to the *same* compiled predicate, so a watch can never drift from query semantics:
   - **BSON** (`find(filter).watch` / `.stream`) — a Mongo `find` filter document,
     translated by `slate_query::translate_filter` → `WHERE` `Expression` →
     `raweval::compile`. The resulting field paths root at the same synthetic alias
     (`slate_query::ALIAS`) the filter compiles against. **Filter-only** (no projection;
     the event carries the whole document). An empty filter `{}` is match-all.
   - **SQL** (`query(sql).watch` / `.stream`) — a `SELECT` string parsed by `slate-sql`;
     the `WHERE` `Expression` (plus identity projection) is extracted. Set-ops
     (`ORDER BY` / `GROUP BY` / `HAVING` / aggregate / `LIMIT` / `OFFSET` / `DISTINCT`),
     joins, and `@params` are rejected at registration.

   The BSON-vs-SQL split is the builder you start from (`find` vs `query`); both expose
   the same `.watch`/`.stream` terminal names.
2. **Two delivery shapes on one detection core → a 2×2 API.** Each filter front-end
   pairs with both deliveries, all over one registry + one detection core:
   - **Callback (push)** — `find(f).watch` / `query(sql).watch`: a sync
     `Fn(&[ChangeEvent])` fired on the writer thread after commit, for low-latency
     in-process reactions.
   - **Cursor (pull)** — `find(f).stream` / `query(sql).stream`: a long-lived
     `WatchStream` subscription the consumer drains on its own thread (`try_next` /
     `next_blocking`, `lagged`) — decoupled, backpressure-friendly, the FFI-clean shape
     for the Swift/wasm bindings. The cursor is a **subscription**, NOT a
     transaction-scoped query cursor — it outlives any one commit.

   A cursor is implemented as a watch whose callback pushes the per-commit batch into the
   subscription's bounded buffer — so callback-vs-cursor is the *only* place delivery
   diverges; registration, capture, recasting, and coalescing are identical.
3. **Cursor overflow → non-blocking, signal-and-resnapshot.** The cursor is backed by a
   bounded buffer (a `Mutex<VecDeque>` + `Condvar`, dependency-free so it works on the
   wasm target too); if a slow consumer fills it, drop the batch + mark the subscription
   *lagged* rather than block the writer (a stalled consumer must never freeze writes in
   an embedded DB). The consumer observes `stream.lagged()` (which clears on read) and
   re-snapshots from current state — consistent with World-1 semantics (state is the
   truth; events are a convenience). Lossless delivery is the durable-feed/sync story
   (deferred).
4. **Events now; reactive result-sets as a documented fold-on-top.** Ship change events;
   a live result set = events + a client-side `HashMap<pk, doc>` fold, spec'd but
   unbuilt. (For that layer: seed the snapshot and subscribe within one consistent
   boundary to avoid a seed/subscribe race.)
5. **Set-transition recasting.** Per-watch: `Insert` on a doc entering the filtered set,
   `Delete` on leaving, `Update` on staying; raw `matched_before/after` also exposed.
   (Makes a watch a declarative enter/leave rule — the IoT alarm case.)
6. **Capture seam → the mutation nodes** (Update/Replace/Upsert/Delete/Insert), which
   already hold old+new.
7. **Buffer → `Rc<RefCell<WatchBuffer>>` threaded into the `Executor`** (like
   `params`/`rand`).
8. **Coalesce by pk within a commit** (net change, last-write-wins).
9. **Callback type `Fn(&[ChangeEvent]) + Send + Sync`,** batch-per-commit,
   `catch_unwind`-isolated.
10. **Registry → `ArcSwap<WatchSnapshot>` + snapshot-at-`begin`** (hook parity).
11. **Ephemeral, feed-ready.** No durable log in v1, but the emit step assembles clean
    change *records* so "also append to a durable feed CF inside the txn" is additive
    when sync arrives.

## Implementation notes (as shipped)

The design above landed almost verbatim; this section pins the concrete surface and the
few places the code names things differently from the prose.

**Public surface — v2 builder terminals (no flat `db.*` methods).**

- BSON: `Collection::find(filter).watch(cb)` / `.stream()` (`crates/slate-db/src/v2/read.rs`).
  Filter-only — a `find` reshaped with `sort`/`offset`/`limit`/`project` is rejected at
  the terminal by `ensure_filter_only`.
- SQL: `Collection::query(sql).watch(cb)` / `.stream()` (`crates/slate-db/src/v2/query.rs`),
  only on the no-parameter builder (`query(sql).params(..).watch(..)` does not compile — a
  watch binds no `@params`). `WHERE` is the filter; `ORDER BY`/`GROUP BY`/`HAVING`/
  aggregates/`LIMIT`/`OFFSET`/`DISTINCT`/joins/`@params`/shaping projections are rejected
  at registration by `parse_watch_filter`.
- The `Collection` handle carries an `Arc<WatchRegistry>` (cloned from the `Database`), so
  the reactive terminals register with **no transaction**; the data terminals
  (`iter_raw`/`iter`) still take one.

**Detection core — `slate_executor::watch` (`crates/slate-executor/src/watch.rs`).**

- `ChangeEvent { Insert { doc }, Update { old, new }, Delete { doc } }` is defined here and
  re-exported from `slate-db`.
- `WatchSink` is the per-transaction sink: `targets` grouped by `(cf, collection)` plus a
  `RefCell<Vec<CapturedEvent>>` buffer. It is threaded into the `Executor` like
  `params`/`rand` and shared by `Rc` — so the design's `Rc<RefCell<WatchBuffer>>` is, as
  built, `Rc<WatchSink>` with the `RefCell` *inside*. `!Sync`, matching the executor.
- The mutation nodes call `WatchSink::capture(cf, collection, pk_path, old, new)` — the one
  place old+new co-exist. It evaluates each watch's compiled filter against both states,
  recasts on the set boundary, and buffers a `CapturedEvent { handle_id, pk, event }`. A
  watch is an **observer**: a filter that *errors* at eval time is a non-match, never a
  write failure. The only hard error is a missing primary key.
- Filters compile once via `compile_filter(expr, alias)` → `Arc<Compiled>`
  (`slate_eval::raweval`), the same machinery the `Filter` node uses. `udf.*` inside a
  watch filter is out of scope (no UDF bag is threaded through the registry path).

**Registry, delivery, emit — `slate-db` (`crates/slate-db/src/watch.rs`).**

- `WatchRegistry` = `ArcSwap<WatchSnapshot>`, lives on `Database` behind an `Arc`,
  snapshotted at `begin()`. `register`/`unregister` rebuild the snapshot copy-on-write via
  `ArcSwap::rcu`. `WatchSnapshot::build_sink()` produces the per-transaction `Rc<WatchSink>`
  (`None` when no watches exist → zero write-path overhead).
- `register_compiled` is the single registration core all four front-ends funnel through;
  `watch_bson`/`watch_sql` (push) and `stream_bson`/`stream_sql` (pull) differ only in how
  they produce `(alias, filter_expr)` and what the callback does.
- `WatchHandle` unregisters on `Drop` (or explicit `unwatch()`); it holds no collection
  lock, so a `drop_collection` proceeds independently and a watch on a dropped collection
  simply goes cold.
- Cursor delivery: `WatchStream` owns its `WatchHandle` + a bounded `Subscription`
  (`Mutex<VecDeque<Vec<ChangeEvent>>>` + `Condvar`, `DEFAULT_STREAM_CAPACITY = 1024`). The
  pull cursor's "callback" just pushes each commit batch into the buffer. Overflow is
  non-blocking lag-drop: a full queue drops the batch and sets `lagged` (cleared on read).
  API: `try_next()` (non-blocking), `next_blocking()` (blocks until a batch or close),
  `lagged()`. It is **not** a std `Iterator` or a `futures::Stream` — that ergonomic upgrade
  is a deferred layer (below).
- `emit(snapshot, sink)` runs at the post-commit `registry.swap` slot: drains the sink,
  `coalesce`s repeated changes to one net change per document per watch (last-write-wins,
  first-seen order; in-and-out within a commit nets to nothing), and fires each callback
  inside `catch_unwind` (the commit already succeeded, so a panicking callback is isolated).

**Deferred layers** (named here so the next iteration can find them):

- **Reactive result-sets** — the § Reactive queries fold (`events + a HashMap<pk, doc>`),
  auto-maintained live views. Unbuilt.
- **Stream / Iterator ergonomics** — `WatchStream` is a bespoke pull type today; a blocking
  `Iterator` and/or an async `futures::Stream` adapter (the FFI-clean shape) are unbuilt.
- **Shaping projections** on the watch filter — the event carries the whole document today;
  a projection would have to keep the pk for coalescing/folding to work.
- **Durable / replayable feed** — delivery is ephemeral, in-memory, and lossy under lag.
  At-least-once with a resumable cursor position is the sync story (deferred).

## Spike validation (confirmed against code)

- Streaming-tap seam exists and is the right shape (`trigger.rs`/`validate.rs`),
  **but** old+new are only co-available inside the mutation nodes (table above) — the
  one real correction to the stub.
- Post-commit side-effect slot already exists (`registry.swap`, `database.rs:1091`).
- Compiled-predicate reuse is direct (`raweval::{compile,eval_compiled}`,
  `filter.rs` template).
- Registration-table prior art is `HookRegistry` (`hooks.rs`), with snapshot-at-
  `begin` semantics to copy.
- Single-writer confirmed (Memory `write_lock`, redb `begin_write`) → serial
  callbacks.
- Buffer can't naively live on the db `Transaction` (executor borrows the *engine*
  txn) — thread it in like `params`/`rand`.
- Multikey dedup is a **non-issue** at the document seam — stated explicitly above.

## Testing

- Executor-seam unit tests mirroring `lib.rs` write-path tests (`:948`): match /
  no-match per op, old+new capture for update/replace/delete.
- Db-level: register → write → commit → assert callback batch; rollback drops events;
  coalesce-by-pk; enter/leave/modify transitions; `unwatch` / `Drop`; collection-drop
  independence.

## Design considerations (retained)

- **Granularity** — fire on insert, update, delete, or any combination; both old and
  new on updates so the handler can diff.
- **Lifecycle** — a droppable handle that unregisters; watches must not block
  collection drops but should clean up gracefully.
- **Threading** — sync on the writer thread by default; channel-based async as an
  option for handlers that shouldn't block writes.
