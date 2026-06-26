# RFC: Public API Ergonomics — the `Collection` handle

> **Status: draft — decisions pending review.** Reorganizes the collection-scoped public
> API around a `Collection` handle (off `db`), a **build-lazily / run-at-a-terminal**
> execution model, and a clean `create` / `remove` / `delete` verb set. v2 is built as a
> real, fully self-contained surface and made canonical only after it's vetted
> (**build-then-invert**); how consumers/FFI plug into it is a later, phased decision. The
> ten design forks are
> resolved in [Decisions](#decisions) (open sub-parts flagged).

## Problem

`Transaction` is a ~40-method god-object (`crates/slate-db/src/database.rs`), and new
capabilities land as new methods, so the surface compounds. Exhibit A is the
**index-creation family** — five methods where four are one verb over two axes
(cardinality: single/compound; kind: secondary/unique), with `create_vector_index` a
fifth, differently-shaped sibling (`path` + dims + metric + dtype, not a path list):

```
create_index   create_unique_index   create_compound_index
create_unique_compound_index   create_vector_index
```

The shipped change-detection feature added four more (`watch`/`watch_query`/`stream`/
`stream_query` — these live on `Database`, not `Transaction`; this RFC folds them onto the
`Collection` handle's reactive terminals), and the roadmap (partial / full-text /
multikey-unique indexes) tempts yet more `create_*`. Discoverable, but it doesn't scale.

## The handle: `db.collection(name)`

`db.collection(name)` returns a lightweight **`Collection`** handle — every
collection-scoped operation hangs off it. It is **rooted on `db`**, not on a transaction,
so it works both with and without one (the reactive registrations below need no txn, and
they outlive every txn). The default column family is implicit; an explicit cf comes from a
`cf` sub-scope (Decision 6):

```rust
let orders = db.collection("orders");                 // default cf
let orders = db.cf("tenant_42").collection("orders"); // explicit cf
```

The handle holds a couple of `Arc`s into `db` (not a borrow — the reactive terminals must
hand out `'static` subscriptions, so the handle owns its roots) — cheap, no per-call
catalog reload, **no transaction captured**. `Transaction` shrinks to lifecycle (`commit` /
`rollback`; `begin` is on `Database`). This is the MongoDB/Cosmos container idiom, on-brand with slate's
existing Mongo-style `find`.

For data-heavy flows, `txn.collection(name)` (and `txn.cf(cf).collection(name)`) returns a
txn-bound handle whose data terminals omit the `&txn` argument —
`orders.find(f).iter::<Order>()?` instead of `orders.find(f).iter::<Order>(&txn)?`
(Decision 1). Same builders; only the terminal's `&txn` differs.

## Execution model: build lazily, run at a terminal

A handle method **builds an operation; it does not run it.** A **terminal** runs it, and
that's where the transaction enters. There are three terminal families:

**Reads expose exactly two iteration terminals**, each taking `&txn` and returning a std
[`Iterator`] — so `collect`/`count`/`next`(first)/`map`/… come free from the standard
library rather than as bespoke terminals:

```rust
orders.find(filter).iter_raw(&txn)?                       // Iterator<Item = Result<RawDocumentBuf>>
orders.find(filter).iter::<Order>(&txn)?                   // Iterator<Item = Result<Order>>   (typed)
orders.find(filter).iter::<Order>(&txn)?.next().transpose()?  // find_one
orders.find(filter).iter_raw(&txn)?.count()               // count
orders.query(sql).explain(&txn)?                          // the plan, without running
```

These are the **`find` (Mongo-flavored) surface** — stages reshape the read
(`.distinct(field)`, `.sort`, `.offset`, `.limit`, `.project`) and the two terminals consume
it. `distinct(field)` is a *stage*, not a terminal: it narrows the read to that field's
distinct values and the same `iter_raw`/`iter` terminals apply. It takes only the field; page
and order it by chaining the usual stages (`.offset` / `.limit` / `.sort`), so the old
`DistinctOptions` folds away. `query` carries none of the reshaping stages — `count`/`first`/
`distinct`/`limit`/`sort` aren't builder methods, because SQL expresses them inline
(`COUNT(…)`, `TOP`/`OFFSET`/`LIMIT`, `SELECT DISTINCT`, `ORDER BY`) and the std `Iterator`
covers `count`/`next`. A SQL read just runs and is consumed with the same `iter_raw`/`iter::<T>`
(plus `.explain`/`.analyze`, and the reactive `.watch`/`.stream`).

*Raw vs typed:* `iter_raw` yields the raw form (`RawDocumentBuf` for `find`, `RawBson` values
for `query`/`distinct`); `iter::<T>` deserializes each row into a caller `T` via serde.
Because v2 builds the `Cursor` itself this is **native** — `Cursor` already exposes
`iter::<T>()`/`iter_values::<T>()` — not a layer bolted over a raw-only flat method.
(Decision 9 originally phased typed reads to a later pass; they're now in. Method type-params
can't default, so we keep the explicit split — `iter_raw` for raw, `iter::<T>` for typed —
rather than one `iter` with a defaulted `T`.)

**Writes & commands expose one terminal, `.execute(&txn)`**, returning the operation's
result — a `WriteResult` (a **new** type added with this work; v2's write terminal composes
the mutation plan and returns it directly, where v1's mutations return a `Cursor` you
`.drain()` for the count):

```rust
orders.find(filter).update(spec).execute(&txn)?            // WriteResult
orders.insert_many(docs).execute(&txn)?                    // WriteResult
orders.indexes().create(&["status"], opts).execute(&txn)?  // ()
```

**Reactive terminals take no txn** — they register a DB-lifetime subscription on the
`Database`'s watch registry and return a `'static` handle/stream that outlives every
transaction:

```rust
orders.query(sql).watch(cb)?;     // push
orders.find(filter).stream()?;    // pull cursor
```

Why this shape: rooting on `db` (not `txn`) means the builder carries no transaction
lifetime — it owns its config — so the txn is introduced *only at the data terminal* and
scoped to the result. Reactive registration, which is inherently DB-lifetime, needs no
txn at all. Two consequences worth stating:

- **Builders are inert until a terminal runs** — nothing happens, and no txn is touched,
  until `.execute()` / `.iter()` / etc. The transaction is always explicit at the point of
  effect.
- **Builders are `#[must_use]`** — `orders.find(f).update(spec);` with no terminal is a
  compiler warning ("unused builder; did you forget to run it?"), not a silent no-op.

`.explain(&txn)` (plan only) and `.analyze(&txn)` (plan + run, with live row counts) are
available on write builders too — `find(f).update(spec).explain(&txn)` shows the mutation
plan without running it. This is *why* writes terminate with an explicit `.execute()` rather
than running on the verb: one builder finishes as "run it" (`.execute`), "just plan it"
(`.explain`), or "plan + measure" (`.analyze`). (Decision 8: the terminal is `.execute`.)
Today v1's `explain` / `explain_analyze` are SQL-string-only; v2 builds explain/analyze on
the BSON `find` and write paths too, which is real new lowering work (not just a renamed
call).

## Indexes — the `indexes()` sub-handle

```rust
orders.indexes().create("status", IndexOptions::default()).execute(&txn)?;                 // single
orders.indexes().create(["status", "created_at"], IndexOptions::default()).execute(&txn)?; // compound
orders.indexes().create("email", IndexOptions::unique()).execute(&txn)?;                   // unique
orders.indexes().create("embedding", VectorIndexOptions::float32(1536, VectorMetric::Cosine)).execute(&txn)?; // vector
orders.indexes().remove("status").execute(&txn)?;
let ix = orders.indexes().list(&txn)?;
```

`indexes()` is the schema sub-handle for a collection's indexes — `create` / `remove` /
`list` — mirroring `collections()` for the set of collections. `create(paths, opts)`
**unifies all five** `create_*_index` methods into one: the **path argument** takes a single
path or a list (single vs compound, via `impl Into<IndexPaths>`), and the **options
argument carries the kind** — `IndexOptions { unique }` for the path-shaped
secondary / unique / compound indexes, `VectorIndexOptions { dims, metric, dtype }` for
vector. The options *type* is the discriminant (a sealed `IndexBuild` trait), so there is
no runtime wrong-variant failure — which was the only reason an earlier draft kept vector
separate (Decision 4 now folds it in). The `_index` suffix is gone (the noun is in
`indexes()`), and future kinds (partial / full-text) add their own options type
implementing `IndexBuild` — no new method.

The path-shaped fold is low-risk surfacing: the engine trait **already** exposes
`create_index_with_options(field, &IndexOptions)` and
`create_compound_index_with_options(fields, &IndexOptions)` — the four named `Transaction`
methods are thin wrappers over it. Only vector's options-dispatch is genuinely new:
`VectorIndexSpec` reshapes to `VectorIndexOptions` (its `path` field moves out to the shared
`create` arg, so the path is specified once). Vector is single-path; passing a list with
vector options is a build-time error.

## Reads + reactivity: `find` / `query` builders

`collection.find(bson)` / `collection.query(sql)` are the two builder entries (BSON vs
SQL, lowering to one planner); the **terminal** decides how the result is consumed:

| builder entry | iteration terminals `(&txn)` | `.watch(cb)` | `.stream()` |
|---|---|---|---|
| `find(bson_filter)` | `.iter_raw`/`.iter::<T>` (std `Iterator`) + stages `.distinct`/`.sort`/`.offset`/`.limit`/`.project` | reactive push | reactive pull |
| `query(sql)`        | `.iter_raw`/`.iter::<T>` — `count`/`first`/`distinct`/`limit` live in the SQL (or the std `Iterator`) | reactive push | reactive pull |

This **folds the shipped-flat watch quartet** (`watch`/`watch_query`/`stream`/
`stream_query`) into `find`/`query` × `.watch`/`.stream`, behavior unchanged. The terminal
fixes the one-shot-vs-reactive semantics: projection / sort / limit apply on the cursor
terminals but are rejected on `.watch()`/`.stream()` (the filter-only rule the watch RFC
settled). For BSON, sort/limit/projection are builder methods (`.sort()`/`.limit()`/
`.project()`); for SQL they're in the string. The rejection is a **runtime** error today —
the reactive terminal returns `Err` if the builder carries a sort/limit/projection; making
it compile-time would need separate cursor-vs-reactive builder types, tracked with the
`ReadTxn`/`WriteTxn` split (Decision 3 keeps it runtime for phase 1).

## Writes: mutations are terminals

A write is a filter builder finished with a mutating terminal + `.execute(&txn)`:

```rust
orders.find(filter).update(spec).execute(&txn)?;          // = update_many
orders.find(filter).update(spec).one().execute(&txn)?;    // = update_one
orders.find(filter).delete().execute(&txn)?;              // = delete_many
orders.find(filter).replace(doc).execute(&txn)?;
orders.insert_many(docs).execute(&txn)?;                  // inserts have no filter — direct
orders.upsert_many(docs).execute(&txn)?;                  // bulk upsert — also direct
```

This **dissolves the `_one`/`_many` × update/delete/replace family** into the builder
(`.one()` selects the single-row variant; `replace` has only a single-row form, so it
needs no `.one()`). Inserts have no filter, so they stay direct builders
(`insert_one(doc)` / `insert_many(docs)`), as do the bulk `upsert_many(docs)` /
`merge_many(docs)` — each finished with `.execute(&txn)`. Note read-one is the std
`Iterator`'s `next()` on the read (`…iter::<T>(&txn)?.next()`) while write-one is a
*modifier* (`.one()`) — kept distinct because reads return rows and writes return a count
(Decision 10).

## Scripts — per-kind sub-handles (`triggers()` / `validators()` / `functions()`)

```rust
collection.triggers().create(name, src).execute(&txn)?;
collection.validators().create(name, src).execute(&txn)?;
collection.functions().create(name, src).execute(&txn)?;   // UDFs
collection.triggers().remove(name).execute(&txn)?;
collection.triggers().list(&txn)?;
```

The three script kinds get **their own** sub-handle each, not one `scripts()` grouping — a
deliberate departure from `indexes()` (one handle for one concept). The kinds genuinely
differ, and not just in name: triggers and validators feed the write-time hook snapshot (so
registering or removing one marks it stale), while UDFs are query-time and touch no hooks —
a split that already exists in v1 (`register_udf` skips the `hooks_dirty` flip the other two
make). And they will diverge *more*: triggers in particular will grow timing (pre/post) and
operation (insert/update/delete) options, eventually a runtime/language (`RuntimeKind::{Js,
Wasm}` is reserved). Per-kind handles mean each `create` owns its own signature, so those
options land as builder stages on `triggers().create` alone — `triggers().create(name,
src).timing(Pre).on([Insert]).execute(&txn)` — with validators/functions untouched. The
*structure* future-proofs the divergence rather than a speculative shared options type.

This also dissolves the (kind, name) ambiguity that a single `scripts()` handle had:
because the handle fixes the kind, `triggers().remove(name)` / `triggers().list(&txn)` are
unambiguous with no `remove_trigger`/`remove_validator` split, and a per-kind `list` is more
useful than a mixed one. Verb is `create`/`remove` (consistent with `indexes()` /
`collections()`, the "create/remove for schema" rule), not `register`/`drop`. `remove` is
uniform across kinds, so the three handles share one removal builder; each `create` is its
own type. (This supersedes the original `scripts()` design and resolves Decision 5.)

## Collections — the `collections()` namespace

```rust
let orders = db.collections().create("orders").execute(&txn)?;   // → the new Collection handle
db.collections().create("events").ttl_path("expires_at").execute(&txn)?;
db.collections().remove("orders").execute(&txn)?;
let names = db.collections().list(&txn)?;
```

`create` / `list` / `remove` on the *set* of collections, parallel to the singular
`collection(name)`. The cf sub-scope applies here too — `db.collections()` for the default
cf, `db.cf("tenant_42").collections()` for an explicit one (Decision 6). `db.collections()`
is sugar for `db.cf(DEFAULT_CF).collections()`. `create(name)` is a **builder** (not
`create(config)`) — consistent with every other v2 create, with `.pk_path(..)` / `.ttl_path(..)`
stages defaulting to `_id` / `ttl`, finishing at `.execute(&txn)` which returns the new
`Collection` handle. The cf comes from the scope, so there is no cf argument and no
`CollectionConfig` (the builder *is* the config, and it can't carry a stale cf). `list` /
`remove` are scoped to the cf — `list` returns the names in that column family.

## The pattern split

- **Data ops on the collection; schema management under named sub-handles.** Frequency
  decides: `find` / `insert_many` / `update_*` are frequent → direct builders on the
  handle; index/script management is infrequent → grouped under `indexes()` and the per-kind
  `triggers()` / `validators()` / `functions()` (and the collection set under
  `collections()`).
- **Reads consume (cursor terminals), writes/commands execute (`.execute`), reactive
  registers (no txn).** Every terminal takes `&txn` except the reactive ones. *Shape rule:*
  a read with many consumption modes (`find`/`query`) is a builder whose cursor terminal
  takes `&txn`; a single-outcome read (`list`/`stats`/`schema`) skips the builder and takes
  `&txn` directly; writes/commands always go through `.execute(&txn)` (so they can `.explain`
  instead of running), uniformly, with no "simple command" exception (Decision 2).
- **Verbs: `create` / `remove` for schema, `delete` for documents, no `drop`** — the word
  is too bound to Rust's `Drop` trait / `mem::drop` to read cleanly. `remove` lives only on
  the sub-handles (`indexes().remove`, `triggers()`/`validators()`/`functions().remove`,
  `collections().remove`), never on `collection` directly, so it never collides with document
  `delete`.

## FFI bindings — plugged into v2 later, route TBD

UniFFI (Swift/iOS) and wasm-bindgen (browser) handle borrowed handles, generic lifetimes,
and fluent builder chains poorly — they want owned types and flat methods. How they end up
driving v2 is the Phase 2 consumer-plumbing decision, **not settled here**. Two routes:

- **Bindings call v2 directly** — each binding function composes the v2 builder (a uniffi
  `create_index(cf, coll, field)` becomes
  `db.cf(cf).collection(coll).indexes().create(field, opts)`), after which slate-db's flat
  `Transaction` methods can retire.
- **Flat methods call v2** — keep the flat `Transaction` methods as the binding-facing API
  but reimplement their bodies over v2. Signatures don't move, so the bindings — *and docs,
  benches, and in-tree callers* — need no immediate change; the flat surface becomes a thin
  shim.

Either keeps the real logic in v2, once. Until Phase 2 the bindings keep binding v1
unchanged.

## Migration

No external clients (embedded; the only callers are in-tree — tests, benches, the CLI,
examples, and the bindings). The plan is **build-then-invert**: stand v2 up as a real,
self-contained implementation, vet it, and only *then* make it canonical — so nothing
existing moves until v2 has earned it.

v2 is **not** a forwarding layer over the flat methods, and it doesn't even share v1's
internal glue: it is a **100% self-contained duplicate** with its own raw bodies (serialize →
lower → plan → `Cursor`). The only things both surfaces share are the lower crates
(slate-query / slate-planner / slate-executor, where the heavy work lives) and the single
engine transaction. So during build + vet the slate-db-level glue is genuinely doubled — and
that's deliberate: a fully independent v2 can be vetted in isolation and made canonical later
without owing anything to v1. The drift risk while both exist is bounded by parity tests
(Phase 1) and by keeping the vet short. v1 stays **fully intact and untouched** the whole
time, and the duplication collapses once v2 becomes canonical.

This rules out a `type Transaction = V2Transaction` **alias flip**. An alias only buys
transparent migration when the two types share a surface and differ only underneath (as the
v1→v2 *query-engine* swap did — the `find`/`query` surface was identical, the planner
changed beneath it). Here the surface itself changes shape — `txn.find(cf, coll, f, opts)`
becomes `db.collection(coll).find(f).iter(&txn)` — so the call sites change regardless of
what `Transaction` aliases to.

Phasing:

- **Phase 0 — build v2 standalone, in reviewable slices.** The whole new surface lives under
  one `slate-db/src/v2/` module (the `Collection` handle, the builders, the sub-handles),
  with **real bodies** — fully self-contained, not delegation and not reusing v1's glue. It
  compiles alongside v1, which is untouched.
  Naming it `v2` (not `handle`) signals "the second-version surface"; the flat methods are
  the de-facto v1. Each slice is types + real impl + v2 unit tests. Order by surface weight:
  (A) `Collection` + `find`/`query` read builders + cursor terminals — the 80% surface, do
  it first; (B) write builders + `.execute` / `WriteResult`; (C) `indexes()` (unified
  `create`, the `IndexBuild` trait, `VectorIndexOptions` reshape); (D) the per-kind
  `triggers()`/`validators()`/`functions()` script handles + `collections()` + the
  `db.cf(…)` sub-scope; (E) reactive `.watch` / `.stream` (folds the quartet).
- **Phase 1 — vet v2.** New v2 tests exercise the API directly — this is where "are we
  satisfied" gets answered; benches confirm v2 is at **parity** with v1 (same composition, so
  no regression is expected and the bench proves it). v1 stays intact, so every existing
  consumer, test, and binding keeps working untouched throughout.
- **Phase 2 — plug consumers into v2 (route TBD), then make it canonical** — *gated on being
  satisfied with v2*. How v2 reaches the existing consumers is a **decision for later**, with
  two routes: **(1)** point the uniffi/wasm bindings (and in-tree callers) directly at the v2
  builder API — e.g. a uniffi `create_index(cf, coll, field)` becomes
  `db.cf(cf).collection(coll).indexes().create(field, opts)` — after which the flat methods
  retire; or **(2)** keep the flat `Transaction` methods but reimplement their bodies to call
  v2, which keeps every flat signature, so **docs, benches, and tests that use the flat
  methods need no immediate change** (a strong reason to favor it). Either way the duplication
  collapses (v2 becomes the one real implementation); `#[deprecated]` is available to stage
  any eventual removal of the flat surface. Pick the route when we get there.
- **Phase 3 — docs.** The user-facing API docs (`querying.md`, README examples,
  getting-started) flip to v2 as the *recommended* surface. Per the "update affected docs in
  the same commit" rule, each slice that changes a documented example updates it as it lands.

The risk is isolated to **one** deliberate step (the Phase 2 plug-in); phases 0–1 are
purely additive, so v2 is proven before anything consumers depend on moves.

## Complete surface census

Every current public method and where it lands.

**→ `Collection` handle (off `db.collection(name)`)**

- *Writes (builder + `.execute(&txn)`):* `insert_one`/`insert_many` → `insert_*(docs)`;
  `update_one`/`update_many` → `find(f).update(spec)[.one()]`; `delete_one`/`delete_many`
  → `find(f).delete()[.one()]`; `replace_one` → `find(f).replace(doc)`; `upsert_many` /
  `merge_many` → direct builders `upsert_many(docs)` / `merge_many(docs)` (no filter).
- *Reads (builder + iteration terminals `(&txn)`):* `find` → `find(f).iter_raw`/`.iter::<T>`;
  `find_one` → `.iter…(&txn)?.next().transpose()`; `count` → `.iter_raw(&txn)?.count()`;
  `distinct` → `.distinct(field)` (builder stage taking just the field; page with
  `.offset`/`.limit`, order with `.sort` — the old `DistinctOptions` folds into these);
  `query` / `query_with_params` → `query(sql)[.params(…)]`; `explain` / `explain_analyze` →
  `.explain` / `.analyze`. (`count`/`first`/`distinct` are `find`-side stages or std-`Iterator`
  methods — the SQL builder expresses them inline.)
- *Reactive (no txn):* `watch`/`watch_query`/`stream`/`stream_query` → `find`/`query` ×
  `.watch(cb)`/`.stream()`.
- *Indexes (`indexes()` sub-handle):* all five `create_*_index` collapse into one
  `create(paths, opts)` — the options *type* selects the kind (`IndexOptions` for
  secondary / unique / compound, `VectorIndexOptions` for vector); `drop_index` →
  `remove(field)`; `list_indexes` → `list`.
- *Scripts (per-kind sub-handles):* `register_trigger`/`register_validator`/`register_udf`
  → `triggers()`/`validators()`/`functions()`.`create(name, src)`; `drop_trigger` /
  `drop_validator` / `drop_udf` → the same handle's `remove(name)` (the handle fixes the
  kind, so no ambiguity); + per-kind `list(&txn)`.
- *Metadata:* `collection_stats` → `stats(&txn)`; `collection_schema` → `schema(&txn)`;
  `purge_expired` → `purge(&txn)` (already collection-scoped — `(cf, collection)` today).

**→ `collections()` namespace (off `db`)**

- `create_collection` → `collections().create(name)[.pk_path(..)/.ttl_path(..)]` (builder →
  the new handle), `list_collections` → `collections().list` (scoped to the cf),
  `drop_collection` → `collections().remove(name)`.

**→ Stays on `Transaction` — txn lifecycle**

- `commit`, `rollback`. (`begin` / `begin_with` are on `Database`.) The collection-scoped
  `purge_expired` and the `collection_stats` / db-wide `stats` that hang off `Transaction`
  today move to the handle (`collection.purge` / `collection.stats` / `db.stats`) and
  persist flat for FFI.

**→ Stays on `Database` — db-scoped**

- `begin` / `begin_with`, `backup`, `export`, `import`, `verify`, `repair`,
  `purge_expired`, `list_collections`, `stats`, `collection_stats` (convenience),
  `shutdown`.

**→ `DatabaseBuilder` — unchanged.**

**→ Flat surface — consumer plumbing TBD (Phase 2)** — either the bindings call v2 directly
(flat methods retire) or the flat `Transaction` methods get reimplemented over v2 (signatures
unchanged, so docs / benches / tests don't churn). Decided later.

## Decisions

Resolved here for review; genuinely-open sub-parts are flagged.

1. **Two handle roots.** Ship **both** `db.collection(name)` (Arc-rooted; for reactive and
   one-off ops) and `txn.collection(name)` (txn-bound; data terminals drop `&txn`). One
   shared builder type — only the terminal's `&txn` differs. *Why:* the common multi-op
   path shouldn't repeat `&txn` to pay for the reactive minority's DB-lifetime need. *(Open:
   whether the two roots literally share one builder type or `txn.collection` wraps
   `db.collection`.)*
2. **Uniform `.execute(&txn)` for every write/command.** No "simple command" exception.
   *Why:* "writes/commands execute, reads consume" only reads cleanly exception-free; one
   extra call on an infrequent schema op is worth the consistency.
3. **Runtime read-vs-write txn check for phase 1.** Passing a read txn to a write terminal
   stays a runtime error; a `begin_read()`/`begin_write()` → distinct-types split is a
   fast-follow. *Why:* the split touches `begin()`, every signature, and the FFI mirror —
   too big to bundle, and the safety is additive later.
4. **Vector folds into `create(path, opts)`.** One `create` covers all five index kinds;
   the options *type* selects the kind — `IndexOptions` (secondary / unique / compound) vs
   `VectorIndexOptions` (vector) — via a sealed `IndexBuild` trait. *Why:* static dispatch
   on the options type is the discriminant, so the runtime wrong-variant failure that argued
   against a unified `create(IndexSpec)` *enum* never arises. `VectorIndexSpec`'s `path`
   moves out to the shared `create` arg (it reshapes to `VectorIndexOptions`). Extensible:
   partial / full-text add their own options type, no new method. *(Reverses the earlier
   keep-separate call.)*
5. **Per-kind script sub-handles (`triggers()` / `validators()` / `functions()`),
   `create`/`remove`/`list` on each.** *Why:* the kinds genuinely differ — triggers/validators
   feed the write-hook snapshot, UDFs don't (already true in v1), and triggers will grow
   timing/operation/runtime options — so each kind owning its `create` lets that divergence
   land as builder stages on one type without touching the others. As a bonus the handle fixes
   the kind, so `remove(name)`/`list` need no kind suffix and the `(kind, name)` ambiguity that
   forced `remove_*` disappears. *(Supersedes the original single-`scripts()` design; closes
   the earlier open question about collapsing to a `(kind, …)` enum — per-kind handles are the
   answer.)*
6. **`cf` is a sub-scope, not a positional arg.** `db.collection("orders")` for the default
   cf; `db.cf("tenant_42").collection("orders")` (and `db.cf(…).collections()`) for an
   explicit one. *Why:* `db.collection(None, "orders")` put a usually-defaulted storage-ism
   first on the most-called method; the sub-scope reads better and homes `collections()`
   symmetrically. The default-cf entries are sugar: `db.collection(…)` / `db.collections()`
   call `db.cf(DEFAULT_CF).collection(…)` / `.collections()` internally. cf comes from the
   scope, so `CollectionConfig` drops its `cf` field (the scope supplies it) — no two
   sources of truth.
7. **`collection.purge(&txn)`.** `purge_expired` is already collection-scoped, so it maps
   straight onto the handle. *(Open: whether to add a db-wide "purge every collection"
   sweep — deferred until a real need; it's a trivial loop over `list_collections`.)*
8. **The write/command terminal is `.execute`.** Not `.run`. *Why:* conventional verb for
   running a prepared operation (JDBC/ORM idiom); `.run` reads as closure/process.
9. **Raw and typed reads via `iter_raw` / `iter::<T>` (both shipped).** Originally phased
   (raw first, typed later); typed reads are now in. The two are explicit terminals rather
   than one `iter` with a defaulted `T`, because Rust method type-params can't have defaults.
   *Why native:* v2 builds the `Cursor` itself, which already exposes `iter::<T>()` /
   `iter_values::<T>()`, so the typed path is the cursor's own, not a layer bolted over a
   raw-only flat method. Each terminal returns a std `Iterator`, so `collect`/`count`/`next`
   come from the standard library.
10. **Read-one is `.iter…(&txn)?.next()`, write-one is the `.one()` modifier — kept
    distinct.** *Why:* principled, not accidental: reads return rows (a single one is the
    std `Iterator`'s `next()` on the lazy cursor — no bespoke `.first` terminal needed),
    writes return a count (a single one is a modifier on the op). Forcing `.limit(1)` onto
    writes reads worse. *(Updated: an earlier draft had a dedicated `.first()` read terminal;
    folding reads to `iter_raw`/`iter::<T>` makes `next()` the read-one, but the
    read-terminal-vs-write-modifier asymmetry stands.)*

## Non-goals / constraints

- **Keep "two surfaces, one engine."** The `find`/`query` builder entries preserve
  BSON-and-SQL-lower-to-one-planner; this is an ergonomic reshape, not a semantic one.
- **Embedded, in-process** — no wire- or client-compat concern; the only cost is in-tree
  caller churn.
- **Ergonomics, not engine.** No planner / executor / query-semantics changes — v2 reuses
  the same lower-crate primitives, so it reshapes the *surface*, not the engine. Conveniences
  the flat v1 surface lacks (serde-typed reads — Decision 9; `WriteResult`) are native to
  v2's real bodies, not bolt-ons.

Related: `crates/slate-db/improvements.md` (the `create_*_index` unify note that seeded
this); `book/src/rfcs/watch-queries.md` (the quartet that folds into the terminals).
