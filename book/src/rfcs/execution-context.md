# RFC: Execution Context (env bundle & plan-derived scope)

> **Status: proposed.** Extracted from the design discussion around the
> [Native Functions RFC](./native-functions.md). On `main`, each per-query
> capability — SQL `@`-params, the `RAND()` source, the injected `$now` clock, the
> watch-capture sink — is threaded **individually** through the execution stack.
> This RFC bundles them so the *next* scoped capability (a UDF resolver, then
> Lua-UDF context, triggers, validators) is a one-field change rather than a
> re-thread, and sources a query's collection **scope** from the plan (which
> already encodes it) instead of from the call site.
>
> **This lands first — before the native-function work.** The native-UDF spike
> (which threaded a UDF resolver, used as the running example below) is set aside
> as a reference donor, *not* shipped to `main`; so the bundle is built on the
> four capabilities that *are* on `main`, and the native-function capabilities
> (udf, validator, trigger) then land **on** it — one field each — and resolve at
> plan-build. The udf-resolver examples below show the shape udf takes once it
> lands on the bundle.

## Summary

Every per-query execution capability — SQL `@`-params, the `RAND()` source, the
injected `$now` clock, the watch-capture sink, and now the UDF resolver — is
threaded **individually** from `Database` → `Transaction` → `Cursor` → `Executor`
→ each binding-aware node → `RawEnv`. Adding one capability means: a field on
`Transaction`, an argument on `Cursor::new`/`new_with_params`, an argument on
every binding-aware node (`filter`/`project`/`sort`/`aggregate`/`unwind`/
`vector_topk`), a bridge in `nodes::env`, and a `None` added to every node's test
call sites. The UDF slice paid that tax once; triggers, validators, and Lua-UDF
context will each pay it again.

Two moves remove the tax:

1. **Bundle the capabilities** into one value per layer instead of N loose
   fields/arguments — a classic interpreter "environment" / reader-context.
2. **Source the collection scope from the plan.** A query's container is already
   in the plan (`CollectionRef` on the source node; top-level on write plans).
   The UDF resolver is scoped to `(cf, collection)`; today that scope is passed
   beside the plan. It should come *from* the plan, so `cf/collection` never
   crosses the `db → executor` handoff.

## The layers today

The UDF resolver travels this chain (every other per-query capability travels a
parallel one):

```
Database.udf_registry: Arc<UdfRegistry>                  database.rs   (canonical store)
   │  Arc::clone at begin (wrap_txn)
Transaction.udf_registry: Arc<UdfRegistry>               database.rs
   │  txn.udf_resolver(cf, collection) → None-gated, scopes to (cf,collection)
Option<Rc<dyn UdfResolver>>
   │  passed into Cursor::new by each builder
find_cursor / query_cursor / distinct_cursor             v2/{read,query,distinct}.rs
   │
Cursor.udf: Option<Rc<dyn UdfResolver>>                  cursor.rs     → execute(): .with_udf(self.udf)
   │
Executor.udf  →  execute_node passes self.udf.clone()    slate-executor/lib.rs
   │
node execute(..., udf)  →  per row: env::raw_env(bindings, params, rand, udf)   nodes/env.rs
   │
RawEnv.udf_resolver: Option<&dyn UdfResolver>            slate-eval/raweval.rs
   │  Expression::Udf → dispatch_udf → resolver.resolve(name, &args)
ScopedUdfResolver::resolve → registry.get(cf, collection, name)
```

Two facts shape the design:

- **`RawEnv` is already a bundle.** It carries `bindings`, `params`, `rand`, and
  `udf_resolver`. The leaf already works the way this RFC wants the whole stack
  to. The job is to extend that pattern up the stack rather than pass the same
  capabilities as a loose tuple.
- **The `Cursor` holds the *engine* transaction, not the db `Transaction`.**
  `Cursor.txn: &KvTxn` (`<KvEngine<S> as Engine>::Txn`), so by execute time it
  cannot reach `txn.udf_resolver(...)`; that is why the builders pre-extract
  `pool`/`rand`/`watch_sink`/`udf` into separate `Cursor` fields. Having the
  `Cursor` hold `&Transaction` + the query scope instead is the keystone that
  lets `execute()` pull capabilities straight off the txn.

## Goals

- Adding a per-query capability is **one field on a bundle** — no new `Cursor`
  argument, no node-signature change, no per-test `None`.
- The collection **scope comes from the plan**, the single source of truth; the
  `db → executor` handoff carries no `cf/collection`.
- **Crate layering preserved:** the env a lower crate consumes is defined in that
  lower crate; `slate-db` populates it (never the reverse).
- **Zero-cost when unused:** an absent capability stays `None` (the UDF
  empty-registry gate is the precedent).

## Non-goals

- No change to UDF / trigger / validator *semantics*.
- No cross-container queries (Cosmos is one container per query; this RFC relies
  on that).
- Not a perf change. It is a structure change that should be allocation-neutral
  (the bundles are the same `Arc`/`Rc` handles, grouped).

## Design

### Three bundles, by lifetime

The capabilities do not share a lifetime, so they do not share one struct. They
split cleanly into three, one per layer:

| Bundle | Crate | Lifetime | Holds | Built |
|---|---|---|---|---|
| `Services` (a.k.a. `TxnEnv`) | slate-db | per-transaction | **unscoped** capabilities: `pool`, `rand: Arc`, `clock`, `udf_registry: Arc`, `watch_registry`/sink, future `trigger`/`validator` registries | cloned once at `begin` (Arc bumps) |
| `ExecEnv<'txn>` | slate-executor | per-query | the evaluator's inputs: `engine_txn`, `pool`, `params: Rc`, `rand: Rc`, `watch: Rc`, `udf: Rc<dyn UdfResolver>`, `container: CollectionRef` | by the `Cursor`, translating `Services` + per-query info |
| `EvalEnv<'a>` | slate-eval | per-row | borrowed forms: `bindings`, `params`, `rand: &dyn Fn`, `udf: &dyn UdfResolver` (today's `RawEnv`) | by each node, per row, from `ExecEnv` |

### Why three and not one

A single shared env is **unsound**, not merely inelegant:

- `slate-executor` and `slate-eval` sit *below* `slate-db`. If the executor took
  one `slate_db::Env`, the executor would have to depend on `slate-db` to name
  it — dependency inversion. The env the executor consumes must be a
  `slate-executor` type that `slate-db` populates.
- Two pieces of `ExecEnv` are **not known at `begin`**: the **scope** (the UDF
  resolver is per-`(cf, collection)`, unknown until the query names its
  container) and **params** (per-query, plus the injected `$now`). So the txn can
  only hold *unscoped capabilities*; the scoped/parameterized view is per-query.

### The translation step is the per-query setup

`Services → ExecEnv` is a real translation, not a copy — note what changes
membership across the boundary:

- the clock **source** resolves to a concrete `$now` injected into params;
- the UDF **registry** scopes to a **resolver** (or stays a registry + a carried
  `container`, see below);
- `Arc` handles bridge to the executor's single-threaded `Rc`.

That translation is exactly the work the `Cursor` does today across four
`.with_*` calls — collected into one constructor:

```rust
// today, at each cursor build:
Executor::with_pool_and_params(txn.engine_txn(), txn.pool(), params)
    .with_rand(txn.exec_rand())
    .with_watch(txn.watch_sink().cloned())
    .with_udf(txn.udf_resolver(cf, collection))

// proposed:
let env = txn.exec_env(&plan, params);   // one translation point; scope from plan
Executor::with_env(txn.engine_txn(), env).execute(plan)  // plan stays an execute() arg
```

The `.with_*` builders can remain as thin sugar over `ExecEnv` so call sites
(e.g. `analyze`) migrate incrementally.

### Plan-derived scope

The plan already encodes the container:

- **Write plans** carry `collection: CollectionRef { cf, collection }` at the top
  level (`plan.rs`, `Insert`/`Update`/`Delete`/`Replace`/`Upsert`).
- **Read plans** (`Plan::Query(Node)`) carry it in the source node — `Scan` /
  `IndexScan` / `CompoundIndexScan` / `KeyLookup`, each with a `CollectionRef`
  (this is the `default_cf.<collection>` shown in `EXPLAIN`).
- The lowerer **already receives** the collection (`lower(query, collection,
  meta)`), so stamping a uniform top-level `container` on the query plan is
  trivial — no tree walk.

Add `Plan::container() -> &CollectionRef`. Then the **executor** — which already
receives the plan — reads `plan.container()` and scopes the UDF resolver itself.
The resolver seam becomes **unscoped**:

```rust
// slate-value
trait UdfResolver {
    fn resolve(&self, cf: &str, collection: &str, name: &str, args: &[Value]) -> Result<Value, String>;
}
```

and the executor carries the `container` in `ExecEnv`/`EvalEnv`, supplying it at
resolve time. The `db → executor` handoff now passes only an *unscoped*
registry-backed resolver; `cf/collection` never crosses the boundary. (Variant:
keep the pre-scoped seam but build `ScopedUdfResolver` in the `Cursor` from
`plan.container()` — smaller, but leaves scoping in the db layer rather than at
the plan's point of consumption.)

### Preferred: resolve at plan-build, like triggers do

There is a cleaner realization that keeps per-collection isolation **and** sheds
the resolver entirely — and it's not new machinery, it's exactly how
triggers/validators already work. A trigger is per-collection too, yet it costs
the executor *no* scope threading: the **write planner** resolves the
collection's hooks (`txn.triggers(cf, collection)` in `exec.rs`'s
`write_context`) and **bakes them into the plan** as `ResolvedHook` lists
(`Plan::Trigger { hooks }`, `Plan::Upsert { hooks }`). The executor just fires
what's embedded; it never looks up a scope.

UDFs can follow the same model. The planner already knows the container
(`PlanContext.container`), so during lowering it can resolve each
`Expression::Udf { name }` against the scoped registry and embed the resolved
`Arc<dyn Udf>` handle in the node (`Udf { resolved, args }`), exactly as triggers
embed `ResolvedHook`. Then:

- evaluation is just `resolved.call(args)` — **no resolver seam, no scope, no env
  threading for UDFs at all**, even though they stay per-collection;
- an unregistered `udf.foo` is caught at **plan time** (a nicer error than a
  per-row eval failure);
- it unifies UDFs with the trigger/validator resolution model — one story for
  "hooks resolve at plan-build."

The cost is an **expression-resolution pass**: triggers dodge it because they're
plan-level wrappers, while UDF calls nest inside `Project`/`Filter` expressions,
so the planner must walk and rewrite expression trees (or carry resolution in a
parallel resolved-expression form). That pass is the one real piece of new work
this approach adds.

**Recommendation:** prefer plan-build resolution (embed the handle) over the
executor-scopes-from-`plan.container()` variant — it deletes the resolver seam,
moves the not-registered error earlier, and matches how triggers/validators
already resolve. Fall back to the dynamic-resolver-with-`plan.container()`
variant only if the expression-resolution pass proves too invasive to land
alongside the env work.

### Node signatures collapse

With `EvalEnv`, the binding-aware nodes go from

```rust
fn execute(expr, binding, source, params, rand, udf) -> ValueIter
```

to

```rust
fn execute(expr, binding, source, env) -> ValueIter
```

and per-row construction is `env.eval_env(bindings)` → `RawEnv`. Crucially, **no
node ever takes `cf/collection` or a scope** — the scope rides inside the env,
sourced once from `plan.container()`. Test call sites become `EvalEnv::empty()`
instead of `None, None, None`, and adding the next capability touches **zero**
node signatures or node tests.

## Scope granularity

> **Decision (2026-06-27): UDFs stay scoped per-`(cf, collection)`.** We weighed
> global (per-database, like SQLite/PG-ignoring-schemas) and per-cf (≈ PG schema)
> against the current per-collection model. Per-collection is kept **deliberately**
> — the project values the isolation it gives (a UDF registered on one collection
> is invisible to others, matching how `cf`/collection already isolate data,
> indexes, and scripts). This is a chosen design property, not an inherited
> Cosmos-ism, and shouldn't be re-litigated. The consequence is that the scope
> machinery below (`plan.container()`) is load-bearing, not optional.

A query has exactly one container — `FROM c`, joins are intra-document, and
subqueries read the same container's arrays. So:

- **One scope per query/plan** is the correct granularity. Per-node UDF
  name-qualification (stamping each `Udf` node with its container, e.g. rewriting
  `udf.tax` → a fully-qualified form) is **redundant** — every node in a plan
  would get the same `CollectionRef`. Reach for per-node scope only if a query
  could ever span containers (it can't today).
- If qualification is ever needed, carry a **structured `CollectionRef`** on the
  `Udf` node, *not* a dotted string (`udf.cf.collection.tax`): cf/collection
  names can contain dots, so a flat string is ambiguous to parse back, whereas a
  struct field is not, and the registry stays keyed by its `(cf, collection,
  name)` tuple.

## Consistency: live vs snapshot stays the registry's choice

The env holds an `Arc` *handle* to each registry; it never copies or rebuilds on
change. "The registry changed" is handled by the registry's own concurrency
primitive, and the two existing registries deliberately differ:

- **UDF registry** — `RwLock`, read **live**: a UDF registered before a query
  runs is visible (no reason to freeze the function set per-txn).
- **Watch registry** — `ArcSwap`, **snapshot at `begin`**: a transaction sees a
  frozen watch set even if one is registered/dropped mid-transaction.

So there is **no** "drop the old env / build a new one on change" — that model
would only fit if we wanted uniform snapshot semantics across all capabilities,
which we don't. Each registry picks live vs snapshot; the env is agnostic.

## Migration / sequencing

Each step is independently shippable and green; land them **first**, before the
native-function work, so those capabilities land on the bundle instead of
re-threading — step 3 is also what lets UDF resolution resolve at plan-build:

1. **`ExecEnv` in slate-executor.** Introduce the struct; make `.with_*` delegate
   to it (`Executor::with_env(txn, env)` + builders as sugar). Mechanical, no behavior
   change.
2. **`EvalEnv` in slate-eval / executor nodes.** Collapse per-node
   `params/rand/udf` into one env argument; `RawEnv` is built from it. Node tests
   switch to `EvalEnv::empty()`.
3. **`plan.container()` + executor-derived scope.** Stamp the container at
   lowering; change the resolver seam to unscoped; move UDF scoping into the
   executor using `plan.container()`. Removes `cf/collection` from the db→executor
   handoff. (Touches the `raw_matches_owned` differential test in slate-eval.)
4. **`Services`/`TxnEnv` on the `Transaction`** (optional, last). Bundle the
   txn's capability fields; `wrap_txn` clones one value. Highest payoff once the
   txn carries many capabilities (post triggers/validators).

The `Cursor` holding `&Transaction` + scope (rather than the engine txn +
pre-extracted fields) is the enabling change for steps 1 and 3.

## Alternatives considered

- **One shared env type.** Unsound across the crate boundary (executor can't name
  a `slate-db` type) and impossible to fully build at `begin` (scope/params are
  per-query). Rejected.
- **Recreate the env when a registry changes.** Unnecessary — interior
  mutability (RwLock live / ArcSwap snapshot) already handles change behind the
  `Arc` the env holds. Rejected.
- **Per-node UDF name-qualification.** Redundant for single-container queries;
  the same `CollectionRef` on every node. Deferred until/unless queries span
  containers; if adopted, structured ref, not dotted string.
- **Status quo (thread each capability).** Works, but re-threads
  `Transaction` → `Cursor` → every node → tests for *each* new capability;
  doesn't scale to triggers + validators + Lua-UDF context. This RFC exists
  because that cost is about to be paid three more times.

## Open questions

- Does `container` live as a field on `Plan::Query` or on a thin `Plan` wrapper
  struct so all variants expose `container()` uniformly (writes already carry
  one)?
- Resolver-seam change (`resolve` gains `cf, collection`) touches `slate-value`
  and the `slate-eval` owned/raw differential — confirm both evaluators and the
  mock resolver in tests move together.
- `ExecEnv<'txn>` borrows `pool: &VmPool` and holds `Rc` capabilities; confirm
  the single lifetime parameter is enough (it should be — everything else is
  owned/shared).
