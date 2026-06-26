# RFC: Native Functions (first-class Rust hooks)

> **Status: proposed.** Sibling of the [User-Defined Logic RFC](./user-defined-logic.md),
> which covers the script (Lua/JS/Wasm) side. This RFC adds native Rust functions as a
> first-class hook backend and, in doing so, unifies *how every hook is instantiated and
> fired* — native and scripted alike.
>
> **Depends on the [db-api cleanup RFC](./db-api-cleanup.md)** (in progress, `db-api-v2-phase0`
> worktree). The API surface below is built on its conventions — per-kind `triggers()` /
> `validators()` / `functions()` handles, `create`/`remove` for durable schema, and the
> "reactive registers — no txn" category — and the `register`/`unregister` pair this RFC adds
> needs to be ratified inside that RFC.

## Summary

Slate is an embedded database: the application author writes the triggers, validators, and
UDFs, and they run in the same process. There is no untrusted-tenant boundary. So we should
let those hooks be **native Rust functions** — registered as closures, run at native speed
with no VM, no BSON marshalling, and full type safety — not only Lua/JS source.

The key observation is that this is not a *second* hook system bolted onto the scripted one.
A Lua trigger *is already* a native Rust function whose body happens to drive a VM. Once we
name that fact, native and scripted hooks collapse onto a single firing primitive, and the
scripting backends become *constructors* that produce that primitive.

## Two layers, kept distinct

The single most important framing in this RFC — everything else follows from it:

| Layer | What it manages | Transactional? | Who calls it |
|---|---|---|---|
| **Runtime management** | the live, in-process set of `Hook` instances | no (process-lifetime) | the consumer **and** the engine |
| **Script CRUD** | durable script *source* in the catalog | yes (`.execute(&txn)`) | the consumer |

- **Runtime management** is `triggers().register(name, hook)` / `.unregister(name)` (and the
  `validators()` / `functions()` equivalents). It puts a live `Hook` into, or takes it out of,
  the runtime registry. It is *not* persisted and takes no transaction. The consumer calls it
  for native handlers; the engine calls the *same primitive* internally to instantiate
  script-backed hooks from stored source. **Per-kind, not a `register_trigger`/`register_validator`
  suffix split** — the handle already fixes the kind, and runtime instantiation takes
  *kind-specific options* (a trigger's pre/post + operation; a validator/UDF none), so each kind
  owns its own `register` builder, mirroring v2's per-kind `create` in `v2/scripts.rs`.
- **Script CRUD** is the durable door: it stores and removes script source in the catalog.
  **v2 already exposes this as `triggers().create(name, src)` / `.remove(name)`**
  (`crates/slate-db/src/v2/scripts.rs`), lowering to the engine's `create_function` /
  `drop_function`; v1's `Transaction::register_trigger` / `drop_trigger`
  (`crates/slate-db/src/database.rs:1346`, `:1406`) are the pre-v2 spelling. Because v2 took the
  `create`/`remove` verbs, the `register`/`unregister` names are already free for runtime management.

The bridge between the layers is the elegant part:

```
CRUD: triggers().create(name, src)  --(engine, on commit / at open)-->  <runtime>.register(name, lua_handler(src))
CRUD: triggers().remove(name)       --(engine)--------------------->    <runtime>.unregister(name) + pool.invalidate(name)
native: (no CRUD record)            --(consumer, at open)---------->     triggers().register(name, |ctx| { ... })
```

Same runtime primitive, two callers. The durable layer *feeds* the runtime layer through the
public API the consumer also uses.

## The unified `Hook`

Today the seam is VM-shaped: `ScriptRuntime::load(source) -> ScriptHandle`, then
`ScriptHandle::call(input, caps)` (`crates/slate-vm/src/lib.rs:88`, `:101`). A native closure
has no source to compile and shouldn't pay the marshalling cost, so the unification lives one
layer *above* that seam — the thing the engine actually fires:

```rust
/// The unit the firing loop runs. Native and scripted hooks are both `Hook`s.
trait Hook: Send + Sync {
    fn fire(&self, ctx: &mut HookContext<'_>) -> Result<HookOutcome, HookError>;
}
```

- **Native** — the consumer's closure, wrapped: `fire` runs it directly on typed values.
- **Scripted** — produced by a backend constructor; `fire` is the native fn that drives the VM:

```rust
// built-in constructors; each returns a `Hook`
fn lua_handler(pool: Arc<VmPool>, name: String, source: Vec<u8>) -> impl Hook;
fn wasm_handler(/* … */) -> impl Hook;   // RuntimeKind::Wasm, reserved
```

`lua_handler`'s body is exactly today's `run_validators` inner step:
`pool.get_or_load(runtime, name, source_hash, source)?.call(&input, &caps)?`
(`crates/slate-executor/src/nodes/validate.rs:47`). Crucially the closure captures the
**pool**, not a pre-compiled handle, so compilation stays **lazy and LRU-cached** exactly as
today — registration is cheap, the warm VM is built on first fire and evicted by the existing
`VmPool` (`crates/slate-vm/src/pool/mod.rs`). Native handlers bypass the pool entirely.

### `HookContext` — the canonical API the VMs mirror

The closure receives a context, **never the transaction** — this matches the decision already
baked into the scripted path. `ScriptCapabilities` + `ScopedMethod`
(`crates/slate-vm/src/lib.rs:61`–`77`) already hand scripts *borrowed scoped callbacks* over
the txn rather than the txn itself; the doc comment literally notes this "enables capture of
borrowed transaction references." So:

- `HookContext` is the **typed, canonical** version of that context — `ctx.doc()`,
  `ctx.query(..)`, `ctx.set(..)`, `ctx.reject(reason)` — backed by the same borrowed-callback
  mechanism. The native handler uses it directly.
- Each VM gets a **projection** of `HookContext` (the Lua table of scoped methods). The native
  API is the reference; the bindings mirror it.

Two properties carry straight over from the scripted path:

- **Capability tiers.** `Pure` / `ReadOnly` / `ReadWrite` (`lib.rs:70`) become *which methods
  exist on `HookContext`* for a given hook kind — a native validator fires `Pure`
  (`validate.rs:42`) and structurally cannot write.
- **Panic boundary.** A native closure is user code in-process; `fire` must wrap it in
  `catch_unwind` so a panicking handler **aborts the transaction** instead of poisoning the
  engine — honoring the repo's no-panic rule at the seam even though the body is the user's.

## The lifecycle matrix

Crossing the hook backend with the lifecycle makes the whole design legible — and exposes the
one structural asymmetry:

| | durable (CRUD → catalog) | ephemeral (runtime-only) |
|---|---|---|
| **lua / wasm** | engine reconstitutes via internal `register(lua_handler(src))` — survives restart (Cosmos-parity case) | consumer calls `triggers().register(name, lua_handler(src))` for an in-process script it doesn't persist |
| **native** | ✗ **impossible** — a closure can't be serialized into the catalog | the native case: consumer registers a closure at open |

- The **empty cell** (native + durable) *is* the reason native and scripted hooks differ at
  all. It's a structural fact, not a caveat: there is no source to store, so a native hook only
  ever exists in the runtime registry and must be re-registered each process start. The catalog
  may keep a thin *descriptor* ("a native validator named `x` should exist on `c`") purely for
  reconciliation — see Open Questions.
- The **lua/wasm ephemeral** cell falls out for free the moment runtime management is a public
  primitive: an app can register an in-process script without persisting it, with nothing extra
  built.

## API surface — on the db-api cleanup conventions

Both layers live on the per-kind sub-handles `triggers()` / `validators()` / `functions()`.
The durable side reuses the verbs that RFC already defines; the runtime side joins its
**reactive (no-txn)** category:

```rust
// ── Script CRUD — durable schema (builder → .execute(&txn)) ───────────────
collection.triggers().create("audit", src).execute(&txn)?;   // persists source
collection.triggers().remove("audit").execute(&txn)?;
collection.triggers().list(&txn)?;

// ── Runtime management — reactive category, no txn, process-lifetime ───────
collection.triggers().register("audit", |ctx| { ctx.set("audited_at", now()); Ok(Continue) });
collection.triggers().unregister("audit");
collection.triggers().register("tmp", lua_handler(src));      // in-process script, same primitive
```

How each piece lands in the db-api categories:

- **`create` / `remove` are the durable script ops** — already in the db-api RFC verbatim
  (per-kind handle, builder ending in `.execute(&txn)`, `create`/`remove` *not* `register`/`drop`).
  Language/runtime is a *future builder stage* on `triggers().create`, not a `Script::lua(..)`
  wrapper — matching how that RFC already plans `triggers().create(name, src).timing(Pre).on([Insert])`
  (`RuntimeKind::{Js,Wasm}` reserved).
- **`register` / `unregister` ARE the reactive category**, not a stray schema verb. The db-api
  RFC names that category in its own words — "reactive registers — no txn" — and `register` is
  its verb, sibling to `watch` / `stream`. The `create`/`remove` rule governs *durable schema*;
  native registration is the no-txn runtime category, so there's no conflict. Both take no
  `&txn` and are effective for transactions that *begin after* them.
- **Per-kind options, shared across a kind's `create` and `register`.** Options are
  *kind-specific* — a trigger's pre/post timing and insert/update/delete operation
  (`.timing(Pre).on([Insert])`), which validators and functions don't have. Within a kind, the
  durable `create` and the native `register` configure the *same* conceptual hook, so they share
  that kind's option vocabulary and differ only at the **terminal** (durable `create` →
  `.execute(&txn)` persisting source; native `register` → installs the live closure, no txn) and
  the **body** (source string vs Rust closure). This is exactly why each kind owns its handle and
  builders — the reason `v2/scripts.rs` splits `CreateTrigger`/`CreateValidator`/`CreateFunction`.
- **Unregister by name, not a drop-guard.** `triggers().unregister(name)` rather than returning
  an RAII handle the caller must hold — symmetric with durable `remove(name)` and the existing
  `pool.invalidate(name)`. (The repo uses the drop-guard style where it fits — `WatchHandle`
  unregisters on `Drop` because watches are *anonymous and many*,
  `crates/slate-db/src/database.rs:199`. Triggers are *named and few*, so by-name wins.) A
  `must_use` scope guard could be an optional convenience later; by-name is the contract.

This pair (`register`/`unregister` next to `create`/`remove` on each per-kind handle) is the
one thing this RFC adds to the db-api surface, so it must be **ratified inside the db-api
cleanup RFC** rather than diverging from it.

## Implementation sketch

- **One runtime registry on `Database`, reached like the watch registry.** Generalize today's
  `HookRegistry` (`ArcSwap<HookSnapshot>`, `crates/slate-db/src/hooks.rs:106`) to hold
  `Arc<dyn Hook>` keyed by `(cf, collection, kind, name)` rather than `ResolvedHook` source.
  `register` / `unregister` swap it; transactions snapshot it at `begin()` so a txn sees a stable
  hook set under concurrent modification — preserving the snapshot-at-begin guarantee.
- **The per-kind handle must carry the registry, exactly like `Collection` carries `watch`.**
  v2 already routes the no-txn reactive category this way: `Collection` holds
  `watch: Arc<WatchRegistry>` and `find(f).watch`/`.stream` register on it with no txn
  (`crates/slate-db/src/v2/collection.rs:39`). Native `register`/`unregister` are the same
  category, so `Triggers`/`Validators`/`Functions` — today txn-only `{cf, collection}`
  (`v2/scripts.rs:98`) — must additionally carry an `Arc` of the runtime hook registry. That
  added field is the one concrete handle-shape change the native side introduces.
- **Reconciliation must merge, not replace.** Script hooks are catalog-derived (rebuilt from
  `HookSnapshot::load_all`, `hooks.rs:30`); native hooks are not. When the engine rebuilds the
  script portion after a CRUD change, it must *not* wipe consumer-registered native hooks, so
  the registry tracks provenance (`Script` vs `Native`). CRUD-create/remove drive the script
  entries via internal `register` / `unregister`; the consumer drives the native entries.
- **Firing loop refactor.** `run_validators` (`validate.rs:37`) and `fire_hooks`
  (`crates/slate-executor/src/nodes/trigger.rs`, reused by `upsert.rs`) stop calling
  `pool.get_or_load` + `handle.call` directly and instead iterate the resolved `&[Arc<dyn Hook>]`,
  calling `hook.fire(ctx)`. The VM call becomes one `Hook` arm; native becomes the other. This
  is the only change to the hot path.
- **`ResolvedHook` stays as the durable descriptor.** Resolving a `ResolvedHook` to a `Hook` is
  where the script-vs-native branch lives; for scripts it builds `lua_handler(pool, name, source)`,
  for native it looks the name up in the runtime registry.

## Decisions

1. **`create`/`remove` = durable script CRUD; `register`/`unregister` = runtime (native) mgmt,
   the reactive no-txn category.** Both pairs live on the per-kind `triggers()`/`validators()`/
   `functions()` handles. **v2 already implements the durable side as `create`/`remove`**
   (`v2/scripts.rs`), so `register`/`unregister` are already free for the runtime side, where they
   slot into v2's already-wired reactive category (`Collection.watch`) rather than inventing a
   verb. Per-kind because both the durable and runtime ops take kind-specific options.
2. **One firing primitive (`Hook`); scripting backends are constructors.** No parallel native
   pipeline — native and scripted hooks are the same `Hook` behind the same firing loop.
3. **`HookContext`, not `&Transaction`, to the closure.** The native context is the typed
   canonical form of the existing `ScopedMethod`/`ScriptCapabilities` contract; VMs project from it.
4. **Capability tiers and the panic boundary apply to native hooks.** Validators fire `Pure`;
   `fire` wraps native bodies in `catch_unwind` and turns a panic into a transaction abort.
5. **Unregister by name, no required handle.** Symmetric with durable `remove` and
   `pool.invalidate`; RAII guard is an optional future convenience, not the contract.

## Open questions

- **Missing-native-handler policy at open.** A collection's catalog descriptor says a native
  validator `x` should exist, but the app didn't `register` it this run. **Recommended:
  fail-closed for validators** (a silently-absent validator lets bad writes through — a
  data-integrity hole), warn-or-skip for post-triggers. Needs to be a per-kind policy, and
  hinges on whether we store native descriptors at all (next point).
- **Do we persist native *descriptors*?** Storing "native hook `x` exists" enables the
  reconciliation check above and `list()` honesty, at the cost of a catalog write for a thing
  that has no body. Alternative: runtime-registry-only, where native hooks are invisible to the
  catalog and `list()` reports them from the live registry. Leaning registry-only for Phase 1.
- **Concurrent VM reuse (scripted side, pre-existing).** A cached `LuaScriptHandle` owns one
  `Lua` yet is `Send + Sync` (`crates/slate-vm/src/lua/runtime.rs:85`); two transactions firing
  the same script share one VM. Orthogonal to native (closures have no shared VM state) but
  worth resolving for the script path — serialize, VM-per-call, or VM-per-thread pool.

## Phasing

1. **Phase 1 — native triggers + validators.** `Hook` trait, `HookContext`, runtime registry
   generalization, firing-loop refactor, `register_*`/`unregister_*` on the per-kind handles,
   capability tiers + panic boundary. Scripted hooks rebuilt on the new `Hook` path (build-then-
   invert: real bodies, not a delegation shim). Registry-only native, no descriptors.
2. **Phase 2 — native UDFs.** Native functions callable from the query surface, mirroring the
   scripted UDF path.
3. **Phase 3 — reconciliation + descriptors.** Optional catalog descriptors for native hooks and
   the fail-closed-on-open policy.

## Non-goals

- Sandboxing native hooks. They run in-process with host privileges *by design* — that is the
  embedded-DB argument for the feature. Untrusted logic still goes through a VM.
- Hot-reloading native hooks without a recompile. That is the durable-script story; native hooks
  change when the host binary changes.

## References

- Scripting seam: `crates/slate-vm/src/lib.rs` (`ScriptRuntime`, `ScriptHandle`, `ScopedMethod`,
  `ScriptCapabilities`, `ResolvedHook`).
- Compile cache: `crates/slate-vm/src/pool/mod.rs` (`VmPool::get_or_load` / `invalidate`).
- Hook snapshot/registry: `crates/slate-db/src/hooks.rs` (`HookSnapshot`, `HookRegistry`).
- Firing sites: `crates/slate-executor/src/nodes/validate.rs`, `.../nodes/trigger.rs`.
- Durable CRUD (the standard): v2 per-kind handles in `crates/slate-db/src/v2/scripts.rs`
  (`triggers()`/`validators()`/`functions()` → `create`/`remove`/`list`, each kind its own
  `Create*` builder). Pre-v2 spelling: `crates/slate-db/src/database.rs:1344`–`1426`.
- No-txn reactive routing precedent: `crates/slate-db/src/v2/collection.rs:39` — `Collection`
  carries `watch: Arc<WatchRegistry>`; `.watch`/`.stream` register with no txn. The native
  `register`/`unregister` runtime registry is its sibling.
- API shape: [db-api cleanup RFC](./db-api-cleanup.md) — per-kind `triggers()`/`validators()`/
  `functions()` handles, `create`/`remove` verb rule, and the "reactive registers — no txn"
  category. **The copy on `main` is stale** (still the single `scripts()` design); the
  `db-api-v2-phase0` worktree RFC and the shipped `v2/` code are the per-kind standard this RFC
  follows. This RFC's `register`/`unregister` pair must be ratified there.
