# RFC: Native Functions — runtime-agnostic hooks & UDFs

> **Status: implemented — the native core is shipped on `main`** (the `slate-udf`
> / `slate-trigger` / `slate-validator` crates; `slate-vm` deleted). This is *the*
> native-hooks RFC. It absorbs and replaces two
> earlier drafts — "Native Functions (first-class Rust hooks)" and "Native Function
> Core (runtime-agnostic hooks & UDFs)" — into one design. It builds on:
>
> - the [db-api cleanup RFC](./db-api-cleanup.md) — per-kind `functions()` /
>   `triggers()` / `validators()` handles and the `create`/`remove` vs
>   `register`/`unregister` verb split;
> - the [Execution Context RFC](./execution-context.md) — the *internal* env-bundle
>   + plan-build-resolution refactor, which lands **first**; this work builds on
>   the bundle (see *Resolution & consistency*); and it is the script-side counterpart to the
> - [User-Defined Logic RFC](./user-defined-logic.md) — the stored-script (Lua/JS)
>   triggers/validators this RFC reframes as one backend among many.
>
> **Shipped: the native core** — UDFs, validators, and triggers are all native
> Rust functions on `main`. Lua and Wasmtime as companion runtimes remain
> **deferred** — their shape is captured here so the core doesn't paint them out,
> but they are not built. The near-term integration targets after the native core
> are **`slate-wasm` (a JavaScript function)** and **`slate-uniffi` (a Swift
> function)**.

## Summary

Slate's core should know exactly three kinds of hook — a **UDF**, a **validator**,
and a **trigger** — each a plain **native Rust function**, differing only by role
(when fired / how called). Scripting runtimes (Lua, Wasm, a JS function, a Swift
function) become **companion adapters** that *produce* one of those role-typed
functions; the database core depends on none of them.

Concretely, this RFC proposes to:

1. **Drop `slate-vm`, `with_scripting`, `RuntimeRegistry`, `VmPool`** from the
   core. The database no longer bakes in a VM.
2. **Model each role as its own native trait** in its own low crate
   (`slate-udf`, `slate-trigger`, `slate-validator`), each **self-contained —
   no shared `slate-hook` crate**: every role crate owns its own context type,
   and common vocabulary is factored out only if a second role proves it earns
   the indirection. The planner/executor embed role-typed handles where they
   embed `slate-vm::ResolvedHook` today.
3. **Store only *bindings*** in the catalog — a per-collection mapping from a
   trigger/validator/UDF to a **native function name** — never code or blobs.
4. **Register native functions at open *and* at runtime**, in role-typed bags on
   the builder / per-kind handle. Lifecycle (pooling, VM instances) lives *inside*
   each function object.
5. Validate bindings **lazily** — open always succeeds; a dangling binding fails
   only when exercised, and is repaired by registering the function or dropping
   the binding.

> **Historical reference.** The scripting stack being removed — `slate-vm`, the
> Lua runtime, `RuntimeRegistry`/`VmPool`, `DatabaseBuilder::with_scripting`, and
> the catalog's stored-source path (`create_function(.., LUA, source)`) — exists
> as of **main `2a33620`**. The removal commit must quote this SHA so the
> implementation is always recoverable via `git show`.

## Motivation

An embedded database has a single trust domain: the application author writes the
hooks and runs them in-process. There is no untrusted-tenant boundary that forces
a sandboxed VM. So baking a specific VM (mlua) into the core buys little and costs
a lot:

- a heavy dependency (`mlua`) and OS-specific build concerns in a crate that many
  others sit on;
- a fixed runtime choice — the consumer can't pick rquickjs over mlua, or
  wasmtime over wasmer;
- friction on the `wasm32` target, where native VM deps don't belong.

A Lua trigger *is already* a native Rust function whose body happens to drive a
VM. Take that to its conclusion: the core holds only `dyn Udf` / `dyn Validator` /
`dyn Trigger`, and every runtime is a library the consumer optionally pulls in to
construct one. **Mechanism, not policy** — the consumer dictates the runtime, the
core stays lean and portable.

## The function model: three role-typed traits

The roles have genuinely different signatures, so each is its own trait — not one
unified `Hook` with a role-tagged context enum:

```rust
// shapes are illustrative; exact context types in slate-trigger/-validator are detailed design
trait Udf:       Send + Sync { fn call(&self, args: &[Value]) -> Result<Value, UdfError>; }
trait Validator: Send + Sync { fn check(&self, ctx: &ValidatorCtx) -> Result<Verdict, HookError>; }
trait Trigger:   Send + Sync { fn fire(&self, ctx: &mut TriggerCtx) -> Result<(), HookError>; }
```

### Why role-typed, not one unified `Hook`

An earlier draft proposed a single `Hook::fire(ctx) -> HookOutcome` for triggers
and validators. Role-typed traits are stronger, for four reasons:

- **Purity already forces a split.** A UDF is `call(&[Value]) -> Value`,
  contextless (see below); it does not fit `fire(ctx)` at all. Once UDF is its own
  trait, the only question is whether validator and trigger merge — and they
  shouldn't, because:
- **The signatures differ.** A UDF returns a value; a validator returns a verdict;
  a trigger has side effects. A unified trait would encode this in a role-tagged
  outcome enum re-interpreted per role — stringly-typed where the type system
  could enforce it.
- **The firing sites are already separate.** Validators fire in
  `slate-executor/src/nodes/validate.rs` (`Pure`, accept/reject); triggers in
  `nodes/trigger.rs` + `upsert.rs` (read/write caps, side effects). They are
  different nodes with different semantics today; a unified `fire` would make both
  speak a common shape and then re-split. The unification that *is* worth keeping
  lives one layer down, at the `dyn`-function seam.
- **Compile-time safety.** With `Validator::check` exposing no mutation method, a
  validator *structurally cannot* write — a compile-time guarantee, versus a
  runtime capability-tier check.

### Crate layout

Each role trait + its registry + **its own context type** lives in its own low
crate, mirroring the existing `slate-udf` — **no shared `slate-hook` crate**:

| Crate | Holds |
|---|---|
| `slate-udf` *(exists)* | `Udf` trait + bag/registry |
| `slate-validator` *(new)* | `Validator` trait, `ValidatorCtx` (the read-only `ctx.doc()` view), `Verdict` + bag/registry |
| `slate-trigger` *(new)* | `Trigger` trait, `TriggerCtx` (read-only candidate + the `ReadWrite` capability surface) + bag/registry |

A shared foundation was considered (`slate-hook`, holding the common `ctx.doc()`
view / `Verdict` / registry plumbing) and **rejected**: with the roles built one
at a time, it would have a single consumer and abstract prematurely. Each crate
carries its own context newtype; if validator and trigger turn out to share
enough vocabulary, that slice is extracted *then*, when a second consumer
justifies it. The bag/registry pattern is small enough that mirroring it per
crate (as `slate-udf` already does) costs less than the indirection.

The planner/executor depend on these (for the role-typed handles embedded in
plans) **instead of `slate-vm`** — which is what finally lets `slate-vm` leave the
planner/executor/`slate-db` dependency trees. A bare closure *is* the function
(blanket impls, as `slate-udf` already does for `Udf`), so native registration has
zero ceremony.

### Per-role contracts

**UDF — pure, contextless.** A UDF is assumed to be a **pure function of its
arguments**. This is a *documented contract*, not an enforced invariant (nothing
stops a native closure calling the network) — the same stance Postgres takes with
`IMMUTABLE`. The engine is therefore free to call a UDF **zero or more times, in
any order, and may cache the result**. If a UDF performs I/O or mutates external
state, behavior with respect to call count and timing is undefined — the contract
was broken; it is not the engine's concern. Non-determinism stays an **injected
capability** (`RAND()`, `$now` — already `with_rand`/`with_clock`), *not*
reachable from a UDF, so "pure" stays honest.

**Validator — `Pure`.** Validators see only the candidate document, exactly as
today (`validate.rs` fires `Pure`). `ctx.doc()` returns a read-only
`&RawDocument`; `check` returns a `Verdict` (accept / reject-with-reason). A
validator has *no* transaction access — keeping it free of phantom-read /
consistency questions. (Promoting validators to read other documents is a future
`ReadOnly` option, deliberately out of scope.)

**Trigger — `ReadWrite`.** Triggers keep today's full power: `ctx.doc()` for the
read-only candidate, **plus** the ability to mutate (set fields) and read other
documents, via the existing borrowed scoped-callback mechanism
(`ScriptCapabilities` / `ScopedMethod`, `slate-vm/src/lib.rs:61-77` — relocated
into `slate-trigger`). The exact `TriggerCtx` shape (event payload, pre/post timing)
is trigger-specific detailed design; the requirement here is that it **inherits
today's `ReadWrite` capability**, not that it resets to read-only-candidate-only.

**Panic boundary (all three roles).** A native closure is user code in-process, so
`call`/`check`/`fire` must wrap the body in `catch_unwind`:

- a **UDF** panic → **fail the query** (read path);
- a **validator** or **trigger** panic → **abort the transaction** (write path,
  fail-safe).

This honors the repo's no-panic rule at the seam even though the body is the
user's. Two caveats to record: the boundary needs `AssertUnwindSafe` around the
`&self` call, and it only works under `panic = "unwind"` (the default) — a
consumer building `panic = "abort"` will take down the process on a hook panic,
which no seam can prevent.

## The binding model (catalog stores mappings, not code)

The catalog stores **only bindings**, per collection — never code or blobs:

```jsonc
// on collection `orders`
{ role: "trigger",   func: "audit",       when: ["on-insert"] }   // `when` = future trigger timing
{ role: "validator", func: "adults_only" }
{ role: "udf",       name: "tax", func: "compute_tax" }            // query-facing name → native func
```

**Two levels — think dynamic linking:**

- **The bag (code)** — a flat, **database-scoped** map `name → Arc<dyn Role>`,
  populated by `register(name, closure)` (no transaction). This is the *shared
  library*: one `compute_tax`, reused everywhere.
- **The binding (intent)** — a **per-collection**, durable map
  `(cf, collection, query_name) → func_name`, written by `create(name, func)`
  (transactional). This is the *symbol reference*.

Resolution links the two: `udf.tax` on `orders` → binding → `"compute_tax"` → bag
→ `Arc<dyn Udf>`. A dangling binding is an *unresolved symbol* — caught at
resolution (see below), repaired by registering the function or dropping the
binding.

The indirection is deliberate and buys three things: **reuse** (one `compute_tax`,
bound on many collections), **isolation** (`udf.tax` resolves only where bound —
the per-collection scoping we keep deliberately, matching how `cf`/collection
isolate data and indexes), and **rebind** (`orders.tax → compute_tax_v2` without
touching queries or the bag). Note the isolation property is at the *binding*
layer: the function `compute_tax` is globally visible *as code* in the flat bag;
what is per-collection is the *name binding*, so a query on another collection
cannot invoke `udf.tax` unless that collection also binds it.

**Why a durable binding but ephemeral code?** Between process starts, the
**binding survives** (it is schema, like an index definition) while the
**implementation is re-supplied** at startup (`register`). On reload the catalog
already knows `orders.udf.tax → compute_tax`; the app re-registers `compute_tax`;
linking succeeds. You persist the *reference*, never the closure — which is also
how "native + durable" is possible at all (a closure can't be serialized, but a
name can).

**Catalog encoding — reuse the existing machinery.** Today `create_function`
stores `(kind, cf, collection, name) → [runtime_tag: u8][source]`
(`slate-engine/src/kv/catalog.rs:555`). A native binding is the same row with a
new **`runtime_tag::NATIVE`** whose "source" bytes are the **target function
name** instead of code. No new catalog subsystem; the same shape serves all three
roles. (Consequence: UDF *bindings* must now be reconstituted at open — closing
the current gap where `HookSnapshot::load_all` loads triggers/validators but never
UDFs.)

**`create` is lazy, and overwrites.** Writing a binding does **not** require its
target function to be registered yet — create-order vs register-order is
app-dependent (catalog setup may run before startup registration). The dangling
check fires at resolution, never at `create`. `create` over an existing binding
**overwrites** (rebinding is a feature), which also keeps a `create`-in-boot-path
idempotent.

**`list()` returns the mapping only** — the symbol table (durable bindings from
the catalog), not the closures (which can't be serialized or displayed). Pair it
with **`dangling_bindings()`** = bindings minus the live bag = unresolved symbols.

## Resolution & consistency

**Resolve at plan-build, on the env bundle.** The
[Execution Context RFC](./execution-context.md) lands **first**, building the
per-query env bundle (`ExecEnv`/`EvalEnv`) + `plan.container()` + the
expression-resolution pass on `main`'s existing `params`/`rand`/`clock`/`watch`
threading. Native functions then land **on** that bundle and resolve at
**plan-build**: the planner resolves each binding to its native-function handle
and embeds it in the plan — exactly as triggers embed hooks into
`Plan::Trigger` / `Plan::Upsert` / `Node::Validate` today
(`slate-planner/src/plan.rs:55,66,400`). So:

- **Triggers and validators** already resolve at plan-build; converting them to
  native only changes *what sits in the slot* — a role-typed `Arc<dyn Trigger>` /
  `Arc<dyn Validator>` instead of a `slate-vm::ResolvedHook` source blob.
- **UDFs** resolve their two-step binding (binding → func name → bag) at
  plan-build too, so a dangling UDF errors at **plan time**, not per row.

> The native-UDF spike (set aside as a reference donor, not on `main`) resolved
> `Expression::Udf` at *eval time* via a dynamic resolver (`dispatch_udf`,
> `slate-eval/src/raweval.rs:209`). That eval-time path remains a valid fallback
> if the expression-resolution pass proves too invasive, but the chosen design is
> plan-build.

**Consistency model — by layer:**

- **The binding is snapshot-at-begin.** It is schema; a binding created
  mid-transaction is not seen by in-flight transactions — exactly the
  `HookSnapshot` (`ArcSwap`) guarantee triggers/validators have today
  (`slate-db/src/hooks.rs`).
- **The bag is read live.** It merely supplies an implementation for an
  already-declared symbol; a function registered mid-process takes effect
  immediately (the `RwLock` UDF-registry behavior the spike already has).

## Registration: pre-open *and* runtime

Two layers, two lifetimes — runtime-management vs durable-CRUD:

| Verb | Layer | Scope | Txn? | Effect |
|---|---|---|---|---|
| `register(func, closure)` | bag (code) | database | no | put a native function in the live bag |
| `unregister(func)` | bag (code) | database | no | remove it from the bag |
| `create(name, func)` | binding | collection | yes | durably map `udf.name` → bag function `func` |
| `remove(name)` | binding | collection | yes | remove the binding |
| `list()` | binding | collection | yes (read) | list the collection's bindings |

The **bag** verbs are database-scoped and take **no transaction** — they mutate
live runtime state, joining the db-api "reactive (no-txn)" category alongside
`watch`/`stream` (`Collection` already carries an `Arc` registry this way,
`v2/collection.rs:39`). The **binding** verbs are collection-scoped and
**transactional**. Triggers and validators get the same five-verb shape; a
trigger's `create` carries role-specific options (its `when` timing).

> **`remove` is asymmetric with `create` on purpose.** `create` writes a binding;
> `remove` drops *only* the binding — it must **never** unregister the bag
> function, which may be shared across collections. Pulling code out of the bag is
> the explicit `unregister(func)` door.

Plus the **pre-open** path: `DatabaseBuilder::with_udf(func, closure)` (and
`with_trigger`/`with_validator`) is bag registration *at open* — the same bag,
populated before the first transaction.

**Why runtime registration, not only pre-open.** Pre-open fits **compiled
applications** (an IoT controller, an n8n-style host) rebuilt each run, carrying
functions in the binary. A **REPL** can't work that way — you define a function
interactively, and reopening the database to install it is absurd — so the bag
must be mutable *after* open. Runtime registration also strengthens lazy
validation: a dangling binding can be **repaired live** by registering the missing
function, no restart.

**Blob lifecycle is the application's, not the core's.** Since the core stores no
code, where a script blob lives is the app's choice: a REPL persists scripted
functions as ordinary data in a slate collection and re-registers on reopen;
IoT/n8n keep blobs on disk and re-register at open. One mechanism, many
persistence policies.

## Lazy validation & repair

Open **always succeeds**, even with dangling bindings. This is deliberate:
eager-fail-at-open would make a database with a missing function *impossible to
repair*, since you couldn't open it to drop the binding. A dangling binding fails
only **when exercised**:

| Role | When a missing function bites | Blocks |
|---|---|---|
| **UDF** (read path) | a query that references it | just that query — writes never blocked |
| **Validator / Trigger** (write path) | any write to the collection | **all writes to that collection** (fail-safe) |

> The UDF row holds **only as long as UDFs stay read-only**. If a future SQL
> `UPDATE` makes `udf.*` reachable on the write path, a dangling UDF could block
> writes too — revisit then.

**Repair** is always possible because dropping a binding never invokes its
function: register the function next open, or `collection.triggers().remove(name)`
before issuing writes. Two opt-in niceties:

- **strict-open** — eager-validate every binding against the registered bags and
  fail fast. This fits the **compiled-app boot path only** (the bags are fully
  populated before open); a REPL that registers *after* open cannot use it.
- **`db.dangling_bindings()`** — introspection so an app can check at startup
  instead of discovering at use.

## Consequence: the database is no longer self-contained for *logic*

With no code in the catalog, a slate file carries **data + bindings (intent)** but
not the logic. A restored backup needs the same app to function; a CLI sees
binding names it can't execute. For an embedded DB where the app *is* the trust
domain this is a clean separation — **data lives in the DB, logic lives in the
app** — but it is an explicit reversal of stored-Lua-source behavior, chosen here
deliberately. (If self-containment is ever required, that is the
store-blobs-bring-the-runtime hybrid in *Alternatives*, out of scope.)

## Runtimes as companion adapters

A runtime is a companion crate that turns source/bytecode into a role-typed native
function. The **core depends on none of them**; the consumer wires what it wants
and hands the builder a finished `dyn Udf` / `dyn Trigger` / `dyn Validator`.
Lifecycle lives **inside the function object** — the `Send + Sync` + `&self`
contract already requires internal reentrancy, so any VM instances / pooling are
the function's own concern. The core never sees a factory and never manages a pool.

Slate's executor is **single-threaded per query** (`Rc` throughout
`slate-executor`), so *within* a query one reused instance suffices; a pool only
earns its keep across *concurrent* queries.

| Runtime | Reuse | Concurrency |
|---|---|---|
| **native** | n/a | nothing — just the closure |
| **Lua** (`!Sync`) *(deferred)* | compile once | thread-local instance, a bounded pool, or a shared `LuaRuntime` pool — today's `VmPool`, relocated out of the core |
| **Wasmtime** *(deferred)* | share the compiled `Module` | `Mutex<Store>` or an instance pool — **honest, safe `Send + Sync`, no `unsafe`** (see below) |

### Near-term target — `slate-wasm` (a JavaScript function)

In the browser build the natural "script" is a **JS function** the host passes in;
wrap a `js_sys::Function` as a `Udf` marshalling `&[Value]` ↔ JS values.

> **Wrinkle, and its resolution.** `js_sys::Function` is `!Send + !Sync` by
> construction (`JsValue` holds `PhantomData<*mut u8>`), so it can never satisfy
> `Udf: Send + Sync` directly — and `cfg`-relaxing the bound on the trait would
> **cascade** (`!Send` "trickles down" to every core type holding `Arc<dyn Udf>`:
> the registry's `RwLock`/`ArcSwap`, `Collection.udfs`). **Resolution: a
> wasm-local newtype wrapper, not a trait relaxation.** A `JsUdf(js_sys::Function)`
> in `slate-wasm` with `unsafe impl Send for JsUdf {} / unsafe impl Sync` — sound
> because `wasm32-unknown-unknown` is single-threaded — keeps the core trait bound
> honest and contains the `unsafe` to one adapter type. (Invalid under future
> wasm-threads; pin the assumption in a comment.) Today `slate-wasm` only injects
> `clock`/`rand`; passing JS *functions* as hooks is new surface.

### Near-term target — `slate-uniffi` (a Swift function)

uniffi **foreign traits** let foreign code implement a Rust trait. A Swift type
implementing a `Udf`-shaped callback interface becomes a `dyn Udf` the builder
accepts.

> **Feasible with the toolchain in tree.** Foreign traits are supported in the
> **uniffi 0.29.5** this repo already uses, and uniffi *requires* foreign-trait
> impls to be `Send + Sync` — mapping cleanly onto Swift 6 `Sendable` and onto our
> `Udf: Send + Sync` contract, enforced by the bindgen. The only genuinely-open
> question is the per-call `Value ↔ Swift` marshalling cost (measurable, not a
> feasibility risk). `slate-uniffi` has no callback surface today, so this is new.

### Future authoring format — WIT components (wasmtime / JCO)

Authoring hooks as **WebAssembly components** (WIT-defined, run via wasmtime
natively or transpiled to the browser via JCO) slots into this seam with **zero
core change** — a `slate-wasmtime` adapter would construct a `dyn Trigger`/`dyn
Udf` whose body drives a component, store-pool and marshalling inside the function.
Three notes for when it's picked up:

- **No `!Send` problem.** wasmtime `Engine`/`Module` are `Send + Sync`; a `Store`
  is `Send` but single-threaded-at-a-time (`&mut` per call). A `Mutex<Store>` or
  instance pool gives *honest* `Send + Sync` — **no `unsafe`**, unlike the
  `js_sys` path.
- **WIT has no closures.** A hook is modeled as a **component exporting the role
  interface** (and importing the host capabilities it needs) — which maps directly
  onto these role-typed traits; the WIT `world` *is* the role interface.
  First-class closures remain an open component-model proposal.
- **Browser ≠ embedding.** wasmtime can't run inside `wasm32`; in the browser JCO
  transpiles the component to JS + wasm that the JS host runs, mediated by glue —
  so the browser still crosses a JS boundary (the #wasm `unsafe`-containment story
  applies to whatever handle `slate-wasm` holds). WIT buys a portable, typed
  authoring format and a clean *native* path; it does not erase the browser
  boundary.

```rust
// deferred — shapes only, to confirm the seam accommodates them
let lua = LuaRuntime::new().pool(8);
builder.with_udf(lua.function("tax", "return …"))
       .with_trigger(lua.trigger("audit", "…"));
builder.with_udf(Wasmtime::from_path("tax.wasm"));   // module shared, Mutex<Store>/pool inside
```

## Removal of `slate-vm` from the core

> **Not a `slate-db`-local change.** `slate-vm` owns `ResolvedHook`
> (`slate-vm/src/lib.rs:34`), which the **planner** embeds in plans and the
> **executor** fires (`nodes/{trigger,upsert,validate}.rs`). Exactly **three**
> crates pull `slate-vm` today (planner, executor, db), all because of
> `ResolvedHook`. So the first real task is to **relocate the hook vocabulary** —
> the role traits + `ValidatorCtx`/`TriggerCtx` + `Verdict` — into the role
> crates (each self-contained), with no VM. Only then can `slate-vm` leave the
> planner/executor/`slate-db` dependency trees.

The removal covers `with_scripting`, `RuntimeRegistry`, `VmPool`, the Lua runtime,
and the durable stored-source path — quoting `2a33620` in the removal commit.

> **No migration required.** There are no consumers today, so existing on-disk
> `FunctionConfig` (stored Lua source) entries need no orphan-handling or repair
> sweep — a clean break. This is a *time-boxed* freedom: make the breaking
> on-disk/API decisions (catalog key layout, trait shapes) now, while they cost
> nothing.

## Migration / sequencing

Land the **execution-context env-bundle first** (it is *internal* — see that
RFC), then build the native-function work **on** the bundle. Within the native
work, do **UDFs first** (the simplest role), **then validators, then triggers**.
The native-UDF spike is a **reference donor** — its eval machinery (`Value`, the
`Udf` trait, the `udf.*` parser, `dispatch_udf`) is lifted; its
`(cf, collection, name)` runtime registry is replaced by the flat-bag +
durable-binding model — but the spike itself is not shipped to `main`.

1. **Execution-context refactor.** The per-query env bundle (`ExecEnv`/`EvalEnv`)
   + `plan.container()` + the expression-resolution pass, on `main`'s existing
   `params`/`rand`/`clock`/`watch` threading — see that RFC.
2. **Native core.** Relocate the hook vocabulary into the role crates (each
   self-contained). Drop `slate-vm`, `with_scripting`, `RuntimeRegistry`, `VmPool`, the
   stored-source path (quote `2a33620`). Add the builder bags + binding catalog +
   lazy validation. UDFs → validators → triggers, resolving at plan-build (the
   machinery landed in step 1).
3. **`slate-wasm` / a JS function.** Wrap `js_sys::Function` behind the
   `unsafe`-newtype wrapper.
4. **`slate-uniffi` / a Swift function.** Foreign-trait adapter; settle the
   marshalling cost.
5. **Deferred:** Lua and Wasmtime companion crates; WIT-component authoring.

## Alternatives considered

- **Keep `slate-vm` in the core (status quo).** A fixed VM choice, a heavy dep on
  a foundational crate, `wasm32` friction. Rejected — the inversion removes all
  three for the cost of the consumer wiring a runtime.
- **Store blobs in the catalog, inject runtime factories at open.** Preserves
  self-containment (logic travels with the file) with zero core VM deps. Rejected
  *for now* — self-containment isn't needed for the embedded, single-trust-domain
  case, and bindings-only is simpler. Noted as the fallback if ever required.
- **One unified `Hook` trait with a role-tagged context enum.** Rejected — see
  *Why role-typed*: the signatures genuinely differ, the firing sites are already
  separate, and role-typed makes "a validator can't write" a compile-time
  guarantee.
- **`create`-with-closure sugar** (`create({name, func: UdfFunction::new(n, cb)})`
  registering *and* binding in one call). Considered and dropped — it folds a
  non-transactional bag mutation into a transactional builder (rollback orphans,
  asymmetric `remove`), for ergonomics that the plain `register` + `create` pair
  already covers cleanly.

## Open questions

- **Trigger context shape** — event payload, pre/post timing (`when`), the exact
  `TriggerCtx` mutation/query surface. Deferred to a trigger-specific detailed
  design; the constraint is that it inherits today's `ReadWrite` power.
- ~~**`slate-hook` boundary**~~ — **resolved: no shared crate.** Each role crate
  (`slate-validator`, `slate-trigger`) carries its own context type, `Verdict`,
  and registry; a shared foundation is extracted only if a second role proves it
  earns its keep.
- **`Send + Sync` under future wasm-threads** — the `slate-wasm` `unsafe` newtype
  is sound only while `wasm32` is single-threaded; revisit if wasm-threads become
  a target.
- **uniffi marshalling cost** — measure the per-call `Value ↔ Swift` overhead for
  a foreign `Udf`.
