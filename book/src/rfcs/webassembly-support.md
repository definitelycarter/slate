# RFC: WebAssembly Support

> **Status: partially implemented.** Extracted from the roadmap; the
> [roadmap](../roadmap.md) tracks status at a glance.

## What works today

The full database stack (`slate-store`, `slate-engine`, `slate-query`, `slate-vm`,
`slate-db`) compiles to `wasm32-unknown-unknown`. The MemoryStore backend works
out of the box — no C dependencies, no filesystem, no threads.

```bash
cargo build -p slate-db --target wasm32-unknown-unknown --no-default-features --features js
```

This produces a `slate_db.wasm` binary with the complete query engine, planner,
executor, index support, and JS scripting bridge.

**Scripting on wasm32:** The native Lua runtime (`mlua`) vendors C code and cannot
compile to wasm. The `js` feature in `slate-vm` provides an alternative:
`JsScriptRuntime` and `JsScriptHandle` implement the `ScriptRuntime`/`ScriptHandle`
traits via `wasm-bindgen`, delegating script execution to the JS host. The JS side
plugs in any Lua engine (e.g. wasmoon for Lua 5.4 compiled to wasm, or Fengari for
a pure-JS Lua VM).

The bridge exposes three wasm-bindgen contract points:

- **`slate_vm_load(name, source) → handle_id`** — JS compiles script source, returns
  an integer handle
- **`slate_vm_call(handle_id, input_bson, caps) → output_bson`** — JS executes the
  compiled script with BSON input
- **`slate_vm_invoke_method(name, args_bson) → result_bson`** — exported from Rust,
  called by JS when a script invokes `ctx.get()`, `ctx.put()`, or `ctx.delete()`,
  routing back into the Rust transaction layer

Feature flags:

| Feature | Runtime | Target | Use case |
|---------|---------|--------|----------|
| `lua`   | mlua (C Lua 5.4) | Native | Default for Rust/Swift apps |
| `js`    | wasm-bindgen → JS host | wasm32 | Browser, Node.js |
| `wasm`  | (reserved) | Any | Future: wasmtime/wasmi for embedded wasm modules |

## Remaining work

### Platform adapters

`slate-db` currently owns two platform-specific concerns: a `SystemTime::now()` clock
and a background sweep thread (`std::thread::spawn`). These need to be gated for wasm.

Gate platform-specific code behind a `runtime` feature in `slate-db` (default on).
With `runtime` enabled, `Database::open` uses `SystemTime::now()` as the clock and
spawns a background sweep thread — batteries included for native Rust consumers.
Without it, the caller provides a clock function and handles sweep manually via
`purge_expired()`.

```
slate-db (features = ["runtime"])   → default: SystemTime clock, sweep thread
slate-db (default-features = false) → pure logic, caller provides clock, no sweep

slate-uniffi  → depends on slate-db (default features → runtime on)
                UniFFI bindings for Swift/Kotlin

slate-wasm    → depends on slate-db with default-features = false
                wasm-bindgen, injects Date.now(), no sweep (or JS setInterval)
```

Runtime blockers (compile succeeds, but these panic at runtime on wasm32):

- ~~**`SystemTime::now()`** — `KvEngine::with_clock()` escape hatch already exists~~
- ~~**`std::thread::spawn`** — sweep is gated behind `#[cfg(feature = "runtime")]`~~
- ~~**`RAND()` RNG** — the default seeded PRNG is gated behind `runtime`; the wasm
  host injects `Math.random` via `DatabaseBuilder::with_rand()` (mirrors `with_clock`)~~
- **`getrandom`** — needs `features = ["js"]` for `crypto.getRandomValues()` entropy
  (used by bson for ObjectId generation)

### Browser storage

MemoryStore works on wasm32 but is ephemeral — data is lost on page reload. Two
browser-native storage options could provide persistence:

**OPFS (Origin Private File System)** — `createSyncAccessHandle()` in Web Workers
provides synchronous file I/O. This maps directly to the existing `Store` trait
without any API changes. Could potentially run redb on top of it since redb is
file-backed. Limited to Web Workers (not the main thread).

**IndexedDB via flush** — IndexedDB is async, and the `Store`/`Transaction` traits
are synchronous. Rather than making the entire store layer async (which would bubble
up through the engine, database, and public API), MemoryStore can persist to
IndexedDB using a write-behind flush strategy. MemoryStore remains the hot path for
reads and writes; a background flush (driven by `setInterval` or after N writes)
serializes dirty state to IndexedDB asynchronously. On startup, the store hydrates
from IndexedDB before becoming available. This keeps the `Store` trait sync and
avoids infecting the core API with async — persistence is a separate concern bolted
on from the outside. (See the
[MemoryStore Persistence RFC](./memorystore-persistence.md).)

This is the same pattern RocksDB and redb use internally: writes hit memory
(memtable / B-tree cache), and actual disk I/O happens in the background. The sync
API isn't blocking on disk for every operation — it's "sync API, async I/O
internally."

### ~~slate-wasm crate~~ — Done

A thin binding crate (similar to `slate-uniffi`) that wraps `Database`, `Transaction`,
and `Cursor` with `wasm-bindgen` exports. Depends on `slate-db` with
`default-features = false`. Injects `Date.now()` as the clock. Exposes
`purge_expired()` for manual or `setInterval`-driven cleanup.

## Decoupling the VM backend — done

`slate-executor` previously hardcoded `slate-vm = { features = ["lua"] }`,
which dragged `mlua` (native Lua, C-vendored) into *every* build — so
`slate-wasm` couldn't compile to `wasm32`.

No propagated feature turned out to be necessary. The executor touches
scripting only through `slate-vm`'s trait objects (`VmPool`,
`dyn ScriptRuntime`/`ScriptHandle`, `VmError`) and never names a concrete
runtime, so the fix was simply to drop the forced `lua` feature from the
normal dep and move it to a dev-dependency (tests still build a real
`LuaScriptRuntime`). Under resolver v2 the dev-dep feature does not leak into
the normal build, so the query stack — and `slate-wasm`, which takes
`slate-db` with `default-features = false` — is now mlua-free and compiles to
`wasm32-unknown-unknown`. A CI job (`cargo build -p slate-wasm --target
wasm32-unknown-unknown`) guards against regression. Native keeps `lua` via
`slate-db`'s default features.

Concrete runtimes stay pluggable: register them into a `VmPool` and inject it
via `DatabaseBuilder::with_scripting(pool)`. The `js` VM backend (JS-side Lua
via wasm-bindgen, see "Scripting on wasm32" above) remains the path for
actually running scripts on wasm32.
