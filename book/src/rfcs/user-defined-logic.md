# RFC: User-Defined Logic

> **Status: partially implemented.** Extracted from the roadmap; the
> [roadmap](../roadmap.md) tracks status at a glance.

## Implemented

**Triggers**, **validators**, and **UDFs** are implemented as **native Rust** functions —
see the [Native Functions RFC](./native-functions.md), now fully built. Each is a role-typed
trait (`dyn Trigger` / `dyn Validator` / `dyn Udf`) registered into a database-scoped *bag*
of code, with a durable per-collection *binding* mapping a name to a function. Validators and
triggers are resolved once per query and run under `catch_unwind`; UDFs resolve at plan
build. A dangling binding (code missing from the bag) aborts the affected writes, fail-safe,
and is reported by `dangling_bindings()`.

There is **no scripting VM**: `slate-vm` (the former Lua/JS runtime) was deleted when
triggers went native, so the whole stack compiles to `wasm32` with no runtime linked. A
future scripted runtime (Lua via `mlua`, JS via `rquickjs`/`wasm-bindgen`) returns as just
another `dyn Trigger`/`Validator`/`Udf` implementor whose body drives the VM — a companion
adapter, not a change to this model.

## Remaining hook points

- **Computed fields** — derive a field value from the rest of the document on
  insert/update. The result is stored in the document, making it indexable and
  queryable like any real field.
- **Custom index key extractors** — produce a synthetic index key from document fields
  (e.g. a normalized/lowercased string, a composite key). The engine indexes the
  output; the function defines *what* to index.
- **Partial index filters** — a native predicate that controls whether a document is
  included in an index. Evaluated on every insert/update during index maintenance.
  See the [Partial Indexes RFC](./partial-indexes.md).
- **Transform pipelines** — chain multiple functions on a document before storage.
  Schema migration, field normalization, enrichment.

## Future runtime: scripted / Wasm

With the native model in place, a *scripted* runtime is purely additive: any language that
can be hosted in-process (Lua via `mlua`, JS via `rquickjs`, or a Wasm sandbox via
`wasmtime`/`wasmi` for polyglot Rust/Swift/Go/AssemblyScript) returns as a companion adapter
that produces a `dyn Trigger`/`Validator`/`Udf` whose body drives the VM. Sandboxing, fuel
metering, and capability granting live in that adapter, not in the core — the bag/binding
model and the `catch_unwind` boundary are unchanged. (The old `slate-vm` `RuntimeKind`/
`ScriptRuntime` seam was removed; this would be built fresh against the native trait.)
