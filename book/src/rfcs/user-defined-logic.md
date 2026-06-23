# RFC: User-Defined Logic

> **Status: partially implemented.** Extracted from the roadmap; the
> [roadmap](../roadmap.md) tracks status at a glance.

## Implemented

**Triggers** and **validators** are implemented. Scripts are registered per-collection,
resolved at plan time via `HookSnapshot`, and executed as typed plan nodes
(`Node::Validate`, `Node::Trigger`, `Plan::Trigger`). See the
[mutation pipeline](../querying.md) documentation for details.

**Lua runtime** (`mlua`) is the default scripting backend. Sandboxed execution with
instruction limits, BSON type preservation, and scoped transaction callbacks
(`ctx.get`, `ctx.put`, `ctx.delete`).

**JS runtime** (`wasm-bindgen`) provides the same `ScriptRuntime`/`ScriptHandle`
traits on wasm32 targets, delegating script execution to the JS host.

## Remaining hook points

- **Computed fields** — derive a field value from the rest of the document on
  insert/update. The result is stored in the document, making it indexable and
  queryable like any real field.
- **Custom index key extractors** — produce a synthetic index key from document fields
  (e.g. a normalized/lowercased string, a composite key). The engine indexes the
  output; the function defines *what* to index.
- **Partial index filters** — a Lua predicate that controls whether a document is
  included in an index. Evaluated on every insert/update during index maintenance.
  See the [Partial Indexes RFC](./partial-indexes.md).
- **Transform pipelines** — chain multiple functions on a document before storage.
  Schema migration, field normalization, enrichment.

## Future runtime: Wasm (wasmtime / wasmi)

Polyglot — users write functions in any language that compiles to wasm32 (Rust, Swift,
Go, JS via QuickJS, AssemblyScript). Sandboxed by default with no filesystem, network,
or memory access beyond what's explicitly granted. Fuel metering provides hard
computation bounds. The `RuntimeKind::Wasm` variant and `wasm` feature flag are
reserved for this.
