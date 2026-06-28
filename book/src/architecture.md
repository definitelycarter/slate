# Architecture

## Overview

A layered system: a key-value storage backend, an engine layer for key encoding and index maintenance, and a query stack where two surfaces — a MongoDB-style `find` and a CosmosDB-style SQL — lower to one shared AST, planner, and executor, all behind the user-facing database API.

## Crate Structure

```
slate/
  ├── slate-store            → Store/Transaction traits, RocksDB + redb + MemoryStore impls (feature-gated)
  ├── slate-rawbson          → Fast raw byte-level BSON field scanner + path traversal (incl. multikey arrays) + document merge (shared leaf, no deps but bson)
  ├── slate-engine           → Storage engine: BSON key encoding, TTL, indexes, catalog, record format
  ├── slate-ast              → Shared query AST — the single IR every query surface targets
  ├── slate-query            → MongoDB find front-end: FindOptions/Sort DTOs + filter→AST translation
  ├── slate-sql              → CosmosDB-style SQL front-end: SQL text → AST
  ├── slate-value            → The shared value domain (`Value`) — function crates speak it without the evaluator
  ├── slate-udf              → Native UDF trait + the database-scoped code bag (baked into the plan/executor)
  ├── slate-validator        → Native validator trait + the database-scoped validator bag (write-path gate)
  ├── slate-trigger          → Native trigger trait + the database-scoped trigger bag (write-path side effects)
  ├── slate-eval             → Evaluation semantics for the AST (owned + zero-copy raw evaluators)
  ├── slate-planner          → Logical planning: AST → Plan/Node IR (sargability, index choice)
  ├── slate-executor         → Physical execution: streams a Plan against a transaction
  ├── slate-db               → Database layer: public API + query-stack wiring
  ├── slate-uniffi           → UniFFI bindings for Swift/Kotlin (XCFramework builds)
  └── slate-wasm             → wasm-bindgen bindings for JavaScript/WebAssembly
```

The query stack (`slate-ast` … `slate-executor`) is the engine: both query surfaces lower to one shared AST, planner, and executor, so they can't drift. See [Roadmap — Query Engine](roadmap.md).


## The tiers

Slate is layered; each tier has its own page:

- **[Storage Layer](./architecture-storage.md)** (`slate-store`) — the pluggable key-value backends (memory, redb, RocksDB).
- **[Query Stack, Engine & Scripting](./architecture-engine.md)** — the parser, planner, executor, evaluator, the storage engine, and the scripting VM.
- **[Database & Bindings](./architecture-database.md)** (`slate-db`) — the public `Database`/`Transaction` API, plus the Swift and WASM bindings.
