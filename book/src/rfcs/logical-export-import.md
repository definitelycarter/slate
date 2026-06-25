# RFC: Logical Export / Import

> **Status: shipped (BSON path).** `Database::export` / `Database::import` and the
> CLI `.export` / `.import` commands implement the BSON-canonical manifest+streams
> design below; cross-backend migration (a redb→RocksDB round-trip test) works.
> JSONL interop remains deferred — `.seed` already ingests JSONL, so it covers the
> human-inspectable lossy path until a matching exporter is built.
> Slate already had *physical* hot backup (`Database::backup`); this adds the
> *logical* dump/reload needed for cross-backend migration, dev seeding, and
> recovery when a physical file is suspect or unreadable by another backend.

## Problem

Physical backup copies a backend's native files (RocksDB checkpoint, redb file
copy) and can only be restored *by the same backend*. That leaves three needs
unmet:

1. **Cross-backend migration** — move a database from redb to RocksDB (or to
   MemoryStore for tests). A physical redb file means nothing to RocksDB.
2. **Seeding & fixtures** — dump a known dataset and reload it into a fresh
   instance, dev environment, or test.
3. **Logical recovery** — when a physical backup is suspect, a logical dump in a
   neutral, inspectable format is the escape hatch.

## Current state

- **Physical backup only.** `BackupStore::backup`
  (`crates/slate-store/src/store.rs:96-98`) is implemented per backend
  (`rocks/store.rs:95-101` checkpoint, `redb_store/store.rs:109-115` file copy,
  MemoryStore errors). Same-backend restore, offline.
- **An import primitive already exists.** The CLI's `.seed <path>`
  (`crates/slate-cli`) bulk-loads a JSON array or JSONL/NDJSON file
  (mongoexport's default) into a collection named after the file stem, streaming
  large JSONL line by line. So one direction, one collection, no metadata —
  half the machinery is here.
- **The DB layer owns the metadata that matters.** Collection config, index +
  unique-index definitions, `pk_path` / `ttl_path`, and registered functions all
  live in `slate-db` / the engine catalog (`slate-engine/src/traits.rs:347-410`);
  the store sees only keys and bytes. So export/import **belongs in `slate-db`** —
  it's the only layer that can reproduce a collection's full definition, not just
  its documents.

## Design

### Format: BSON canonical, JSONL for interop

- **BSON** is the canonical, lossless wire format — it's the native document
  representation, so a round-trip is bit-exact (Decimal128, Date, ObjectId all
  survive). This is the format for migration and recovery where fidelity is
  non-negotiable.
- **JSONL / NDJSON** is the interop format — human-inspectable, mongoexport-
  compatible, and already half-supported by `.seed`. It's lossy on BSON-specific
  types (the same Decimal128 / `$date` caveats documented under
  [Numbers and comparison](../querying.md)), which the export must call out, not
  hide.

### A manifest, not just documents

A logical dump is **catalog + document streams**, not bytes:

```
dump/
  manifest.bson      ← collections, indexes (+unique), pk_path, ttl_path, functions
  <cf>.<collection>.bson   ← one streamed document-per-record file per collection
```

The manifest lets import *recreate* each collection with its full definition, then
load documents. Indexes are **not** exported as entries — they're rebuilt from the
records on import (records are the source of truth; the index-encoding migration
already does exactly this rebuild, so the path exists). That keeps the dump small
and backend-neutral, and means an import always produces correctly-encoded indexes
for the *target* backend's current encoding version.

### Streaming, both directions

Dump collection-by-collection, document-by-document; never materialize the whole
DB. Reload the same way — reuse the CLI's existing line-by-line JSONL streaming so
a multi-GB dump imports in bounded memory. Read documents through an open
read-only transaction (a consistent snapshot) so an online export is coherent.

### Scope & restore semantics

- **Whole-DB** and **per-collection** export/import.
- **Round-trip guarantee:** `export → import` into a fresh DB yields identical
  documents *and* catalog (the testable contract; mirrors the Cosmos golden-replay
  discipline of committing an expected output and asserting against it).
- **Import target:** fresh DB (recreate from manifest) or existing DB (merge —
  define collision behavior on `_id`: error / overwrite / skip, defaulting to
  error, reusing the `upsert`/`merge` semantics already in the mutation API).

## Recommendation

Build on what exists: generalize the `.seed` importer into a full
manifest-driven import, and add the matching streaming export beside it in
`slate-db`. **BSON canonical + manifest first** (the migration/recovery use case
that physical backup can't serve), **JSONL interop second** (mostly already
there via `.seed`). Expose on the public API and as CLI `.export` / `.import`
commands. Cross-backend migration then falls out for free — export from a redb
`Database`, import into a RocksDB one — which is the headline payoff and the
obvious round-trip test.

No perf spike needed (this is bounded-memory streaming I/O, not a hot path); the
design question to settle up front is the **manifest schema** and the
**JSONL type-fidelity contract** (exactly which BSON types degrade, and how
they're flagged on export).

## Non-goals

- **No incremental / continuous export.** A change-data-capture stream is the
  separate [Watch Queries](../roadmap.md) roadmap item; this is point-in-time.
- **No compression / encryption in v1** — orthogonal, layer it on the byte stream
  later.
- **Not a replacement for physical backup.** `Database::backup` stays the fast
  same-backend snapshot; logical export is the slower, neutral, portable one.
  They coexist for different jobs.
- **No schema transformation on import** — documents land as dumped; reshaping is
  the Lua transform-pipeline roadmap item's job, not the importer's.

## What shipped

- **API:** `Database::export(...)` / `Database::import(...)`, with public types
  re-exported from `slate-db`: `Manifest`, `CollectionDef`, `ExportOptions`,
  `ImportOptions`, `ExportReport`, `ImportReport`, `OnCollision`.
- **Dump layout:** a directory holding `manifest.bson` (versioned collection
  definitions — `pk_path`, `ttl_path`, indexes including the unique subset) plus
  one `<cf>.<collection>.bson` document-stream file per collection (concatenated
  native raw BSON).
- **Index rebuild on import:** index *entries* are never dumped. Import recreates
  collections from the manifest, reloads documents, and rebuilds indexes from the
  records — the same "records are the source of truth" contract as repair and the
  encoding migration. The target backend therefore gets correctly-encoded indexes,
  which is what makes this the headline **cross-backend migration** path
  (redb→RocksDB round-trip test).
- **Scope:** whole-DB and per-collection, both directions.
- **Collision modes:** `OnCollision::Error` (default) / `Overwrite` / `Skip`.
- **Fidelity:** the BSON canonical format is lossless — ObjectId, DateTime, and
  Decimal128 all survive a round-trip.
- **CLI:** `.export <dir>` and `.import <dir>` (whole-DB; the whole argument is the
  path, like `.backup`; default error-on-collision).

### Deferred follow-up

- **JSONL / NDJSON interop export.** The human-inspectable, mongoexport-compatible,
  lossy-on-BSON-types interop format is not yet emitted on export. The ingest half
  already exists via `.seed`, so this is the remaining direction; the type-fidelity
  contract (which BSON types degrade and how they're flagged) is the open design
  point when it's built.
