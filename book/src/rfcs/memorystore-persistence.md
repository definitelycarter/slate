# RFC: MemoryStore Persistence (Write-Behind Flush)

> **Status: proposed.** Extracted from the roadmap; the [roadmap](../roadmap.md)
> tracks status at a glance.

## Concept

MemoryStore is fast but ephemeral. A write-behind persistence layer lets MemoryStore
act as the hot path while flushing durable state to a pluggable backend
asynchronously. This is the same pattern used by RocksDB (memtable + background
flush) and Redis (RDB snapshots / AOF) — the sync API never blocks on I/O, and
persistence is a separate concern.

## Flush targets

- **Disk (native)** — serialize snapshots or append to a WAL on macOS/Linux
- **IndexedDB (browser)** — async flush from MemoryStore to browser storage,
  hydrate on startup
- **S3 / remote** — cloud backup for embedded deployments

## Flush strategies

- **Snapshot** — serialize the full store state periodically or after N writes
- **WAL (write-ahead log)** — append each mutation, replay on startup. More
  granular than snapshots, slightly more complex
- **Dirty tracking** — only flush changed column families or key ranges

## Design

The flush layer wraps a `MemoryStore` and owns the persistence lifecycle. It does
not change the `Store` trait — consumers interact with MemoryStore as usual. The
flush runs on a background thread (native) or via `setInterval` / microtask
(browser). On startup, the store hydrates from the durable backend before accepting
operations.

This avoids making the `Store` trait async. The async boundary lives entirely inside
the flush layer, invisible to the engine, database, and public API.

## Relation to BackupStore

`BackupStore::backup()` is on-demand, point-in-time. The flush layer is continuous
and automatic. They can coexist — `backup()` remains useful for explicit snapshots
even when flush is running.
