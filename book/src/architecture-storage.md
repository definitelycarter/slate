# Storage Layer

## Tier 1: Storage Layer (`slate-store`)

### Overview

The store is a dumb, schema-unaware key-value storage layer. It stores and retrieves raw bytes within column-family-scoped transactions. It knows nothing about records, collections, or query optimization — those are higher-level concerns handled by `slate-db`.

### Store Trait

The `Store` trait provides column family management, range deletion, and transaction creation. Uses a GAT for the transaction lifetime.

```rust
pub trait Store {
    type Txn<'a>: Transaction where Self: 'a;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, StoreError>;
    fn create_cf(&self, name: &str) -> Result<(), StoreError>;
    fn drop_cf(&self, name: &str) -> Result<(), StoreError>;
    fn delete_range(&self, cf: &str, range: impl RangeBounds<Vec<u8>>) -> Result<(), StoreError>;
}
```

### Transaction Trait

All read/write operations go through a transaction. Read-only transactions return errors on write operations (enforced at runtime). Everything is raw bytes — serialization is the caller's responsibility.

```rust
pub trait Transaction {
    type Cf: Clone;
    fn cf(&self, name: &str) -> Result<Self::Cf, StoreError>;

    // Reads
    fn get(&self, cf: &Self::Cf, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError>;
    fn multi_get(&self, cf: &Self::Cf, keys: &[&[u8]])
        -> Result<Vec<Option<Vec<u8>>>, StoreError>;
    fn scan_prefix<'a>(&'a self, cf: &Self::Cf, prefix: &[u8])
        -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'a>, StoreError>;
    fn scan_prefix_rev<'a>(&'a self, cf: &Self::Cf, prefix: &[u8])
        -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'a>, StoreError>;

    // Writes
    fn put(&self, cf: &Self::Cf, key: &[u8], value: &[u8]) -> Result<(), StoreError>;
    fn put_batch(&self, cf: &Self::Cf, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError>;
    fn delete(&self, cf: &Self::Cf, key: &[u8]) -> Result<(), StoreError>;

    // Schema
    fn create_cf(&mut self, name: &str) -> Result<(), StoreError>;
    fn drop_cf(&mut self, name: &str) -> Result<(), StoreError>;

    // Lifecycle
    fn commit(self) -> Result<(), StoreError>;
    fn rollback(self) -> Result<(), StoreError>;
}
```

### BackupStore Trait

The `BackupStore` trait extends `Store` with a single `backup()` method for online
snapshots. It is a separate trait so that backends without meaningful backup support
(e.g. `MemoryStore`) can still implement `Store` without providing a no-op.

```rust
pub trait BackupStore: Store {
    fn backup(&self, dest: &Path) -> Result<(), StoreError>;
}
```

`Database::backup(path)` and `KvEngine::backup(path)` are conditionally available
when `S: BackupStore`.

- **RocksDB** — `rocksdb::Checkpoint::create_checkpoint()`. Hardlinks SST files for
  a near-instant, consistent snapshot while the DB is live.
- **redb** — `std::fs::copy`. redb's CoW B-tree design keeps the file in a
  consistent state at all times.
- **MemoryStore** — returns `StoreError::Storage` (nothing on disk to back up).

Restore is offline: open the backup directory (RocksDB) or file (redb) as a new store.

`BackupStore::backup` is the *physical*, same-backend snapshot. The *logical*,
backend-neutral counterpart — `Database::export` / `Database::import` — lives one
tier up in `slate-db` (it needs the collection catalog the store layer can't see);
see [Database & Bindings](./architecture-database.md). A logical import rebuilds
index entries from the dumped records, so it round-trips *across* backends
(e.g. redb → RocksDB) where a physical copy cannot.

### Error Type

Custom `StoreError` enum with variants: `TransactionConsumed`, `ReadOnly`, `Storage`.

### Implementation: RocksDB

The default implementation uses RocksDB (feature-gated). RocksDB provides:

- Embedded storage, no separate server process
- Native transaction support (`OptimisticTransactionDB`) with begin/commit/rollback
- Snapshot isolation for read-only transactions
- MVCC-like behavior built in — no need to implement our own

### Implementation: redb (`RedbStore`)

The redb implementation (feature-gated behind `redb`) is a pure-Rust embedded key-value store designed for environments where C dependencies are problematic — notably Apple platforms (macOS/iOS) where RocksDB's C toolchain complicates cross-compilation and distribution.

**Why redb?** RocksDB is faster (2-4x on raw throughput) but requires a C compiler, `libclang`, and platform-specific build configuration. redb is ~15k LOC of pure Rust with no C dependencies — it compiles cleanly for all Apple targets and produces smaller binaries. For frontend applications with user-generated data, redb's performance is more than adequate (sub-15ms for all operations at 10k records).

**Architecture:**

- **Copy-on-write B-trees** with MVCC. Single writer, multiple concurrent readers.
- **`redb::Database`** — single file on disk, created via `Database::create(path)`.
- **Tables as column families** — each `cf` string maps to a `TableDefinition<&[u8], &[u8]>`. Tables are opened inline per operation (lightweight handle, no caching needed).
- **Separate transaction types** — redb has distinct `ReadTransaction` / `WriteTransaction` types, wrapped in an `Inner` enum inside `RedbTransaction`.

**Key differences from RocksDB:**

- **No native `delete_range`** — implemented via iterate-and-delete within a write transaction.
- **Eager scan collection** — redb iterators borrow the table handle and can't outlive the method. Prefix scans collect results into a `Vec` before returning. Acceptable because prefix scans in slate are bounded by collection size.
- **Returns owned data** — like RocksDB, values must be copied out of redb's `AccessGuard`. No zero-copy borrows across the transaction boundary.

### Implementation: In-Memory (`MemoryStore`)

The in-memory implementation (feature-gated behind `memory`) is designed for ephemeral cache workloads where data is populated from an upstream source and doesn't need to survive process restarts.

**Why not RocksDB for caching?** RocksDB is a disk-backed LSM engine — it pays for durability (WAL, compaction, fsync) that an ephemeral cache doesn't need. At 500k records, MemoryStore writes are 2x faster and reads are 1.5-1.9x faster.

**Why not an external database (MongoDB, Redis)?** The data model is already document-shaped (`bson::Document` with nested types). Adding an external database means shipping a server dependency, managing connections, and translating between type systems. An embedded in-memory store gives zero operational overhead, no network round-trips, and direct Rust type access.

**Architecture** (inspired by SurrealDB's [echodb](https://github.com/surrealdb/echodb)):

- **`imbl::OrdMap`** per column family — immutable B-tree with structural sharing. Snapshot clones are O(1), not O(n). Ordered keys give sorted iteration for `scan_prefix` and `delete_range`.
- **`arc_swap::ArcSwap`** per column family — lock-free atomic pointer swap. Readers load the current pointer without blocking. Writers swap in a new pointer on commit.
- **`std::sync::Mutex`** write lock — serializes writers. Acquired at `begin(false)`, released on commit/rollback.

**Concurrency model:**

- Readers snapshot via `ArcSwap::load` (lock-free) and see a consistent point-in-time view.
- Writers acquire the mutex, clone the OrdMap (cheap via structural sharing), mutate locally, and atomically swap on commit.
- Multiple concurrent readers never block each other or writers.
- A reader that started before a commit continues seeing old data (snapshot isolation).

**Memory footprint:** ~1.2 KB per record on disk/in-store (960 bytes BSON data + keys + index entries for a 50-field document). At 500k records, ~0.7 GB; at 1M records, ~1.4 GB including BTreeMap overhead — fits comfortably in a 2-4 GB container.

## Durability & Integrity

Two storage concerns decide whether you can trust the bytes on disk: *what survives a crash* (durability) and *whether the on-disk structures stay internally consistent* (integrity).

### Durability levels

`commit()` makes a guarantee about what is on disk if the power dies one instruction later. That guarantee is selectable per database (a builder default) and overridable per transaction. The three levels map onto each persistent backend's native control:

| Level | RocksDB | redb | Guarantee on `Ok(commit)` |
|-------|---------|------|---------------------------|
| `Strict` | `WriteOptions` sync | `Durability::Immediate` | fsync'd; survives power loss |
| `Buffered` *(default)* | default WAL, no sync | `Durability::Eventual` | survives a process crash, not power loss |
| `Relaxed` | `disable_wal` | `Durability::None` | survives neither; fastest |

`Buffered` is the default — the balance a typical embedded workload wants. Reach for `Strict` on data you can't reconstruct (accepting the fsync cost), and `Relaxed` for rebuildable/derived data where throughput dominates. `MemoryStore` is ephemeral, so the level is inert there (every commit is equally non-durable).

Set the database-wide default on the builder:

```rust
use slate_db::{DatabaseBuilder, Durability};

let db = DatabaseBuilder::new()
    .with_durability(Durability::Strict)
    .open(store)?;
```

Override it for a single write transaction — e.g. run a hot ingest path `Buffered` while a money-moving commit asks for `Strict`:

```rust
let txn = db.begin_with(Durability::Strict)?;
// … writes …
txn.commit()?;
```

`db.begin(false)` uses the builder default; `db.begin_with(level)` overrides it for that one transaction.

### Integrity verification

The engine maintains cross-structure invariants — every live record has its index (`i`) entries, every unique slot (`u`) has exactly one owner, and each index entry's metadata matches its record. `verify()` walks a collection and reports any drift; `repair()` rebuilds the index structures from the records, which are the source of truth.

```rust
use slate_db::DEFAULT_CF;

let report = db.verify(DEFAULT_CF, "users")?;
if !report.ok() {
    // report.issues enumerates every problem; the counts give the scale walked.
    db.repair(DEFAULT_CF, "users")?;
}
```

`verify()` is a pure read path — it opens its own read-only snapshot and is safe to run on a live database. `IntegrityReport` carries the counts walked (`records_checked`, `index_entries_checked`, `unique_slots_checked`) and an `issues` list; `report.ok()` is `true` when that list is empty. Each `IntegrityIssue` names the field and document involved and falls into one of:

- an index entry that is **missing**, **orphaned**, or has **mismatched** metadata;
- a unique slot that is **missing**, **orphaned**, or owned by the **wrong** document;
- a stored record that is **undecodable**.

`repair()` reuses the same reindex path as the on-disk encoding migration: records are authoritative, so rebuilding the `i`/`u` structures from them restores consistency.

