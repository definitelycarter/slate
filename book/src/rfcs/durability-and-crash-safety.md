# RFC: Durability & Crash Safety

> **Status: proposed.** Surfaced in a "what's missing for a *proper embedded
> database*" survey. Slate has invested heavily in the read path (two query
> surfaces, one planner/executor, Cosmos-validated correctness, index-key
> encoding). The layer a user actually stakes their data on — *what survives a
> crash, and how we know* — is delegated wholesale to the backends, exposes no
> knob, makes no documented guarantee, and is untested. This is the defining
> property of a database versus a cache. No code lands until the durability
> contract below is decided and a spike measures the fsync cost.

## Problem

A `commit()` that returns `Ok` should mean something specific about what is on
disk if the power dies one instruction later. Today Slate cannot answer that
question, for three independent reasons:

1. **No durability knob, no stated guarantee.** Every commit takes whatever the
   backend's default write path does — the user can neither tighten it (fsync
   per commit) nor loosen it (batch for throughput), and nothing documents which
   they're getting.
2. **The crash-recovery story is asserted, never tested.** We lean on RocksDB's
   WAL and redb's CoW B-tree for recovery, but never validate that *Slate's own*
   cross-structure invariants — record ↔ index ↔ unique-slot — survive a
   kill-mid-commit.
3. **There is no way to check integrity.** If those structures ever drift, a
   query returns a wrong count and nothing detects it.

These are one class of gap — "can I trust my data after a crash?" — so they
share an RFC, but they decompose into three independently shippable threads (A,
B, C below).

## Current state

### A — durability is a black box

`Store` / `Transaction` (`crates/slate-store/src/store.rs:23-89`) expose no
durability surface at all. `Transaction::commit(self)` (`store.rs:87`) takes no
argument; there is nowhere to say "fsync this one."

- **RocksDB** — `RocksStore::open` (`crates/slate-store/src/rocks/store.rs:18-33`)
  opens with `Options::default()` plus `create_if_missing` /
  `create_missing_column_families`. No `WriteOptions`, so every commit uses
  rocksdb's defaults: WAL **on**, `sync` **off** — the WAL is written but flushed
  lazily by the OS, so a power cut can lose the last writes that were `Ok`-acked.
  `disable_wal` is never touched either. None of `sync`, `disable_wal`, or
  per-commit overrides are reachable.
- **redb** — `RedbStore::open` (`crates/slate-store/src/redb_store/store.rs:17-23`)
  uses `Database::create(path)` and never calls
  `WriteTransaction::set_durability`, so it takes redb's default and never
  exposes `Durability::{None, Eventual, Immediate}`.
- **MemoryStore** — ephemeral by definition; out of scope here (its durability
  story is the separate write-behind-flush roadmap item).

### B — recovery is trusted, not tested

The only recovery-adjacent code we own is the index-encoding migration
(`crates/slate-engine/src/kv/migrate.rs:24-100`): on open, if the stored encoding
version is behind, it rebuilds every collection's `i` entries from the records in
one atomic transaction. It is carefully reasoned (records are the source of
truth; a failure rolls back and retries next open) — but it has **no crash
test**. More broadly, the test suite (≈463 tests across the workspace) is
entirely example-based: no `proptest`/`quickcheck`, no `cargo-fuzz`, no `loom`,
and crucially **no kill-during-commit → reopen → invariant-check harness**. The
[Raw BSON Robustness RFC](./rawbson-robustness.md) makes the same observation
about the byte-scanner: line coverage proves a line *ran*, never that we fed it
the input that breaks it. Crash safety is the same gap one layer up.

### C — invariants are assumed, not checkable

The engine maintains several cross-structure invariants — every live record has
its `i` index entries; every `u` unique-slot has exactly one owner (the
[Unique Indexes](../roadmap.md) slot-stealing proof depends on it); the TTL entry
matches the record. There is no `verify()` that walks these and reports drift, so
a latent bug (the boundary mis-decode the string-index fix repaired was exactly
this shape — an index undercount versus a full scan) is invisible until it
corrupts a result.

## Thread A — a durability knob with a documented guarantee

Add a `Durability` setting on `DatabaseBuilder`, threaded to each backend's
native control, and **document what a committed transaction guarantees** at each
level. Strawman three levels that map cleanly onto both persistent backends:

| Slate level | RocksDB | redb | Guarantee on `Ok(commit)` |
|---|---|---|---|
| `Strict` | `WriteOptions::set_sync(true)` | `Durability::Immediate` | fsync'd; survives power loss |
| `Buffered` (default) | default WAL, no sync | `Durability::Eventual` | survives process crash, not power loss |
| `Relaxed` | `disable_wal` / no sync | `Durability::None` | survives neither; fastest |

The knob lives at two scopes: a builder default and an optional per-transaction
override (`db.begin_with(Durability::Strict)`), so a hot ingest path can run
`Buffered` while a money-moving commit asks for `Strict`. This needs one new
method on the `Transaction` trait (a durability hint consumed at `commit`) — the
first change to that trait's shape, so it deserves the spike's scrutiny.

**Open question for the spike:** is the per-transaction override worth the trait
change, or is a per-`Database` default enough for v1? Measure `Strict` vs
`Buffered` commit throughput on each backend first — the answer decides whether
the knob even needs to be granular.

## Thread B — a crash-test harness

The substance, regardless of A. Ordered by leverage:

1. **Kill-during-commit → reopen → invariant-check.** A test harness that drives
   a workload, `SIGKILL`s (or, for redb/in-process, aborts at a chosen syscall
   boundary), reopens, and asserts: (a) the DB opens, (b) every committed txn is
   wholly present or wholly absent (atomicity), (c) the Thread-C invariants hold.
   This is the test that actually validates the recovery we currently assume.
2. **Property/fuzz pass over key encoding + index maintenance.** Random document
   + mutation streams, asserting record ↔ index ↔ unique-slot consistency after
   each commit — the same differential-against-an-oracle idiom the repo already
   uses (`numeric_key`'s SplitMix64 fuzz, the Cosmos golden replays). Mirror that
   style; stay dependency-light unless the spike shows a hand-rolled generator
   lacks reach.
3. **Migration crash test.** Specifically kill `migrate_index_encoding`
   mid-rebuild and assert the version never half-advances.

## Thread C — integrity verification

An engine-level `verify(cf, collection) -> IntegrityReport` that walks the
records and recomputes the expected `i` / `u` / TTL entries, diffing against
what's stored. This is the read-only half; a `repair` that rebuilds indexes from
records (records are the source of truth — the migration already does exactly
this rebuild) is the natural follow-on and reuses the migration's reindex path.
Surface it on the public API and as a CLI `.verify` command. This is also the
assertion oracle Thread B's harness calls after each crash.

## Recommendation

Sequence **B → A → C**, because the crash harness is pure upside and unblocks the
rest:

1. **Spike: build the kill/reopen harness (B1) and run it against the code as-is.**
   It either surfaces real recovery bugs or proves the backends carry us — either
   way it becomes the regression net for A and the oracle for C. In the same
   spike, measure `Strict` vs `Buffered` commit cost per backend.
2. **Thread A** — land the `Durability` knob and, decided by the spike's numbers,
   the per-transaction override. Document the guarantee table; this is the
   user-facing headline.
3. **Thread C** — `verify()` + `repair()`, with C as B1's assertion oracle.
4. **Thread B2/B3** — the property/fuzz pass and migration crash test, once the
   harness and `verify()` exist to lean on.

## Cosmos, for reference only

Durability and crash recovery are *local-store* concerns; the Cosmos oracle has
nothing to say about them (it validates which rows a query returns, not what
survives an fsync). We pick guarantees that fit slate's three backends, not
Cosmos's hosted replication model. This is squarely "extend beyond the oracle."

## Non-goals

- No replication, no multi-node consensus, no point-in-time recovery from a WAL
  archive — those are server-database concerns, not embedded.
- No new on-disk format; the `Durability` knob only chooses *when* the backend
  flushes, never *what* it writes.
- MemoryStore persistence is out of scope — it's the separate write-behind-flush
  roadmap item.
- Not chasing a specific coverage number; the crash harness and `verify()` are
  the deliverables, not a percentage.
