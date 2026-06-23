# RFC: Transaction & Concurrency Contract

> **Status: proposed.** Surfaced in the "proper embedded database" survey.
> Multi-statement transactions exist and work, but the *guarantee* they make
> differs by backend, is undocumented, and pushes conflict handling onto the
> caller with no help. This RFC is mostly about pinning a contract and adding one
> ergonomic helper — little new machinery, but it's the difference between
> "transactions happen to work" and "transactions are a documented guarantee."

## Problem

`db.begin(read_only)` returns a `Transaction` a caller can drive through many
operations before `commit()` (`crates/slate-db/src/database.rs:193`, `:808`,
`:827`). That much is real and tested. What's missing is the *contract*:

- **No documented isolation level.** A user can't read what they're guaranteed.
- **Write-conflict handling is the caller's problem, with no tooling.** On the
  one backend that detects conflicts (RocksDB, optimistic), a commit can fail
  with a conflict and there is no retry helper — the caller must hand-roll a
  loop, and the conflict isn't even a distinct error variant.
- **The model is uneven across backends** and that unevenness isn't surfaced.

## Current state

### The three backends agree on *reads*, differ on *writes*

(From the storage survey; see also [Storage Layer](../architecture-storage.md).)

- **MemoryStore** (`crates/slate-store/src/memory/`) — readers snapshot
  lock-free via `ArcSwap::load_full()`; writers serialize behind a single
  `Mutex`. Snapshot isolation, read-your-writes, **single writer**.
- **redb** (`crates/slate-store/src/redb_store/`) — native MVCC, single writer
  at a time, concurrent readers on a consistent snapshot. Conflicts impossible
  by construction (writers serialize).
- **RocksDB** (`crates/slate-store/src/rocks/store.rs:11`,
  `OptimisticTransactionDB`) — **multiple** concurrent writers; write-write
  conflicts are detected *at commit* and one side fails. Slate adds **no retry**.

So **snapshot isolation + read-your-writes** is the common guarantee, but the
*write* concurrency model splits: serialize-writers (memory, redb) vs
optimistic-conflict-at-commit (rocks). A correct program written against redb
(never expects a commit conflict) can start failing under load on RocksDB.

### Conflicts have no first-class shape

A RocksDB commit conflict surfaces as a generic `StoreError` wrapped into
`DbError::Store` (`crates/slate-db/src/error.rs`), indistinguishable from an I/O
error, so a caller can't even reliably detect "retryable conflict" to loop on.

### `delete_range` is a documented escape hatch from the transactional model

`Store::delete_range` (`crates/slate-store/src/store.rs:31-39`) operates *outside*
transactions — its own doc-comment warns that in-flight transaction iterators
won't see the deletes and "a transaction could re-insert keys that were just
wiped." It's used for coarse pruning today, but it's a sharp edge in the
concurrency story worth either fencing or documenting as a contract exception.

### No savepoints, no nested transactions, no timeout/deadlock handling

Transactions are flat: begin → ops → commit/rollback. No partial rollback.

## Design

### 1. Write down the contract

Document, in [Database & Bindings](../architecture-database.md), the guarantee
Slate makes *regardless of backend*:

> A transaction observes a consistent snapshot taken at `begin`, sees its own
> writes (read-your-writes), and commits atomically (all-or-nothing). Concurrent
> writers are serialized **or** detected as conflicts at commit; in the conflict
> case `commit()` returns `DbError::Conflict` and the transaction may be retried.

Stating the conflict possibility *as part of the contract on every backend* (even
where it can't currently fire) means correct programs are portable across
backends — a redb-tested app won't break on RocksDB.

### 2. A first-class conflict error + a retry helper

- Add `DbError::Conflict` and have each backend's commit map its
  conflict/busy condition onto it (RocksDB's optimistic conflict today; room for
  redb/memory busy conditions later).
- Add a closure-driven helper that owns the begin/commit/retry loop:

  ```rust
  db.transact(|txn| {
      txn.update_one(cf, "accounts", filter, update)?;
      Ok(())
  })?; // begins, runs, commits; on DbError::Conflict re-runs with backoff
  ```

  This is the single most useful ergonomic addition — it makes the optimistic
  path safe-by-default instead of a footgun, and it's the natural home for a
  bounded retry count + backoff.

### 3. Decide `delete_range`'s status

Either (a) route it through a transaction so it participates in isolation, or
(b) keep it as an explicit out-of-band fast-path and document it in the contract
as the one operation that is *not* transactional. Lean (b) — it exists precisely
because it's cheaper than a transactional range delete — but the contract must
name it.

### 4. Savepoints — defer, but note

Nested transactions / savepoints (partial rollback) are a real gap versus SQLite,
but none of the three backends expose savepoints natively, so this would be an
engine-level emulation (buffer writes, discard a suffix). Defer to its own RFC;
record it here as a known non-goal for v1 so the contract doesn't imply it.

## Recommendation

Land **1 + 2 together** — the contract doc and the `Conflict` variant + `transact`
helper are a single coherent change and the highest-value, lowest-risk slice.
Treat **3** as a one-line contract decision plus a doc note. **4** is explicitly
deferred. No spike needed for 1–3 (no perf-sensitive new path; the helper is a
thin loop); a spike *is* warranted before 4 to prototype write-buffering cost.

## Cosmos, for reference only

Cosmos's concurrency model (ETag-based optimistic concurrency, per-partition
consistency levels) is a *hosted-service* contract, not a local-store one. It's
our oracle for query *results*, not for transaction semantics — we define our own
contract to fit the three embedded backends. The optimistic-conflict shape we'd
document for RocksDB happens to rhyme with Cosmos's ETag retries, which is
reassuring but not the reason.

## Non-goals

- No new isolation level (serializable, etc.); snapshot isolation is the
  contract.
- No savepoints / nested transactions in v1 (deferred to its own RFC).
- No deadlock detection — the serialize-writers backends can't deadlock on the
  store lock, and the optimistic backend fails-and-retries rather than blocking.
- No async API; the contract is for the existing synchronous surface.
- No distributed/2-phase commit — embedded, single-process.
