# RFC: Encryption at Rest

> **Status: proposed.** Surfaced in the same "what's missing for a *proper embedded
> database*" survey as the [Database Hardening](../roadmap.md#database-hardening--proposed)
> track, as its missing companion. That track answered *can I trust my data
> survives a crash* ([Durability & Crash Safety](./durability-and-crash-safety.md)).
> It never answered *is my data protected if someone reads the file*. For a database
> that embeds in a native app on a user's device, the on-disk file is exactly what
> an attacker with device or backup access reads. There is zero coverage today, and
> the default backend (redb) has no native cipher. This RFC's job is to **decide the
> layer and document the guarantee** before writing any crypto — the right answer
> for an *indexed* store is not the obvious one.

## Problem

Slate persists user documents and their indexed values to a plain file. SQLite has
SQLCipher; the RocksDB C++ library has an encryption `Env`; Slate has neither, and
its pure-Rust default backend has no equivalent. Three concrete gaps:

1. **No encryption, no key management, anywhere.** A full-repo grep for any
   cipher, AEAD, or key-handling dependency returns nothing. This is greenfield.
2. **The threat model is undocumented.** Even "we rely on the OS / full-disk
   encryption" is a legitimate answer for an embedded DB — but it is nowhere
   stated, so a user staking sensitive data on Slate cannot know what is and isn't
   protected.
3. **A naive fix would be worse than honest deferral.** The obvious move —
   encrypt record values at the store seam — leaks more than it protects in *this*
   engine, for a structural reason (see [the crux](#the-crux-indexed-values-live-in-keys)).
   Shipping it would be security theater.

## Current state

### No crypto, and the byte seam is value-by-value

The `Store` / `Transaction` traits (`crates/slate-store/src/store.rs:23-89`) are
the point where bytes cross to disk: values flow as `&[u8]` through `put`
(`store.rs:77`) and back through `get` (`store.rs:52`) and the scans. There is no
interception layer and no crypto dependency in the tree.

### Backends: one has no hook, one's hook is unreachable, one is N/A

- **redb (pure-Rust default)** — `RedbStore::open` (`crates/slate-store/src/redb_store/store.rs:17-23`)
  is `Database::create(path)` with no options; redb memory-maps the file with no
  pluggable cipher. **No native encryption exists.** Its `backup` (`redb_store/store.rs:109-114`)
  is a raw `std::fs::copy` of the file — so whatever protects the live file must
  also protect the copy.
- **RocksDB** — `RocksStore::open` (`crates/slate-store/src/rocks/store.rs:18-33`)
  sets only `create_if_missing` / `create_missing_column_families` on
  `rocksdb::Options`. RocksDB's C++ encryption `Env` exists, but the `rust-rocksdb`
  binding does not expose a `set_env`/cipher surface — it is **not reachable**
  without forking the binding.
- **MemoryStore** — in-memory (`crates/slate-store/src/memory/store.rs`), no disk;
  at-rest encryption is **N/A**, and it refuses `backup` outright.

### Open paths take no key

`DatabaseBuilder::open` (`crates/slate-db/src/database.rs:120`) receives an
*already-constructed* store, so any key must be threaded into store construction,
not the builder. The Swift/UniFFI binding opens by path only —
`SlateDatabase::open(path)` (`crates/slate-uniffi/src/db.rs:88-102`) — with no key
parameter. The WASM binding (`crates/slate-wasm/src/lib.rs:103-119`) is
`MemoryStore`-only: ephemeral, and the browser has no secure persistent key store,
so at-rest encryption there would be theater regardless.

## The crux: indexed values live in keys

Slate is an *indexed* store, and that changes the answer. Index entries are keyed,
not valued: an entry is `i\0{collection}\0{field}\0{value_bytes}{doc_id}` — the
indexed field value is **in the key**, and record keys embed the `_id`. Range
scans (the whole point of an ordered index) require those keys to remain
**byte-ordered** on disk.

So encrypting *values* at the store seam — the cheap, backend-agnostic move —
leaves every indexed value and every `_id` in **plaintext** in the keyspace.
Encrypting *keys* too would destroy the ordering that range scans depend on
(order-preserving encryption is weak and explicitly out of scope). The conclusion
is structural: **at-rest encryption for an indexed store wants to live below the
key/value abstraction — whole-file or page-level — not as a record-value wrapper
above it.** This is the single most important input to the decision below, and the
reason the obvious app-level approach is rejected.

## Design alternatives

### 1. Rely on the platform (recommended default for device embeddings)

Lean on OS full-disk / file-level encryption: iOS Data Protection (per-file keys
tied to the passcode and Secure Enclave), FileVault / APFS on macOS, and the
equivalents elsewhere. **Zero crypto code**, and it encrypts *everything* — keys,
values, WAL, the redb backup copy — so the [index-key leak](#the-crux-indexed-values-live-in-keys)
does not arise. On Apple platforms, where Slate's primary embedding lives, this is
genuinely strong. The cost is an honest threat model: it protects data at rest on a
locked/powered-off device, not against a compromised running process, and it trusts
the OS key hierarchy. The real gap today is that this is **undocumented** — making
it the stated, supported story (with the right iOS Data Protection class selected)
is most of the value, at almost no cost.

### 2. Backend-native page encryption (only if OS trust is insufficient)

Encrypt at the storage engine's page layer, below the keyspace — so keys and values
are both protected and ordering is preserved in memory. For RocksDB this means
exposing its encryption `Env`, which requires patching/forking `rust-rocksdb`; for
redb there is no cipher surface at all, so it would mean a fork. High cost,
per-backend, and it pulls in a vetted AEAD and a real key-management story.
Justified only by a threat model that cannot rely on OS encryption (e.g. a shared
host where the OS account is not the trust boundary).

### 3. App-level value encryption (rejected as primary)

A `Store` decorator that encrypts values on `put` and decrypts on `get`. Cheap and
backend-agnostic — but per [the crux](#the-crux-indexed-values-live-in-keys) it
leaves all indexed values and `_id`s in plaintext, so for an indexed DB it is
security theater. Note it *only* fits an opaque-blob use with no secondary indexes,
which is not Slate's shape. Documented here so the option is explicitly closed, not
silently skipped.

## Key management (the harder half)

Encryption is the easy part; the key lifecycle is where embedded DBs actually
differ, and it is per-embedding:

- **Swift/Apple** — key in the iOS/macOS Keychain or derived in the Secure Enclave;
  retrieved before open. This composes with alternative 1 (the OS already does
  this) and would be *required* for alternative 2.
- **CLI / desktop** — derive from a passphrase via a memory-hard KDF (argon2 /
  scrypt); never store the raw key.
- **Browser (WASM)** — no safe persistent key store and no persistent backend; out
  of scope.

If alternative 2 is ever pursued, the open surface needs a key seam — a
`with_encryption(KeyProvider)` on the builder or an `open_encrypted(path, key)`
constructor threaded through the bindings (today `SlateDatabase::open` takes only a
path). Key **rotation** and the redb **raw-copy backup** (`redb_store/store.rs:109-114`,
which inherits the file's protection — fine for OS/page-level, fatal for
value-level) must be in that design.

## Recommendation

1. **Adopt OS-level encryption as the documented at-rest story now.** It is cheap,
   strong on the target Apple platform, and covers keys+values+backup without the
   index-key leak. Write down the guarantee and the iOS Data Protection class —
   that documentation *is* the deliverable for v1.
2. **Reserve a `KeyProvider` seam** on the builder/bindings for a future
   page-level option, but build no crypto until a threat model demands more than OS
   trust.
3. **Spike, only if (2) is greenfield-justified:** assess whether exposing the
   `rust-rocksdb` encryption `Env` is tractable, and prototype the iOS Data
   Protection class selection end-to-end so the documented guarantee is real, not
   aspirational.

## Cosmos, for reference only

Encryption at rest is a hosted-infrastructure property in Cosmos (service-managed
keys), not a query-result property — so the Cosmos oracle is silent here, as it is
for durability. This is a local-store decision driven by Slate's embeddings and
threat model, not by parity.

## Non-goals

- No order-preserving / searchable encryption — it is weak, and the
  [index-key constraint](#the-crux-indexed-values-live-in-keys) is better solved
  below the keyspace.
- No app-level value-only encryption as the primary mechanism (rejected above).
- No encryption for MemoryStore or the WASM binding — ephemeral, and no secure key
  store in the browser.
- No bespoke cipher — if app/page-level crypto is ever built, use a vetted AEAD,
  never hand-rolled primitives.
- No external KMS / HSM integration in v1 — key custody is the OS keystore or a
  passphrase-derived key.
- No field-level / per-document client-side encryption (a different feature with a
  different threat model).
