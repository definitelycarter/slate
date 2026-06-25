# RFC: On-Disk Format Versioning

> **Status: v1 implemented.** The lone index-encoding version marker is now a
> small `_sys_` format registry (`kv/formats.rs`): a `Format` enum
> (`index_encoding`, `catalog`), a shared `refuse_if_too_new` gate, and a
> `check_and_migrate_formats` open pass that runs in place of the old
> index-encoding-only migration. The catalog carries a version
> (`CATALOG_VERSION = 1`, store-level marker + a `#[serde(default)]`
> `CollectionMeta.version`); a too-new store on disk refuses cleanly with the
> typed `EngineError::UnsupportedFormatVersion`. The **record-format version is
> design-reserved** (documented seam, no marker stamped — the tag byte stays the
> backstop), per the scope discipline below. Surfaced in the same "what's missing
> for a *proper embedded database*" survey as the
> [Database Hardening](../roadmap.md#database-hardening--proposed) track. Slate
> persists three independent on-disk formats — index entries, record blobs, and
> the catalog — but only *one* (index encoding) carried a version and a migration
> path. The other two had no compatibility story, so the first time either needed
> to evolve, there was no "newer binary opens an older file, or refuses cleanly"
> mechanism — versioning would be retrofitted under pressure, against
> already-written user data. This RFC generalises the precedent that already
> exists, before it is needed a second time. No format changes for their own sake:
> the deliverable is the *contract* and the seam, not new encodings.

## Problem

A database outlives the binary that wrote it. The moment a user upgrades Slate and
reopens a store written by an older build — or, worse, downgrades — the engine
needs a defined answer to "can this binary read this file?" Today it can answer
that for index entries and nothing else:

1. **Two of three formats are unversioned.** The record blob and the catalog carry
   no version field. Evolving either (compression, a new header field, a changed
   catalog schema) has nowhere to record *which* layout is on disk.
2. **No uniform compatibility contract.** "Newer binary migrates an older file
   forward, or refuses cleanly with a legible error, but never silently
   mis-reads" is implemented once, ad hoc, for index encoding. It is not stated as
   a rule the other formats follow.
3. **The first evolution becomes a fire drill.** The next catalog field (compound
   indexes, partial indexes, and multikey are all *proposed* and all add catalog
   state — see those RFCs) lands with no way to distinguish an old catalog from a
   new one except by hoping serde tolerates it. Retrofitting a version dimension
   onto data already in the field is the hardest possible time to do it.

These are one class of gap — "can a newer binary safely open an older store?" — so
they share an RFC.

## Current state

### A — index encoding *is* versioned (the precedent to generalise)

`crates/slate-engine/src/kv/migrate.rs` is the model. A `u8` version
(`INDEX_ENCODING_VERSION`, `migrate.rs:30`) is stored under a reserved `_sys_`
meta key (`m\0index_encoding_version`, `migrate.rs:35`) whose `m` tag is inert to
`Key::decode`. On open, `migrate_index_encoding` (`migrate.rs:47-62`) peeks the
stored version read-only; if it is behind, it rebuilds the affected entries in one
atomic transaction and stamps the new version, so a crash mid-migration rolls back
and retries rather than half-advancing. A store written before versioning existed
reads as version `0` (`migrate.rs:68-75`) and is migrated forward. This is exactly
the right shape — it is simply applied to only one of the three formats.

### B — the record blob has a tag, not a version

The record wire format (`crates/slate-engine/src/encoding/record.rs:6-14`) is a
one-byte tag (`TAG_NO_TTL = 0x00` / `TAG_TTL = 0x01`) followed by an optional TTL
and the BSON bytes. `Record::from_bytes` (`record.rs:98-120`) rejects an unknown
leading byte with `MalformedRecord("unknown tag: 0x..")` (`record.rs:113-117`).

That fail-closed behaviour is good — a newer record never gets silently
mis-parsed. But the tag byte is *semantic* (does this record carry a TTL?), not a
*version*. The two concerns are conflated in one byte. The next record-format
change — say, a compressed body or an added header field — has no home except
burning another tag value, and an old binary meeting it sees "unknown tag" and
refuses with no notion that a *migration* might exist. There is no version
dimension to branch on.

### C — the catalog is plain serde-BSON with no version

`CollectionMeta` (`crates/slate-engine/src/kv/mod.rs:24-27`) is a serde struct
serialized with `bson::serialize_to_vec` on create (`catalog.rs:198`) and read back
with `bson::deserialize_from_slice` on load (`catalog.rs:53`), surfacing any
mismatch as a generic `invalid collection meta` (`catalog.rs:54`). Index config is
worse — uniqueness is a single leading byte tested as `value.first() == Some(&1)`
(`catalog.rs:59-76`), an ad-hoc one-bit format with a "legacy/empty values are
non-unique" rule and no version at all.

serde-BSON is *additively* tolerant: a new **optional** field deserializes fine
against old data, and old data tolerates a removed field. But a new **required**
field, a renamed field, or a changed meaning of an existing field mis-reads
silently or fails with the generic error above — with no way to say "this catalog
was written by format v2; this binary only understands v1." The catalog is both
the weakest link *and* the format most likely to change next.

## Design — one versioning contract, reserved across all three formats

The deliverable is a single rule plus the seams to honour it, not three new
encodings.

**The contract.** Every persisted format declares a version. On open, for each
format, the engine:

- **matches** — proceeds normally;
- **older on disk** — migrates forward (the `migrate.rs` pattern: rebuild from the
  unambiguous source where possible, atomically, idempotent on retry), then stamps
  the new version;
- **newer on disk** — refuses cleanly with a typed error naming both versions
  (`store written by format v{N}; this binary supports v{M} — upgrade Slate`),
  never a silent mis-read.

**One format registry.** Generalise the lone `INDEX_ENCODING_VERSION_KEY` into a
small set of versioned markers in the existing `_sys_` `m\0…` meta keyspace — one
per format (`index_encoding`, `record`, `catalog`). The open-time check
(`migrate_index_encoding` today) becomes a `check_formats`/`migrate_formats` pass
that validates every marker, migrates what it can, and refuses what it can't,
before any transaction is served.

**Record.** Reserve a record-format version. Two viable placements, to settle in
the spike: (a) a format-version marker in the `_sys_` registry that gates how
`from_bytes` interprets the tag space, keeping the hot per-record path a single
byte; or (b) widen the record header itself. Prefer (a) — it adds no per-record
bytes and reuses the registry — and keep `from_bytes`'s fail-closed unknown-tag
rejection as the backstop. The point is only to give the *next* record format a
defined place to be recorded and a defined refusal when it is too new.

**Catalog.** Add an explicit `version` to `CollectionMeta` and to the index-config
value (subsuming the ad-hoc uniqueness byte under a real header). State the
compatibility rule: an unknown-newer catalog version refuses cleanly; an older one
is upgraded on open. This is the highest-leverage piece, because the catalog is
what compound/partial/multikey indexes will extend first.

## Scope discipline

This is **reserve the dimension and write the contract**, not build a migration
framework for formats that do not exist yet. Exactly one real migration exists
(index encoding); inventing a generic plugin system for hypothetical future ones
is the YAGNI trap. The concrete v1 deliverables are small: the catalog `version`
field plus its refuse-cleanly path, the generalised `_sys_` registry read, and the
typed "too new to open" error. The record-format version is *design-reserved* —
specified here, implemented when a second record format actually needs it, so the
day it does, the seam and the contract are already in place.

## Relationship to other RFCs

- The [Durability & Crash Safety](./durability-and-crash-safety.md) RFC already
  names a **migration crash test** (its Thread B3) and an integrity `verify()`;
  both apply directly here — a format migration is the riskiest open-time write,
  and `verify()` is the natural post-migration assertion.
- That RFC lists "no new on-disk format" as a non-goal *for the durability knob*;
  this RFC is the companion that owns format evolution when it does happen.
- [Compound Indexes](./compound-indexes.md), [Partial Indexes](./partial-indexes.md),
  and [Multikey (Array) Indexes](./multikey-indexes.md) are the first catalog-format
  consumers — each adds catalog state and is a reason to land the catalog version
  field before, not after, them.

## Cosmos, for reference only

On-disk format is a purely local concern; the Cosmos oracle has nothing to say
about it (it validates which rows a query returns, not how bytes are laid out or
versioned). This is squarely a local-store decision.

## Non-goals

- No format change for its own sake — the deliverable is the version dimension and
  the migrate-or-refuse contract, not new encodings.
- No generic migration *framework* — one real migration exists; generalise the
  contract, not a plugin system (see [Scope discipline](#scope-discipline)).
- No cross-version *wire* compatibility — Slate is embedded and single-process;
  there is no network protocol to version (see the README's embedded scope).
- MemoryStore is out of scope — it is ephemeral, so it has no on-disk format to
  version.
- No downgrade migrations (newer → older rewrite). The contract for a too-new file
  is a clean refusal, not a backward rewrite.

## Recommendation / spike

A small spike, leaning on the existing `migrate.rs` precedent:

1. Generalise the `_sys_` version-marker read into a `check_formats` pass and add
   the typed "too new to open" error.
2. Add the catalog `version` field and its refuse-cleanly path — the
   highest-leverage, soonest-needed piece.
3. Settle record-format version *placement* (registry vs header) on paper and
   reserve it; defer the implementation until a second record format exists.
4. Add a migration crash test (shared with the Durability RFC's Thread B3):
   kill mid-migration, reopen, assert the version never half-advances.
