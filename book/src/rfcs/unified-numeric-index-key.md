# RFC: Unified Numeric Index Key

> **Status: implemented — scheme A (project to `f64`).** The encoder + property
> tests (the spike) and the full index-path integration (Phase 2) have landed and
> are green; numeric `Eq`/`Range` are now tight seeks. Two follow-ups were
> deliberately deferred — see [Deferred work](#deferred-work).

## Problem

Index keys store a value's *sortable bytes* with **no type tag in the key** (the
`ElementType` lives in the entry metadata). Each numeric type encodes the **same
logical value to different bytes**, so a single seek can only find one type. Take
the value `5`:

```
encode_i32_sortable(5)   = (5 ^ 0x8000_0000).to_be_bytes()  = [80, 00, 00, 05]
encode_i64_sortable(5)   = (5 ^ 0x8000_…).to_be_bytes()     = [80, 00, 00, 00, 00, 00, 00, 05]
encode_f64_sortable(5.0) = flip(0x4014_…)                   = [C0, 14, 00, 00, 00, 00, 00, 00]
```

`5 == 5L == 5.0` must all match `WHERE x = 5`. But a tight `Eq` seek lands on
exactly one of those byte strings and misses the other two. So the executor's
`needs_full_scan` routes **every numeric `Eq` and numeric-bounded `Range`** to
`IndexRange::Full` + a `CoercingFilter` that re-compares *every entry in the
field* with `compare_bson`.

The cost: **a numeric equality loses all selectivity** — `WHERE score = 5` reads
the entire `score` index, not just the `5`s. Strings and bools (one encoding
each) keep the tight seek; numerics are the exception, and they're a common one.

## Current state

- **Indexable numerics are exactly `{Int32, Int64, Double}`.** `from_bson`
  returns `None` for `Decimal128` (and null / arrays / objects / binary), so
  **decimal128 is not indexed today.** `DateTime` is `i64`-encoded but
  semantically distinct.
- **Per-type sortable encodings**, fixed width, no length suffix:
  `Int32` = 4 B (sign-flip, big-endian), `Int64` = 8 B, `Double` = 8 B
  (IEEE-754 sortable transform). Only variable-width types (strings) carry the
  trailing `u32` value-length suffix from the boundary fix.
- **The type tag is already stored** per entry in the index metadata
  (`IndexEntry` carries the `ElementType`). Nothing in the key uses it; it exists
  for record reconstruction and boundary resolution.
- **The executor special-case**: `needs_full_scan` (numeric → `true`) →
  `IndexRange::Full` + `CoercingFilter::{Eq,Range}` → per-row `compare_bson`
  recheck. The engine's `scan_index` therefore never sees a numeric `Eq`/`Range`.

## Decision: key by the `f64` projection

The index is required to return the same rows as a full scan + `compare_bson` —
its own doc says so (*"re-exported so the index scan's cross-type post-filter
agrees with evaluation rather than re-deriving its own numeric rules"*). And
`compare_bson` compares **every** numeric type by projecting through `as f64`:

```rust
// slate-eval  eval.rs
(Scalar::Num(x), Scalar::Num(y)) => num_f64(x).partial_cmp(&num_f64(y)),   // :322
Num::Int(i) => *i as f64,                                                   // :410
```

This is deliberate, not incidental: `decimal_to_f64`'s comment calls it *"slate's
single f64 number tower (matching Cosmos's one-number model)."* Arithmetic
(`Add`/`Sub`/`Mul → Double`) and the `SUM`/`AVG` accumulators are f64 too. So
`compare_bson` already reports `Int64(2⁵³)` and `Int64(2⁵³+1)` as **equal** (they
share an `f64`).

That **decides the encoding**: the canonical key is the **`f64` projection** of
the value — `encode_f64_sortable(value as f64)` — an 8-byte, fixed-width,
suffix-free key on which all numeric types collapse:

1. **Equal values (under the f64 oracle) produce identical bytes** — `Int32(5)`,
   `Int64(5)`, `Double(5.0)`, and even `Int64(2⁵³)`/`Int64(2⁵³+1)` share one key.
2. **Byte order is numeric (f64) order** — a byte range is a numeric range.

Then numeric `Eq` is a tight seek (it lands all types at once), numeric `Range` is
an exact byte range, and the executor's numeric special-case disappears (see
[The payoff](#the-payoff)). The **type tag (already stored)** reconstructs the
original BSON type for *covered* reads only; the non-covered path uses the index
for doc-ids and never reads it. (Mongo's KeyString is the same shape — a collapsed
numeric comparison key + separate "type bits"; slate already had the type-bits
half.)

**Validated by the spike** (`numeric_key.rs`): a boundary-corpus property test plus
a 100k-pair SplitMix64 fuzz confirm `encode(a).cmp(encode(b))` and `encode(a) ==
encode(b)` reproduce `compare_bson`'s order and equality across cross-type `5`s,
signed zero, the 2⁵³ boundary, type extremes, and infinities. Two edges surfaced
and are handled: **`-0.0` normalises to `+0.0`** (they compare equal, so must
share a key) and **`NaN` is not keyed** (`compare_bson` treats it as incomparable,
so it can never match a sargable predicate).

## Why not an exact (BSON-faithful) key?

An earlier draft proposed an *exact* `i64 ∪ double` canonical order (so `2⁵³` and
`2⁵³+1` stay distinct, Mongo-style). Two reasons that's the wrong target here:

1. **It would contradict the oracle.** The index cannot be more precise than the
   `compare_bson` it must agree with. An exact key would distinguish values
   `compare_bson` calls equal, so the index would return different rows than a
   full scan. Making an exact key *correct* means first making `compare_bson`
   exact — a whole-engine number-model change (comparison **and** arithmetic) that
   diverges from the f64 one-number model slate deliberately adopted. That's a
   separate, deliberate project, not an index feature.
2. **Even on its own terms, "widen to the largest physical type" is unsound.**
   decimal128 (16 B) is the largest type but is *not* a lossless superset of
   `double`: the two use different mantissa radices (`m × 2^e` vs `m × 10^e`), so
   neither set contains the other. The double nearest `0.1` is
   `0.10000000000000000555…` (55 significant digits, past decimal128's 34); decimal
   `0.1` is not an exact double. There is no physical type to cast into without
   losing values. (An exact *encoding* over the dyadic rationals `{i32,i64,double}`
   — all `k/2ʲ` — is possible, but moot: the f64 projection is simpler and is what
   the oracle demands.)

## Storage cost

Scheme A keys every number as the 8-byte f64 sortable form. Value-bytes only (the
prefix `i\0{collection}\0{field}\0` and the length-prefixed doc-id are unchanged
and dominate the entry at ~20–40 B):

| type | today | scheme A |
|---|---|---|
| `Int32` | 4 B | 8 B (**+4**) |
| `Int64` | 8 B | 8 B (unchanged size; bytes re-derived from the f64) |
| `Double` | 8 B | 8 B (unchanged) |
| `Decimal128` | *not indexed* | not indexed (could join later — already f64 in `compare_bson`) |

The only growth is `Int32` fields, +4 B on the value — a fraction of the entry,
and zero for `Int64`/`Double`. The metadata/TTL layout is untouched.

**No migration.** With no users yet there is nothing to preserve: change the
encoding and rebuild/recreate indexes. (If that ever changes, the string-boundary
reindex path — rebuild `i` entries from records under a version bump — is the
precedent.)

## The payoff

This isn't only a numeric-`Eq` speedup; it **removes** machinery:

- `needs_full_scan` numeric branch → gone. Numeric `Eq` becomes a tight seek.
- `CoercingFilter::{Eq,Range}` and the per-row `compare_bson` recheck → gone.
  Numeric `Range` becomes an exact byte range over the unified f64 order.
- The engine's `scan_index` already handles tight `Eq` + exact `Range`; numerics
  simply start flowing through it like strings and bools.

So the special case that opened this investigation doesn't get *optimized* — it
**stops existing**. Net: less executor code, one scan path for all scalar types,
and selectivity restored on the single most common index predicate.

## Round-tripping / covered reads

The only consumers that reconstruct a number *from the key* are **covered**:

- **Covered projection** (`SELECT score FROM nba WHERE score = 5`, `score`
  indexed) — the value comes from the index, so decode the f64 and read the
  **type tag** to emit `Int32(5)` vs `Double(5.0)`.
- **Covered aggregates / `ORDER BY` value emission** — same.

Reconstruction is **f64-precision**: exact for every value `|v| ≤ 2⁵³`, and
f64-rounded for larger `Int64` (the key already lost those low bits at write
time). This is the same line the f64 tower already draws everywhere else — `find` /
`SELECT *` stay exact because they read the **record**; computed and now covered
positions are f64. **Non-covered scans never reconstruct** — they use the index
for doc-ids, fetch records, and return original BSON.

## Cosmos / Mongo, for reference only

- **Mongo (WiredTiger KeyString)** — collapses all numerics into one
  order-preserving comparison key (`5`, `5L`, `5.0` → identical bytes) and stores
  separate *type bits* for reconstruction. It keeps numbers **exact** (i64 and
  double compared exactly), which is why KeyString is intricate. slate is *not*
  matching this — its comparator is f64.
- **Cosmos** — models every number as IEEE-754 `double`. This is what slate's eval
  layer independently is. We are not adopting f64 *for Cosmos parity* — slate
  already chose it in `compare_bson`/arithmetic; the index is just catching up.

## Resolved questions & remaining scope

- **NaN** — resolved: *not keyed* (`compare_bson` says incomparable).
- **Signed zero** — resolved: `-0.0` normalises to `+0.0` (and `Int(0)` shares it).
- **Fixed vs variable encoding** — moot: scheme A is fixed 8 B.
- **decimal128** — excluded for now; it already projects to f64 in `compare_bson`,
  so it could join the index later with no new machinery.
- **DateTime** — stays on its own `i64` encoding (a timestamp, not a member of the
  numeric line). Do not fold it in.
- **Unique index** — decided: unique (`u`) entries will **collapse** to the f64 key
  too, so `5`/`5L`/`5.0` are one value (matching `compare_bson`, the `i` index, and
  Cosmos). Shipped behavior is still per-`(type, value)`; the decision, its rationale
  (slate already chose f64; Cosmos is pure f64 and owns the 2⁵³ ceiling; Mongo's
  exact model considered and deferred), and the string-key guidance live in
  [Unique Indexes → Numeric uniqueness across types](./unique-indexes.md#numeric-uniqueness-across-types).
- **Exact large-int covered reads** — open *only if* slate ever needs exact
  `Int64 > 2⁵³` in computed/covered positions; that's the engine-wide
  exact-number project (rework `compare_bson` + arithmetic), not this index.

**v1 scope:** `{Int32, Int64, Double}`; decimal128 excluded; key =
`encode_f64_sortable(value as f64)`; retire the executor numeric special-case;
covered reads decode via the type tag (f64-precision); no migration (rebuild
indexes).

## Implementation

**Spike** (`crates/slate-engine/src/encoding/numeric_key.rs`): `encode_index_f64`
plus the `Bson`/`RawBson` decode helpers, and six tests — cross-type collapse,
oracle-matched byte order & collapse over the boundary corpus, exact round-trip
within 2⁵³, documented f64-rounding beyond it, NaN/non-numeric not keyed, and a
100k-pair fuzz against the f64 oracle.

**Phase 2 — done.** *Write side:* `extract_all`'s regular (`i`) entries project
numerics to the f64 key via `BsonValue::into_index_value`, `fixed_value_len(Int32)`
is now 8, and the index-value decode (`IndexEntry::value`) is split from the
per-type doc_id decode. *Read side:* `encode_index_value` projects numeric bounds
to f64, numeric `Eq` rechecks value-bytes with **no tag** (the f64 key already
collapses the numeric types, which must all match), and the executor's
`needs_full_scan` + numeric `CoercingFilter` are deleted — numerics now flow
through the tight-seek path. Measured: numeric `Eq` **−93%** at 10k rows
(592.7 µs → 41.8 µs), covered numeric `Eq` −90%, numeric ranges −9–13%; no
regressions.

## Deferred work

Two items were intentionally left out of Phase 2, to be picked up separately:

- **Unique-index numeric collapse — decided, not yet implemented.** Only the regular
  (`i`) index projects numerics to f64 today; unique (`u`) entries still encode
  per-`(type, value)`. The [decision](./unique-indexes.md#numeric-uniqueness-across-types)
  is to collapse `u` too, so `5`/`5L`/`5.0` are one value (matching `compare_bson`,
  the `i` index, and the Cosmos oracle, which is itself pure f64). To implement:
  project `u` values through `into_index_value`, settle the type-byte question, flip
  the per-type test, and ship it with the unique-index planner point-get (gated on
  it). The 2⁵³ within-type cost is accepted — it is the f64 model slate already runs,
  and Cosmos's documented stance is to use a string key for exact integers beyond
  2⁵³.
- **`into_index_value` decode-then-reencode.** The write path builds a per-type
  `BsonValue` (`from_raw_bson_ref`) and then decodes it back to a number to
  re-encode as the f64 key — three steps where one would do. Negligible per insert,
  but a micro-opt could project straight from `RawBsonRef` into the f64 key,
  skipping the intermediate per-type encode.
