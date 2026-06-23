# RFC: Unified Numeric Index Key

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

Cosmos/Mongo semantics require `5 == 5L == 5.0` — all three must match
`WHERE x = 5`. But a tight `Eq` seek lands on exactly one of those byte strings
and misses the other two. So the executor's `needs_full_scan` routes **every
numeric `Eq` and numeric-bounded `Range`** to `IndexRange::Full` + a
`CoercingFilter` that re-compares *every entry in the field* with `compare_bson`.

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
  recheck. The engine's `scan_index` therefore never sees a numeric `Eq`/`Range`
  — only the executor's full-scan-plus-recheck.

## Proposal

Encode every number into a **single order-preserving canonical key** so that all
numeric types collapse onto one number line:

1. **Equal values produce identical comparison bytes** — `Int32(5)`, `Int64(5)`,
   `Double(5.0)` all encode to the *same* key bytes.
2. **Byte order is numeric order** — a byte range is a numeric range.

Then a numeric `Eq` is a tight seek (it lands all three types at once) and a
numeric `Range` is an exact byte range — numerics rejoin the string/bool
tight-seek path, and the executor's numeric special-case **disappears** (see
[The payoff](#the-payoff-what-it-deletes)).

The **type tag (already stored)** reconstructs the original BSON type for
*covered* reads. Crucially, reconstruction is needed **only when the index is the
source of a returned value** — i.e. covered projections (and covered aggregates /
`ORDER BY` value emission). On the common non-covered path the index yields
doc-ids, the *record* supplies the values, and the tag is never read. So the tag
is **write-always, read-only-when-covered**.

This is exactly WiredTiger's KeyString design: a unified numeric comparison key
plus separate **"type bits"** for round-tripping. slate already has the type-bits
half; this RFC adds the unified-key half.

## Why "widen to the largest type" doesn't work

The intuitive version — "store every number as decimal128, it's the largest
(16 B), and cast back via the type tag" — **is unsound**, and it's worth being
precise about why, because it shapes the whole design.

decimal128 is the largest *physical* type, but it is **not a lossless superset of
`double`**. The two use different mantissa radices:

- `double` represents `m × 2^e` (53-bit binary mantissa).
- `decimal128` represents `m × 10^e` (34-decimal-digit mantissa).

Neither set contains the other. The double nearest to `0.1` is exactly

```
0.1000000000000000055511151231257827021181583404541015625   (55 significant digits)
```

which **exceeds decimal128's 34 digits** — casting it to decimal128 *rounds*, so
the round-trip is lossy and two distinct doubles can collide. Conversely, decimal
`0.1` (exact, `1 × 10⁻¹`) is *not* an exact double. So there is **no physical type
to cast into** without losing values or ordering.

The fix is an order-preserving **encoding**, not a **cast**. And here the
restricted scope is a gift: the currently-indexed numerics `{Int32, Int64,
Double}` are **all dyadic rationals** (`k / 2ʲ` — integers are `j = 0`, doubles
are `m × 2^e`). A common *exact* total order over dyadic rationals exists. The
*only* type that breaks it is decimal128 (base-10, non-dyadic) — and it's already
excluded from indexing. So scoping decimal128 out isn't a compromise; it's what
makes a clean unified key possible at all.

## Encoding options

| | scheme | exact? | size | notes |
|---|---|---|---|---|
| **A** | all numbers → `double` (8 B) | ❌ lossy for `i64 > 2⁵³` | 8 B fixed | the *Cosmos number model*; abandons BSON int/long fidelity |
| **B** | fixed-width canonical (sign · biased-exponent · significand) | ✅ for `i64 ∪ double` | ~9–10 B fixed | simplest; no length suffix; uniform |
| **C** | variable-length canonical (self-delimiting, Mongo-style) | ✅ | 1–10 B | compact for small ints (the common case); intricate; must self-delimit |

**A** is the radical alternative: adopt Cosmos's "a number is a double" model and
the problem evaporates (one type, one encoding). It's the cleanest code but a
real semantic change — `Int64` values past 2⁵³ stop round-tripping. Flagged for
discussion against the existing *number-model* parity question; **not** the
recommendation here, since slate is otherwise BSON-faithful.

**B** normalizes each value to `(sign, exponent, significand)` with equal values
sharing a representation, then lays them out big-endian with the usual sign/order
flips. `i64` needs up to 63 significand bits (8 B) and exponent 0; `double` needs
an 11-bit exponent — together ≈ 9–10 B fixed, with the sign folded into the high
bit. Fixed width keeps the **no-length-suffix** property.

**C** keeps small integers small (often *cheaper* than today's 4-byte `i32`) but
must be self-delimiting or carry the `u32` suffix (`+4 B`), which erodes the win.

Recommendation: **B for v1** (uniform, suffix-free, easy to property-test), **C
as a follow-up** if index size on small-int fields proves to matter.

## Storage cost analysis

Value-bytes only (the prefix `i\0{collection}\0{field}\0` and the
length-prefixed doc-id are unchanged, and typically dominate the entry at
~20–40 B):

| type | today | **B** (~10 B fixed) | **C** (variable) |
|---|---|---|---|
| `Int32` | 4 B | 10 B (**+6**) | 1–5 B (small ints often **< 4**) |
| `Int64` | 8 B | 10 B (**+2**) | 1–9 B |
| `Double` | 8 B | 10 B (**+2**) | 3–10 B |
| `Decimal128` | *not indexed* | out of scope | out of scope |

Because the value is a *minority* of each entry (prefix + doc-id dominate), even
the worst case — an `Int32`-heavy field under scheme **B** — grows total index
size by roughly **10–20 %**, and `Int64`/`Double` fields by low single digits.
Scheme **C** is roughly neutral-to-smaller on typical small-int fields. No new
per-entry suffix under **B** (fixed width). The metadata/TTL layout is untouched.

**Migration is a full reindex** — rebuild every collection's `i` entries from the
records under a version bump, identical in shape to the string-boundary migration
(records are the source of truth, atomic, rolls back on failure, never
half-migrated). Unique (`u`) entries use the same value encoding and migrate the
same way.

## The payoff (what it deletes)

This isn't only a numeric-`Eq` speedup; it **removes** machinery:

- `needs_full_scan` numeric branch → gone. Numeric `Eq` becomes a tight seek.
- `CoercingFilter::{Eq,Range}` and the per-row `compare_bson` recheck → gone.
  Numeric `Range` becomes an exact byte range over the unified order.
- The engine's `scan_index` already handles tight `Eq` + exact `Range`; numerics
  simply start flowing through it like strings and bools.

So the special case that opened this whole investigation doesn't get *optimized*
— it **stops existing**. Net: less executor code, one scan path for all scalar
types, and selectivity restored on the single most common index predicate.

## Round-tripping / covered reads

The only consumers that reconstruct a number *from the key* are **covered**:

- **Covered projection** (`SELECT score FROM nba WHERE score = 5`, `score`
  indexed) — the value comes from the index, so decode the canonical bytes and
  read the **type tag** to emit `Int32(5)` vs `Double(5.0)` (the comparison bytes
  are deliberately type-collapsed and can't self-identify — that's *why* the tag
  rides separately).
- **Covered aggregates / `ORDER BY` value emission** — same.

**Non-covered scans never reconstruct** — they use the index for doc-ids, fetch
records, and return original BSON. The tag is untouched. (Strings already follow
this: covered reads strip the length suffix; non-covered ignore it.)

## Cosmos / Mongo, for reference only

- **Mongo (WiredTiger KeyString)** — collapses all numerics into one
  order-preserving comparison key (`5`, `5L`, `5.0` → identical bytes) and stores
  separate *type bits* for reconstruction. It does **not** multi-probe; `x = 5` is
  a single seek. slate's type-tag-in-metadata is already the type-bits half.
- **Cosmos** — models every number as IEEE-754 `double` (JSON/JS semantics).
  *One* numeric type means no cross-type problem, no multi-probe, one seek. This
  is scheme **A**, and the open *number-model* parity question.

## Open questions

1. **decimal128.** Keep it excluded (recommended for v1), or define cross-radix
   (base-2 vs base-10) equality/order? `double 0.1` and `decimal 0.1` are
   *different real numbers*; Mongo has documented edge cases here. Out of scope
   until there's a demand.
2. **NaN / ±Infinity.** Where do they sit in the order, and is `NaN == NaN`
   inside an index? (compare_bson's current answer is the oracle to match.)
3. **Signed zero.** `-0.0`, `+0.0`, and `Int(0)` must collapse to one key.
4. **Fixed (B) vs variable (C).** Storage vs simplicity; decide via the spike's
   size measurement on a real numeric corpus.
5. **DateTime** stays on its own `i64` encoding — semantically a timestamp, not a
   member of the numeric line. Do not fold it in.
6. **Unique index.** A unified key makes cross-type numeric uniqueness automatic
   (`5` conflicts with `5.0`). Confirm that's the desired semantic (Mongo says
   yes) and that `u` entries migrate cleanly.
7. **Migration/versioning** mechanics — reuse the string-boundary reindex path
   and bump the index-encoding version.

## Minimal v1 scope

- `{Int32, Int64, Double}` only; decimal128 stays excluded.
- Scheme **B** (fixed-width canonical, suffix-free); **C** deferred.
- Retire `needs_full_scan` numeric branch + `CoercingFilter` + the numeric
  recheck; numerics flow through the engine's tight `Eq` / exact `Range` path.
- Covered reads decode via the existing type tag; non-covered unchanged.
- Reindex migration + version bump (string-boundary precedent).

## Spike

Before committing to the byte format, prototype scheme **B** for
`{i32, i64, double}` and **property-test it against `compare_bson` as the oracle**:

- *order-preservation* — `encode(a) < encode(b)  ⇔  compare_bson(a, b) == Less`,
- *collapse* — `compare_bson(a, b) == Equal  ⇔  encode(a) == encode(b)` (covers
  `5 == 5L == 5.0`, `-0.0 == 0`, the 2⁵³ boundary),
- *round-trip* — `decode(encode(v), tag) == v` for every type,

across a generated corpus spanning the `i64`/`double` exactness boundary, then
**measure index-size delta** on a numeric-heavy collection to settle B-vs-C.
