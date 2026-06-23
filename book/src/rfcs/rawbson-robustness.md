# RFC: Raw BSON Robustness & Malformed-Input Contract

> **Status: proposed.** Surfaced while consolidating the engine's raw-BSON
> reading into `slate-rawbson` (the `for_each_path_value` / `extract` /
> TTL-scan move). `cargo llvm-cov` reports the crate at **96.6% region /
> 97% line** coverage, but that number hides the gap this RFC is about: the
> dangerous byte-walking paths run *green on valid input* while being untested
> — and undefended — against malformed input. No code lands until the decision
> below is made and a spike measures the hot-path cost.

## Problem

`slate-rawbson` is the leaf crate every layer leans on to read raw documents:
the storage engine (`RawField::get_value` on validated `RawDocument` bytes),
expression evaluation (`raweval` calls `RawField::get` **once per row** — the
hot path), and mutation (`raw_merge`). It is small, `#![forbid]`-clean, and
well covered on the happy path. It is also pure index-math-free byte
manipulation, where a wrong length field is a panic or an out-of-bounds read,
not a wrong answer.

The coverage number says we are fine. We are not, in one specific way: **line
coverage proves a line ran on *some* input, never that we fed it the input that
breaks it.** For this crate that distinction is the whole ballgame.

## Current state

### The scanner trusts every length field

`skip_bson_value(type_byte, bytes, pos) -> Option<usize>` returns the offset
just past a value. Today:

- **4 of 12 arms bounds-check** — String, Document, Array, Binary verify the
  4-byte length *header* fits (`pos + 4 > bytes.len()`), and there are
  `skip_truncated_{string,document,binary}` tests for exactly those.
- **8 fixed-width arms do not** — Double, ObjectId, Bool, DateTime, Int32,
  Timestamp, Int64, Decimal128 return `Some(pos + N)` with no check. There is
  **no** `skip_truncated_objectid` / `_int32` / `_double` test. They read green
  because the happy path exercises them.
- **Even the checked arms only guard the header, not the result.** The String
  arm verifies `pos + 4` fits, then returns `pos + 4 + len` without verifying
  *that* fits. So no arm guarantees the returned offset is in-bounds.

Concretely, `skip_bson_value(0x07 /* ObjectId */, &[0; 5], 0)` returns
`Some(12)` — twelve, into a five-byte buffer.

### `RawField::value()` panics on truncation

Every arm slices `self.bytes[value_start .. value_start + N]` — an
out-of-bounds slice **panics** before the trailing `try_into().ok()?` ever runs.
The String arm computes `value_start + 4 + len - 1`, which **underflows `usize`**
when `len == 0` (a valid empty string has `len == 1`, the nul; `0` only appears
on corruption). So malformed bytes reach a panic, not an `Err`.

### The contract was never decided

In practice the engine and eval always pass bytes from a `bson::RawDocument`,
which is fully validated at construction — so today's callers are safe. But
`skip_bson_value` and `RawField::get`/`get_path`/`get_value` are **`pub` and
take raw `&[u8]`**. Nothing documents "valid BSON only," nothing pins the
behavior on bad input, and corrupt-on-disk bytes (a torn write, a backend bug)
arrive through the same door as a validated document.

### Coverage gaps that are real but minor

For completeness, the genuinely-unexecuted *product* lines llvm-cov flags are
small: the `Decimal128` arm of `value()`, two malformed guards in `scan_field`
(short header, field name with no nul), and `RawMergeError`'s `Display` /
`From<bson::error::Error>`. Worth closing, but not the point of this RFC.

### `raw_merge` is single-field-only tested

Every `merge.rs` test mutates one field. The correctness-critical path — an
early splice shifts later fields' byte offsets, so the next `locate` must
re-scan against the rewritten buffer — has **no** multi-field test. The
offset-invalidation logic is exactly the kind of thing that is "obviously
correct" until a fuzzer disagrees.

## The decision

What is `slate-rawbson`'s contract when handed bytes that are not valid BSON?
Three doors:

1. **Document "valid BSON only; may panic otherwise."** Zero runtime cost. Adds
   `# Safety`/contract docs to the `pub` raw-bytes entry points and tests that
   assert the *valid-input* invariants. Honest, but leaves a `pub` API that
   panics on corrupt storage — and panics cross FFI/uniffi badly.
2. **Harden to `Option::None`.** Bounds-check the 8 fixed-width `skip` arms and
   guard `value()`'s slices; return `None` on truncation. Cheap and total —
   but `None` from `get_value` already means "absent / null," so this
   **silently conflates corrupt-bytes with missing-field**, masking the very
   corruption we want to notice.
3. **Harden to a typed error.** Same checks, but the byte-level fallible API
   distinguishes `Truncated`/`BadLength` from "absent." Strongest signal;
   costs an error type and a small ripple at call sites that currently
   `?`-propagate `None`.

These are not mutually exclusive across layers: the *internal* scan primitive
can stay `Option`-based (door 2) while the *public, raw-`&[u8]`* entry points
surface a typed error (door 3). The spike exists to choose, and to measure
door 2/3's cost on the per-row scan path before committing.

## Robustness test plan (how we land it, regardless of door)

The decision is small; the test pass is the substance. Ordered by leverage:

1. **Randomized differential against the `bson` crate as oracle** — the highest
   value add for byte code, and it matches the repo idiom (`numeric_key`'s
   SplitMix64 fuzz vs. its `compare_bson` oracle; the cosmos golden replays).
   Generate random documents spanning every element type — nested docs, arrays,
   duplicate keys, empty doc/array/string, all numeric extremes — then assert,
   for every field, that `RawField::get(bytes, name).value()` equals
   `RawDocument::get(name)`, and that `for_each_path_value` matches a reference
   walk built on the `bson` crate. This guards the *whole* scanner, not one arm.
2. **`raw_merge` property test** — random `old` + random `update`, compare the
   merged bytes against a reference `$set` applied via `bson::Document`. Locks
   the multi-field offset-shift path that single-field tests can't reach.
3. **Per-type truncation tests** — one `skip_truncated_*` and one
   `value()`-on-truncated per fixed-width type, plus the `len == 0` String
   underflow. These are the tests that *pin* whichever door we pick (panic vs.
   `None` vs. typed error).
4. **Cleanup** — `Decimal128` `value()`, the two `scan_field` guards, and the
   `RawMergeError` `Display`/`From` paths. Trivial, closes the llvm-cov gaps.

## Open questions (for the spike)

- **Hot-path cost.** `raweval` calls `RawField::get` per row. What do 8 extra
  bounds checks in `skip_bson_value` cost on the engine/eval benches? If it's
  noise, door 2/3 is free; if not, the contract may stay door 1 on the internal
  path and door 3 only at the public boundary.
- **`None`-conflation.** Is silently treating corruption as "absent" ever
  acceptable, or does the public boundary always owe a typed error? (Leaning:
  typed error at the boundary.)
- **Fuzz reach.** Does a hand-rolled SplitMix64 generator give enough type/shape
  diversity, or is this worth a dev-only `arbitrary`/`proptest` dependency?
  (Leaning: stay dependency-free, mirror `numeric_key`.)

## Sequencing

1. **Spike** — write test plan item 1 (the differential oracle) and run it
   against the *current* code. It will either surface real scanner bugs or
   prove the happy path is sound; either way it's the safety net for any
   hardening. Measure the bench delta of a prototype bounds-check pass.
2. **Decide the door** from the spike's perf + the `None`-conflation call.
3. **Land** the chosen contract + test plan items 2–4, in one robustness pass.

## Non-goals

- No new wire format, no API surface beyond a possible error type.
- Not chasing 100% line coverage for its own sake — the test-scaffolding and
  error-formatting "gaps" are noise.
- No defensive validation *inside* the engine/eval call paths, which already
  hold validated `RawDocument`s — the cost belongs only where raw `&[u8]`
  enters.

## Recommendation

Treat the **differential oracle (item 1) as a spike we run now** — it's pure
upside (catches latent bugs, becomes the regression net) and commits us to
nothing. Use its perf measurement to choose between door 2 and door 3, with a
prior toward **door 3 at the public boundary, door 2 internally**. The merge
property test and truncation tests follow once the door is chosen.
