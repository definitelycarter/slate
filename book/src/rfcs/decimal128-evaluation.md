# RFC: Decimal128 in Query Evaluation

> **Status: implemented.** Extracted from the roadmap; the [roadmap](../roadmap.md)
> tracks status at a glance.

A stored `Decimal128` (`$numberDecimal`) was correct in storage and visible to `find` (raw passthrough) but invisible to per-field evaluation: it projected as missing, never matched a comparison, and made aggregates return nothing. The data was always stored correctly — the gap was on the read/eval side. Surfaced by loading a real `mongoexport` (Atlas `sample_airbnb`, whose `price`/fees are all `$numberDecimal`) via `.seed`.

## Design: one f64 number tower

slate evaluates numbers in a single `f64` tower (Cosmos's one-number model — see [Numbers and comparison](../querying.md#numbers-and-comparison)). A `Decimal128` joins it by its `f64` value, fixed across three layers:

- **`slate-rawbson`** — `RawField::value()` decoded every element type except Decimal128, so per-field access (`c.price`) returned `None`. (`skip_bson_value` already knew the type, which is why whole-doc `find` survived but field access didn't.)
- **`slate-eval` number tower** — `scalar_of_bson`/`as_number`, the raw `scalar_of_raw`/`raw_as_number`, and aggregation's `num_f64` enumerated `Int32`/`Int64`/`Double`; they now read a Decimal128 as its `f64` via a shared `decimal_to_f64`. `type_rank` groups it with numbers, and `MIN`/`MAX` route through `order_bson` so they follow for free.

Contract: comparison and sort use the `f64` value (exact for realistic magnitudes; only *computed* results are `f64`, never stored ones); `SUM`/`AVG` return a double; `MIN`/`MAX` preserve the original `Decimal128`; stored bytes are untouched, so `find` round-trips losslessly. The canonical-string conversion is the dependency-free path.

## Follow-ups

- **Decimal-preserving arithmetic** — a decimal crate (or a manual coefficient/exponent decode) would make `SUM`/`AVG`/arithmetic bit-exact and drop the per-value string allocation. Deferred; the `f64` path is reversible.
- **Date vs string-literal comparison** — a stored `$date` is a real BSON `DateTime`, so `c.when > "2024-01-01"` compares across domains and matches nothing. Documented under [Numbers and comparison](../querying.md#numbers-and-comparison); no fix planned unless we add date literals/coercion.
- **`SUM` over integers renders as a float** (`153354.0` vs Cosmos's `153354`) — same number-model bucket; confirm against hosted Cosmos before changing.
