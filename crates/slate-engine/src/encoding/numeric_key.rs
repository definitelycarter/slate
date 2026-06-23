//! Unified numeric index key (RFC: *Unified Numeric Index Key*).
//!
//! slate is a single-`f64` number-tower engine: `compare_bson` projects every
//! numeric type (`Int32`/`Int64`/`Double`) through `as f64` before comparing
//! (`slate-eval` `eval.rs`: `num_f64(x).partial_cmp(&num_f64(y))`), so the
//! engine's equality and ordering of numbers *are* their f64 projection. An
//! index is required to return the same rows as a full scan + `compare_bson`, so
//! it must key numbers by that same projection. Then `Int32(5)`, `Int64(5)`, and
//! `Double(5.0)` collapse onto one 8-byte key and a cross-type `Eq` becomes a
//! single tight seek — no full scan, no per-row `compare_bson` recheck.
//!
//! Scope is `{Int32, Int64, Double}`. `Decimal128` also projects to f64 in
//! `compare_bson` and could join later, but is not indexed today. The original
//! typed value lives in the record, and the type tag in the index entry, so a
//! covered projection reconstructs the BSON type via [`decode_numeric_index`];
//! that reconstruction is f64-precision, hence lossy only for `Int64` magnitudes
//! past 2^53 — consistent with the rest of the f64 tower (`find` stays exact
//! because it reads the record).

#![allow(dead_code)] // wired into the index path in Phase 2; exercised by tests until then

use bson::Bson;
use bson::spec::ElementType;

use super::bson_value::{decode_f64_sortable, encode_f64_sortable};

/// Normalise and encode an `f64` as the 8-byte index key. `None` for `NaN`
/// (incomparable under `compare_bson` → can never match a sargable predicate, so
/// it is not keyed); `-0.0` collapses to `+0.0` (they compare equal). The write
/// and read paths funnel through this one chokepoint, so a stored key and a seek
/// key for the same value are always byte-identical.
pub(crate) fn encode_index_f64(f: f64) -> Option<[u8; 8]> {
    if f.is_nan() {
        return None;
    }
    let f = if f == 0.0 { 0.0 } else { f }; // -0.0 → +0.0
    Some(encode_f64_sortable(f))
}

/// Project a numeric `Bson` onto the f64 number tower; `None` for non-numeric.
fn numeric_to_f64(value: &Bson) -> Option<f64> {
    Some(match value {
        Bson::Int32(i) => *i as f64,
        Bson::Int64(i) => *i as f64,
        Bson::Double(f) => *f,
        _ => return None,
    })
}

/// The 8-byte index key for a numeric `Bson` bound, or `None` if non-numeric or
/// `NaN`. Used by the read path to seek the key matching a query value.
pub(crate) fn encode_numeric_index(value: &Bson) -> Option<[u8; 8]> {
    encode_index_f64(numeric_to_f64(value)?)
}

/// Reconstruct the original-typed value from a numeric index key and its stored
/// type tag, for covered projections.
///
/// f64-precision: exact for every value with `|v| <= 2^53`, and for larger
/// `Int64` it returns the f64-rounded integer the key already recorded at write
/// time. `None` if `tag` is not a numeric type.
pub(crate) fn decode_numeric_index(key: [u8; 8], tag: ElementType) -> Option<Bson> {
    let f = decode_f64_sortable(key);
    Some(match tag {
        ElementType::Int32 => Bson::Int32(f as i32),
        ElementType::Int64 => Bson::Int64(f as i64),
        ElementType::Double => Bson::Double(f),
        _ => return None,
    })
}

/// Decode an index *value* (key bytes + metadata tag) back to `RawBson`.
///
/// Numeric tags decode the 8-byte f64 key (the unified numeric key) and cast to
/// the stored type — f64-precision, exact within 2^53. Every other type decodes
/// per-type via [`BsonValue::to_raw_bson`]. Distinct from doc_id decoding, which
/// is always per-type — a doc_id is never f64-projected.
pub(crate) fn decode_index_value(tag: ElementType, bytes: &[u8]) -> Option<bson::RawBson> {
    match tag {
        ElementType::Int32 | ElementType::Int64 | ElementType::Double => {
            let f = decode_f64_sortable(bytes.try_into().ok()?);
            Some(match tag {
                ElementType::Int32 => bson::RawBson::Int32(f as i32),
                ElementType::Int64 => bson::RawBson::Int64(f as i64),
                _ => bson::RawBson::Double(f),
            })
        }
        _ => super::bson_value::BsonValue::from_parts(tag, bytes).to_raw_bson(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    /// The numeric comparison `compare_bson` performs: every number via `as f64`,
    /// then `partial_cmp`. A literal mirror of `slate-eval` `eval.rs` (the
    /// `Scalar::Num` arm `num_f64(x).partial_cmp(&num_f64(y))` plus `Num::Int(i)
    /// => *i as f64`), replicated because `slate-engine` does not depend on
    /// `slate-eval`. This is the oracle the encoder must reproduce.
    fn oracle(a: &Bson, b: &Bson) -> Option<Ordering> {
        fn as_f64(b: &Bson) -> Option<f64> {
            match b {
                Bson::Int32(i) => Some(*i as f64),
                Bson::Int64(i) => Some(*i as f64),
                Bson::Double(f) => Some(*f),
                _ => None,
            }
        }
        as_f64(a)?.partial_cmp(&as_f64(b)?)
    }

    /// Boundary corpus: cross-type duplicates, signed zero, the 2^53 exactness
    /// boundary, type extremes, fractionals, and infinities.
    fn corpus() -> Vec<Bson> {
        const TWO_53: i64 = 1 << 53; // 9_007_199_254_740_992
        vec![
            // cross-type duplicates of 5 — must collapse to one key
            Bson::Int32(5),
            Bson::Int64(5),
            Bson::Double(5.0),
            // signed zero across types — must collapse
            Bson::Int32(0),
            Bson::Int64(0),
            Bson::Double(0.0),
            Bson::Double(-0.0),
            // small magnitudes / signs
            Bson::Int32(1),
            Bson::Int32(-1),
            Bson::Int64(-5),
            Bson::Double(0.5),
            Bson::Double(-0.5),
            Bson::Double(3.25),
            Bson::Double(-3.25),
            // the 2^53 boundary: these three i64 collapse to two f64 (2^53 and
            // 2^53+2 are exact; 2^53+1 rounds down to 2^53)
            Bson::Int64(TWO_53),
            Bson::Int64(TWO_53 + 1),
            Bson::Int64(TWO_53 + 2),
            // type extremes
            Bson::Int32(i32::MIN),
            Bson::Int32(i32::MAX),
            Bson::Int64(i64::MIN),
            Bson::Int64(i64::MAX),
            Bson::Double(f64::MIN),
            Bson::Double(f64::MAX),
            Bson::Double(f64::MIN_POSITIVE),
            Bson::Double(f64::INFINITY),
            Bson::Double(f64::NEG_INFINITY),
        ]
    }

    #[test]
    fn collapses_equal_values_across_types() {
        let key = |b: &Bson| encode_numeric_index(b).expect("numeric");
        // 5 across all three types → one key.
        assert_eq!(key(&Bson::Int32(5)), key(&Bson::Int64(5)));
        assert_eq!(key(&Bson::Int64(5)), key(&Bson::Double(5.0)));
        // signed/typed zero → one key.
        assert_eq!(key(&Bson::Double(-0.0)), key(&Bson::Double(0.0)));
        assert_eq!(key(&Bson::Double(0.0)), key(&Bson::Int64(0)));
        assert_eq!(key(&Bson::Int32(0)), key(&Bson::Int64(0)));
        // 2^53 and 2^53+1 are *equal* under the oracle, so they share a key.
        const TWO_53: i64 = 1 << 53;
        assert_eq!(
            oracle(&Bson::Int64(TWO_53), &Bson::Int64(TWO_53 + 1)),
            Some(Ordering::Equal)
        );
        assert_eq!(key(&Bson::Int64(TWO_53)), key(&Bson::Int64(TWO_53 + 1)));
        // …but 2^53+2 is a distinct f64, so a distinct key.
        assert_ne!(key(&Bson::Int64(TWO_53)), key(&Bson::Int64(TWO_53 + 2)));
    }

    #[test]
    fn byte_order_and_collapse_match_the_oracle() {
        let values = corpus();
        for a in &values {
            for b in &values {
                let (ka, kb) = (
                    encode_numeric_index(a).expect("numeric"),
                    encode_numeric_index(b).expect("numeric"),
                );
                match oracle(a, b) {
                    Some(ord) => {
                        // order-preservation
                        assert_eq!(
                            ka.cmp(&kb),
                            ord,
                            "byte order disagrees with oracle for {a:?} vs {b:?}"
                        );
                        // collapse: equal bytes iff oracle says Equal
                        assert_eq!(
                            ka == kb,
                            ord == Ordering::Equal,
                            "collapse disagrees with oracle for {a:?} vs {b:?}"
                        );
                    }
                    // No NaN in the corpus, so the oracle is always Some here.
                    None => unreachable!("corpus is comparable: {a:?} vs {b:?}"),
                }
            }
        }
    }

    #[test]
    fn roundtrip_is_exact_within_2_53() {
        let tag = |b: &Bson| match b {
            Bson::Int32(_) => ElementType::Int32,
            Bson::Int64(_) => ElementType::Int64,
            Bson::Double(_) => ElementType::Double,
            _ => unreachable!(),
        };
        // every corpus value whose magnitude is representable round-trips exactly
        for v in corpus() {
            let representable = match &v {
                Bson::Int32(_) => true,
                Bson::Int64(i) => i.unsigned_abs() <= (1 << 53),
                Bson::Double(_) => true, // doubles round-trip their own bits
                _ => unreachable!("corpus is numeric"),
            };
            if !representable {
                continue;
            }
            let key = encode_numeric_index(&v).expect("numeric");
            assert_eq!(
                decode_numeric_index(key, tag(&v)),
                Some(v.clone()),
                "roundtrip {v:?}"
            );
        }
    }

    #[test]
    fn roundtrip_beyond_2_53_is_f64_rounded() {
        // Int64(2^53 + 1) was keyed as the f64 2^53, so it decodes to 2^53 — the
        // documented, intentional precision loss of the f64 tower.
        const TWO_53: i64 = 1 << 53;
        let key = encode_numeric_index(&Bson::Int64(TWO_53 + 1)).expect("numeric");
        assert_eq!(
            decode_numeric_index(key, ElementType::Int64),
            Some(Bson::Int64(TWO_53))
        );
    }

    #[test]
    fn nan_and_non_numeric_are_not_keyed() {
        assert_eq!(encode_numeric_index(&Bson::Double(f64::NAN)), None);
        assert_eq!(encode_numeric_index(&Bson::String("5".into())), None);
        assert_eq!(encode_numeric_index(&Bson::Boolean(true)), None);
        assert_eq!(encode_numeric_index(&Bson::Null), None);
    }

    /// SplitMix64 — a dependency-free, deterministic PRNG for the fuzz corpus.
    struct SplitMix64(u64);
    impl SplitMix64 {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
    }

    #[test]
    fn fuzz_order_and_collapse_match_oracle() {
        let mut rng = SplitMix64(0x1234_5678_9ABC_DEF0);
        // A random numeric value: i32, i64, or a non-NaN f64.
        let sample = |rng: &mut SplitMix64| -> Bson {
            match rng.next() % 3 {
                0 => Bson::Int32(rng.next() as i32),
                1 => Bson::Int64(rng.next() as i64),
                _ => {
                    // f64::from_bits can yield NaN; resample until it isn't.
                    loop {
                        let f = f64::from_bits(rng.next());
                        if !f.is_nan() {
                            break Bson::Double(f);
                        }
                    }
                }
            }
        };
        for _ in 0..100_000 {
            let (a, b) = (sample(&mut rng), sample(&mut rng));
            let (ka, kb) = (
                encode_numeric_index(&a).expect("non-NaN numeric"),
                encode_numeric_index(&b).expect("non-NaN numeric"),
            );
            let ord = oracle(&a, &b).expect("non-NaN comparable");
            assert_eq!(ka.cmp(&kb), ord, "order: {a:?} vs {b:?}");
            assert_eq!(ka == kb, ord == Ordering::Equal, "collapse: {a:?} vs {b:?}");
        }
    }
}
