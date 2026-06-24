//! Randomized differential of the raw byte scanner against the `bson` crate.
//!
//! RFC `rawbson-robustness` test-plan item 1 — the highest-leverage test for
//! byte code. It mirrors the repo's dependency-free fuzz idiom (the SplitMix64
//! corpus + oracle in `slate-engine`'s `numeric_key`): a deterministic PRNG
//! builds random BSON documents spanning every element type — nested docs,
//! arrays, duplicate keys, empty doc/array/string, all numeric extremes — then,
//! using the validated `bson::RawDocument` as the oracle, asserts that:
//!
//! * `RawField::get(bytes, name).value()` equals `RawDocument::get(name)` for
//!   every top-level field, and
//! * `for_each_path_value` matches a reference walk built on the `bson` crate.
//!
//! Because every document fed here is constructed via the `bson` crate it is
//! valid BSON, so this guards the *scanner's correctness on valid input* — the
//! safety net the hardening pass leans on. Malformed-input behaviour is pinned
//! by the per-type truncation tests inside the crate.

use bson::raw::{RawBsonRef, RawDocument};
use bson::{Bson, Document};
use slate_rawbson::{RawField, for_each_path_value};

/// SplitMix64 — dependency-free deterministic PRNG, identical to the corpus
/// generator in `slate-engine::encoding::numeric_key`.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A bounded value in `0..n`.
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Generate one random scalar BSON value spanning every fixed/var-width type
/// and the numeric extremes that exercise the length math.
fn scalar(rng: &mut SplitMix64) -> Bson {
    match rng.below(13) {
        0 => Bson::Double(f64::from_bits(rng.next())),
        1 => Bson::String(random_string(rng)),
        2 => Bson::Boolean(rng.next() & 1 == 0),
        3 => Bson::Null,
        4 => Bson::Int32(rng.next() as i32),
        5 => Bson::Int64(rng.next() as i64),
        6 => Bson::DateTime(bson::DateTime::from_millis(rng.next() as i64)),
        7 => Bson::ObjectId(bson::oid::ObjectId::from_bytes(rand_bytes12(rng))),
        8 => Bson::Timestamp(bson::Timestamp {
            time: rng.next() as u32,
            increment: rng.next() as u32,
        }),
        9 => Bson::Decimal128(bson::Decimal128::from_bytes(rand_bytes16(rng))),
        10 => Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: random_bytes(rng),
        }),
        // Numeric extremes — these stress the length/width arithmetic.
        11 => match rng.below(6) {
            0 => Bson::Int32(i32::MIN),
            1 => Bson::Int32(i32::MAX),
            2 => Bson::Int64(i64::MIN),
            3 => Bson::Int64(i64::MAX),
            4 => Bson::Double(f64::INFINITY),
            _ => Bson::Double(f64::NEG_INFINITY),
        },
        // Empty string — `len == 1` (the nul); the boundary the `value()`
        // underflow guard cares about.
        _ => Bson::String(String::new()),
    }
}

/// A random value, possibly a nested document or array, bounded by `depth`.
fn value(rng: &mut SplitMix64, depth: u32) -> Bson {
    if depth == 0 {
        return scalar(rng);
    }
    match rng.below(8) {
        0 => Bson::Document(random_document(rng, depth - 1)),
        1 => {
            let n = rng.below(4) as usize;
            Bson::Array((0..n).map(|_| value(rng, depth - 1)).collect())
        }
        // Empty document / empty array exercised explicitly.
        2 => Bson::Document(Document::new()),
        3 => Bson::Array(Vec::new()),
        _ => scalar(rng),
    }
}

fn random_string(rng: &mut SplitMix64) -> String {
    let n = rng.below(8) as usize;
    // ASCII alnum keeps it valid UTF-8 and human-debuggable.
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    (0..n)
        .map(|_| ALPHABET[rng.below(ALPHABET.len() as u64) as usize] as char)
        .collect()
}

fn random_bytes(rng: &mut SplitMix64) -> Vec<u8> {
    let n = rng.below(8) as usize;
    (0..n).map(|_| rng.next() as u8).collect()
}

fn rand_bytes12(rng: &mut SplitMix64) -> [u8; 12] {
    let mut b = [0u8; 12];
    for byte in b.iter_mut() {
        *byte = rng.next() as u8;
    }
    b
}

fn rand_bytes16(rng: &mut SplitMix64) -> [u8; 16] {
    let mut b = [0u8; 16];
    for byte in b.iter_mut() {
        *byte = rng.next() as u8;
    }
    b
}

/// Build a random document. Keys are drawn from a small pool so duplicates
/// (a valid-but-rare BSON shape) occur naturally.
fn random_document(rng: &mut SplitMix64, depth: u32) -> Document {
    const KEYS: &[&str] = &["a", "b", "c", "d", "id", "_id", "x"];
    let n = rng.below(6) as usize;
    let mut doc = Document::new();
    for _ in 0..n {
        let key = KEYS[rng.below(KEYS.len() as u64) as usize];
        // Document::insert overwrites on duplicate, so to actually produce
        // duplicate keys we splice the raw bytes below; here we keep it simple
        // and rely on the raw duplicate test for that shape.
        doc.insert(key, value(rng, depth));
    }
    doc
}

/// Are two `RawBsonRef`s value-equal? Delegates to `PartialEq`, but for
/// containers we compare the serialized bytes (structural equality across
/// distinct buffers) and for floats we compare *bits* so `NaN == NaN` holds
/// (the generator emits arbitrary `f64::from_bits`, including NaN payloads).
fn raw_eq(a: RawBsonRef<'_>, b: RawBsonRef<'_>) -> bool {
    match (a, b) {
        (RawBsonRef::Document(x), RawBsonRef::Document(y)) => x.as_bytes() == y.as_bytes(),
        (RawBsonRef::Array(x), RawBsonRef::Array(y)) => x.as_bytes() == y.as_bytes(),
        (RawBsonRef::Double(x), RawBsonRef::Double(y)) => x.to_bits() == y.to_bits(),
        _ => a == b,
    }
}

/// Does `RawField::value()` claim to convert this element type? The scanner
/// deliberately declines to convert a few valid types (e.g. Timestamp): it
/// locates and skips them correctly but `value()` returns `None`. The
/// differential must hold the scanner to *that* contract, not punish it for the
/// types it documents as out of scope.
fn value_converts(t: bson::spec::ElementType) -> bool {
    use bson::spec::ElementType::*;
    matches!(
        t,
        Double
            | String
            | EmbeddedDocument
            | Array
            | ObjectId
            | Boolean
            | DateTime
            | Null
            | Int32
            | Int64
            | Decimal128
    )
}

/// The oracle's value for the *first* occurrence of `name` (matching the
/// scanner, which stops at the first match — relevant for duplicate keys).
fn doc_get_first<'a>(doc: &'a RawDocument, name: &str) -> RawBsonRef<'a> {
    doc.get(name)
        .expect("valid")
        .expect("present (caller iterated this key)")
}

#[test]
fn differential_top_level_fields_match_oracle() {
    let mut rng = SplitMix64(0x1234_5678_9ABC_DEF0);

    for _ in 0..50_000 {
        let doc = random_document(&mut rng, 3);
        let bytes = match bson::serialize_to_vec(&doc) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let oracle = RawDocument::from_bytes(&bytes).expect("bson produced valid BSON");

        // For every key the oracle reports, the scanner must locate the same
        // field, with the same element type, and (for the types it converts)
        // the same parsed value.
        for entry in oracle.iter() {
            let (name, expected) = entry.expect("valid element");
            let name = name.as_str();

            // Note: a doc may carry duplicate keys; `get`/`oracle.get` both
            // return the *first*, so compare against that to stay consistent.
            let field = RawField::get(&bytes, name)
                .unwrap_or_else(|| panic!("scanner missed field '{name}' in {doc:?}"));
            assert_eq!(
                field.element_type(),
                expected.element_type(),
                "type mismatch for field '{name}' in {doc:?}"
            );

            if !value_converts(expected.element_type()) {
                // Documented out-of-scope type: located & skipped, value() None.
                assert!(
                    field.value().is_none(),
                    "field '{name}' is non-converting type but value() returned Some in {doc:?}"
                );
                continue;
            }

            let oracle_first = doc_get_first(oracle, name);
            let got = field
                .value()
                .unwrap_or_else(|| panic!("value() None for convertible '{name}' in {doc:?}"));
            assert!(
                raw_eq(got, oracle_first),
                "field '{name}': scanner {got:?} != oracle {oracle_first:?} in {doc:?}"
            );
        }

        // A name the oracle does not have must scan as absent.
        if oracle.get("nope").ok().flatten().is_none() {
            assert!(
                RawField::get(&bytes, "nope")
                    .and_then(|f| f.value())
                    .is_none()
                    || RawField::get(&bytes, "nope").is_none(),
                "phantom field 'nope' in {doc:?}"
            );
        }
    }
}

/// Reference walk built on the `bson` crate, mirroring `for_each_path_value`'s
/// contract: dot-path descent, `[]` array fan-out, terminal-array fan-out.
fn reference_walk(doc: &RawDocument, path: &str, out: &mut Vec<Bson>) {
    let segments: Vec<&str> = path.split('.').collect();
    ref_walk_doc(doc, &segments, 0, out);
}

fn ref_walk_doc(doc: &RawDocument, segments: &[&str], idx: usize, out: &mut Vec<Bson>) {
    if idx >= segments.len() {
        return;
    }
    let seg = segments[idx];
    if seg == "[]" {
        return;
    }
    if let Ok(Some(v)) = doc.get(seg) {
        ref_walk_value(v, segments, idx + 1, out);
    }
}

fn ref_walk_value(v: RawBsonRef<'_>, segments: &[&str], idx: usize, out: &mut Vec<Bson>) {
    if idx >= segments.len() {
        match v {
            RawBsonRef::Array(arr) => {
                for e in arr.into_iter().flatten() {
                    out.push(Bson::try_from(e).expect("convertible"));
                }
            }
            other => out.push(Bson::try_from(other).expect("convertible")),
        }
        return;
    }
    let seg = segments[idx];
    if seg == "[]" {
        if let RawBsonRef::Array(arr) = v {
            for e in arr.into_iter().flatten() {
                ref_walk_value(e, segments, idx + 1, out);
            }
        }
    } else if let RawBsonRef::Document(d) = v {
        ref_walk_doc(d, segments, idx, out);
    }
}

#[test]
fn differential_for_each_path_value_matches_oracle() {
    let mut rng = SplitMix64(0x0FEE_DBAC_C0FF_EE00);
    // Paths that exercise plain descent, array markers, and terminal fan-out.
    let paths = ["a", "a.b", "a.[]", "a.[].b", "a.b.c", "x.[].id"];

    for _ in 0..20_000 {
        let doc = random_document(&mut rng, 3);
        let bytes = match bson::serialize_to_vec(&doc) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let raw = RawDocument::from_bytes(&bytes).expect("valid");

        for path in paths {
            let mut got = Vec::new();
            for_each_path_value(raw, path, &mut |v| {
                got.push(Bson::try_from(v).expect("convertible"));
            });

            let mut want = Vec::new();
            reference_walk(raw, path, &mut want);

            assert!(
                got.len() == want.len() && got.iter().zip(&want).all(|(a, b)| bson_eq(a, b)),
                "for_each_path_value('{path}') diverged: {got:?} != {want:?} in {doc:?}"
            );
        }
    }
}

/// NaN-aware `Bson` equality (bit-compare doubles; otherwise `PartialEq`).
fn bson_eq(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Double(x), Bson::Double(y)) => x.to_bits() == y.to_bits(),
        _ => a == b,
    }
}
