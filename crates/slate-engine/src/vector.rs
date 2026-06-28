//! Vector (flat) index domain types and packing.
//!
//! A vector index is a derived `doc_id → packed-vector` copy of a document field
//! that holds an embedding (a BSON array of numbers). The document's array stays
//! the canonical source of truth — exactly like a secondary index is a derived
//! copy of a scalar field — and the engine maintains this copy on the write path
//! (and rebuilds it from records on backfill).
//!
//! The stored copy is a packed little-endian blob (not BSON): contiguous and
//! scan-friendly for the brute-force top-k. Its element width is the spec's
//! [`VectorDataType`] — `float32` stores each component exactly; the Phase 2
//! quantized widths (`float16`, …) pack a smaller, *approximate* copy that the
//! executor refines with a full-precision rescore read from the document (the
//! BSON array is the canonical source of truth, so no exact copy is stored
//! separately).
//!
//! These types are catalog config, parallel to [`IndexSpec`](crate::IndexSpec) —
//! the math that consumes them (`VECTORDISTANCE`) lives in `slate-eval`.

use bson::raw::{RawBsonRef, RawDocument};
use half::f16;
use serde::{Deserialize, Serialize};
use slate_rawbson::RawField;

use crate::error::EngineError;

/// The distance/similarity function a vector index is built for.
///
/// A flat index is consulted only when a `VECTORDISTANCE` call's metric matches
/// the index's declared metric (a mismatch falls back to a correct full scan), so
/// the metric is part of the index identity, not just a query-time choice. Mirrors
/// the three metrics `VECTORDISTANCE` implements (Cosmos's set).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorMetric {
    /// Cosine similarity — higher is closer (`ORDER BY … DESC`).
    Cosine,
    /// Inner product — higher is closer (`ORDER BY … DESC`).
    DotProduct,
    /// Euclidean (L2) distance — lower is closer (`ORDER BY … ASC`).
    Euclidean,
}

/// The on-disk element width of a stored vector.
///
/// `float32` stores full precision (the Phase 1 default — exact, no rescore);
/// Phase 2 adds quantized widths for the on-device footprint. The variant is the
/// decode schema for the packed blob, so it is carried in the persisted spec.
///
/// A quantized index stores only the *approximate* copy: the exact float32 stays
/// on the document, and the executor reads it back to rescore the shortlist, so
/// the final top-k is exact-scored regardless of width.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorDataType {
    /// IEEE-754 single precision (4 bytes/component). The Phase 1 default —
    /// stored exactly, so a `float32` index needs no rescore.
    Float32,
    /// IEEE-754 half precision (2 bytes/component) via [`half::f16`]. ~2× smaller
    /// and near-lossless; the approximate scan is refined by a full-precision
    /// rescore from the document (recall@k ≈ 1.0 with a small rescore window).
    Float16,
    /// Symmetric int8 with a per-vector scale: `[4-byte LE f32 scale][dims i8
    /// codes]` → `dims + 4` bytes (~4× smaller). Approximate; the rescore restores
    /// exact ordering (spike recall@k = 1.0 at a 2× window). The per-vector scale
    /// needs no training pass, so it suits the incremental write path.
    Int8,
}

impl VectorDataType {
    /// Pack a full `f32` vector into this width's little-endian blob. The whole
    /// vector is taken (not one component at a time) because a per-vector scheme —
    /// int8's `scale = max|x| / 127` — must see every component before encoding.
    fn encode(self, v: &[f32]) -> Vec<u8> {
        match self {
            VectorDataType::Float32 => {
                let mut out = Vec::with_capacity(v.len() * 4);
                for &x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
                out
            }
            VectorDataType::Float16 => {
                let mut out = Vec::with_capacity(v.len() * 2);
                for &x in v {
                    out.extend_from_slice(&f16::from_f32(x).to_le_bytes());
                }
                out
            }
            VectorDataType::Int8 => encode_int8(v),
        }
    }

    /// Decode this width's packed blob back to (approximate) `f32` — the scan side
    /// of quantization. `None` on a malformed length (a corruption guard on the
    /// read path). `Float32` is exact; the quantized widths dequantize.
    pub(crate) fn decode_components(self, bytes: &[u8]) -> Option<Vec<f32>> {
        match self {
            VectorDataType::Float32 => {
                decode_chunks(bytes, 4, |c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            }
            VectorDataType::Float16 => {
                decode_chunks(bytes, 2, |c| f16::from_le_bytes([c[0], c[1]]).to_f32())
            }
            VectorDataType::Int8 => decode_int8(bytes),
        }
    }
}

/// Decode a flat little-endian blob of fixed-`width` components. `None` if the
/// length is not a whole number of components.
fn decode_chunks(bytes: &[u8], width: usize, f: impl Fn(&[u8]) -> f32) -> Option<Vec<f32>> {
    if !bytes.len().is_multiple_of(width) {
        return None;
    }
    Some(bytes.chunks_exact(width).map(f).collect())
}

/// Pack a vector as symmetric int8 with a per-vector scale: `scale = max|x| / 127`
/// (1.0 for an all-zero vector), stored as a leading little-endian `f32` followed
/// by one `i8` code per component (`round(x / scale)` clamped to ±127). Total
/// `v.len() + 4` bytes.
fn encode_int8(v: &[f32]) -> Vec<u8> {
    let maxabs = v.iter().fold(0f32, |m, &x| m.max(x.abs()));
    let scale = if maxabs == 0.0 { 1.0 } else { maxabs / 127.0 };
    let mut out = Vec::with_capacity(4 + v.len());
    out.extend_from_slice(&scale.to_le_bytes());
    for &x in v {
        let code = (x / scale).round().clamp(-127.0, 127.0) as i8;
        out.push(code as u8);
    }
    out
}

/// Decode the int8 layout: read the leading `f32` scale, then dequantize each
/// `i8` code (`code * scale`). `None` if shorter than the 4-byte scale header.
fn decode_int8(bytes: &[u8]) -> Option<Vec<f32>> {
    let (scale, codes) = bytes.split_at_checked(4)?;
    let scale = f32::from_le_bytes([scale[0], scale[1], scale[2], scale[3]]);
    Some(codes.iter().map(|&b| (b as i8) as f32 * scale).collect())
}

/// A loaded vector index definition: the field path plus the embedding shape.
///
/// Self-contained — it is serialized whole into the catalog's vector-index config
/// value (a self-describing blob, mirroring how `pk_path`/`ttl_path` collection
/// config is serialized), so the on-disk value is just the serialized form of
/// this struct and grows cleanly as later phases add fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorIndexSpec {
    /// The document field holding the embedding (dot-paths allowed).
    pub path: String,
    /// The fixed dimensionality every stored vector must have.
    pub dims: u32,
    /// The distance function the index is built for.
    pub metric: VectorMetric,
    /// The on-disk element width.
    pub dtype: VectorDataType,
}

impl VectorIndexSpec {
    /// A Phase-1 `float32` spec with the given path, dimensionality, and metric.
    pub fn float32(path: impl Into<String>, dims: u32, metric: VectorMetric) -> Self {
        VectorIndexSpec {
            path: path.into(),
            dims,
            metric,
            dtype: VectorDataType::Float32,
        }
    }

    /// A `float16` spec (Phase 2 quantization — ~2× smaller, near-lossless with
    /// the full-precision rescore).
    pub fn float16(path: impl Into<String>, dims: u32, metric: VectorMetric) -> Self {
        VectorIndexSpec {
            path: path.into(),
            dims,
            metric,
            dtype: VectorDataType::Float16,
        }
    }

    /// An `int8` spec (Phase 2 quantization — ~4× smaller via a per-vector scale,
    /// high-recall with the full-precision rescore).
    pub fn int8(path: impl Into<String>, dims: u32, metric: VectorMetric) -> Self {
        VectorIndexSpec {
            path: path.into(),
            dims,
            metric,
            dtype: VectorDataType::Int8,
        }
    }
}

/// Extract a vector field from a document and pack it to a little-endian blob in
/// the spec's `dtype` width, ready to store under the doc's vector key.
///
/// - **Absent** field → `Ok(None)` — the index is sparse, exactly like a
///   secondary index skips a document missing the indexed field.
/// - **Present and a numeric array of the declared length** → `Ok(Some(bytes))`,
///   each component widened to `f32` then encoded in `dtype` (exact for
///   `float32`, quantized for the narrower widths).
/// - **Wrong length** → `Err` ([`EngineError::VectorDimsMismatch`]) — Cosmos
///   rejects a vector whose dimensionality disagrees with the policy, and so do
///   we, loudly, rather than storing a vector the query math can't compare.
/// - **Present but not a numeric array** (non-array, or an element that isn't a
///   number) → `Err` ([`EngineError::InvalidDocument`]) — the path was declared a
///   vector; a non-vector value there is a data error.
pub(crate) fn pack_vector(
    doc: &RawDocument,
    field: &str,
    dims: u32,
    dtype: VectorDataType,
) -> Result<Option<Vec<u8>>, EngineError> {
    // `get_value` resolves dot-paths and returns `None` for a missing field or a
    // Null leaf — both mean "no embedding here", so the index stays sparse.
    let Some(value) = RawField::get_value(doc.as_bytes(), field) else {
        return Ok(None);
    };
    let RawBsonRef::Array(arr) = value else {
        return Err(EngineError::InvalidDocument(format!(
            "vector field '{field}' must be an array of numbers"
        )));
    };

    // Collect the numeric array into an `f32` vector first, then encode in the
    // width: int8's per-vector scale needs the whole vector, and f32/f16 cost only
    // a transient `Vec<f32>` on the (cold, write-side) pack path.
    let mut v: Vec<f32> = Vec::with_capacity(dims as usize);
    for element in arr.into_iter() {
        let el = element.map_err(|e| {
            EngineError::InvalidDocument(format!("vector field '{field}' is malformed: {e}"))
        })?;
        let f = match el {
            RawBsonRef::Int32(i) => i as f32,
            RawBsonRef::Int64(i) => i as f32,
            RawBsonRef::Double(d) => d as f32,
            _ => {
                return Err(EngineError::InvalidDocument(format!(
                    "vector field '{field}' must contain only numbers"
                )));
            }
        };
        v.push(f);
    }

    if v.len() != dims as usize {
        return Err(EngineError::VectorDimsMismatch {
            field: field.to_string(),
            expected: dims,
            found: v.len() as u32,
        });
    }
    Ok(Some(dtype.encode(&v)))
}

// ── Stored vector entry (TTL header + packed blob) ───────────────
//
// A vector data entry is the packed `f32` blob behind an optional TTL header,
// mirroring the record blob's framing (`encoding/record.rs`):
//
//   [0x00][packed f32…]                       — no TTL
//   [0x01][8-byte LE i64 millis][packed f32…] — has TTL
//
// The TTL is carried *in the entry* so a vector scan can drop expired documents
// reading only the vector keyspace — exactly how an index scan reads the TTL from
// the index entry's metadata rather than the record. Without it, a scan would
// either yield expired vectors (returning fewer than k from a top-k once the
// downstream key-lookup drops them) or have to read every full record, defeating
// the keyspace's locality.

const VEC_TAG_NO_TTL: u8 = 0x00;
const VEC_TAG_TTL: u8 = 0x01;
const VEC_TTL_SIZE: usize = 8;
/// Length of a TTL header (the tag byte plus the 8-byte millis).
const VEC_TTL_HEADER_LEN: usize = 1 + VEC_TTL_SIZE;

/// Frame a packed `f32` blob into a stored vector entry, prepending the TTL
/// header when the document carries a TTL (so the read path can drop expired
/// entries without a record read).
pub(crate) fn encode_vector_entry(ttl_millis: Option<i64>, packed: &[u8]) -> Vec<u8> {
    match ttl_millis {
        Some(millis) => {
            let mut bytes = Vec::with_capacity(1 + VEC_TTL_SIZE + packed.len());
            bytes.push(VEC_TAG_TTL);
            bytes.extend_from_slice(&millis.to_le_bytes());
            bytes.extend_from_slice(packed);
            bytes
        }
        None => {
            let mut bytes = Vec::with_capacity(1 + packed.len());
            bytes.push(VEC_TAG_NO_TTL);
            bytes.extend_from_slice(packed);
            bytes
        }
    }
}

/// O(1) TTL expiry check on a stored vector entry's raw bytes — the same
/// fast, no-record-read check the record/index paths use. `false` for an entry
/// with no TTL header (the common case), so a TTL-free collection pays only a
/// single tag-byte read per entry.
#[inline]
pub(crate) fn is_vector_entry_expired(bytes: &[u8], now_millis: i64) -> bool {
    match bytes.first() {
        Some(&VEC_TAG_TTL) if bytes.len() >= VEC_TTL_HEADER_LEN => {
            let millis = i64::from_le_bytes(
                bytes[1..VEC_TTL_HEADER_LEN]
                    .try_into()
                    .unwrap_or([0; VEC_TTL_SIZE]),
            );
            millis < now_millis
        }
        _ => false,
    }
}

/// Decode a stored vector entry's bytes (TTL header + packed blob) into its
/// (approximate) `f32` vector, skipping the header and dequantizing per `dtype`.
/// Returns `None` if the header is malformed or the packed portion is not a whole
/// number of `dtype`'s components.
pub(crate) fn decode_vector_entry(bytes: &[u8], dtype: VectorDataType) -> Option<Vec<f32>> {
    let packed = match *bytes.first()? {
        VEC_TAG_NO_TTL => &bytes[1..],
        VEC_TAG_TTL if bytes.len() >= VEC_TTL_HEADER_LEN => &bytes[VEC_TTL_HEADER_LEN..],
        _ => return None,
    };
    dtype.decode_components(packed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_then_unpack_roundtrips() {
        let doc = bson::rawdoc! { "_id": "a", "embedding": [1.0_f64, 2.0, 3.0] };
        let packed = pack_vector(&doc, "embedding", 3, VectorDataType::Float32)
            .unwrap()
            .unwrap();
        assert_eq!(packed.len(), 12);
        assert_eq!(
            VectorDataType::Float32.decode_components(&packed).unwrap(),
            vec![1.0_f32, 2.0, 3.0]
        );
    }

    #[test]
    fn pack_widens_mixed_numeric_elements() {
        let doc = bson::rawdoc! { "_id": "a", "v": [1_i32, 2_i64, 3.5_f64] };
        let packed = pack_vector(&doc, "v", 3, VectorDataType::Float32)
            .unwrap()
            .unwrap();
        assert_eq!(
            VectorDataType::Float32.decode_components(&packed).unwrap(),
            vec![1.0_f32, 2.0, 3.5]
        );
    }

    #[test]
    fn pack_float16_halves_the_blob_and_roundtrips_exact_for_representable() {
        let doc = bson::rawdoc! { "_id": "a", "embedding": [1.0_f64, 2.0, 3.5] };
        let packed = pack_vector(&doc, "embedding", 3, VectorDataType::Float16)
            .unwrap()
            .unwrap();
        // 2 bytes/component vs 4 — half the float32 footprint.
        assert_eq!(packed.len(), 6);
        // 1.0/2.0/3.5 are exactly representable in f16, so this round trip is exact.
        assert_eq!(
            VectorDataType::Float16.decode_components(&packed).unwrap(),
            vec![1.0_f32, 2.0, 3.5]
        );
    }

    #[test]
    fn pack_float16_dequantizes_within_tolerance() {
        // Values not exactly representable in f16 round-trip to within f16's
        // ~1e-3 precision — the approximation the executor's rescore refines.
        let doc = bson::rawdoc! { "_id": "a", "v": [0.1_f64, 0.2, 0.333] };
        let packed = pack_vector(&doc, "v", 3, VectorDataType::Float16)
            .unwrap()
            .unwrap();
        let back = VectorDataType::Float16.decode_components(&packed).unwrap();
        for (got, want) in back.iter().zip([0.1_f32, 0.2, 0.333]) {
            assert!((got - want).abs() < 1e-3, "got {got}, want {want}");
        }
    }

    #[test]
    fn pack_int8_footprint_and_dequantizes_within_tolerance() {
        // int8 stores a 4-byte scale + one byte per component, and round-trips to
        // within ~scale/2 (= max|x| / 254) of the original — the approximation the
        // rescore refines. Here max|x| = 1.0, so the tolerance is ~0.004.
        let doc = bson::rawdoc! { "_id": "a", "v": [1.0_f64, -0.5, 0.25, 0.0] };
        let packed = pack_vector(&doc, "v", 4, VectorDataType::Int8)
            .unwrap()
            .unwrap();
        // 4-byte scale + 4 i8 codes.
        assert_eq!(packed.len(), 8);
        let back = VectorDataType::Int8.decode_components(&packed).unwrap();
        for (got, want) in back.iter().zip([1.0_f32, -0.5, 0.25, 0.0]) {
            assert!((got - want).abs() < 0.01, "got {got}, want {want}");
        }
    }

    #[test]
    fn pack_int8_all_zero_vector_is_safe() {
        // An all-zero vector has no magnitude; the scale falls back to 1.0 (no
        // divide-by-zero) and every code is 0, decoding back to zeros.
        let doc = bson::rawdoc! { "_id": "a", "v": [0.0_f64, 0.0, 0.0] };
        let packed = pack_vector(&doc, "v", 3, VectorDataType::Int8)
            .unwrap()
            .unwrap();
        assert_eq!(packed.len(), 7);
        assert_eq!(
            VectorDataType::Int8.decode_components(&packed).unwrap(),
            vec![0.0_f32, 0.0, 0.0]
        );
    }

    #[test]
    fn pack_absent_field_is_sparse() {
        let doc = bson::rawdoc! { "_id": "a", "name": "no vector" };
        assert!(
            pack_vector(&doc, "embedding", 3, VectorDataType::Float32)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn pack_nested_dot_path() {
        let doc = bson::rawdoc! { "_id": "a", "meta": { "vec": [1.0_f64, 2.0] } };
        let packed = pack_vector(&doc, "meta.vec", 2, VectorDataType::Float32)
            .unwrap()
            .unwrap();
        assert_eq!(
            VectorDataType::Float32.decode_components(&packed).unwrap(),
            vec![1.0_f32, 2.0]
        );
    }

    #[test]
    fn pack_wrong_dims_is_an_error() {
        let doc = bson::rawdoc! { "_id": "a", "embedding": [1.0_f64, 2.0] };
        match pack_vector(&doc, "embedding", 3, VectorDataType::Float32) {
            Err(EngineError::VectorDimsMismatch {
                expected, found, ..
            }) => {
                assert_eq!(expected, 3);
                assert_eq!(found, 2);
            }
            other => panic!("expected VectorDimsMismatch, got {other:?}"),
        }
    }

    #[test]
    fn pack_non_array_is_an_error() {
        let doc = bson::rawdoc! { "_id": "a", "embedding": "not a vector" };
        assert!(matches!(
            pack_vector(&doc, "embedding", 3, VectorDataType::Float32),
            Err(EngineError::InvalidDocument(_))
        ));
    }

    #[test]
    fn pack_non_numeric_element_is_an_error() {
        let doc = bson::rawdoc! { "_id": "a", "embedding": [1.0_f64, "x", 3.0] };
        assert!(matches!(
            pack_vector(&doc, "embedding", 3, VectorDataType::Float32),
            Err(EngineError::InvalidDocument(_))
        ));
    }

    #[test]
    fn decode_components_rejects_a_partial_component() {
        // Not a whole number of components for the width → None (corruption guard).
        assert!(
            VectorDataType::Float32
                .decode_components(&[0, 1, 2])
                .is_none()
        );
        assert!(VectorDataType::Float16.decode_components(&[0]).is_none());
        // int8 needs at least the 4-byte scale header.
        assert!(VectorDataType::Int8.decode_components(&[0, 1, 2]).is_none());
    }

    #[test]
    fn spec_serde_roundtrips_through_bson() {
        // Every width serializes whole into the catalog config value.
        for spec in [
            VectorIndexSpec::float32("embedding", 768, VectorMetric::Cosine),
            VectorIndexSpec::float16("embedding", 768, VectorMetric::Cosine),
            VectorIndexSpec::int8("embedding", 768, VectorMetric::Cosine),
        ] {
            let blob = bson::serialize_to_vec(&spec).unwrap();
            let back: VectorIndexSpec = bson::deserialize_from_slice(&blob).unwrap();
            assert_eq!(spec, back);
        }
    }

    #[test]
    fn vector_entry_no_ttl_roundtrips() {
        let packed = pack_vector(
            &bson::rawdoc! { "_id": "a", "v": [1.0_f64, 2.0, 3.0] },
            "v",
            3,
            VectorDataType::Float32,
        )
        .unwrap()
        .unwrap();
        let entry = encode_vector_entry(None, &packed);
        assert_eq!(entry[0], VEC_TAG_NO_TTL);
        // A TTL-free entry is never expired and decodes to the original vector.
        assert!(!is_vector_entry_expired(&entry, i64::MAX));
        assert_eq!(
            decode_vector_entry(&entry, VectorDataType::Float32).unwrap(),
            vec![1.0_f32, 2.0, 3.0]
        );
    }

    #[test]
    fn vector_entry_with_ttl_roundtrips_and_expires() {
        let packed = pack_vector(
            &bson::rawdoc! { "_id": "a", "v": [4.0_f64, 5.0, 6.0] },
            "v",
            3,
            VectorDataType::Float32,
        )
        .unwrap()
        .unwrap();
        let entry = encode_vector_entry(Some(1_000), &packed);
        assert_eq!(entry[0], VEC_TAG_TTL);
        assert!(is_vector_entry_expired(&entry, 2_000));
        assert!(!is_vector_entry_expired(&entry, 500));
        // The header is skipped on decode — the vector is intact regardless of TTL.
        assert_eq!(
            decode_vector_entry(&entry, VectorDataType::Float32).unwrap(),
            vec![4.0_f32, 5.0, 6.0]
        );
    }

    #[test]
    fn float16_entry_roundtrips_through_the_ttl_frame() {
        // A float16 entry frames + decodes like any other, just at half width.
        let packed = pack_vector(
            &bson::rawdoc! { "_id": "a", "v": [1.0_f64, 2.0, 3.5] },
            "v",
            3,
            VectorDataType::Float16,
        )
        .unwrap()
        .unwrap();
        assert_eq!(packed.len(), 6);
        let entry = encode_vector_entry(Some(1_000), &packed);
        assert!(is_vector_entry_expired(&entry, 2_000));
        assert_eq!(
            decode_vector_entry(&entry, VectorDataType::Float16).unwrap(),
            vec![1.0_f32, 2.0, 3.5]
        );
    }

    #[test]
    fn decode_vector_entry_rejects_malformed() {
        // Empty bytes, an unknown tag, and a truncated TTL header all decode to None.
        assert!(decode_vector_entry(&[], VectorDataType::Float32).is_none());
        assert!(decode_vector_entry(&[0x09, 0, 0, 0, 0], VectorDataType::Float32).is_none());
        assert!(decode_vector_entry(&[VEC_TAG_TTL, 0, 0, 0], VectorDataType::Float32).is_none());
    }
}
