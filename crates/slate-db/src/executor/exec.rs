use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use bson::RawBson;
use bson::raw::RawBsonRef;

// ── Raw BSON value comparison for sorting ───────────────────────

pub(crate) fn raw_compare_field_values(a: Option<RawBsonRef>, b: Option<RawBsonRef>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (Some(RawBsonRef::Null), None)
        | (None, Some(RawBsonRef::Null))
        | (Some(RawBsonRef::Null), Some(RawBsonRef::Null)) => Ordering::Equal,
        (None, Some(_)) | (Some(RawBsonRef::Null), Some(_)) => Ordering::Less,
        (Some(_), None) | (Some(_), Some(RawBsonRef::Null)) => Ordering::Greater,
        (Some(a), Some(b)) => raw_compare_two_values(&a, &b),
    }
}

fn raw_compare_two_values(a: &RawBsonRef, b: &RawBsonRef) -> Ordering {
    match (a, b) {
        (RawBsonRef::String(a), RawBsonRef::String(b)) => a.cmp(b),
        (RawBsonRef::Int32(a), RawBsonRef::Int32(b)) => a.cmp(b),
        (RawBsonRef::Int64(a), RawBsonRef::Int64(b)) => a.cmp(b),
        (RawBsonRef::Int32(a), RawBsonRef::Int64(b)) => (*a as i64).cmp(b),
        (RawBsonRef::Int64(a), RawBsonRef::Int32(b)) => a.cmp(&(*b as i64)),
        (RawBsonRef::Double(a), RawBsonRef::Double(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (RawBsonRef::Double(a), RawBsonRef::Int64(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        (RawBsonRef::Double(a), RawBsonRef::Int32(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        (RawBsonRef::Int64(a), RawBsonRef::Double(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (RawBsonRef::Int32(a), RawBsonRef::Double(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (RawBsonRef::Boolean(a), RawBsonRef::Boolean(b)) => a.cmp(b),
        (RawBsonRef::DateTime(a), RawBsonRef::DateTime(b)) => {
            a.timestamp_millis().cmp(&b.timestamp_millis())
        }
        _ => Ordering::Equal,
    }
}

// ── Distinct helpers ────────────────────────────────────────────

pub(crate) fn hash_raw(raw_ref: RawBsonRef<'_>) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    match raw_ref {
        RawBsonRef::String(s) => {
            0u8.hash(&mut hasher);
            s.hash(&mut hasher);
        }
        RawBsonRef::Int32(i) => {
            1u8.hash(&mut hasher);
            i.hash(&mut hasher);
        }
        RawBsonRef::Int64(i) => {
            2u8.hash(&mut hasher);
            i.hash(&mut hasher);
        }
        RawBsonRef::Double(f) => {
            3u8.hash(&mut hasher);
            f.to_bits().hash(&mut hasher);
        }
        RawBsonRef::Boolean(b) => {
            4u8.hash(&mut hasher);
            b.hash(&mut hasher);
        }
        RawBsonRef::DateTime(dt) => {
            5u8.hash(&mut hasher);
            dt.timestamp_millis().hash(&mut hasher);
        }
        RawBsonRef::ObjectId(oid) => {
            6u8.hash(&mut hasher);
            oid.bytes().hash(&mut hasher);
        }
        RawBsonRef::Document(d) => {
            7u8.hash(&mut hasher);
            d.as_bytes().hash(&mut hasher);
        }
        RawBsonRef::Array(a) => {
            8u8.hash(&mut hasher);
            a.as_bytes().hash(&mut hasher);
        }
        _ => {
            255u8.hash(&mut hasher);
        }
    }
    hasher.finish()
}

pub(crate) fn push_raw(buf: &mut bson::RawArrayBuf, raw_ref: RawBsonRef<'_>) {
    match raw_ref {
        RawBsonRef::String(s) => buf.push(s),
        RawBsonRef::Int32(i) => buf.push(i),
        RawBsonRef::Int64(i) => buf.push(i),
        RawBsonRef::Double(f) => buf.push(f),
        RawBsonRef::Boolean(b) => buf.push(b),
        RawBsonRef::DateTime(dt) => buf.push(dt),
        RawBsonRef::ObjectId(oid) => buf.push(oid),
        RawBsonRef::Document(d) => buf.push(d.to_owned()),
        RawBsonRef::Array(a) => buf.push(a.to_owned()),
        _ => {}
    }
}

pub(crate) fn try_insert(
    seen: &mut HashSet<u64>,
    buf: &mut bson::RawArrayBuf,
    raw_ref: RawBsonRef<'_>,
) {
    let h = hash_raw(raw_ref);
    if seen.insert(h) {
        push_raw(buf, raw_ref);
    }
}

pub(crate) fn to_raw_bson(raw_ref: RawBsonRef<'_>) -> Option<RawBson> {
    Some(match raw_ref {
        RawBsonRef::String(s) => RawBson::String(s.to_string()),
        RawBsonRef::Int32(i) => RawBson::Int32(i),
        RawBsonRef::Int64(i) => RawBson::Int64(i),
        RawBsonRef::Double(f) => RawBson::Double(f),
        RawBsonRef::Boolean(b) => RawBson::Boolean(b),
        RawBsonRef::DateTime(dt) => RawBson::DateTime(dt),
        RawBsonRef::ObjectId(oid) => RawBson::ObjectId(oid),
        RawBsonRef::Document(d) => RawBson::Document(d.to_owned()),
        RawBsonRef::Array(a) => RawBson::Array(a.to_owned()),
        _ => return None,
    })
}
