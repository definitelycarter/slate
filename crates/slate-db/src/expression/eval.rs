use std::cmp::Ordering;

use bson::Bson;
use bson::raw::RawBsonRef;
use bson::RawDocument;

use crate::error::DbError;
use crate::executor::raw_bson::RawField;
use super::Expression;

/// Evaluate whether a raw document matches the given expression.
pub(crate) fn matches(raw: &RawDocument, expr: &Expression) -> Result<bool, DbError> {
    match expr {
        Expression::And(children) => {
            for child in children {
                if !matches(raw, child)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Expression::Or(children) => {
            for child in children {
                if matches(raw, child)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expression::Eq(field, val) => {
            // $eq: null matches both missing fields and explicit null values
            if std::matches!(val, Bson::Null) {
                return Ok(RawField::get_value(raw.as_bytes(), field).is_none());
            }
            match RawField::get_value(raw.as_bytes(), field) {
                Some(RawBsonRef::Array(arr)) => {
                    for elem in arr.into_iter().flatten() {
                        if value_eq(&elem, val) {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                Some(v) => Ok(value_eq(&v, val)),
                None => Ok(false),
            }
        }
        Expression::Gt(field, val)
        | Expression::Gte(field, val)
        | Expression::Lt(field, val)
        | Expression::Lte(field, val) => {
            let predicate: fn(Ordering) -> bool = match expr {
                Expression::Gt(..) => |o| o == Ordering::Greater,
                Expression::Gte(..) => |o| o != Ordering::Less,
                Expression::Lt(..) => |o| o == Ordering::Less,
                Expression::Lte(..) => |o| o != Ordering::Greater,
                _ => unreachable!(),
            };
            let field_value = RawField::get_value(raw.as_bytes(), field);
            match field_value {
                Some(RawBsonRef::Array(arr)) => {
                    for elem in arr.into_iter().flatten() {
                        if value_cmp(Some(&elem), val, predicate) {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                _ => Ok(value_cmp(field_value.as_ref(), val, predicate)),
            }
        }
        Expression::Regex(field, re) => match RawField::get_value(raw.as_bytes(), field) {
            Some(RawBsonRef::String(s)) => Ok(re.is_match(s)),
            _ => Ok(false),
        },
        Expression::Exists(field, expected) => {
            // $exists checks physical presence — even a null value counts as "exists"
            let present = RawField::get_path(raw.as_bytes(), field).is_some();
            Ok(*expected == present)
        }
    }
}

/// Equality: stored `RawBsonRef` (from document) vs query `Bson` (from Expression).
fn value_eq(store_val: &RawBsonRef, query_val: &Bson) -> bool {
    match (store_val, query_val) {
        // ── Direct type matches ─────────────────────────────────
        (RawBsonRef::String(a), Bson::String(b)) => *a == b.as_str(),
        (RawBsonRef::Int32(a), Bson::Int32(b)) => *a == *b,
        (RawBsonRef::Int32(a), Bson::Int64(b)) => (*a as i64) == *b,
        (RawBsonRef::Int64(a), Bson::Int64(b)) => *a == *b,
        (RawBsonRef::Int64(a), Bson::Int32(b)) => *a == (*b as i64),
        (RawBsonRef::Double(a), Bson::Double(b)) => *a == *b,
        (RawBsonRef::Double(a), Bson::Int64(b)) => *a == (*b as f64),
        (RawBsonRef::Double(a), Bson::Int32(b)) => *a == (*b as f64),
        (RawBsonRef::Int64(a), Bson::Double(b)) => (*a as f64) == *b,
        (RawBsonRef::Int32(a), Bson::Double(b)) => (*a as f64) == *b,
        (RawBsonRef::Boolean(a), Bson::Boolean(b)) => *a == *b,
        (RawBsonRef::DateTime(a), Bson::DateTime(b)) => {
            a.timestamp_millis() == b.timestamp_millis()
        }

        // ── Cross-type coercion: Bson::String → stored type ─
        (RawBsonRef::Int32(a), Bson::String(s)) => s.parse::<i64>().is_ok_and(|b| (*a as i64) == b),
        (RawBsonRef::Int64(a), Bson::String(s)) => s.parse::<i64>().is_ok_and(|b| *a == b),
        (RawBsonRef::Double(a), Bson::String(s)) => s.parse::<f64>().is_ok_and(|b| *a == b),
        (RawBsonRef::Boolean(a), Bson::String(s)) => match s.as_str() {
            "true" => *a,
            "false" => !*a,
            _ => false,
        },
        (RawBsonRef::DateTime(a), Bson::String(s)) => bson::DateTime::parse_rfc3339_str(s)
            .is_ok_and(|dt| a.timestamp_millis() == dt.timestamp_millis()),

        // ── Cross-type coercion: Int → DateTime (epoch seconds) ─
        (RawBsonRef::DateTime(a), Bson::Int64(b)) => a.timestamp_millis() == (*b * 1000),
        (RawBsonRef::DateTime(a), Bson::Int32(b)) => a.timestamp_millis() == (*b as i64 * 1000),

        // ── Incompatible types: silent exclusion ────────────────
        _ => false,
    }
}

/// Comparison: stored `RawBsonRef` (from document) vs query `Bson` (from Expression).
fn value_cmp(
    field_value: Option<&RawBsonRef>,
    query_val: &Bson,
    predicate: fn(Ordering) -> bool,
) -> bool {
    match field_value {
        Some(store_val) => match (store_val, query_val) {
            // ── Direct type matches ─────────────────────────────
            (RawBsonRef::Int32(a), Bson::Int32(b)) => predicate(a.cmp(b)),
            (RawBsonRef::Int32(a), Bson::Int64(b)) => predicate((*a as i64).cmp(b)),
            (RawBsonRef::Int64(a), Bson::Int64(b)) => predicate(a.cmp(b)),
            (RawBsonRef::Int64(a), Bson::Int32(b)) => predicate(a.cmp(&(*b as i64))),
            (RawBsonRef::Double(a), Bson::Double(b)) => {
                predicate(a.partial_cmp(b).unwrap_or(Ordering::Equal))
            }
            (RawBsonRef::Double(a), Bson::Int64(b)) => {
                predicate(a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal))
            }
            (RawBsonRef::Double(a), Bson::Int32(b)) => {
                predicate(a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal))
            }
            (RawBsonRef::Int64(a), Bson::Double(b)) => {
                predicate((*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal))
            }
            (RawBsonRef::Int32(a), Bson::Double(b)) => {
                predicate((*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal))
            }
            (RawBsonRef::DateTime(a), Bson::DateTime(b)) => {
                predicate(a.timestamp_millis().cmp(&b.timestamp_millis()))
            }
            (RawBsonRef::String(a), Bson::String(b)) => predicate((*a).cmp(b.as_str())),

            // ── Cross-type coercion: Bson::String → stored type ─
            (RawBsonRef::Int32(a), Bson::String(s)) => s
                .parse::<i64>()
                .is_ok_and(|b| predicate((*a as i64).cmp(&b))),
            (RawBsonRef::Int64(a), Bson::String(s)) => {
                s.parse::<i64>().is_ok_and(|b| predicate(a.cmp(&b)))
            }
            (RawBsonRef::Double(a), Bson::String(s)) => s
                .parse::<f64>()
                .is_ok_and(|b| predicate(a.partial_cmp(&b).unwrap_or(Ordering::Equal))),
            (RawBsonRef::DateTime(a), Bson::String(s)) => bson::DateTime::parse_rfc3339_str(s)
                .is_ok_and(|dt| predicate(a.timestamp_millis().cmp(&dt.timestamp_millis()))),

            // ── Cross-type coercion: Int → DateTime (epoch seconds) ─
            (RawBsonRef::DateTime(a), Bson::Int64(b)) => {
                predicate(a.timestamp_millis().cmp(&(*b * 1000)))
            }
            (RawBsonRef::DateTime(a), Bson::Int32(b)) => {
                predicate(a.timestamp_millis().cmp(&(*b as i64 * 1000)))
            }

            // ── Incompatible types: silent exclusion ────────────
            _ => false,
        },
        None => false,
    }
}
