//! The `Distinct` node — deduplicate the input stream by value identity.
//!
//! A lazy transform: emit the first occurrence of each distinct value, drop
//! later duplicates. Dedup is hash-based (matching v1). Because v2 has
//! expression `Project`, this needs no field argument — `Project(c.city) →
//! Distinct` yields distinct cities; over documents it dedups whole documents.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use bson::raw::RawBsonRef;

use crate::ValueIter;

/// Wrap `source`, emitting each distinct value once.
pub(crate) fn execute<'a>(source: ValueIter<'a>) -> ValueIter<'a> {
    let mut seen = HashSet::new();
    Box::new(source.filter_map(move |item| match item {
        Ok(Some(value)) => {
            let h = hash_value(value.as_raw_bson_ref());
            if seen.insert(h) {
                Some(Ok(Some(value)))
            } else {
                None
            }
        }
        Ok(None) => None,
        Err(e) => Some(Err(e)),
    }))
}

fn hash_value(r: RawBsonRef) -> u64 {
    let mut h = std::hash::DefaultHasher::new();
    match r {
        RawBsonRef::String(s) => {
            0u8.hash(&mut h);
            s.hash(&mut h);
        }
        RawBsonRef::Int32(i) => {
            1u8.hash(&mut h);
            i.hash(&mut h);
        }
        RawBsonRef::Int64(i) => {
            2u8.hash(&mut h);
            i.hash(&mut h);
        }
        RawBsonRef::Double(f) => {
            3u8.hash(&mut h);
            f.to_bits().hash(&mut h);
        }
        RawBsonRef::Boolean(b) => {
            4u8.hash(&mut h);
            b.hash(&mut h);
        }
        RawBsonRef::DateTime(dt) => {
            5u8.hash(&mut h);
            dt.timestamp_millis().hash(&mut h);
        }
        RawBsonRef::ObjectId(oid) => {
            6u8.hash(&mut h);
            oid.bytes().hash(&mut h);
        }
        RawBsonRef::Document(d) => {
            7u8.hash(&mut h);
            d.as_bytes().hash(&mut h);
        }
        RawBsonRef::Array(a) => {
            8u8.hash(&mut h);
            a.as_bytes().hash(&mut h);
        }
        RawBsonRef::Null => {
            9u8.hash(&mut h);
        }
        _ => {
            255u8.hash(&mut h);
        }
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::collect;
    use crate::nodes::test_support::{bind_c, sv};
    use crate::nodes::{project, values};
    use bson::{RawBson, rawdoc};

    #[test]
    fn dedups_scalars_first_occurrence_order() {
        let out = collect(execute(values::execute(vec![
            RawBson::Int32(1),
            RawBson::Int32(2),
            RawBson::Int32(1),
            RawBson::Int32(3),
            RawBson::Int32(2),
        ])))
        .unwrap();
        assert_eq!(
            out,
            vec![RawBson::Int32(1), RawBson::Int32(2), RawBson::Int32(3)]
        );
    }

    #[test]
    fn dedups_identical_documents() {
        let doc = RawBson::Document(rawdoc! { "a": 1 });
        let out = collect(execute(values::execute(vec![doc.clone(), doc.clone()]))).unwrap();
        assert_eq!(out, vec![doc]);
    }

    #[test]
    fn distinct_over_project_yields_distinct_keys() {
        // SELECT DISTINCT VALUE c.team  →  ["a", "b"]
        let docs = vec![
            RawBson::Document(rawdoc! { "team": "a" }),
            RawBson::Document(rawdoc! { "team": "b" }),
            RawBson::Document(rawdoc! { "team": "a" }),
        ];
        let projected = project::execute(sv("c.team"), bind_c(docs));
        let out = collect(execute(projected)).unwrap();
        assert_eq!(
            out,
            vec![RawBson::String("a".into()), RawBson::String("b".into())]
        );
    }
}
