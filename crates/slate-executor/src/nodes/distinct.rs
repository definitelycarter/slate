//! The `Distinct` node — deduplicate the input stream by value identity.
//!
//! A lazy transform: emit the first occurrence of each distinct value, drop
//! later duplicates. Dedup is hash-based (matching v1). Because v2 has
//! expression `Project`, this needs no field argument — `Project(c.city) →
//! Distinct` yields distinct cities; over documents it dedups whole documents.
//!
//! **Multikey:** an array input is flattened one level — its elements are the
//! distinct values, not the array as a whole. This matches v1's `distinct`
//! (the node's only caller today): `distinct("tags")` over `["a","b"]` and
//! `["b","c"]` yields `a, b, c`.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use bson::RawBson;
use bson::raw::RawBsonRef;

use slate_eval::EvalError;

use crate::budget;
use crate::{ExecError, ValueIter};

/// Wrap `source`, emitting each distinct value once. When `flatten` is set an
/// array row is flattened one level (Mongo `distinct` multikey); otherwise the
/// whole array is one value (SQL `SELECT DISTINCT`, matching Cosmos).
///
/// `cap` is the materialization cap (Resource Limits RFC, B): the distinct set
/// (`seen`) is the buffer that grows, so the stream aborts with `LimitExceeded`
/// once it exceeds `cap`. `None` is unbounded.
pub(crate) fn execute<'a>(
    source: ValueIter<'a>,
    flatten: bool,
    cap: Option<usize>,
) -> ValueIter<'a> {
    let mut seen = HashSet::new();
    Box::new(source.flat_map(move |item| {
        let mut emitted: Vec<Result<Option<RawBson>, ExecError>> = match item {
            Ok(Some(value)) => emit_distinct(value, flatten, &mut seen),
            Ok(None) => Vec::new(),
            Err(e) => vec![Err(e)],
        };
        // OOM guard: the distinct set is the buffer. Surface the cap breach on the
        // stream right after this row's values; the consumer stops on the error.
        if let Err(e) = budget::check_cap(seen.len(), cap, "Distinct") {
            emitted.push(Err(e));
        }
        emitted.into_iter()
    }))
}

/// Emit the not-yet-seen values from `value`. With `flatten`, an array
/// contributes each of its elements (multikey); otherwise — and for any other
/// value — the value contributes itself.
fn emit_distinct(
    value: RawBson,
    flatten: bool,
    seen: &mut HashSet<u64>,
) -> Vec<Result<Option<RawBson>, ExecError>> {
    match value {
        RawBson::Array(arr) if flatten => {
            let mut out = Vec::new();
            for entry in &arr {
                match entry {
                    Ok(elem) => {
                        if seen.insert(hash_value(elem)) {
                            out.push(Ok(Some(RawBson::from(elem))));
                        }
                    }
                    Err(e) => out.push(Err(ExecError::Eval(EvalError {
                        message: format!("could not read array element: {e}"),
                    }))),
                }
            }
            out
        }
        other => {
            if seen.insert(hash_value(other.as_raw_bson_ref())) {
                vec![Ok(Some(other))]
            } else {
                Vec::new()
            }
        }
    }
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
    use slate_planner::RowBinding;

    #[test]
    fn dedups_scalars_first_occurrence_order() {
        let out = collect(execute(
            values::execute(vec![
                RawBson::Int32(1),
                RawBson::Int32(2),
                RawBson::Int32(1),
                RawBson::Int32(3),
                RawBson::Int32(2),
            ]),
            true,
            None,
        ))
        .unwrap();
        assert_eq!(
            out,
            vec![RawBson::Int32(1), RawBson::Int32(2), RawBson::Int32(3)]
        );
    }

    #[test]
    fn dedups_identical_documents() {
        let doc = RawBson::Document(rawdoc! { "a": 1 });
        let out = collect(execute(
            values::execute(vec![doc.clone(), doc.clone()]),
            true,
            None,
        ))
        .unwrap();
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
        let projected = project::execute(
            sv("c.team"),
            RowBinding::Env,
            bind_c(docs),
            crate::ExecEnv::new(),
        );
        let out = collect(execute(projected, true, None)).unwrap();
        assert_eq!(
            out,
            vec![RawBson::String("a".into()), RawBson::String("b".into())]
        );
    }

    #[test]
    fn flatten_true_unwraps_arrays_one_level() {
        // Mongo `distinct("tags")`: [a,b] and [b,c] → a, b, c.
        let rows = vec![bson::rawbson!(["a", "b"]), bson::rawbson!(["b", "c"])];
        let out = collect(execute(values::execute(rows), true, None)).unwrap();
        assert_eq!(
            out,
            vec![
                RawBson::String("a".into()),
                RawBson::String("b".into()),
                RawBson::String("c".into())
            ]
        );
    }

    #[test]
    fn flatten_false_keeps_whole_arrays() {
        // SQL `SELECT DISTINCT VALUE c.tags`: arrays are dedup'd whole, matching
        // Cosmos — [a,b] (×2) collapses to one, [b,c] stays.
        let ab = bson::rawbson!(["a", "b"]);
        let bc = bson::rawbson!(["b", "c"]);
        let rows = vec![ab.clone(), ab.clone(), bc.clone()];
        let out = collect(execute(values::execute(rows), false, None)).unwrap();
        assert_eq!(out, vec![ab, bc]);
    }
}
