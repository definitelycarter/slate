use std::cmp::Ordering;

use bson::raw::{RawArrayBuf, RawBson, RawBsonRef};
use slate_query::{Sort, SortDirection};

use crate::error::DbError;
use crate::executor::exec;
use crate::executor::{RawIter, RawValue};
use crate::executor_v2::field_tree::{FieldTree, walk_raw};

pub(crate) fn execute<'a>(
    sorts: &'a [Sort],
    mut source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    match source.next() {
        Some(Ok(Some(RawValue::Owned(RawBson::Array(arr))))) => {
            let elements: Vec<RawBson> = arr
                .into_iter()
                .filter_map(|r| exec::to_raw_bson(r.ok()?))
                .collect();
            let iter: RawIter<'a> =
                Box::new(elements.into_iter().map(|v| Ok(Some(RawValue::Owned(v)))));
            let sorted = sort_records(sorts, iter)?;
            // Re-pack into a single array
            let mut buf = RawArrayBuf::new();
            for result in sorted {
                if let Ok(Some(val)) = result {
                    exec::push_raw(&mut buf, val.as_ref());
                }
            }
            Ok(Box::new(std::iter::once(Ok(Some(RawValue::Owned(
                RawBson::Array(buf),
            ))))))
        }
        Some(first) => {
            let iter: RawIter<'a> = Box::new(std::iter::once(first).chain(source));
            sort_records(sorts, iter)
        }
        None => Ok(Box::new(std::iter::empty())),
    }
}

fn sort_records<'a>(sorts: &[Sort], source: RawIter<'a>) -> Result<RawIter<'a>, DbError> {
    let records: Vec<Option<RawValue<'a>>> = source.collect::<Result<Vec<_>, _>>()?;

    if sorts.is_empty() {
        return Ok(Box::new(records.into_iter().map(Ok)));
    }

    // Build field tree once from sort field paths
    let sort_paths: Vec<String> = sorts.iter().map(|s| s.field.clone()).collect();
    let tree = FieldTree::from_paths(&sort_paths);

    // Extract sort keys once per document via single-pass walk
    let mut keyed: Vec<(Vec<Option<RawBson>>, Option<RawValue<'a>>)> = records
        .into_iter()
        .map(|opt_val| {
            // Pre-fill with None for each sort field
            let mut keys: Vec<Option<RawBson>> = vec![None; sorts.len()];

            if let Some(ref val) = opt_val {
                match val.as_ref() {
                    RawBsonRef::Document(doc) => {
                        walk_raw(doc, &tree, |path, raw_ref| {
                            if let Some(idx) = sort_paths.iter().position(|p| p.as_str() == path) {
                                keys[idx] = exec::to_raw_bson(raw_ref);
                            }
                        });
                    }
                    other => {
                        // Non-document value: use it as the key for all sort fields
                        let owned = exec::to_raw_bson(other);
                        for key in keys.iter_mut() {
                            *key = owned.clone();
                        }
                    }
                }
            }

            (keys, opt_val)
        })
        .collect();

    keyed.sort_by(|(a_keys, _), (b_keys, _)| {
        for (i, sort) in sorts.iter().enumerate() {
            let a_ref = a_keys[i].as_ref().map(|v| v.as_raw_bson_ref());
            let b_ref = b_keys[i].as_ref().map(|v| v.as_raw_bson_ref());
            let ord = exec::raw_compare_field_values(a_ref, b_ref);
            let ord = match sort.direction {
                SortDirection::Asc => ord,
                SortDirection::Desc => ord.reverse(),
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });

    Ok(Box::new(keyed.into_iter().map(|(_, val)| Ok(val))))
}
