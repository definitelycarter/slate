use std::cmp::Ordering;

use bson::raw::{RawArrayBuf, RawBson, RawBsonRef};
use slate_query::{Sort, SortDirection};

use crate::error::DbError;
use crate::executor::exec;
use crate::executor::RawIter;

fn as_document(val: &RawBson) -> Option<&bson::RawDocument> {
    match val {
        RawBson::Document(d) => Some(d),
        _ => None,
    }
}

pub(crate) fn execute<'a>(
    sorts: Vec<Sort>,
    mut source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    match source.next() {
        Some(Ok(Some(RawBson::Array(arr)))) => {
            let sort = match sorts.first() {
                Some(s) => s,
                None => {
                    return Ok(Box::new(std::iter::once(Ok(Some(RawBson::Array(arr))))));
                }
            };
            let mut elements: Vec<RawBson> = arr
                .into_iter()
                .filter_map(|r| exec::to_raw_bson(r.ok()?))
                .collect();
            elements.sort_by(|a, b| {
                let a_ref = a.as_raw_bson_ref();
                let b_ref = b.as_raw_bson_ref();
                let ord = match (&a_ref, &b_ref) {
                    (RawBsonRef::Document(a_doc), RawBsonRef::Document(b_doc)) => {
                        let a_field = exec::raw_get_path(a_doc, &sort.field).ok().flatten();
                        let b_field = exec::raw_get_path(b_doc, &sort.field).ok().flatten();
                        exec::raw_compare_field_values(a_field, b_field)
                    }
                    _ => exec::raw_compare_field_values(Some(a_ref), Some(b_ref)),
                };
                match sort.direction {
                    SortDirection::Asc => ord,
                    SortDirection::Desc => ord.reverse(),
                }
            });
            let mut buf = RawArrayBuf::new();
            for elem in &elements {
                exec::push_raw(&mut buf, elem.as_raw_bson_ref());
            }
            Ok(Box::new(std::iter::once(Ok(Some(RawBson::Array(buf))))))
        }
        Some(first) => {
            let iter: RawIter<'a> = Box::new(std::iter::once(first).chain(source));
            sort_records(&sorts, iter)
        }
        None => Ok(Box::new(std::iter::empty())),
    }
}

fn sort_records<'a>(sorts: &[Sort], source: RawIter<'a>) -> Result<RawIter<'a>, DbError> {
    let mut records: Vec<Option<RawBson>> = source.collect::<Result<Vec<_>, _>>()?;

    if sorts.is_empty() {
        return Ok(Box::new(records.into_iter().map(Ok)));
    }

    // Borrow fields at comparison time — zero key allocations.
    records.sort_by(|a_opt, b_opt| {
        for sort in sorts {
            let a_field = a_opt
                .as_ref()
                .and_then(as_document)
                .and_then(|r| exec::raw_get_path(r, &sort.field).ok().flatten());
            let b_field = b_opt
                .as_ref()
                .and_then(as_document)
                .and_then(|r| exec::raw_get_path(r, &sort.field).ok().flatten());
            let ord = exec::raw_compare_field_values(a_field, b_field);
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

    Ok(Box::new(records.into_iter().map(Ok)))
}
