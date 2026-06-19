//! The `IndexScan` source — yields bare document IDs from a field index.
//!
//! Maps the IR's [`IndexScanRange`] onto the engine's `IndexRange` and streams
//! the matching entries' doc-IDs. Numeric `Eq` is matched by a full field scan
//! plus an `i64` post-filter, because `Int32` and `Int64` encode differently in
//! the index's sortable keys (so an `Eq` prefix scan would miss cross-type
//! matches). Pair with [`super::key_lookup`] to fetch the documents.

use bson::{Bson, RawBson};
use slate_engine::{Catalog, EngineTransaction, IndexRange};
use slate_planner::{CollectionRef, IndexScanRange, ScanDirection};

use crate::{ExecError, ValueIter};

pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
    field: String,
    range: &IndexScanRange,
    direction: ScanDirection,
    limit: Option<usize>,
) -> Result<ValueIter<'a>, ExecError> {
    let handle = txn.collection(&collection.cf, &collection.collection)?;

    // Numeric Eq → full scan + i64 post-filter (see module docs).
    let numeric_eq_value: Option<i64> = match range {
        IndexScanRange::Eq(Bson::Int32(n)) => Some(*n as i64),
        IndexScanRange::Eq(Bson::Int64(n)) => Some(*n),
        _ => None,
    };

    let engine_range = match range {
        IndexScanRange::Full => IndexRange::Full,
        IndexScanRange::Eq(_) if numeric_eq_value.is_some() => IndexRange::Full,
        IndexScanRange::Eq(v) => IndexRange::Eq(v),
        IndexScanRange::Range { lower, upper } => IndexRange::Range {
            lower: lower.as_ref().map(|(v, incl)| (v, *incl)),
            upper: upper.as_ref().map(|(v, incl)| (v, *incl)),
        },
    };
    let reverse = matches!(direction, ScanDirection::Reverse);

    let mut iter = txn.scan_index(&handle, &field, engine_range, reverse)?;
    let mut count = 0usize;
    let mut done = false;

    Ok(Box::new(std::iter::from_fn(move || {
        if done {
            return None;
        }
        for result in iter.by_ref() {
            let entry = match result {
                Ok(e) => e,
                Err(e) => {
                    done = true;
                    return Some(Err(ExecError::Engine(e)));
                }
            };

            // Numeric Eq cross-type post-filter.
            if let Some(query_val) = numeric_eq_value {
                match entry.value() {
                    Ok(v) if raw_bson_as_i64(&v) == Some(query_val) => {}
                    Ok(_) => continue,
                    Err(e) => {
                        done = true;
                        return Some(Err(ExecError::Engine(e)));
                    }
                }
            }

            if let Some(n) = limit
                && count >= n
            {
                done = true;
                return None;
            }
            count += 1;

            return match entry.doc_id() {
                Ok(id) => Some(Ok(Some(id))),
                Err(e) => {
                    done = true;
                    Some(Err(ExecError::Engine(e)))
                }
            };
        }
        done = true;
        None
    })))
}

fn raw_bson_as_i64(val: &RawBson) -> Option<i64> {
    match val {
        RawBson::Int32(n) => Some(*n as i64),
        RawBson::Int64(n) => Some(*n),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::collect;
    use crate::nodes::test_support::{people_ref, seeded_people};
    use bson::{Bson, RawBson};
    use slate_engine::Engine;
    use slate_planner::{IndexScanRange, ScanDirection};

    fn scan_ids(
        range: IndexScanRange,
        direction: ScanDirection,
        limit: Option<usize>,
    ) -> Vec<RawBson> {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let iter = execute(&txn, &people_ref(), "age".into(), &range, direction, limit).unwrap();
        collect(iter).unwrap()
    }

    fn id(s: &str) -> RawBson {
        RawBson::String(s.into())
    }

    #[test]
    fn full_forward_yields_ids_in_index_order() {
        // ages 36(1), 41(2), 44(3) ascending
        assert_eq!(
            scan_ids(IndexScanRange::Full, ScanDirection::Forward, None),
            vec![id("1"), id("2"), id("3")]
        );
    }

    #[test]
    fn full_reverse() {
        assert_eq!(
            scan_ids(IndexScanRange::Full, ScanDirection::Reverse, None),
            vec![id("3"), id("2"), id("1")]
        );
    }

    #[test]
    fn limit_truncates() {
        assert_eq!(
            scan_ids(IndexScanRange::Full, ScanDirection::Forward, Some(2)),
            vec![id("1"), id("2")]
        );
    }

    #[test]
    fn numeric_eq_matches_across_int_types() {
        // Stored age is Int32(41); query with Int64(41) still matches id "2".
        assert_eq!(
            scan_ids(
                IndexScanRange::Eq(Bson::Int64(41)),
                ScanDirection::Forward,
                None
            ),
            vec![id("2")]
        );
    }

    #[test]
    fn range_bounds() {
        // age >= 41  → ids 2, 3
        let range = IndexScanRange::Range {
            lower: Some((Bson::Int32(41), true)),
            upper: None,
        };
        assert_eq!(
            scan_ids(range, ScanDirection::Forward, None),
            vec![id("2"), id("3")]
        );
    }
}
