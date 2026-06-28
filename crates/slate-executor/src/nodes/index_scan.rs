//! The `IndexScan` source — yields document IDs from a field index, or — when
//! `covering` — synthesized documents served entirely from the index entries.
//!
//! Maps the IR's [`IndexScanRange`] onto the engine's `IndexRange` and streams
//! the matching entries' doc-IDs. Pair with [`super::key_lookup`] to fetch the
//! documents — unless the scan is **covering**, in which case it emits a
//! synthesized document per entry — `{field: entry.value(), <pk>: doc_id}`, with
//! the value nested when `field` is a dotted path (`user.id` →
//! `{user: {id: value}}`) — and the planner omits the `KeyLookup` (RFC: Covering
//! Index Scans, Part B). The planner only sets `covering` after proving the query
//! reads nothing but `field` and the pk, so the two values the entry carries are
//! all it needs.
//!
//! ## Post-filter: keeping the index identical to a scan
//!
//! The index stores every value in one sortable keyspace with no type tag in the
//! key. An `Eq` — numeric or not — needs no post-filter: the engine's
//! `scan_index` matches the value exactly. Non-numerics match by tag+bytes;
//! numerics match by the *unified f64 key* (`Int32`/`Int64`/`Double` collapse
//! onto one 8-byte key, so a cross-type `Eq` like a SQL `Int64` literal `40`
//! against an `Int32`-stored field is a single tight seek). Prefix-sharing and
//! same-bytes-different-type values never reach us.
//!
//! A `Range`, by contrast, scans a byte range and can sweep in entries of other
//! types whose sortable bytes fall in range (e.g. a `> "m"` string scan reaching
//! numeric/`DateTime` entries). Those are post-filtered with
//! [`slate_eval::compare_bson`] — the same coercing, type-bracketed comparator
//! `WHERE` uses — so the index can't drift from a scan.

use std::cmp::Ordering;

use bson::raw::CString;
use bson::{Bson, RawBson, RawDocumentBuf};
use slate_engine::{Catalog, EngineError, EngineTransaction, IndexEntry, IndexRange};
use slate_eval::compare_bson;
use slate_planner::{CollectionRef, IndexScanRange, ScanDirection};

use crate::budget::Ticker;
use crate::{ExecError, ValueIter};

/// Build the covering row for one entry: the indexed value at `field` — nested
/// when `field` is a dotted path (`user.id` → `{user: {id: value}}`) — and the
/// doc-id under the (always top-level) `pk_path`. The pk is skipped when the
/// index field *is* the pk (avoiding a duplicate key); the planner proved the
/// query reads nothing but these two paths, so the synthesized document is a
/// drop-in for the fetched one.
fn synthesize_row(entry: &IndexEntry, field: &str, pk_path: &str) -> Result<RawBson, EngineError> {
    let mut doc = RawDocumentBuf::new();
    append_path(&mut doc, field, entry.value()?)?;
    if pk_path != field {
        let pk_key = CString::try_from(pk_path)
            .map_err(|e| EngineError::InvalidKey(format!("pk path {pk_path:?}: {e}")))?;
        doc.append(pk_key, entry.doc_id()?);
    }
    Ok(RawBson::Document(doc))
}

/// Append `value` at the dotted `path` into `doc`, materializing intermediate
/// nested objects: a top-level `path` (no `.`) appends directly, while `user.id`
/// builds `{user: {id: value}}`. Single-component synthesis only — one value per
/// call — so the nested objects never need merging (compound covering, a later
/// phase, does). The planner guarantees `path` carries no `.[]` multikey marker.
fn append_path(doc: &mut RawDocumentBuf, path: &str, value: RawBson) -> Result<(), EngineError> {
    match path.split_once('.') {
        None => {
            let key = CString::try_from(path)
                .map_err(|e| EngineError::InvalidKey(format!("index path {path:?}: {e}")))?;
            doc.append(key, value);
        }
        Some((head, rest)) => {
            let key = CString::try_from(head).map_err(|e| {
                EngineError::InvalidKey(format!("index path segment {head:?}: {e}"))
            })?;
            let mut inner = RawDocumentBuf::new();
            append_path(&mut inner, rest, value)?;
            doc.append(key, RawBson::Document(inner));
        }
    }
    Ok(())
}

/// A bounded range predicate matched with the same coercing, type-bracketed
/// comparator `WHERE` uses, so the index path returns exactly what a residual
/// filter would. Built only for a `Range`, whose byte scan can over-return
/// cross-type entries; every `Eq` (numeric included, via the unified f64 key) is
/// already exact at the engine and needs none.
struct CoercingFilter {
    lower: Option<(Bson, bool)>,
    upper: Option<(Bson, bool)>,
}

impl CoercingFilter {
    /// Build the post-filter for a predicate, or `None` when the scan is already
    /// exact (`Eq` and `Full`).
    fn for_range(range: &IndexScanRange) -> Option<Self> {
        match range {
            IndexScanRange::Range { lower, upper } => Some(Self {
                lower: lower.clone(),
                upper: upper.clone(),
            }),
            // `Eq`/`Full` are exact at the engine; `StringPrefix` is exact for
            // string values (the byte range admits exactly the prefix-sharing
            // strings) — its only over-return is cross-type byte coincidences,
            // which the plan's retained `STARTSWITH`/`LIKE` recheck drops.
            IndexScanRange::Full | IndexScanRange::Eq(_) | IndexScanRange::StringPrefix(_) => None,
        }
    }

    /// Whether a stored index value falls in the range. A non-comparable stored
    /// value (cross-type, i.e. `compare_bson` → `None`) is excluded.
    fn keeps(&self, stored: &Bson) -> bool {
        let within = |bound: &Option<(Bson, bool)>, want_below: bool| match bound {
            None => true,
            Some((b, inclusive)) => match compare_bson(stored, b) {
                Some(Ordering::Equal) => *inclusive,
                Some(Ordering::Less) => want_below,
                Some(Ordering::Greater) => !want_below,
                None => false,
            },
        };
        within(&self.lower, false) && within(&self.upper, true)
    }
}

// Each arg is a distinct, meaningful scan parameter (container + field + range +
// direction + limit + covering) plus the deadline ticker; bundling them into a
// struct just to satisfy the lint would add indirection without clarity.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
    field: String,
    range: &IndexScanRange,
    direction: ScanDirection,
    limit: Option<usize>,
    covering: bool,
    mut ticker: Ticker,
) -> Result<ValueIter<'a>, ExecError> {
    let handle = txn.collection(&collection.cf, &collection.collection)?;

    // A covering scan synthesizes rows from entries, so it needs the pk path;
    // own it (a short string) since the closure outlives the borrowed handle. The
    // common non-covering scan pays nothing.
    let pk_path = covering.then(|| handle.pk_path().to_string());

    let post_filter = CoercingFilter::for_range(range);

    let engine_range = match range {
        IndexScanRange::Full => IndexRange::Full,
        IndexScanRange::Eq(v) => IndexRange::Eq(v),
        IndexScanRange::Range { lower, upper } => IndexRange::Range {
            lower: lower.as_ref().map(|(v, incl)| (v, *incl)),
            upper: upper.as_ref().map(|(v, incl)| (v, *incl)),
        },
        IndexScanRange::StringPrefix(prefix) => IndexRange::Prefix(prefix.as_str()),
    };
    let reverse = matches!(direction, ScanDirection::Reverse);

    crate::trace::trace_event!(
        cf = collection.cf.as_str(),
        collection = collection.collection.as_str(),
        field = field.as_str(),
        reverse = reverse,
        "index scan opened"
    );
    let mut iter = txn.scan_index(&handle, &field, engine_range, reverse)?;
    let mut count = 0usize;
    let mut done = false;

    Ok(Box::new(std::iter::from_fn(move || {
        if done {
            return None;
        }
        for result in iter.by_ref() {
            // Cooperative deadline check, once per *examined* entry — so a
            // heavily post-filtered range scan still aborts on time.
            if let Err(e) = ticker.tick() {
                done = true;
                return Some(Err(e));
            }
            let entry = match result {
                Ok(e) => e,
                Err(e) => {
                    done = true;
                    return Some(Err(ExecError::Engine(e)));
                }
            };

            // Any Range: coercing, type-bracketed post-filter that drops the
            // cross-type entries a byte scan sweeps in (see module docs).
            if let Some(ref filter) = post_filter {
                let stored = match entry.value() {
                    Ok(v) => v,
                    Err(e) => {
                        done = true;
                        return Some(Err(ExecError::Engine(e)));
                    }
                };
                match Bson::try_from(stored.as_raw_bson_ref()) {
                    Ok(b) if filter.keeps(&b) => {}
                    _ => continue,
                }
            }

            if let Some(n) = limit
                && count >= n
            {
                done = true;
                return None;
            }
            count += 1;

            // Covering: emit the synthesized `{field, pk}` row; otherwise the
            // bare doc-id for the paired `KeyLookup` to fetch.
            let yielded = match pk_path.as_deref() {
                Some(pk) => synthesize_row(&entry, &field, pk),
                None => entry.doc_id(),
            };
            return match yielded {
                Ok(row) => Some(Ok(Some(row))),
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

#[cfg(test)]
mod tests {
    use super::{append_path, execute};
    use crate::budget::Ticker;
    use crate::collect;
    use crate::nodes::test_support::{people_ref, seeded_people};
    use bson::{Bson, RawBson, RawDocumentBuf};
    use slate_engine::Engine;
    use slate_planner::{IndexScanRange, ScanDirection};

    fn scan_ids(
        range: IndexScanRange,
        direction: ScanDirection,
        limit: Option<usize>,
    ) -> Vec<RawBson> {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let iter = execute(
            &txn,
            &people_ref(),
            "age".into(),
            &range,
            direction,
            limit,
            false,
            Ticker::new(None),
        )
        .unwrap();
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

    #[test]
    fn numeric_range_matches_across_int_types() {
        // Stored ages are Int32 (36, 41, 44). An Int64 bound `> 40` must still
        // return exactly 2 and 3 — not sweep in 1 (the cross-type over-return
        // bug a typed range scan produced).
        let range = IndexScanRange::Range {
            lower: Some((Bson::Int64(40), false)),
            upper: None,
        };
        assert_eq!(
            scan_ids(range, ScanDirection::Forward, None),
            vec![id("2"), id("3")]
        );

        // And the upper-bound / exclusive direction: Int64 `< 41` → only id 1.
        let range = IndexScanRange::Range {
            lower: None,
            upper: Some((Bson::Int64(41), false)),
        };
        assert_eq!(scan_ids(range, ScanDirection::Forward, None), vec![id("1")]);
    }

    #[test]
    fn numeric_range_matches_double_bound() {
        // Int32-stored ages with a Double bound: `>= 41.0` → ids 2, 3.
        let range = IndexScanRange::Range {
            lower: Some((Bson::Double(41.0), true)),
            upper: None,
        };
        assert_eq!(
            scan_ids(range, ScanDirection::Forward, None),
            vec![id("2"), id("3")]
        );
    }

    #[test]
    fn append_path_top_level_appends_directly_and_keeps_type() {
        // A top-level field appends directly; Int32 stays Int32 (40 ≠ 40.0), so
        // synthesis preserves the entry value's type.
        let mut doc = RawDocumentBuf::new();
        append_path(&mut doc, "age", RawBson::Int32(40)).unwrap();
        assert_eq!(doc.get_i32("age").unwrap(), 40);
        assert_eq!(doc.iter().count(), 1);
    }

    #[test]
    fn append_path_builds_nested_object_for_dotted_field() {
        // `user.id` → `{user: {id: value}}`, navigable to the same path the query
        // reads.
        let mut doc = RawDocumentBuf::new();
        append_path(&mut doc, "user.id", RawBson::String("u1".into())).unwrap();
        let user = doc.get_document("user").unwrap();
        assert_eq!(user.get_str("id").unwrap(), "u1");
    }

    #[test]
    fn append_path_handles_three_segments() {
        // Deeper paths nest one object per segment.
        let mut doc = RawDocumentBuf::new();
        append_path(&mut doc, "a.b.c", RawBson::Boolean(true)).unwrap();
        let c = doc
            .get_document("a")
            .unwrap()
            .get_document("b")
            .unwrap()
            .get_bool("c")
            .unwrap();
        assert!(c);
    }
}
