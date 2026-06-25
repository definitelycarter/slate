//! The `CompoundIndexScan` source — yields bare document IDs from a compound
//! (multi-field) index over its leftmost-prefix range.
//!
//! Maps the IR's [`CompoundScanRange`] onto the engine's [`CompoundRange`] and
//! streams the matching entries' doc-IDs. Pair with [`super::key_lookup`] to
//! fetch the documents.
//!
//! ## Post-filter: keeping the compound scan exact
//!
//! The engine seeks a byte range over `field_prefix + concat(eq_value_bytes)`.
//! That range is a *conservative superset*: a variable-width (string) leading
//! component's byte prefix can sweep in entries whose later bytes happen to
//! continue the prefix (e.g. an `"active"` equality reaching `"active2"`
//! entries), and any component after the trailing predicate is unconstrained at
//! the engine. So every entry is rechecked here — each leading equality and the
//! trailing range — with the same coercing, type-bracketed comparator `WHERE`
//! uses ([`slate_eval::compare_bson`]), so the compound index can't drift from a
//! full scan + residual filter.

use std::cmp::Ordering;

use bson::Bson;
use slate_engine::{Catalog, CompoundRange, CompoundTail, EngineTransaction};
use slate_eval::compare_bson;
use slate_planner::{CollectionRef, CompoundScanRange, CompoundScanTail, ScanDirection};

use crate::{ExecError, ValueIter};

/// The exact, in-memory recheck of a compound entry: each leading equality must
/// match its stored component value, and the trailing range (if any) must hold.
/// All comparisons coerce through [`compare_bson`], so the recheck is identical
/// to the residual `Filter` the planner keeps.
struct CompoundFilter {
    eq_prefix: Vec<Bson>,
    tail: CompoundScanTail,
}

impl CompoundFilter {
    fn for_range(range: &CompoundScanRange) -> Self {
        Self {
            eq_prefix: range.eq_prefix.clone(),
            tail: range.tail.clone(),
        }
    }

    /// How many leading components the recheck reads: the equality prefix plus
    /// one for a bounded tail. Pre-decoded by the caller.
    fn component_count(&self) -> usize {
        self.eq_prefix.len() + usize::from(!matches!(self.tail, CompoundScanTail::Unbounded))
    }

    /// Whether the pre-decoded components satisfy the compound predicate.
    /// `components[idx]` is component `idx`'s stored value (`None` if absent).
    /// A missing or non-comparable component fails the check (drops the row).
    fn keeps(&self, components: &[Option<Bson>]) -> bool {
        // Every leading equality must match exactly.
        for (idx, want) in self.eq_prefix.iter().enumerate() {
            match components.get(idx).and_then(|c| c.as_ref()) {
                Some(got) if compare_bson(got, want) == Some(Ordering::Equal) => {}
                _ => return false,
            }
        }
        // The trailing predicate applies to the component right after the prefix.
        let tail_idx = self.eq_prefix.len();
        let got = components.get(tail_idx).and_then(|c| c.as_ref());
        match &self.tail {
            CompoundScanTail::Unbounded => true,
            CompoundScanTail::Eq(want) => {
                matches!(got, Some(g) if compare_bson(g, want) == Some(Ordering::Equal))
            }
            CompoundScanTail::Range { lower, upper } => match got {
                Some(g) => in_bounds(g, lower, upper),
                None => false,
            },
        }
    }
}

/// Whether `value` falls within an optional inclusive/exclusive range, coercing
/// through [`compare_bson`]; a non-comparable (cross-type) value is excluded.
fn in_bounds(value: &Bson, lower: &Option<(Bson, bool)>, upper: &Option<(Bson, bool)>) -> bool {
    let within = |bound: &Option<(Bson, bool)>, want_below: bool| match bound {
        None => true,
        Some((b, inclusive)) => match compare_bson(value, b) {
            Some(Ordering::Equal) => *inclusive,
            Some(Ordering::Less) => want_below,
            Some(Ordering::Greater) => !want_below,
            None => false,
        },
    };
    within(lower, false) && within(upper, true)
}

pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
    field: String,
    range: &CompoundScanRange,
    direction: ScanDirection,
    limit: Option<usize>,
) -> Result<ValueIter<'a>, ExecError> {
    let handle = txn.collection(&collection.cf, &collection.collection)?;
    let post_filter = CompoundFilter::for_range(range);

    // Borrow the IR bounds into the engine's `CompoundRange` (no value clone).
    let tail = match &range.tail {
        CompoundScanTail::Unbounded => CompoundTail::Unbounded,
        CompoundScanTail::Eq(v) => CompoundTail::Eq(v),
        CompoundScanTail::Range { lower, upper } => CompoundTail::Range {
            lower: lower.as_ref().map(|(v, incl)| (v, *incl)),
            upper: upper.as_ref().map(|(v, incl)| (v, *incl)),
        },
    };
    let engine_range = CompoundRange {
        eq_prefix: &range.eq_prefix,
        tail,
    };
    let reverse = matches!(direction, ScanDirection::Reverse);

    let mut iter = txn.scan_compound_index(&handle, &field, engine_range, reverse)?;
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
            // Recheck each leading equality and the trailing range exactly,
            // dropping the conservative over-read (see module docs). Pre-decode
            // the components the filter reads; a decode error aborts the scan.
            let mut components: Vec<Option<Bson>> =
                Vec::with_capacity(post_filter.component_count());
            for idx in 0..post_filter.component_count() {
                match entry.component_value(idx) {
                    // A value the high-level model can't represent becomes `None`,
                    // which fails the recheck and drops the row — the same
                    // not-comparable handling `index_scan` uses (`_ => continue`),
                    // not a silently swallowed error.
                    Ok(Some(raw)) => match Bson::try_from(raw.as_raw_bson_ref()) {
                        Ok(b) => components.push(Some(b)),
                        Err(_) => components.push(None),
                    },
                    Ok(None) => components.push(None),
                    Err(e) => {
                        done = true;
                        return Some(Err(ExecError::Engine(e)));
                    }
                }
            }
            if !post_filter.keeps(&components) {
                continue;
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

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::collect;
    use bson::{RawBson, rawdoc};
    use slate_engine::{Catalog, DEFAULT_CF, Engine, EngineTransaction, KvEngine};
    use slate_planner::{CollectionRef, CompoundScanRange, CompoundScanTail, ScanDirection};
    use slate_store::MemoryStore;

    /// A collection `orders` with a compound index on `["status", "created_at"]`
    /// holding documents that exercise the leading-string over-read (`active` vs
    /// `active2`) and the trailing range.
    fn seeded_orders() -> KvEngine<MemoryStore> {
        let engine = KvEngine::new(MemoryStore::new());
        {
            let txn = engine.begin(false).unwrap();
            txn.create_collection(DEFAULT_CF, "orders", &Default::default())
                .unwrap();
            txn.create_compound_index(
                DEFAULT_CF,
                "orders",
                &["status".to_string(), "created_at".to_string()],
            )
            .unwrap();
            txn.commit().unwrap();
        }
        {
            let txn = engine.begin(false).unwrap();
            let handle = txn.collection(DEFAULT_CF, "orders").unwrap();
            for doc in [
                rawdoc! { "_id": "a", "status": "active", "created_at": 10 },
                rawdoc! { "_id": "b", "status": "active", "created_at": 20 },
                rawdoc! { "_id": "c", "status": "active", "created_at": 30 },
                // "active2" shares the "active" byte prefix — the seek over-reads
                // it, the recheck must drop it.
                rawdoc! { "_id": "d", "status": "active2", "created_at": 15 },
                rawdoc! { "_id": "e", "status": "archived", "created_at": 25 },
            ] {
                txn.put(&handle, &doc).unwrap();
            }
            txn.commit().unwrap();
        }
        engine
    }

    fn orders_ref() -> CollectionRef {
        CollectionRef {
            cf: DEFAULT_CF.into(),
            collection: "orders".into(),
        }
    }

    fn id(s: &str) -> RawBson {
        RawBson::String(s.into())
    }

    fn scan_ids(range: CompoundScanRange) -> Vec<RawBson> {
        let engine = seeded_orders();
        let txn = engine.begin(true).unwrap();
        let identity =
            slate_engine::join_index_fields(&["status".to_string(), "created_at".to_string()]);
        let iter = execute(
            &txn,
            &orders_ref(),
            identity,
            &range,
            ScanDirection::Forward,
            None,
        )
        .unwrap();
        collect(iter).unwrap()
    }

    #[test]
    fn equality_prefix_excludes_byte_prefix_collision() {
        // status = "active" must NOT include the "active2" doc, even though its
        // bytes share the "active" prefix (the seek over-reads it).
        let ids = scan_ids(CompoundScanRange {
            eq_prefix: vec![bson::Bson::String("active".into())],
            tail: CompoundScanTail::Unbounded,
        });
        assert_eq!(ids, vec![id("a"), id("b"), id("c")]);
    }

    #[test]
    fn equality_prefix_with_trailing_range() {
        // status = "active" AND created_at > 15 → b (20), c (30); the range drops
        // a (10), and the recheck still excludes the "active2" doc.
        let ids = scan_ids(CompoundScanRange {
            eq_prefix: vec![bson::Bson::String("active".into())],
            tail: CompoundScanTail::Range {
                lower: Some((bson::Bson::Int64(15), false)),
                upper: None,
            },
        });
        assert_eq!(ids, vec![id("b"), id("c")]);
    }

    #[test]
    fn full_equality_on_both_components() {
        // status = "active" AND created_at = 20 → just b.
        let ids = scan_ids(CompoundScanRange {
            eq_prefix: vec![bson::Bson::String("active".into()), bson::Bson::Int64(20)],
            tail: CompoundScanTail::Unbounded,
        });
        assert_eq!(ids, vec![id("b")]);
    }

    #[test]
    fn distinct_leading_value_isolated() {
        let ids = scan_ids(CompoundScanRange {
            eq_prefix: vec![bson::Bson::String("active2".into())],
            tail: CompoundScanTail::Unbounded,
        });
        assert_eq!(ids, vec![id("d")]);
    }
}
