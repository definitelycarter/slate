//! The `CompoundIndexScan` source — yields bare document IDs from a compound
//! (multi-field) index over its leftmost-prefix range, or — when `covering` —
//! synthesized documents served entirely from the index entries.
//!
//! Maps the IR's [`CompoundScanRange`] onto the engine's [`CompoundRange`] and
//! streams the matching entries' doc-IDs. Pair with [`super::key_lookup`] to
//! fetch the documents — unless the scan is **covering**, in which case it emits a
//! synthesized document per entry, built from each index component's value placed
//! at its dotted path (merging shared prefixes, so `(user.id, user.name)` →
//! `{user: {id, name}}`) plus the doc-id under the pk path, and the planner omits
//! the `KeyLookup` (RFC: Covering Index Scans, Part B, phase 2). The planner only
//! sets `covering` after proving the query reads nothing but the index's
//! components and the pk, so the entry's values are all it needs.
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

use bson::raw::CString;
use bson::{Bson, RawBson, RawDocumentBuf};
use slate_engine::{
    Catalog, CompoundRange, CompoundTail, EngineError, EngineTransaction, IndexEntry,
};
use slate_eval::compare_bson;
use slate_planner::{CollectionRef, CompoundScanRange, CompoundScanTail, ScanDirection};

use crate::budget::Ticker;
use crate::{ExecError, ValueIter};

/// Build the covering row for one compound entry: each `component` path carries
/// the entry's value at that index position (`component_value(idx)`), and the
/// doc-id rides under the (always top-level) `pk_path`. Component paths are merged
/// into one nested tree first — so `(user.id, user.name)` becomes a single
/// `{user: {id, name}}` rather than two conflicting `user` keys — then serialized
/// into the append-only `RawDocumentBuf`. A component absent from the entry
/// (`component_value` → `None`, e.g. a sparse trailing field) is simply omitted,
/// matching a missing field in the fetched document. The planner proved the query
/// reads nothing but these components and the pk, so the row is a drop-in.
fn synthesize_row(
    entry: &IndexEntry,
    components: &[String],
    pk_path: &str,
) -> Result<RawBson, EngineError> {
    let mut tree: Vec<(String, SynthNode)> = Vec::new();
    for (idx, path) in components.iter().enumerate() {
        if let Some(value) = entry.component_value(idx)? {
            insert_path(&mut tree, path, value);
        }
    }
    let mut doc = build_doc(tree)?;
    // The pk rides on the doc-id; skip it if a component already wrote that key
    // (the covering pass bails on a pk-colliding component, so this is defensive).
    if !components.iter().any(|c| c == pk_path) {
        let pk_key = CString::try_from(pk_path)
            .map_err(|e| EngineError::InvalidKey(format!("pk path {pk_path:?}: {e}")))?;
        doc.append(pk_key, entry.doc_id()?);
    }
    Ok(RawBson::Document(doc))
}

/// An intermediate node while merging dotted component paths that share a prefix,
/// before serializing into the append-only `RawDocumentBuf`. A `Leaf` holds a
/// synthesized value; a `Branch` holds ordered child segments.
enum SynthNode {
    Leaf(RawBson),
    Branch(Vec<(String, SynthNode)>),
}

/// Insert `value` at the dotted `path` into `tree`, materializing/merging
/// intermediate objects: a top-level `path` becomes a `Leaf`, while `user.id`
/// descends (and reuses an existing `user` `Branch` so a later `user.name` merges
/// into the same object). A segment that collides with an existing `Leaf` simply
/// appends a new entry (a pathological shape the covering pass never approves).
fn insert_path(tree: &mut Vec<(String, SynthNode)>, path: &str, value: RawBson) {
    match path.split_once('.') {
        None => tree.push((path.to_string(), SynthNode::Leaf(value))),
        Some((head, rest)) => {
            if let Some((_, SynthNode::Branch(inner))) = tree
                .iter_mut()
                .find(|(k, n)| k == head && matches!(n, SynthNode::Branch(_)))
            {
                insert_path(inner, rest, value);
            } else {
                let mut inner = Vec::new();
                insert_path(&mut inner, rest, value);
                tree.push((head.to_string(), SynthNode::Branch(inner)));
            }
        }
    }
}

/// Serialize a merged [`SynthNode`] tree into a `RawDocumentBuf`, recursing into
/// each `Branch` to build the nested objects bottom-up.
fn build_doc(tree: Vec<(String, SynthNode)>) -> Result<RawDocumentBuf, EngineError> {
    let mut doc = RawDocumentBuf::new();
    for (key, node) in tree {
        let cstr = CString::try_from(key.as_str())
            .map_err(|e| EngineError::InvalidKey(format!("index path segment {key:?}: {e}")))?;
        match node {
            SynthNode::Leaf(value) => doc.append(cstr, value),
            SynthNode::Branch(inner) => doc.append(cstr, RawBson::Document(build_doc(inner)?)),
        }
    }
    Ok(doc)
}

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

// Each arg is a distinct, meaningful scan parameter (container + field + range +
// direction + limit + covering) plus the deadline ticker; bundling them into a
// struct just to satisfy the lint would add indirection without clarity.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
    field: String,
    range: &CompoundScanRange,
    direction: ScanDirection,
    limit: Option<usize>,
    covering: Option<Vec<String>>,
    mut ticker: Ticker,
) -> Result<ValueIter<'a>, ExecError> {
    let handle = txn.collection(&collection.cf, &collection.collection)?;
    let post_filter = CompoundFilter::for_range(range);

    // A covering scan synthesizes rows from entries, so it needs the pk path; own
    // it (a short string) since the closure outlives the borrowed handle. The
    // common non-covering scan pays nothing.
    let pk_path = covering.as_ref().map(|_| handle.pk_path().to_string());

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
            // Cooperative deadline check, once per *examined* entry — so a
            // heavily post-filtered compound scan still aborts on time.
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

            // Covering: emit the synthesized `{components…, pk}` row; otherwise the
            // bare doc-id for the paired `KeyLookup` to fetch.
            let yielded = match (covering.as_deref(), pk_path.as_deref()) {
                (Some(components), Some(pk)) => synthesize_row(&entry, components, pk),
                _ => entry.doc_id(),
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
    use super::{SynthNode, build_doc, execute, insert_path};
    use crate::budget::Ticker;
    use crate::collect;
    use bson::{RawBson, RawDocumentBuf, rawdoc};
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
            None,
            Ticker::new(None),
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

    // ── Covering synthesis ───────────────────────────────────────────────────

    /// Collect the synthesized documents a covering compound scan emits for
    /// `range`, covering the two components `["status", "created_at"]`.
    fn scan_covering(range: CompoundScanRange) -> Vec<RawDocumentBuf> {
        let engine = seeded_orders();
        let txn = engine.begin(true).unwrap();
        let components = vec!["status".to_string(), "created_at".to_string()];
        let identity = slate_engine::join_index_fields(&components);
        let iter = execute(
            &txn,
            &orders_ref(),
            identity,
            &range,
            ScanDirection::Forward,
            None,
            Some(components),
            Ticker::new(None),
        )
        .unwrap();
        collect(iter)
            .unwrap()
            .into_iter()
            .map(|v| match v {
                RawBson::Document(d) => d,
                other => panic!("covering scan must yield documents, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn covering_synthesizes_components_and_pk() {
        // status = "active" → the three active docs, each synthesized with its
        // two components and the pk from the entry's doc-id (no fetch).
        let docs = scan_covering(CompoundScanRange {
            eq_prefix: vec![bson::Bson::String("active".into())],
            tail: CompoundScanTail::Unbounded,
        });
        assert_eq!(docs.len(), 3);
        for d in &docs {
            assert_eq!(d.get_str("status").unwrap(), "active");
            assert!(d.get_i64("created_at").is_ok() || d.get_i32("created_at").is_ok());
            assert!(d.get_str("_id").is_ok(), "carries the pk: {d:?}");
        }
        // The doc-ids recovered from the entries are exactly a, b, c.
        let mut ids: Vec<String> = docs
            .iter()
            .map(|d| d.get_str("_id").unwrap().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn covering_still_applies_the_recheck() {
        // The byte-prefix collision (`active2`) must still be excluded by the
        // recheck even on the covering path — synthesis runs *after* it.
        let docs = scan_covering(CompoundScanRange {
            eq_prefix: vec![bson::Bson::String("active".into())],
            tail: CompoundScanTail::Range {
                lower: Some((bson::Bson::Int64(15), false)),
                upper: None,
            },
        });
        let ids: Vec<String> = docs
            .iter()
            .map(|d| d.get_str("_id").unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["b", "c"]);
    }

    #[test]
    fn insert_path_merges_shared_prefix() {
        // `(user.id, user.name)` must merge into one `user` object, not two keys.
        let mut tree: Vec<(String, SynthNode)> = Vec::new();
        insert_path(&mut tree, "user.id", RawBson::String("u1".into()));
        insert_path(&mut tree, "user.name", RawBson::String("ada".into()));
        let doc = build_doc(tree).unwrap();
        assert_eq!(doc.iter().count(), 1, "one top-level `user` key: {doc:?}");
        let user = doc.get_document("user").unwrap();
        assert_eq!(user.get_str("id").unwrap(), "u1");
        assert_eq!(user.get_str("name").unwrap(), "ada");
    }

    #[test]
    fn insert_path_keeps_disjoint_top_level_keys() {
        // `(status, created_at)` → two distinct top-level fields, types preserved.
        let mut tree: Vec<(String, SynthNode)> = Vec::new();
        insert_path(&mut tree, "status", RawBson::String("active".into()));
        insert_path(&mut tree, "created_at", RawBson::Int32(20));
        let doc = build_doc(tree).unwrap();
        assert_eq!(doc.get_str("status").unwrap(), "active");
        assert_eq!(doc.get_i32("created_at").unwrap(), 20);
        assert_eq!(doc.iter().count(), 2);
    }
}
