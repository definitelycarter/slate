//! The `IndexIntersect` node — a galloping N-way skip-merge over equality index
//! streams (Index Intersection RFC, Door A).
//!
//! Opens one seekable cursor per equality part and zig-zags them by their shared
//! doc-id order: take the max current doc-id, `seek` every other cursor to ≥ it;
//! when all agree, emit that doc-id once and advance all strictly past it. The
//! cost is bounded by the *smallest* input — never by materialising and hashing
//! the largest, the way [`super::index_merge`]'s `And` does. The output is a
//! **streaming, deduped** bare-id iterator feeding the existing
//! [`super::key_lookup`], exactly like an `IndexScan`/`IndexMerge` source — so the
//! intersection is invisible to results; only speed changes.
//!
//! Dedup is free: within one equality value the store collapses a doc-id to a
//! single entry (identical key), so advancing a matched cursor by one is strictly
//! past the emitted id — no `Distinct` needed (the lone-multikey dedup the hash
//! path needs is subsumed). See the RFC's Q2.

use slate_engine::{Catalog, EngineTransaction, IndexCursor};
use slate_planner::{CollectionRef, IndexIntersectPart};

use crate::{ExecError, ValueIter};

pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
    parts: &[IndexIntersectPart],
) -> Result<ValueIter<'a>, ExecError> {
    let handle = txn.collection(&collection.cf, &collection.collection)?;

    // One forward cursor per equality part, positioned at its first entry. The
    // cursor clones the cf internally, so `handle` need not outlive this loop.
    let mut cursors: Vec<Box<dyn IndexCursor + 'a>> = Vec::with_capacity(parts.len());
    for part in parts {
        cursors.push(txn.open_index_cursor(&handle, &part.field, &part.value, false)?);
    }

    let mut done = false;
    Ok(Box::new(std::iter::from_fn(move || {
        if done {
            return None;
        }
        loop {
            // Phase 1 (immutable peeks): the max current doc-id across cursors and
            // whether they all already agree. Any exhausted cursor ends the
            // intersection. `max` is owned so the peek borrows release before the
            // mutating phase; the max is monotone non-decreasing across iterations
            // and the cursors are finite, so the loop terminates.
            let mut max: Vec<u8> = Vec::new();
            let mut all_equal = true;
            for (i, cursor) in cursors.iter().enumerate() {
                let bytes = match cursor.peek() {
                    None => {
                        done = true;
                        return None;
                    }
                    Some(entry) => match entry.doc_id_bytes() {
                        Ok(b) => b,
                        Err(e) => {
                            done = true;
                            return Some(Err(ExecError::Engine(e)));
                        }
                    },
                };
                if i == 0 {
                    max = bytes.to_vec();
                } else {
                    if bytes != max.as_slice() {
                        all_equal = false;
                    }
                    if bytes > max.as_slice() {
                        max = bytes.to_vec();
                    }
                }
            }

            // Phase 2 (mutate). All agree → emit the doc-id once and advance every
            // cursor strictly past it. Otherwise → seek every cursor up to ≥ max
            // and re-evaluate.
            if all_equal {
                let id = match cursors[0].peek() {
                    Some(entry) => match entry.doc_id() {
                        Ok(id) => id,
                        Err(e) => {
                            done = true;
                            return Some(Err(ExecError::Engine(e)));
                        }
                    },
                    None => {
                        // Unreachable: phase 1 just saw every cursor positioned.
                        done = true;
                        return None;
                    }
                };
                for cursor in cursors.iter_mut() {
                    if let Err(e) = cursor.advance() {
                        done = true;
                        return Some(Err(ExecError::Engine(e)));
                    }
                }
                return Some(Ok(Some(id)));
            }
            for cursor in cursors.iter_mut() {
                if let Err(e) = cursor.seek(&max) {
                    done = true;
                    return Some(Err(ExecError::Engine(e)));
                }
            }
        }
    })))
}

#[cfg(test)]
mod tests {
    use crate::Executor;
    use bson::{Bson, RawBson, RawDocumentBuf, rawdoc};
    use slate_engine::{Catalog, DEFAULT_CF, Engine, EngineTransaction, KvEngine};
    use slate_planner::{
        CollectionRef, IndexIntersectPart, IndexScanRange, LogicalOp, Node, Plan, ScanDirection,
    };
    use slate_store::MemoryStore;

    fn coll() -> CollectionRef {
        CollectionRef {
            cf: DEFAULT_CF.into(),
            collection: "items".into(),
        }
    }

    fn s(v: &str) -> Bson {
        Bson::String(v.into())
    }

    /// Seed `items` with single-field `indexes` and `docs`.
    fn seed(indexes: &[&str], docs: Vec<RawDocumentBuf>) -> KvEngine<MemoryStore> {
        let engine = KvEngine::new(MemoryStore::new());
        let txn = engine.begin(false).unwrap();
        txn.create_collection(DEFAULT_CF, "items", &Default::default())
            .unwrap();
        for idx in indexes {
            txn.create_index(DEFAULT_CF, "items", idx).unwrap();
        }
        let handle = txn.collection(DEFAULT_CF, "items").unwrap();
        for d in &docs {
            txn.put(&handle, d).unwrap();
        }
        txn.commit().unwrap();
        engine
    }

    fn part(field: &str, value: Bson) -> IndexIntersectPart {
        IndexIntersectPart {
            field: field.into(),
            value,
        }
    }

    fn eq_scan(field: &str, value: Bson) -> Node {
        Node::IndexScan {
            collection: coll(),
            field: field.into(),
            range: IndexScanRange::Eq(value),
            direction: ScanDirection::Forward,
            limit: None,
            covering: false,
        }
    }

    /// Run a bare id-yielding plan and return its sorted string doc-ids.
    fn run_ids(engine: &KvEngine<MemoryStore>, plan: Node) -> Vec<String> {
        let txn = engine.begin(true).unwrap();
        let mut out: Vec<String> = Executor::new(&txn)
            .execute_collect(Plan::Query(plan))
            .unwrap()
            .into_iter()
            .map(|b| match b {
                RawBson::String(s) => s,
                other => panic!("non-string id {other:?}"),
            })
            .collect();
        out.sort();
        out
    }

    /// The intersection via the new `IndexIntersect` node.
    fn intersect(engine: &KvEngine<MemoryStore>, parts: Vec<IndexIntersectPart>) -> Vec<String> {
        run_ids(
            engine,
            Node::IndexIntersect {
                collection: coll(),
                parts,
            },
        )
    }

    /// The same intersection via the hash `IndexMerge(And)` over `Eq` scans — the
    /// path `IndexIntersect` replaces, left-folded like the planner used to. Used
    /// to assert the new node is invisible to results.
    fn hash_merge(engine: &KvEngine<MemoryStore>, scans: &[(&str, Bson)]) -> Vec<String> {
        let mut iter = scans.iter().cloned();
        let (f0, v0) = iter.next().unwrap();
        let tree = iter.fold(eq_scan(f0, v0), |acc, (f, v)| Node::IndexMerge {
            collection: coll(),
            logical: LogicalOp::And,
            lhs: Box::new(acc),
            rhs: Box::new(eq_scan(f, v)),
        });
        run_ids(engine, tree)
    }

    /// Every case asserts the explicit expected id set AND that `IndexIntersect`
    /// equals the hash-merge path (the intersection is invisible to results).
    fn assert_matches(
        engine: &KvEngine<MemoryStore>,
        parts: Vec<IndexIntersectPart>,
        scans: &[(&str, Bson)],
        expected: &[&str],
    ) {
        let got = intersect(engine, parts);
        assert_eq!(got, expected, "explicit id set");
        assert_eq!(got, hash_merge(engine, scans), "vs hash IndexMerge(And)");
    }

    #[test]
    fn skew_is_bounded_by_the_smaller_side() {
        // a="x" matches 10 docs; b="y" matches 5 (3 of which also have a="x").
        let mut docs: Vec<RawDocumentBuf> = (0..10)
            .map(|i| {
                let b = if [2, 5, 7].contains(&i) { "y" } else { "z" };
                rawdoc! { "_id": format!("d{i:02}"), "a": "x", "b": b }
            })
            .collect();
        docs.push(rawdoc! { "_id": "d10", "a": "w", "b": "y" });
        docs.push(rawdoc! { "_id": "d11", "a": "w", "b": "y" });
        let engine = seed(&["a", "b"], docs);
        assert_matches(
            &engine,
            vec![part("a", s("x")), part("b", s("y"))],
            &[("a", s("x")), ("b", s("y"))],
            &["d02", "d05", "d07"],
        );
    }

    #[test]
    fn balanced_interleaved_streams() {
        // a="x" on even indices, b="y" on multiples of 3 → intersect = mult of 6.
        let docs: Vec<RawDocumentBuf> = (0..10)
            .map(|i| {
                let a = if i % 2 == 0 { "x" } else { "p" };
                let b = if i % 3 == 0 { "y" } else { "q" };
                rawdoc! { "_id": format!("d{i:02}"), "a": a, "b": b }
            })
            .collect();
        let engine = seed(&["a", "b"], docs);
        assert_matches(
            &engine,
            vec![part("a", s("x")), part("b", s("y"))],
            &[("a", s("x")), ("b", s("y"))],
            &["d00", "d06"],
        );
    }

    #[test]
    fn three_way_intersection_single_survivor() {
        let engine = seed(
            &["a", "b", "c"],
            vec![
                rawdoc! { "_id": "d00", "a": "x", "b": "y", "c": "z" }, // survivor
                rawdoc! { "_id": "d01", "a": "x", "b": "y", "c": "w" },
                rawdoc! { "_id": "d02", "a": "x", "b": "q", "c": "z" },
                rawdoc! { "_id": "d03", "a": "p", "b": "y", "c": "z" },
            ],
        );
        assert_matches(
            &engine,
            vec![part("a", s("x")), part("b", s("y")), part("c", s("z"))],
            &[("a", s("x")), ("b", s("y")), ("c", s("z"))],
            &["d00"],
        );
    }

    #[test]
    fn multikey_part_with_duplicate_element_emits_once() {
        // d00 has a repeated matching element `tags: ["x","x"]`: the intersection
        // must still yield it exactly once (store key-collapse + strict advance).
        let engine = seed(
            &["tags.[]", "b"],
            vec![
                rawdoc! { "_id": "d00", "tags": ["x", "x"], "b": "y" },
                rawdoc! { "_id": "d01", "tags": ["x"], "b": "y" },
                rawdoc! { "_id": "d02", "tags": ["x"], "b": "z" },
                rawdoc! { "_id": "d03", "tags": ["q"], "b": "y" },
            ],
        );
        assert_matches(
            &engine,
            vec![part("tags.[]", s("x")), part("b", s("y"))],
            &[("tags.[]", s("x")), ("b", s("y"))],
            &["d00", "d01"],
        );
    }

    #[test]
    fn empty_when_streams_share_no_doc() {
        // Both streams non-empty but disjoint over documents → empty intersection.
        let engine = seed(
            &["a", "b"],
            vec![
                rawdoc! { "_id": "d00", "a": "x", "b": "n" },
                rawdoc! { "_id": "d01", "a": "x", "b": "n" },
                rawdoc! { "_id": "d02", "a": "p", "b": "y" },
                rawdoc! { "_id": "d03", "a": "p", "b": "y" },
            ],
        );
        assert_matches(
            &engine,
            vec![part("a", s("x")), part("b", s("y"))],
            &[("a", s("x")), ("b", s("y"))],
            &[],
        );
    }

    #[test]
    fn empty_when_one_side_matches_nothing() {
        let engine = seed(
            &["a", "b"],
            vec![
                rawdoc! { "_id": "d00", "a": "x", "b": "y" },
                rawdoc! { "_id": "d01", "a": "x", "b": "y" },
            ],
        );
        // No doc has b="missing" → empty regardless of the a="x" side.
        assert_matches(
            &engine,
            vec![part("a", s("x")), part("b", s("missing"))],
            &[("a", s("x")), ("b", s("missing"))],
            &[],
        );
    }
}
