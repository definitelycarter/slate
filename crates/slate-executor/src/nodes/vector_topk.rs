//! The `VectorTopK` source — flat-vector-index k-nearest-neighbour.
//!
//! The physical form of `ORDER BY VECTORDISTANCE(c.<field>, <q>[, <m>]) LIMIT k`
//! when `<field>` has a matching flat vector index. Scans the index's stored
//! `(doc_id, vector)` entries ([`EngineTransaction::scan_vectors`], which already
//! drops TTL-expired documents), measures each against the query vector by the
//! index's metric, and yields the `k` nearest **doc-ids** in nearest-first order
//! — paired with [`super::key_lookup`] to fetch the documents, exactly like an
//! `IndexScan`.
//!
//! ## The two correctness rules
//!
//! 1. **Pre-filter before the top-k.** When the query has a constraining `WHERE`,
//!    the planner hands its candidate doc-ids in as `source`. We materialize them
//!    into a membership set and keep only scanned vectors whose doc-id is in it
//!    *before* selecting the k nearest. A global top-k then filtered would
//!    under-return (fewer than k even when more matching docs exist). With no
//!    `WHERE`, `source` is `None` and we scan the whole field.
//! 2. **One distance definition.** The score is [`slate_eval::VectorMetric`]'s
//!    `measure`, the same function `VECTORDISTANCE` calls, so the seek returns the
//!    rows a full scan would. We keep the k **largest** scores for the similarity
//!    metrics (cosine/dotproduct) and the k **smallest** for euclidean distance.
//!
//! ## The bounded heap
//!
//! A `BinaryHeap` of at most `window` entries holds the current best. The heap's
//! ordering is arranged so its *root is the worst kept entry* — the one a new,
//! better candidate evicts — so each candidate is an O(log window) compare-and-
//! maybe-replace, never an O(n log n) full sort. At the end the heap is drained
//! and reversed into nearest-first order.
//!
//! ## Rescore (quantized widths)
//!
//! A `Float32` index stores exact vectors, so `window == k` and the heap's k are
//! the answer. A *quantized* index (e.g. `Float16`) stores an approximate copy:
//! the scan's distance is only approximate, so it would occasionally drop a true
//! neighbour. The fix is **rescore** — over-sample the scan to `window > k`, then
//! re-rank that shortlist against each document's **exact float32** (the BSON
//! array, the canonical source) with the *same* shared metric, and keep k. The
//! final ordering is then identical to a full scan over the shortlist, so
//! quantization can only cost recall (a true neighbour missing the over-sampled
//! shortlist), never mis-order what is returned. The rescore costs `window`
//! point reads of the documents the scan already shortlisted.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};

use bson::raw::{RawBsonRef, RawDocument};
use bson::{Bson, RawBson};
use slate_ast::Expression;
use slate_engine::{Catalog, EngineTransaction};
use slate_eval::VectorMetric;
use slate_eval::raweval;
use slate_eval::value::Value;
use slate_planner::VectorDataType;
use slate_rawbson::RawField;

use super::env::row_env;
use crate::budget::{self, Ticker};
use crate::{ExecEnv, ExecError, ValueIter};

/// One kept candidate: its score and doc-id. Ordered so a `BinaryHeap`'s max-root
/// is the *worst* entry currently kept, i.e. the first to be evicted by a better
/// candidate. `higher_is_closer` flips the sense: for a similarity (bigger =
/// nearer) the worst kept is the *smallest* score, so we invert the comparison.
struct Candidate {
    score: f64,
    doc_id: RawBson,
    /// Stable doc-id ordering bytes — the secondary key that breaks score ties
    /// deterministically (smaller id = "better": kept on eviction, emitted
    /// first), so the same corpus always yields the same k in the same order.
    id_order: Vec<u8>,
    higher_is_closer: bool,
}

impl Candidate {
    /// Compare two candidates by *how good* they are — `Greater` means strictly
    /// better (nearer). NaN scores sort as worst (they can't be nearest), so a
    /// degenerate vector never displaces a real neighbour. Ties on score break by
    /// the doc-id (a *smaller* id is "better") so selection and order are total.
    fn goodness(&self, other: &Self) -> Ordering {
        // For a distance (lower is better) a *smaller* score is better; for a
        // similarity a *larger* one is. `partial_cmp` is total here except NaN.
        let raw = self
            .score
            .partial_cmp(&other.score)
            .unwrap_or_else(|| nan_order(self.score, other.score));
        let by_score = if self.higher_is_closer {
            raw
        } else {
            raw.reverse()
        };
        // A smaller id is "better" → sorts first and survives a tie eviction.
        by_score.then_with(|| other.id_order.cmp(&self.id_order))
    }
}

/// Order two scores when at least one is NaN: a NaN is "worse" (sorts as the
/// lesser), so it is the first evicted and never wins a tie.
fn nan_order(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => Ordering::Equal, // unreachable: partial_cmp handled it
    }
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.goodness(other) == Ordering::Equal
    }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Candidate {
    /// The heap is a *max-heap on worst-ness*: its root must be the worst kept
    /// entry. So a candidate that is **worse** (less good) compares *greater* —
    /// invert `goodness`.
    fn cmp(&self, other: &Self) -> Ordering {
        self.goodness(other).reverse()
    }
}

// The arguments mirror the `VectorTopK` node's fields plus the transaction, the
// optional pre-filter stream, and the execution context ([`ExecEnv`], from which
// the query-vector expression reads its `@`-params) — the same shape the
// dispatcher passes the other node executors. Bundling the node fields into a
// struct would add ceremony, not clarity.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &slate_planner::CollectionRef,
    field: String,
    query_vector: Expression,
    metric: VectorMetric,
    dtype: VectorDataType,
    k: usize,
    source: Option<ValueIter<'a>>,
    env: ExecEnv<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    let handle = txn.collection(&collection.cf, &collection.collection)?;

    // k == 0 selects nothing — short-circuit (the heap logic below also handles
    // it, but this skips the whole scan).
    if k == 0 {
        return Ok(Box::new(std::iter::empty()));
    }

    // Evaluate the query vector ONCE — it is row-independent (a literal array or
    // an `@parameter`), so it binds against no row, only the query parameters.
    let query = eval_query_vector(&query_vector, &env)?;
    let Some(query) = query else {
        // An undefined / non-vector query (e.g. a missing `@q`) can match nothing
        // — the scalar `VECTORDISTANCE` would be undefined for every row, so the
        // ORDER BY key is undefined everywhere and no row is "nearest". Empty.
        return Ok(Box::new(std::iter::empty()));
    };

    // Pre-filter: materialize the candidate doc-ids the WHERE admits into a set,
    // so the scan keeps only members *before* the top-k (rule 1). The ids are
    // lightweight; `source` yields documents or bare ids (extract the pk, like
    // `KeyLookup`). `None` → no constraint, the whole field is in play.
    let allowed = match source {
        Some(src) => Some(materialize_candidate_ids(
            src,
            handle.pk_path(),
            env.materialization_cap,
        )?),
        None => None,
    };

    let higher_is_closer = metric.higher_is_closer();
    // Widen the query to f32 once (the stored side decodes to f32) so the
    // approximate per-candidate measure compares both sides at the same precision.
    let query_f32 = vector_to_f32(&query);
    // A quantized index's scan distance is only approximate, so over-sample: keep
    // the top-`window` candidates and rescore them exactly below. An exact
    // (float32) index uses `window == k` and skips the rescore.
    let window = rescore_window(dtype, k);
    let mut heap: BinaryHeap<Candidate> = BinaryHeap::with_capacity(window + 1);

    // Cooperative deadline (Resource Limits RFC, A): the flat-index scan is a full
    // scan — exactly the long source the deadline bounds — so fold the check into
    // it per candidate. The top-k heap is `window`-bounded, so the materialization
    // cap deliberately does not apply here.
    let mut ticker = Ticker::new(env.deadline.clone());

    for entry in txn.scan_vectors(&handle, &field)? {
        ticker.tick()?;
        let (doc_id, vector) = entry?;

        // Pre-filter membership: skip a vector whose doc is not in the WHERE set.
        if let Some(set) = &allowed
            && !set.contains(&IdKey::from(&doc_id))
        {
            continue;
        }

        // Shape mismatch (different dims than the query) → not comparable, skip
        // — mirrors the scalar function yielding undefined for unequal lengths.
        if vector.len() != query.len() || vector.is_empty() {
            continue;
        }

        let score = metric.measure_f32(&query_f32, &vector);
        let id_order = id_bytes(&doc_id.as_raw_bson_ref());
        push_bounded(
            &mut heap,
            Candidate {
                score,
                doc_id,
                id_order,
                higher_is_closer,
            },
            window,
        );
    }

    // `into_sorted_vec` sorts ascending by `Ord`. Our `Ord` is `goodness`
    // *reversed* (so the heap root is the worst), which means ascending `Ord`
    // orders *best-first* — exactly the nearest-first output we want.
    let candidates: Vec<Candidate> = heap.into_sorted_vec();

    // Exact index: the scan distance is already exact, so the `window == k`
    // candidates *are* the final top-k.
    if dtype.is_exact() {
        return Ok(Box::new(candidates.into_iter().map(|c| Ok(Some(c.doc_id)))));
    }

    // Quantized index: re-rank the shortlist against each document's exact
    // float32 (the canonical array) with the *same* shared metric, then keep the
    // k nearest. This makes the returned order identical to a full scan over the
    // shortlist — quantization can only cost recall (a true neighbour missing the
    // shortlist), never mis-order what we return.
    let mut rescored: BinaryHeap<Candidate> = BinaryHeap::with_capacity(k + 1);
    for cand in candidates {
        // The rescore shortlist is `window`-bounded, not a long scan, so this is
        // for symmetry, not load-bearing — but it keeps every per-entry loop on
        // the deadline. Reuses the scan's ticker (one continuous row budget).
        ticker.tick()?;
        let Some(doc) = txn.get(&handle, &cand.doc_id.as_raw_bson_ref())? else {
            // Vanished between the scan and the rescore (deleted/expired) — drop it.
            continue;
        };
        let Some(exact) = exact_vector_f64(&doc, &field) else {
            continue;
        };
        if exact.len() != query.len() {
            continue;
        }
        let score = metric.measure(&query, &exact);
        push_bounded(
            &mut rescored,
            Candidate {
                score,
                doc_id: cand.doc_id,
                id_order: cand.id_order,
                higher_is_closer,
            },
            k,
        );
    }
    let kept: Vec<Candidate> = rescored.into_sorted_vec();
    Ok(Box::new(kept.into_iter().map(|c| Ok(Some(c.doc_id)))))
}

/// The scan's keep-count for a `dtype`: an exact index keeps exactly `k`; a
/// quantized index over-samples to `max(OVERSAMPLE·k, FLOOR)` so the true top-k
/// survive the approximate distance before the exact rescore narrows to k. The
/// spike measured recall@k = 1.0 at 2·k for float16/int8; 4× plus a floor is
/// comfortable margin at trivial cost (the window is also the count of document
/// reads the rescore performs).
fn rescore_window(dtype: VectorDataType, k: usize) -> usize {
    if dtype.is_exact() {
        k
    } else {
        k.saturating_mul(RESCORE_OVERSAMPLE).max(RESCORE_FLOOR)
    }
}

const RESCORE_OVERSAMPLE: usize = 4;
const RESCORE_FLOOR: usize = 64;

/// Push `cand` into a max-heap-on-worst bounded to `cap`: keep it if the heap
/// isn't full, else only if it is strictly better than the current worst (a tie
/// keeps the incumbent — a stable, deterministic choice). O(log cap) per push.
fn push_bounded(heap: &mut BinaryHeap<Candidate>, cand: Candidate, cap: usize) {
    if heap.len() < cap {
        heap.push(cand);
    } else if let Some(worst) = heap.peek()
        && cand.goodness(worst) == Ordering::Greater
    {
        heap.pop();
        heap.push(cand);
    }
}

/// Read a document's vector field as an exact `f64` vector — the canonical
/// full-precision array the rescore measures against (bit-identical to what the
/// scalar `VECTORDISTANCE` reads on a full scan). `None` if the field is absent,
/// not an array, or holds a non-numeric element (not a comparable vector, so the
/// candidate drops out of the rescore).
fn exact_vector_f64(doc: &RawDocument, field: &str) -> Option<Vec<f64>> {
    let RawBsonRef::Array(arr) = RawField::get_value(doc.as_bytes(), field)? else {
        return None;
    };
    let mut out = Vec::new();
    for el in arr {
        let v = match el {
            Ok(RawBsonRef::Int32(i)) => i as f64,
            Ok(RawBsonRef::Int64(i)) => i as f64,
            Ok(RawBsonRef::Double(d)) => d,
            // Non-numeric element or a decode error → not a usable vector.
            _ => return None,
        };
        out.push(v);
    }
    Some(out)
}

/// Widen the query `f64` vector to `f32` for the shared `measure_f32`, so the
/// stored (`f32`) and query sides are compared at the same precision.
fn vector_to_f32(query: &[f64]) -> Vec<f32> {
    query.iter().map(|&x| x as f32).collect()
}

/// Evaluate the (row-independent) query-vector expression once into a `Vec<f64>`,
/// matching the scalar `VECTORDISTANCE`'s argument reading: a defined array whose
/// every element is a number, widened to `f64`. `Ok(None)` for an undefined or
/// non-vector result (the kNN then matches nothing).
fn eval_query_vector(expr: &Expression, env: &ExecEnv) -> Result<Option<Vec<f64>>, ExecError> {
    // No row bindings — the query vector references only literals / `@params`.
    let program = raweval::compile(expr, None, env.udf_ctx());
    let binds: [(&str, bson::raw::RawBsonRef<'_>); 0] = [];
    let renv = row_env(&binds, env);
    let value = raweval::eval_compiled(&program, &renv)?.into_value()?;
    Ok(match value {
        Value::Defined(Bson::Array(arr)) => bson_array_to_f64(&arr),
        _ => None,
    })
}

/// A BSON array of numbers widened to `f64`, or `None` if any element is not a
/// number (mirrors the scalar function's `vector` helper).
fn bson_array_to_f64(arr: &[Bson]) -> Option<Vec<f64>> {
    arr.iter()
        .map(|e| match e {
            Bson::Int32(i) => Some(*i as f64),
            Bson::Int64(i) => Some(*i as f64),
            Bson::Double(f) => Some(*f),
            _ => None,
        })
        .collect()
}

/// A hashable doc-id key for the pre-filter membership set. Doc-ids are scalar
/// BSON (string / number / ObjectId), so their serialized bytes are a stable
/// identity — the same encoding `KeyLookup` keys on. Owning the bytes keeps the
/// set independent of the scanned entries' lifetimes.
#[derive(PartialEq, Eq, Hash)]
struct IdKey(Vec<u8>);

impl IdKey {
    fn from(id: &RawBson) -> Self {
        IdKey(id_bytes(&id.as_raw_bson_ref()))
    }
    fn from_ref(id: &bson::raw::RawBsonRef<'_>) -> Self {
        IdKey(id_bytes(id))
    }
}

/// Stable identity bytes for a scalar doc-id: the element type tag followed by
/// the raw value bytes, so two ids compare equal exactly when they are the same
/// BSON value (no cross-type collisions — a string `"1"` and an int `1` differ
/// in the tag).
fn id_bytes(id: &bson::raw::RawBsonRef<'_>) -> Vec<u8> {
    use bson::raw::RawBsonRef;
    let mut out = Vec::new();
    let tag = id.element_type() as u8;
    out.push(tag);
    match id {
        RawBsonRef::String(s) => out.extend_from_slice(s.as_bytes()),
        RawBsonRef::Int32(i) => out.extend_from_slice(&i.to_le_bytes()),
        RawBsonRef::Int64(i) => out.extend_from_slice(&i.to_le_bytes()),
        RawBsonRef::Double(d) => out.extend_from_slice(&d.to_le_bytes()),
        RawBsonRef::ObjectId(oid) => out.extend_from_slice(&oid.bytes()),
        RawBsonRef::Boolean(b) => out.push(*b as u8),
        // Any other doc-id shape: fall back to its debug form's bytes. Doc-ids
        // are conventionally scalar, so this is a defensive catch-all.
        other => out.extend_from_slice(format!("{other:?}").as_bytes()),
    }
    out
}

/// Drain the pre-filter `source` into a set of candidate doc-id keys. Accepts a
/// bare id (from an `IndexScan`) or a document carrying the pk (from a `Scan` /
/// `Filter`), matching how `KeyLookup` reads its input.
///
/// `cap` is the materialization cap (Resource Limits RFC, B): this set is the
/// unbounded buffer of the pre-filter path (the analogue of `IndexMerge`'s id
/// set), so a non-selective `WHERE` feeding most of the collection trips
/// `LimitExceeded` as the set grows past `cap`. The top-k/rescore heaps are
/// separately bounded by `window`/`k`, so the cap does not touch them.
fn materialize_candidate_ids(
    source: ValueIter<'_>,
    pk_path: &str,
    cap: Option<usize>,
) -> Result<HashSet<IdKey>, ExecError> {
    let mut set = HashSet::new();
    for item in source {
        let Some(val) = item? else { continue };
        match &val {
            RawBson::Document(d) => match d.get(pk_path) {
                Ok(Some(id)) => {
                    set.insert(IdKey::from_ref(&id));
                }
                // A candidate row missing its pk can't be matched — drop it.
                _ => continue,
            },
            other => {
                set.insert(IdKey::from_ref(&other.as_raw_bson_ref()));
            }
        }
        budget::check_cap(set.len(), cap, "VectorTopK")?;
    }
    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A brute-force reference top-k over `(id, vector)` pairs: measure every
    /// candidate, sort by the metric's sense, take k ids. The node must agree
    /// with this for every case below.
    fn brute_force(
        corpus: &[(&str, Vec<f32>)],
        query: &[f64],
        metric: VectorMetric,
        k: usize,
    ) -> Vec<String> {
        let mut scored: Vec<(f64, String)> = corpus
            .iter()
            .map(|(id, v)| {
                let qf: Vec<f32> = query.iter().map(|&x| x as f32).collect();
                (metric.measure_f32(&qf, v), id.to_string())
            })
            .collect();
        // Nearest-first: descending score for similarity, ascending for distance.
        if metric.higher_is_closer() {
            scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap().then_with(|| a.1.cmp(&b.1)));
        } else {
            scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then_with(|| a.1.cmp(&b.1)));
        }
        scored.into_iter().take(k).map(|(_, id)| id).collect()
    }

    /// Run the node's heap selection directly over an in-memory corpus (no
    /// engine), returning the kept doc-ids in nearest-first order. Mirrors the
    /// production loop so the heap/eviction logic is exercised exactly.
    fn node_topk(
        corpus: &[(&str, Vec<f32>)],
        query: &[f64],
        metric: VectorMetric,
        k: usize,
    ) -> Vec<String> {
        if k == 0 {
            return Vec::new();
        }
        let higher_is_closer = metric.higher_is_closer();
        let qf = vector_to_f32(query);
        let mut heap: BinaryHeap<Candidate> = BinaryHeap::with_capacity(k + 1);
        for (id, vector) in corpus {
            if vector.len() != query.len() || vector.is_empty() {
                continue;
            }
            let score = metric.measure_f32(&qf, vector);
            let doc_id = RawBson::String((*id).to_string());
            let id_order = id_bytes(&doc_id.as_raw_bson_ref());
            let cand = Candidate {
                score,
                doc_id,
                id_order,
                higher_is_closer,
            };
            if heap.len() < k {
                heap.push(cand);
            } else if let Some(worst) = heap.peek()
                && cand.goodness(worst) == Ordering::Greater
            {
                heap.pop();
                heap.push(cand);
            }
        }
        let kept = heap.into_sorted_vec();
        kept.into_iter()
            .map(|c| match c.doc_id {
                RawBson::String(s) => s.to_string(),
                other => format!("{other:?}"),
            })
            .collect()
    }

    fn corpus() -> Vec<(&'static str, Vec<f32>)> {
        vec![
            ("a", vec![1.0, 0.0, 0.0]),
            ("b", vec![0.9, 0.1, 0.0]),
            ("c", vec![0.0, 1.0, 0.0]),
            ("d", vec![0.0, 0.0, 1.0]),
            ("e", vec![0.5, 0.5, 0.0]),
        ]
    }

    #[test]
    fn euclidean_matches_brute_force() {
        let c = corpus();
        let q = vec![1.0, 0.0, 0.0];
        for k in 1..=6 {
            assert_eq!(
                node_topk(&c, &q, VectorMetric::Euclidean, k),
                brute_force(&c, &q, VectorMetric::Euclidean, k),
                "euclidean k={k}"
            );
        }
    }

    #[test]
    fn cosine_matches_brute_force() {
        let c = corpus();
        let q = vec![1.0, 0.0, 0.0];
        for k in 1..=6 {
            assert_eq!(
                node_topk(&c, &q, VectorMetric::Cosine, k),
                brute_force(&c, &q, VectorMetric::Cosine, k),
                "cosine k={k}"
            );
        }
    }

    #[test]
    fn dotproduct_matches_brute_force() {
        let c = corpus();
        let q = vec![1.0, 1.0, 0.0];
        for k in 1..=6 {
            assert_eq!(
                node_topk(&c, &q, VectorMetric::DotProduct, k),
                brute_force(&c, &q, VectorMetric::DotProduct, k),
                "dotproduct k={k}"
            );
        }
    }

    #[test]
    fn k_greater_than_corpus_returns_all() {
        let c = corpus();
        let q = vec![1.0, 0.0, 0.0];
        let got = node_topk(&c, &q, VectorMetric::Euclidean, 100);
        assert_eq!(got.len(), c.len());
        // All ids present, nearest-first (a is the query itself).
        assert_eq!(got[0], "a");
    }

    #[test]
    fn ties_are_broken_deterministically_and_keep_k() {
        // Two equidistant points either side of the query — a tie. We must still
        // return exactly k, picking deterministically (insertion-stable: the
        // first-seen incumbent wins a tie, so the earlier id is kept).
        let c = vec![
            ("x", vec![1.0_f32, 1.0]),
            ("y", vec![1.0_f32, -1.0]), // same distance from [1,0]
            ("z", vec![1.0_f32, 0.0]),  // nearest
        ];
        let q = vec![1.0, 0.0];
        let got = node_topk(&c, &q, VectorMetric::Euclidean, 2);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], "z", "nearest first");
        // The tie between x and y resolves to the first inserted (x).
        assert_eq!(got[1], "x");
    }

    #[test]
    fn k_zero_is_empty() {
        let c = corpus();
        assert!(node_topk(&c, &[1.0, 0.0, 0.0], VectorMetric::Cosine, 0).is_empty());
    }

    #[test]
    fn dimension_mismatch_is_skipped() {
        // A 2-d stray among 3-d vectors is not comparable and is dropped, never
        // crashing or scoring.
        let c = vec![
            ("a", vec![1.0_f32, 0.0, 0.0]),
            ("bad", vec![1.0_f32, 0.0]), // wrong dims
            ("b", vec![0.9_f32, 0.1, 0.0]),
        ];
        let q = vec![1.0, 0.0, 0.0];
        let got = node_topk(&c, &q, VectorMetric::Euclidean, 3);
        assert_eq!(got, vec!["a".to_string(), "b".to_string()]);
    }

    // ── End-to-end through the real Executor + engine vector index ──────
    //
    // These drive the whole `VectorTopK → KeyLookup → Project` plan against a
    // `KvEngine` with a real flat vector index, so the scan_vectors bridge, the
    // pre-filter membership wiring, and the SQL recogniser are all exercised.

    use crate::Executor;
    use slate_engine::{DEFAULT_CF, Engine, KvEngine, VectorIndexSpec};
    use slate_planner::{CollectionMeta, CollectionRef, VectorIndexMeta};
    use slate_store::MemoryStore;

    fn photos_ref() -> CollectionRef {
        CollectionRef {
            cf: DEFAULT_CF.into(),
            collection: "photos".into(),
        }
    }

    /// A `KvEngine` with a `photos` collection holding 3-d cosine-indexed
    /// embeddings, a secondary index on `tenant`, and five documents across two
    /// tenants — enough to prove the pre-filter shrinks the candidate set.
    fn seeded_photos() -> KvEngine<MemoryStore> {
        let engine = KvEngine::new(MemoryStore::new());
        {
            let txn = engine.begin(false).unwrap();
            txn.create_collection(DEFAULT_CF, "photos", &Default::default())
                .unwrap();
            txn.create_index(DEFAULT_CF, "photos", "tenant").unwrap();
            txn.create_vector_index(
                DEFAULT_CF,
                "photos",
                &VectorIndexSpec::float32("embedding", 3, slate_engine::VectorMetric::Cosine),
            )
            .unwrap();
            let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
            for doc in [
                bson::rawdoc! { "_id": "a", "tenant": "acme", "embedding": [1.0, 0.0, 0.0] },
                bson::rawdoc! { "_id": "b", "tenant": "acme", "embedding": [0.9, 0.1, 0.0] },
                bson::rawdoc! { "_id": "c", "tenant": "other", "embedding": [0.95, 0.05, 0.0] },
                bson::rawdoc! { "_id": "d", "tenant": "acme", "embedding": [0.0, 1.0, 0.0] },
                bson::rawdoc! { "_id": "e", "tenant": "other", "embedding": [0.0, 0.0, 1.0] },
            ] {
                txn.put(&handle, &doc).unwrap();
            }
            txn.commit().unwrap();
        }
        engine
    }

    /// `CollectionMeta` for `photos` mirroring what `slate-db` builds: the
    /// `tenant` secondary index and the `embedding` cosine vector index.
    fn photos_meta() -> CollectionMeta {
        CollectionMeta {
            indexes: vec!["tenant".into()],
            compound_indexes: Vec::new(),
            vector_indexes: vec![VectorIndexMeta {
                field: "embedding".into(),
                metric: slate_planner::VectorMetric::Cosine,
                dtype: slate_planner::VectorDataType::Float32,
            }],
            pk_path: "_id".into(),
        }
    }

    /// Lower `sql` against `photos_meta` and run it end-to-end, returning each
    /// result document's `_id` as a string (the order the plan emits them).
    fn run_ids(engine: &KvEngine<MemoryStore>, sql: &str) -> Vec<String> {
        let txn = engine.begin(true).unwrap();
        let plan =
            slate_planner::lower(slate_sql::parse(sql).unwrap(), photos_ref(), &photos_meta());
        let out = Executor::new(&txn).execute_collect(plan).unwrap();
        out.into_iter()
            .map(|v| match v {
                RawBson::Document(d) => d.get_str("_id").unwrap().to_string(),
                other => panic!("expected a document, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn e2e_cosine_topk_matches_brute_force() {
        // Nearest to [1,0,0] by cosine: a (1.0), c (~0.998), b (~0.994), then d/e.
        let engine = seeded_photos();
        let got = run_ids(
            &engine,
            "SELECT VALUE c FROM c ORDER BY VECTORDISTANCE(c.embedding, [1.0, 0.0, 0.0]) DESC LIMIT 3",
        );
        assert_eq!(got, vec!["a", "c", "b"]);
    }

    #[test]
    fn e2e_prefilter_returns_k_from_the_filtered_set() {
        // CORRECTNESS TRAP 1: with `WHERE tenant='acme'`, the global cosine top-3
        // would be [a, c, b] — but c is tenant 'other'. A pre-filter (the WHERE
        // shrinks the candidate set *before* the top-k) must instead return the 3
        // nearest *acme* docs: a, b, d — never fewer than 3, never the 'other' c.
        let engine = seeded_photos();
        let got = run_ids(
            &engine,
            "SELECT VALUE c FROM c WHERE c.tenant = 'acme' \
             ORDER BY VECTORDISTANCE(c.embedding, [1.0, 0.0, 0.0]) DESC LIMIT 3",
        );
        assert_eq!(
            got,
            vec!["a", "b", "d"],
            "k from the filtered set, no 'other'"
        );
        // And it returns the full k even though the global top-3 includes a
        // non-matching doc — the under-return bug the trap warns about.
        assert_eq!(got.len(), 3);
    }

    #[test]
    fn e2e_k_greater_than_corpus_returns_all_filtered() {
        // k larger than the filtered set returns every acme doc, nearest-first.
        let engine = seeded_photos();
        let got = run_ids(
            &engine,
            "SELECT VALUE c FROM c WHERE c.tenant = 'acme' \
             ORDER BY VECTORDISTANCE(c.embedding, [1.0, 0.0, 0.0]) DESC LIMIT 50",
        );
        assert_eq!(got, vec!["a", "b", "d"]);
    }

    #[test]
    fn e2e_euclidean_metric_via_explicit_arg() {
        // A collection whose index is euclidean; the call names it explicitly and
        // sorts ASC (nearest = smallest distance).
        let engine = KvEngine::new(MemoryStore::new());
        {
            let txn = engine.begin(false).unwrap();
            txn.create_collection(DEFAULT_CF, "photos", &Default::default())
                .unwrap();
            txn.create_vector_index(
                DEFAULT_CF,
                "photos",
                &VectorIndexSpec::float32("embedding", 3, slate_engine::VectorMetric::Euclidean),
            )
            .unwrap();
            let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
            for doc in [
                bson::rawdoc! { "_id": "a", "embedding": [1.0, 0.0, 0.0] },
                bson::rawdoc! { "_id": "b", "embedding": [0.0, 1.0, 0.0] },
                bson::rawdoc! { "_id": "c", "embedding": [0.9, 0.1, 0.0] },
            ] {
                txn.put(&handle, &doc).unwrap();
            }
            txn.commit().unwrap();
        }
        let meta = CollectionMeta {
            indexes: Vec::new(),
            compound_indexes: Vec::new(),
            vector_indexes: vec![VectorIndexMeta {
                field: "embedding".into(),
                metric: slate_planner::VectorMetric::Euclidean,
                dtype: slate_planner::VectorDataType::Float32,
            }],
            pk_path: "_id".into(),
        };
        let txn = engine.begin(true).unwrap();
        let sql = "SELECT VALUE c FROM c \
             ORDER BY VECTORDISTANCE(c.embedding, [1.0, 0.0, 0.0], 'euclidean') ASC LIMIT 2";
        let plan = slate_planner::lower(slate_sql::parse(sql).unwrap(), photos_ref(), &meta);
        let out = Executor::new(&txn).execute_collect(plan).unwrap();
        let ids: Vec<String> = out
            .into_iter()
            .map(|v| match v {
                RawBson::Document(d) => d.get_str("_id").unwrap().to_string(),
                other => panic!("got {other:?}"),
            })
            .collect();
        // Nearest to [1,0,0]: a (0), c (~0.14), then b.
        assert_eq!(ids, vec!["a", "c"]);
    }

    #[test]
    fn e2e_float16_topk_matches_exact_via_rescore() {
        // A float16 index stores an approximate copy; the executor over-samples
        // the scan and rescores the shortlist against each document's exact f32.
        // The returned order must equal the exact (f64) brute-force ranking — the
        // chosen values aren't f16-exact and sit in near-ties, so a broken rescore
        // (emitting the approximate order) would diverge.
        let corpus: [(&str, [f64; 3]); 5] = [
            ("a", [0.11, 0.93, 0.21]),
            ("b", [0.10, 0.95, 0.19]),
            ("c", [0.90, 0.05, 0.10]),
            ("d", [0.33, 0.33, 0.88]),
            ("e", [0.12, 0.90, 0.25]),
        ];
        let engine = KvEngine::new(MemoryStore::new());
        {
            let txn = engine.begin(false).unwrap();
            txn.create_collection(DEFAULT_CF, "photos", &Default::default())
                .unwrap();
            txn.create_vector_index(
                DEFAULT_CF,
                "photos",
                &VectorIndexSpec::float16("embedding", 3, slate_engine::VectorMetric::Cosine),
            )
            .unwrap();
            let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
            for (id, e) in corpus {
                let doc = bson::rawdoc! { "_id": id, "embedding": [e[0], e[1], e[2]] };
                txn.put(&handle, &doc).unwrap();
            }
            txn.commit().unwrap();
        }
        let meta = CollectionMeta {
            indexes: Vec::new(),
            compound_indexes: Vec::new(),
            vector_indexes: vec![VectorIndexMeta {
                field: "embedding".into(),
                metric: slate_planner::VectorMetric::Cosine,
                dtype: slate_planner::VectorDataType::Float16,
            }],
            pk_path: "_id".into(),
        };
        let txn = engine.begin(true).unwrap();
        let sql = "SELECT VALUE c FROM c ORDER BY VECTORDISTANCE(c.embedding, [0.1, 0.92, 0.2]) DESC LIMIT 3";
        let plan = slate_planner::lower(slate_sql::parse(sql).unwrap(), photos_ref(), &meta);
        let out = Executor::new(&txn).execute_collect(plan).unwrap();
        let got: Vec<String> = out
            .into_iter()
            .map(|v| match v {
                RawBson::Document(d) => d.get_str("_id").unwrap().to_string(),
                other => panic!("got {other:?}"),
            })
            .collect();

        // Exact (f64) brute-force expectation — the same shared metric the rescore
        // uses, so float16 + rescore must reproduce this order exactly.
        let q = [0.1_f64, 0.92, 0.2];
        let mut scored: Vec<(f64, &str)> = corpus
            .iter()
            .map(|(id, e)| (VectorMetric::Cosine.measure(&q, e), *id))
            .collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap().then_with(|| a.1.cmp(b.1)));
        let want: Vec<String> = scored
            .iter()
            .take(3)
            .map(|(_, id)| id.to_string())
            .collect();

        assert_eq!(got, want, "float16 rescore must match the exact ranking");
    }

    #[test]
    fn e2e_int8_topk_matches_exact_via_rescore() {
        // int8 is coarser than float16, so its approximate scan order diverges more
        // — but the rescore reads each shortlisted document's exact float32, so the
        // returned order must still equal the exact (f64) brute-force ranking.
        let corpus: [(&str, [f64; 3]); 5] = [
            ("a", [0.11, 0.93, 0.21]),
            ("b", [0.10, 0.95, 0.19]),
            ("c", [0.90, 0.05, 0.10]),
            ("d", [0.33, 0.33, 0.88]),
            ("e", [0.12, 0.90, 0.25]),
        ];
        let engine = KvEngine::new(MemoryStore::new());
        {
            let txn = engine.begin(false).unwrap();
            txn.create_collection(DEFAULT_CF, "photos", &Default::default())
                .unwrap();
            txn.create_vector_index(
                DEFAULT_CF,
                "photos",
                &VectorIndexSpec::int8("embedding", 3, slate_engine::VectorMetric::Cosine),
            )
            .unwrap();
            let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
            for (id, e) in corpus {
                let doc = bson::rawdoc! { "_id": id, "embedding": [e[0], e[1], e[2]] };
                txn.put(&handle, &doc).unwrap();
            }
            txn.commit().unwrap();
        }
        let meta = CollectionMeta {
            indexes: Vec::new(),
            compound_indexes: Vec::new(),
            vector_indexes: vec![VectorIndexMeta {
                field: "embedding".into(),
                metric: slate_planner::VectorMetric::Cosine,
                dtype: slate_planner::VectorDataType::Int8,
            }],
            pk_path: "_id".into(),
        };
        let txn = engine.begin(true).unwrap();
        let sql = "SELECT VALUE c FROM c ORDER BY VECTORDISTANCE(c.embedding, [0.1, 0.92, 0.2]) DESC LIMIT 3";
        let plan = slate_planner::lower(slate_sql::parse(sql).unwrap(), photos_ref(), &meta);
        let out = Executor::new(&txn).execute_collect(plan).unwrap();
        let got: Vec<String> = out
            .into_iter()
            .map(|v| match v {
                RawBson::Document(d) => d.get_str("_id").unwrap().to_string(),
                other => panic!("got {other:?}"),
            })
            .collect();

        let q = [0.1_f64, 0.92, 0.2];
        let mut scored: Vec<(f64, &str)> = corpus
            .iter()
            .map(|(id, e)| (VectorMetric::Cosine.measure(&q, e), *id))
            .collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap().then_with(|| a.1.cmp(b.1)));
        let want: Vec<String> = scored
            .iter()
            .take(3)
            .map(|(_, id)| id.to_string())
            .collect();

        assert_eq!(got, want, "int8 rescore must match the exact ranking");
    }

    #[test]
    fn e2e_quantized_index_composes_with_where_prefilter() {
        // The WHERE pre-filter (rule 1) and the quantized rescore are orthogonal
        // and must compose: the filter shrinks the candidate set *before* the
        // top-k, then the int8 scan + exact rescore runs over only the filtered
        // docs. The result must equal the exact f64 brute-force over the *acme*
        // subset — never the globally-nearest `c` (tenant `other`), never fewer
        // than k while the subset has them.
        let corpus: [(&str, &str, [f64; 3]); 6] = [
            ("a", "acme", [0.11, 0.93, 0.21]),
            ("b", "acme", [0.10, 0.95, 0.19]),
            ("c", "other", [0.10, 0.96, 0.18]), // globally nearest, but filtered out
            ("d", "acme", [0.90, 0.05, 0.10]),
            ("e", "other", [0.12, 0.90, 0.25]),
            ("f", "acme", [0.13, 0.89, 0.26]),
        ];
        let engine = KvEngine::new(MemoryStore::new());
        {
            let txn = engine.begin(false).unwrap();
            txn.create_collection(DEFAULT_CF, "photos", &Default::default())
                .unwrap();
            txn.create_index(DEFAULT_CF, "photos", "tenant").unwrap();
            txn.create_vector_index(
                DEFAULT_CF,
                "photos",
                &VectorIndexSpec::int8("embedding", 3, slate_engine::VectorMetric::Cosine),
            )
            .unwrap();
            let handle = txn.collection(DEFAULT_CF, "photos").unwrap();
            for (id, tenant, e) in corpus {
                let doc =
                    bson::rawdoc! { "_id": id, "tenant": tenant, "embedding": [e[0], e[1], e[2]] };
                txn.put(&handle, &doc).unwrap();
            }
            txn.commit().unwrap();
        }
        let meta = CollectionMeta {
            indexes: vec!["tenant".into()],
            compound_indexes: Vec::new(),
            vector_indexes: vec![VectorIndexMeta {
                field: "embedding".into(),
                metric: slate_planner::VectorMetric::Cosine,
                dtype: slate_planner::VectorDataType::Int8,
            }],
            pk_path: "_id".into(),
        };
        let txn = engine.begin(true).unwrap();
        let sql = "SELECT VALUE c FROM c WHERE c.tenant = 'acme' \
             ORDER BY VECTORDISTANCE(c.embedding, [0.1, 0.92, 0.2]) DESC LIMIT 3";
        let plan = slate_planner::lower(slate_sql::parse(sql).unwrap(), photos_ref(), &meta);
        let out = Executor::new(&txn).execute_collect(plan).unwrap();
        let got: Vec<String> = out
            .into_iter()
            .map(|v| match v {
                RawBson::Document(d) => d.get_str("_id").unwrap().to_string(),
                other => panic!("got {other:?}"),
            })
            .collect();

        // Exact (f64) brute-force over the ACME subset only.
        let q = [0.1_f64, 0.92, 0.2];
        let mut scored: Vec<(f64, &str)> = corpus
            .iter()
            .filter(|(_, tenant, _)| *tenant == "acme")
            .map(|(id, _, e)| (VectorMetric::Cosine.measure(&q, e), *id))
            .collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap().then_with(|| a.1.cmp(b.1)));
        let want: Vec<String> = scored
            .iter()
            .take(3)
            .map(|(_, id)| id.to_string())
            .collect();

        assert_eq!(got, want, "int8 rescore over the WHERE-filtered set");
        assert!(
            !got.contains(&"c".to_string()),
            "the globally-nearest but filtered-out doc must not leak in"
        );
        assert_eq!(got.len(), 3, "full k from the filtered subset");
    }

    // ── Cooperative deadline over the flat-vector scan (Resource Limits A) ──

    use crate::budget::Deadline;
    use std::rc::Rc;

    /// Lower `sql` and run it with the given [`ExecEnv`] (carrying a deadline).
    fn run_with_env(
        engine: &KvEngine<MemoryStore>,
        sql: &str,
        env: ExecEnv<'_>,
    ) -> Result<Vec<RawBson>, ExecError> {
        let txn = engine.begin(true).unwrap();
        let plan =
            slate_planner::lower(slate_sql::parse(sql).unwrap(), photos_ref(), &photos_meta());
        Executor::with_env(&txn, env).execute_collect(plan)
    }

    const KNN_SQL: &str =
        "SELECT VALUE c FROM c ORDER BY VECTORDISTANCE(c.embedding, [1.0, 0.0, 0.0]) DESC LIMIT 3";

    #[test]
    fn deadline_trips_during_vector_scan() {
        // An already-expired deadline (clock reads past `at`): the scan's first
        // candidate check (counter starts at 0) aborts the kNN with Timeout,
        // proving the flat-vector source is now deadline-bounded.
        let engine = seeded_photos();
        let deadline = Rc::new(Deadline::new(Rc::new(|| 1_000_000_i64), 0));
        let env = ExecEnv::new().with_deadline(Some(deadline));
        let result = run_with_env(&engine, KNN_SQL, env);
        assert!(matches!(result, Err(ExecError::Timeout)), "got {result:?}");
    }

    #[test]
    fn generous_deadline_lets_the_knn_complete() {
        // A deadline far in the future never trips: the kNN returns its top-3
        // (a, c, b — the cosine ordering the e2e test pins).
        let engine = seeded_photos();
        let deadline = Rc::new(Deadline::new(Rc::new(|| 0_i64), i64::MAX));
        let env = ExecEnv::new().with_deadline(Some(deadline));
        let ids: Vec<String> = run_with_env(&engine, KNN_SQL, env)
            .unwrap()
            .into_iter()
            .map(|v| match v {
                RawBson::Document(d) => d.get_str("_id").unwrap().to_string(),
                other => panic!("got {other:?}"),
            })
            .collect();
        assert_eq!(ids, vec!["a", "c", "b"]);
    }

    #[test]
    fn materialization_cap_trips_on_a_non_selective_prefilter() {
        // The WHERE pre-filter set is the node's unbounded buffer (the analogue of
        // IndexMerge's id set). `tenant = 'acme'` admits three candidates; a cap of
        // 1 aborts with LimitExceeded as that set grows, before the top-k runs.
        let engine = seeded_photos();
        let env = ExecEnv::new().with_materialization_cap(Some(1));
        let sql = "SELECT VALUE c FROM c WHERE c.tenant = 'acme' \
             ORDER BY VECTORDISTANCE(c.embedding, [1.0, 0.0, 0.0]) DESC LIMIT 3";
        let result = run_with_env(&engine, sql, env);
        assert!(
            matches!(result, Err(ExecError::LimitExceeded(_))),
            "got {result:?}"
        );
    }

    #[test]
    fn materialization_cap_passes_under_the_limit() {
        // The same pre-filtered kNN under a generous cap returns its top-k.
        let engine = seeded_photos();
        let env = ExecEnv::new().with_materialization_cap(Some(1000));
        let sql = "SELECT VALUE c FROM c WHERE c.tenant = 'acme' \
             ORDER BY VECTORDISTANCE(c.embedding, [1.0, 0.0, 0.0]) DESC LIMIT 3";
        let ids: Vec<String> = run_with_env(&engine, sql, env)
            .unwrap()
            .into_iter()
            .map(|v| match v {
                RawBson::Document(d) => d.get_str("_id").unwrap().to_string(),
                other => panic!("got {other:?}"),
            })
            .collect();
        assert_eq!(ids, vec!["a", "b", "d"]);
    }
}
