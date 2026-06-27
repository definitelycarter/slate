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
//! A `BinaryHeap` of at most k entries holds the current best. The heap's
//! ordering is arranged so its *root is the worst kept entry* — the one a new,
//! better candidate evicts — so each candidate is an O(log k) compare-and-maybe-
//! replace, never an O(n log n) full sort. At the end the heap is drained and
//! reversed into nearest-first order.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};

use bson::{Bson, RawBson};
use slate_ast::Expression;
use slate_engine::{Catalog, EngineTransaction};
use slate_eval::VectorMetric;
use slate_eval::raweval;
use slate_eval::value::Value;

use super::env::row_env;
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
        Some(src) => Some(materialize_candidate_ids(src, handle.pk_path())?),
        None => None,
    };

    let higher_is_closer = metric.higher_is_closer();
    // Widen the query to f32 once (the stored side is f32) so the per-candidate
    // measure compares both sides at the same precision.
    let query_f32 = vector_to_f32(&query);
    let mut heap: BinaryHeap<Candidate> = BinaryHeap::with_capacity(k + 1);

    for entry in txn.scan_vectors(&handle, &field)? {
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
            // Strictly better than the worst kept — evict it. (A tie keeps the
            // incumbent, a stable, deterministic choice.)
            heap.pop();
            heap.push(cand);
        }
    }

    // `into_sorted_vec` sorts ascending by `Ord`. Our `Ord` is `goodness`
    // *reversed* (so the heap root is the worst), which means ascending `Ord`
    // orders *best-first* — exactly the nearest-first output we want.
    let kept: Vec<Candidate> = heap.into_sorted_vec();
    Ok(Box::new(kept.into_iter().map(|c| Ok(Some(c.doc_id)))))
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
fn materialize_candidate_ids(
    source: ValueIter<'_>,
    pk_path: &str,
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
}
