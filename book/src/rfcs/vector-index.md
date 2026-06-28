# RFC: Vector Index & `VECTORDISTANCE`

> **Status: Phase 1 (`VECTORDISTANCE` + flat index) and Phase 2 (`float16` /
> `int8` quantization) shipped; ANN (Phase 3) deferred.** The scalar
> `VECTORDISTANCE(a, b, metric?)` (cosine / dotproduct / euclidean) and a flat
> (exact, brute-force) vector index ship: `ORDER BY VECTORDISTANCE(…) … LIMIT k`
> seeks a per-field index instead of scanning. **Phase 2** adds quantized storage
> widths — `float16` (2× smaller) and `int8` (~4× smaller, per-vector scale) —
> with a full-precision **rescore**: the scan measures the small approximate copy,
> then re-ranks an over-sampled shortlist against the document's exact `float32`,
> so the returned ranking stays effectively exact (measured recall@k ≈ 1.0 with a
> bounded rescore window). `binary` (sign-bit Hamming) was prototyped and
> **descoped** — recall@10 ≈ 0.5 even at a 16× rescore window on clustered data, a
> silently-lossy index. ANN (Phase 3) remains deferred; this RFC does **not**
> commit to it. The evidence behind every claim — how Cosmos / MongoDB / pgvector /
> embedded stores lay vectors out on disk, the ANN algorithm families, and the use
> cases — lives in the companion [research notes](./vector-index-research-notes.md)
> (84 cited sources). The [roadmap](../roadmap.md) tracks status at a glance.

## Concept

A *vector* is the numeric embedding an ML model produces for a piece of text or an
image (typically 384–3072 `float32`s). Two pieces of content that *mean* similar
things have embeddings that are *close* in that space. A **vector index** stores
those embeddings and answers "which stored vectors are nearest this query vector?"
— k-nearest-neighbour (kNN) search.

The division of labour matters: **the application generates embeddings** (OpenAI,
Cohere, sentence-transformers, on-device CoreML); **Slate only stores, indexes, and
searches them.** Slate never runs a model. This mirrors how pgvector, MongoDB Atlas,
and Cosmos all work — the vector is just a document field, plus a derived index.

The query surface is one scalar function, `VECTORDISTANCE(a, b, …)`, used in
`ORDER BY` with `TOP k` — Cosmos's exact shape:

```sql
SELECT TOP 5 c.id, VECTORDISTANCE(c.embedding, @q) AS score
FROM c
WHERE c.tenant = 'acme'
ORDER BY VECTORDISTANCE(c.embedding, @q)
```

## Motivation

- **The one glaring missing capability.** Slate has SQL, aggregates, spatial
  functions, full secondary/compound indexing — but no vector search, which in 2026
  is table stakes for any document store backing an AI application. Cosmos has it;
  the [cosmos parity backlog](../roadmap.md) flagged `VECTORDISTANCE` as
  deferred-for-discussion. This is that discussion.
- **Slate's differentiated angle: on-device.** Slate is *embedded*, with Swift/Apple
  bindings (`slate-uniffi`). An on-device vector store is the sweet spot for
  **RAG** — chunk documents, embed the chunks, store vectors + text + metadata,
  embed the question at query time, retrieve the top-k chunks, feed them to an LLM —
  with **privacy, offline operation, and zero per-query cost** a hosted vector DB
  can't match. Local semantic search over a user's own data, no round-trip.
- **Cheap to start, on the substrate we already have.** The lowest-risk first cut
  (a *flat* index) is a range scan + a top-k heap — Slate's ordered-KV store's
  single best access pattern. No new algorithm, no ANN dependency, exact results.

## Use cases (what `VECTORDISTANCE` is *for*)

Semantic search; **RAG** retrieval; recommendations / "more like this";
near-duplicate detection; kNN classification; hybrid search (combine BM25 full-text
+ vector via Reciprocal Rank Fusion). Two clarifications, since they came up:

- **Not map plotting.** Geospatial distance (lat/long, `ST_DISTANCE`, which Slate
  already has) is 2-D physical distance and is *unrelated* to high-dimensional
  embedding distance. You *can* visualise embeddings by reducing them to 2-D/3-D
  (PCA/t-SNE/UMAP), but that is app-side analytics, not the index's job.
- **Clustering, two senses.** An IVF index uses k-means *internally as its index
  structure* (invisible plumbing). Separately, running k-means/HDBSCAN over stored
  embeddings for topic discovery is a *user* analytics task — a different feature,
  out of scope here.

## Current state (Slate)

- **Function-first precedent.** `ST_DISTANCE`/`ST_WITHIN`/… ship as `slate-eval`
  functions with **no spatial index** — spatial predicates do full scans (see the
  [Spatial Index RFC](./spatial-index.md)). The same shape applies: a
  `VECTORDISTANCE` function can land first (full scan), then a flat index makes it
  fast. Adding it is one file (`functions/vectordistance.rs`) + one match arm in
  `functions/mod.rs`.
- **The store is range-scan-shaped.** `Store::scan_range(cf, range, reverse)`
  already exists (added recently); a flat vector scan is exactly that plus a
  bounded top-k heap.
- **No index-type dimension yet.** Indexes are `IndexSpec { path, unique }`; the
  catalog has no notion of an index *kind*. A vector index needs that — which is a
  **catalog format change**, and the catalog is now versioned (see the
  [On-Disk Format Versioning RFC](./on-disk-format-versioning.md), v1 done): the
  vector index config is precisely the kind of new catalog state that the
  `CATALOG_VERSION` seam was put in place to absorb cleanly.
- **The planner already does sargable pre-filtering** (`sargable.rs`,
  `IndexAccess`), which is the key advantage for *filtered* vector search (below).

## Design — phased, flat-first

### Phase 1 — `VECTORDISTANCE` + a flat (brute-force) index

- **`VECTORDISTANCE(a, b, metric?)`** in `slate-eval`: `cosine`, `dotproduct`,
  `euclidean` (Cosmos's three), over two BSON arrays of numbers. Pure Rust, so it
  compiles to wasm32 and iOS unchanged. Ships usable immediately as a full-scan
  function (the spatial precedent).
- **Vector field = a BSON array on the document** (the source of truth), exactly as
  pgvector/Mongo/Cosmos store it.
- **Flat index = a thin keyspace** (`doc_id → vector bytes`, or a marked CF) built
  from that field. A query is a `scan_range` over the candidate set + an in-memory
  top-k heap. **Exact**, KV-native, and — crucially for a document DB —
  **filter-friendly.**
- **Filtered search = pre-filter into brute force.** Resolve the metadata predicate
  through Slate's existing secondary/compound indexes → a candidate doc-id set →
  brute-force `VECTORDISTANCE` over just those vectors. For a flat index this is
  **free and exact** (there is no ANN graph to disconnect) — a genuine advantage
  over hosted ANN stores, which must choose pre- vs post-filter and lose recall.
- This is Cosmos's `flat` vector index type, on Slate's most natural access path.

### Phase 2 — quantization (on-device footprint) — **shipped (`float16`/`int8`)**

A 1536-dim `float32` vector is ~6 KB; 100K of them ~600 MB of raw floats —
material on a phone. Phase 2 adds `dataType` parity so the index stores a smaller,
*approximate* copy and the seek refines it:

- **Widths.** `float16` (2 bytes/component, 2× smaller, via the pure-Rust `half`
  crate), `int8` (`dims + 4` bytes, ~4× smaller, symmetric **per-vector** scale
  `max|x| / 127` — no training pass, so it fits the incremental write path). The
  packed blob carries the same TTL frame as the `float32` entry; the dtype is the
  decode schema, persisted whole in the existing catalog config (no migration).
- **Rescore (the key move).** The index stores *only* the quantized copy — the
  document's BSON array remains the canonical full-precision source. So the seek is
  *scan the approximate copy → keep an over-sampled shortlist of `N > k` →
  re-rank that shortlist against each document's exact `float32` with the same
  shared metric → return `k`*. The final ordering is identical to a full scan over
  the shortlist, so quantization can only cost **recall** (a true neighbour missing
  the shortlist), never mis-order what is returned. The rescore window is
  `max(4·k, 64)` (the spike found recall@k = 1.0 already at `2·k`).
- **`binary` descoped.** Sign-bit Hamming was prototyped and rejected: recall@10
  ≈ 0.10 at `N = k` and only ≈ 0.50 even at a 16× window on clustered
  near-duplicate data — a silently-lossy index, which the gate forbids. Revisit
  only with a better scheme (asymmetric Hamming + large oversample, or a learned
  rotation) if a corpus ever needs the 32×.

Created via `indexes().create(path, VectorIndexOptions::float16(dims, metric))` /
`::int8(...)` — no SQL grammar (parity rule; Cosmos can't see the index *type*).
This is Cosmos's `quantizedFlat` direction and is where the on-device story gets
serious: a 100K × 1536-d corpus drops from ~600 MB (`float32`) to ~300 MB
(`float16`) or ~150 MB (`int8`). The recall, footprint, and latency numbers are in
[benchmarks](../benchmarks.md#vector-search-knn).

### Phase 3 — ANN, only if a corpus demands it

Large on-device corpora may outgrow brute force. The KV-friendly options, in order
of preference: **IVF over KV** (k-means centroids + posting lists keyed
`(centroid_id, doc_id)` → one `scan_range` per probed cell — still range scans), or
a **pure-Rust HNSW rebuilt in memory on open** (sidesteps incremental graph
persistence; bounded by corpus size). Graph search is serially-dependent *random*
`get`s — the KV store's *worst* pattern — so ANN is deliberately last. Cosmos's
`diskANN` is the rough analogue.

## The decisions this RFC asks for

1. **Flat-first?** Recommended **yes** — exact, KV-native, filter-friendly, no new
   dependency. (Jumping straight to ANN trades exactness and a clean KV fit for
   speed Slate's likely corpus sizes don't need yet.)
2. **ANN persistence, when pursued** — rebuild-on-open vs persist-graph-as-KV
   (DiskANN co-location) vs IVF-over-KV. Defer; recommend IVF-over-KV or
   rebuild-on-open when the time comes.
3. **Dependency vs build-our-own.** The **wasm32 constraint forces pure Rust** for
   anything in the shared crates: `instant-distance` or `hnsw_rs` (no C deps).
   USearch is the best *feature* fit (iOS + view-from-disk + quantization) but is
   C++ FFI with a per-target binding split — usable from `slate-uniffi` (iOS), not
   the `slate-wasm` Rust crate. **Phase 1 (flat) needs no ANN dependency at all**,
   so this decision can wait for Phase 3.
4. **Quantization & dtype** — **resolved & shipped.** `float32` (Phase 1);
   `float16` + `int8` with full-precision rescore (Phase 2). The exact vector is
   *not* stored twice — it already lives on the document, which the rescore reads.
   `binary` was prototyped and **descoped** for poor recall (see the spike). int8
   uses a per-vector scale (no training pass) over a per-component codebook, to
   keep the write path incremental.
5. **`VECTORDISTANCE` surface & filtered semantics** — match Cosmos's signature and
   `TOP k` (parity, per the "Cosmos is the oracle" rule — no SQL surface Cosmos
   lacks); **pre-filter-into-brute-force** as the filtered-search model.

## Cosmos, for parity

Cosmos exposes a container **vector embedding policy** (path, `dataType`,
`distanceFunction`, `dimensions`) and a **vector index** with types `flat`,
`quantizedFlat`, `diskANN` — which map onto the three phases above. The query
function `VECTORDISTANCE` and `ORDER BY … TOP k` are the surface to match. The
oracle validates *results* (which rows, in what order); the index *type* is a
physical choice it can't see — so a flat index is oracle-equivalent to Cosmos's
ANN for the same query, just slower at scale. We can extend beyond Cosmos where it
helps (exact pre-filtered search), but we add no SQL grammar Cosmos lacks.

## Non-goals

- **No embedding generation.** Slate stores and searches vectors; producing them is
  the application's job (an external or on-device model).
- **No geospatial conflation.** `VECTORDISTANCE` (high-dim) is not `ST_DISTANCE`
  (2-D); they share nothing but the word "distance."
- **No user-facing clustering API.** k-means/HDBSCAN over embeddings is a separate
  analytics product decision, independent of whether an index uses k-means inside.
- **No ANN in the first cut.** Phase 1 is exact brute force; ANN is gated on a
  corpus that needs it.

## Spike — what to validate before Phase 1 lands

1. **`VECTORDISTANCE` eval surface** — implement the function (three metrics) and
   diff results against the Cosmos emulator for a small corpus (the parity harness).
2. **Flat scan + top-k** — prototype `scan_range` + bounded heap; measure latency at
   on-device scale (10K–100K vectors, 768/1536 dims) to confirm brute force is
   adequate before reaching for ANN.
3. **Catalog wiring** — add an index *kind* to the catalog behind the
   `CATALOG_VERSION` seam (migrate-or-refuse already handles the format bump).
4. **Pre-filter integration** — confirm the planner can hand a candidate doc-id set
   to a brute-force `VECTORDISTANCE` source.
5. **Pure-Rust build gate** *(only if/when an ANN dep is considered)* — `cargo build
   --target wasm32-unknown-unknown` for `slate-wasm` and an iOS arch build for
   `slate-uniffi`, as a hard gate on any candidate crate.

Related: [Spatial Index](./spatial-index.md) (function-first precedent),
[On-Disk Format Versioning](./on-disk-format-versioning.md) (the catalog seam),
[SQL Query Surface](./sql-query-surface.md) (where full-text + vector were flagged).
