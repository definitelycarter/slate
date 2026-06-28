# Vector Index Phase 2 — Quantization Spike (gate)

> Spike output for [vector-index.md](../book/src/rfcs/vector-index.md) Phase 2
> (lines ~112–117) + decision 4. Companion: [phase-1 plan](../book/src/rfcs/vector-index-phase1-plan.md),
> [research notes](../book/src/rfcs/vector-index-research-notes.md). Branch
> `rfc/vector-quantization`, worktree `vector-quant`, off `main ae11f4c`.
>
> **Gate verdict: SHIP `float16` + `int8`; DESCOPE `binary`.** Both shipped widths
> hit **recall@10 = 1.000** with a small bounded rescore window; binary sign-bit
> Hamming tops out at **recall@10 ≈ 0.5** even at a 16× window — a silently-lossy
> index, which the RFC's own rule forbids shipping.

Reproduce the numbers below:

```
cargo test -p slate-engine --release --test quant_spike -- --ignored --nocapture
```

(`crates/slate-engine/tests/quant_spike.rs` — a throwaway, `#[ignore]`d measurement
harness; `half` added as a `slate-engine` dev-dep for it. Synthetic *clustered*
embeddings: 64 random unit centroids, each vector `normalize(centroid + 0.35·𝒩)`,
a near-neighbour-rich proxy for real embeddings — i.i.d. random unit vectors are
close to the worst case and would understate recall. Corpus 10 000, 100 queries,
k = 10, cosine.)

---

## 1. The key design point (why this works at all)

The index stores the **quantized** vector (small, cheap approximate distance); the
**full-precision `f32` is already on the document** (the BSON array is the source
of truth — [`vector.rs:1-14`](../crates/slate-engine/src/vector.rs)). So the seek
becomes:

```
quantized scan  →  approximate top-N (N > k)  →  fetch exact f32 from docs  →  exact rescore  →  top-k
```

The decisive consequence: **the final top-k is always exact-*scored*.** The rescore
reads the document's array and measures with the one shared
[`VectorMetric`](../crates/slate-eval/src/vector.rs) in f64 — bit-identical to what
a full scan / the scalar `VECTORDISTANCE` computes. Quantization therefore affects
**only one thing: whether a true top-k neighbour survives into the rescored
shortlist** (candidate-set recall). It can never produce a *wrongly ordered* result
among the rows it returns. That is what makes a bounded rescore window a real
correctness lever and not a fudge factor.

No full-precision copy is stored separately (the RFC's decision 4 wording): the
document already holds it.

---

## 2. Encoding per width (resolved)

Pure Rust only (wasm32 + iOS — no C deps). `half` 2.7.1 is pure-Rust bit
manipulation (no `libm`, no intrinsics by default), compiles to wasm32/iOS clean →
**`half` is acceptable**; we promote it from dev-dep to a real `slate-engine`
dependency in slice 1. int8 and binary are hand-rolled (trivial).

| Width | Code | Per-vector bytes | Stored entry |
|---|---|---|---|
| **float16** | `half::f16::from_f32` / `to_f32` | `2·dims` | `dims` LE f16 |
| **int8** | symmetric, **per-vector** scale `= max\|x\| / 127` | `dims + 4` | LE f32 scale, then `dims` i8 |
| ~~binary~~ | ~~sign bit~~ | ~~`ceil(dims/8)`~~ | — descoped — |

**int8 scheme decision — per-vector symmetric scale (chosen over the alternatives):**

- **Per-vector symmetric (chosen).** One f32 scale per entry, self-contained, **no
  training step** — fits Slate's incremental write path (each `put` packs one entry
  in isolation, exactly like a secondary index). Measured recall 0.985 @ N=k, 1.000
  @ N=2k. The 4-byte scale overhead is the only cost (3.98× vs the ideal 4×).
- *Per-component (per-dim scale/offset, shared codebook in the catalog).* Hits a
  clean 4× (no per-entry overhead) but needs a **global training pass** over the
  corpus and a codebook that goes stale on data drift (Cosmos demands ≥1 000 vectors
  before its quantizer is trusted). Rejected: ~0.5% footprint for a training quorum
  + retrain-on-drift complexity, against an embedded incremental-write model.
- *Per-vector asymmetric (min+scale, `dims + 8`).* Helps skewed distributions;
  embeddings are ~symmetric around 0 and symmetric already reaches recall 1.000, so
  the extra 4 bytes buy nothing here. Rejected.

The stored entry keeps the existing TTL framing
([`encode_vector_entry`/`is_vector_entry_expired`](../crates/slate-engine/src/vector.rs)) —
quantization changes only the packed payload, behind the same tag byte.

---

## 3. Footprint (measured — matches prediction exactly)

| Width | dims=768 | dims=1536 | shrink |
|---|---|---|---|
| float32 (Phase 1) | 3072 B | 6144 B | 1.00× |
| float16 | 1536 B | 3072 B | **2.00×** |
| int8 (+4 B scale) | 772 B | 1540 B | **3.98–3.99×** |
| ~~binary~~ | ~~96 B~~ | ~~192 B~~ | ~~32×~~ |

At the RFC's headline scale (100K × 1536-d): float32 **≈ 600 MB** → float16 **≈ 300 MB**
→ int8 **≈ 150 MB**. The 4× int8 cut is the on-device lever the RFC asks for.

---

## 4. Recall@10 vs rescore window N (the gate metric)

`recall@10 = |rescored top-10 ∩ exact-f32 top-10| / 10`, averaged over 100 queries.

**dims = 768**

| N (window) | 10 | 20 | 40 | 80 | 160 |
|---|---|---|---|---|---|
| float16 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| int8 | 0.987 | **1.000** | 1.000 | 1.000 | 1.000 |
| binary | 0.115 | 0.165 | 0.273 | 0.385 | 0.510 |

**dims = 1536**

| N (window) | 10 | 20 | 40 | 80 | 160 |
|---|---|---|---|---|---|
| float16 | 0.998 | **1.000** | 1.000 | 1.000 | 1.000 |
| int8 | 0.984 | **1.000** | 1.000 | 1.000 | 1.000 |
| binary | 0.100 | 0.157 | 0.250 | 0.354 | 0.489 |

- **float16 — effectively lossless.** 1.000 at N = 2k; even N = k (no oversampling)
  is 0.998–1.000. The f16 approximate distance preserves the exact ordering.
- **int8 — high-recall.** 1.000 at N = 2k; 0.984–0.987 at N = k. A 2× oversample
  fully closes the gap.
- **binary — fails the gate.** 0.10 at N = k, and **still only ~0.50 at N = 16k**.
  Sign-bit Hamming is too coarse to resolve the *within-cluster* near-neighbours that
  make up a real top-10 (the discarded magnitude is exactly the signal that separates
  close vectors). MongoDB's "~95%" binary figure relies on a more elaborate scheme
  (asymmetric distance, learned rotation) and much larger oversampling at >1M scale;
  plain sign-bit + symmetric Hamming + rescore does not reach acceptable recall on
  Slate's on-device target sizes. Shipping it would be a **silently-lossy index** —
  forbidden by the spike's own gate ("don't ship a silently-lossy index").

**Chosen rescore window:** `N = min(corpus, max(OVERSAMPLE·k, FLOOR))` with
**`OVERSAMPLE = 4`, `FLOOR = 64`**. The spike shows N = 2k already gives 1.000; 4k
plus a 64 floor is comfortable margin for real (non-synthetic) distributions and for
small k, at trivial cost — N is also the number of `txn.get` point-reads the rescore
does (≤ 64 for typical k). Slice 5's MongoDB-oracle validation confirms/tunes the
exact constants; `f32` skips rescore entirely (stored == exact).

---

## 5. Scan latency (honest reading — the win is footprint, not in-cache CPU)

µs/query, whole-corpus distance over 10 000 vectors, **release**:

| path | dims=768 | dims=1536 |
|---|---|---|
| float32 (direct) | 4589 | 9876 |
| float16 (→f32 + cosine) | 4537 | 9929 |
| int8 (→f32 + cosine) | 4587 | 9891 |
| binary (Hamming) | **25** | **30** |

The design path — **dequantize → the shared f64 cosine** — is dominated by the f64
distance arithmetic, so f16/int8 scan time is **statistically identical to float32**
in an in-cache microbench. They do the *same* float work (Phase-1 `measure_f32`
already widens f32→f64, [`vector.rs:78-82`](../crates/slate-eval/src/vector.rs)); the
dequant is cheap. **There is no in-cache CPU win from f16/int8, and I am not
claiming one.**

The f16/int8 win is **footprint → I/O + cache residency**: the vector keyspace is
read from the KV store (redb/rocks), and a 2–4× smaller blob is 2–4× less data to
read and far more likely to stay cache-resident — the latency lever that shows up
once the corpus exceeds cache (the regime slice 5 benches on a real backend, not
this in-RAM loop). Binary *does* have a ~180–330× arithmetic win (Hamming popcount),
but its recall makes that moot for shipping. (A future int8-native integer-dot fast
path could reclaim a CPU win without f64 widening — a separate perf lever, out of
scope for footprint parity, noted for later.)

**Gate against the criterion "footprint shrinks AND recall acceptable with a bounded
window":** float16 and int8 both pass decisively; the latency bullet is satisfied as
a footprint/IO win (proven by the size reduction; the CPU path is honestly flat).

---

## 6. Implementation seam (mapped, bottom-up — for the slices)

The change is narrow because the rescore reuses machinery already present:

1. **`slate-engine/src/vector.rs`** — add `VectorDataType::{Float16, Int8}`; make
   `pack_vector` and the entry decode **dtype-aware** (pack per width; dequantize per
   width back to approximate `f32`).
2. **`scan_vectors`** ([`traits.rs:260`](../crates/slate-engine/src/traits.rs),
   impl [`transaction.rs:763`](../crates/slate-engine/src/kv/transaction.rs)) —
   dequantize per `spec.dtype` (the handle already carries the spec via
   `vector_indexes()`), still yielding approximate `f32`. **The executor's approximate
   scan loop is unchanged** — it keeps measuring with the shared metric.
3. **`slate-executor/src/nodes/vector_topk.rs`** — add the rescore pass: scan keeps
   top-N (N = window), then for each candidate `txn.get(handle, doc_id)` → read the
   `field` array → exact f64 measure → re-sort → top-k. The node already holds `txn`
   + `handle`, so no new plumbing to fetch documents. `f32` dtype ⇒ skip rescore.
4. **dtype to the node** — add `dtype` to
   [`VectorIndexMeta` (`sargable.rs:41`)](../crates/slate-planner/src/sargable.rs),
   populate it in [`database.rs:744`](../crates/slate-db/src/database.rs), carry it
   on [`Node::VectorTopK` (`plan.rs:289`)](../crates/slate-planner/src/plan.rs),
   thread through [`recognize_vector_topk` (`lower.rs:519`)](../crates/slate-planner/src/lower.rs)
   and the [executor dispatch (`lib.rs:305`)](../crates/slate-executor/src/lib.rs).
   So the executor knows whether/how-wide to rescore.
5. **Surface** — `VectorIndexOptions::float16(dims, metric)` / `::int8(dims, metric)`
   in [`v2/index.rs:114`](../crates/slate-db/src/v2/index.rs); the sealed
   `IndexBuild` dispatch already exists. No SQL grammar (parity rule — Cosmos can't
   see the index *type*).

The catalog already serializes `VectorIndexSpec` whole and "grows cleanly"
([`vector.rs:49-65`](../crates/slate-engine/src/vector.rs)), so the new dtype variants
ride the existing config value with **no migration** (and no on-disk DBs to migrate).

---

## 7. Verdict & sequencing

- **float16** — ship. Lossless (recall 1.000 @ N=2k), 2×, `half`. Slices 1–2.
- **int8** — ship. High-recall (1.000 @ N=2k, 0.985 @ N=k), 3.98×, per-vector
  symmetric scale, no training. Slice 3.
- **binary** — **descope.** Recall ≈ 0.5 at a 16× window on clustered data; would be
  a silently-lossy index. Revisit only if a future corpus needs the 32× *and* a
  better scheme (asymmetric Hamming + large oversample, or learned rotation) clears
  the recall bar. Recorded here so the descope is evidenced, not silent.

Each width is its own commit; `f32` results stay invariant (cosmos-parity skill); new
dtypes validated against the MongoDB exact-kNN oracle (slice 5), not Cosmos.
