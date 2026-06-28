//! Spike prototype (throwaway) for Vector Index Phase 2 — quantized vector
//! storage with full-precision rescore. On synthetic *clustered* embeddings it
//! measures, per candidate width (float16 / int8 / binary):
//!
//!   * **recall@k** as a function of the rescore window `N` (how many approximate
//!     candidates we re-rank with exact float32), and
//!   * **footprint** (bytes/vector) vs the Phase-1 float32 blob, and
//!   * **scan latency** (the design path: dequantize → shared cosine; plus the
//!     specialised binary Hamming fast path).
//!
//! This gates the implementation: a width ships only if its footprint shrinks as
//! expected *and* recall@k is acceptable with a bounded `N`. The numbers land in
//! `tasks/vector-quantization-spike.md`.
//!
//! Run (release — debug is ~10× slower and the numbers are noise):
//!   cargo test -p slate-engine --release --test quant_spike -- --ignored --nocapture
//!
//! Not a shipped test — `#[ignore]`d (long-running, prints a report), no asserts.

use std::time::Instant;

use half::f16;

// ── deterministic PRNG (xorshift64*) — reproducible, no `rand` dep ────────────
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    /// Uniform in [0, 1).
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    /// Standard normal via Box–Muller.
    fn gauss(&mut self) -> f64 {
        let u1 = self.unit().max(1e-12);
        let u2 = self.unit();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
    }
}

fn normalize(v: &mut [f32]) {
    let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n > 0.0 {
        for x in v.iter_mut() {
            *x /= n;
        }
    }
}

fn random_unit(rng: &mut Rng, dims: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dims).map(|_| rng.gauss() as f32).collect();
    normalize(&mut v);
    v
}

/// A clustered embedding: a random centroid plus Gaussian noise, normalized —
/// a closer proxy for real embeddings (genuine near-neighbour neighbourhoods)
/// than i.i.d. random unit vectors, which are close to the worst case.
fn clustered(rng: &mut Rng, centroids: &[Vec<f32>], sigma: f32, dims: usize) -> Vec<f32> {
    let c = &centroids[(rng.next_u64() as usize) % centroids.len()];
    let mut v: Vec<f32> = (0..dims)
        .map(|i| c[i] + sigma * rng.gauss() as f32)
        .collect();
    normalize(&mut v);
    v
}

/// Exact cosine similarity in f64 (the shared metric's arithmetic).
fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let mut dot = 0.0;
    let mut na = 0.0;
    let mut nb = 0.0;
    for (&x, &y) in a.iter().zip(b) {
        let (x, y) = (x as f64, y as f64);
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let denom = na.sqrt() * nb.sqrt();
    if denom == 0.0 { 0.0 } else { dot / denom }
}

// ── quantizers ────────────────────────────────────────────────────────────────

fn quant_f16(v: &[f32]) -> Vec<u8> {
    v.iter()
        .flat_map(|&x| f16::from_f32(x).to_le_bytes())
        .collect()
}
fn dequant_f16(bytes: &[u8], dims: usize) -> Vec<f32> {
    (0..dims)
        .map(|i| f16::from_le_bytes([bytes[2 * i], bytes[2 * i + 1]]).to_f32())
        .collect()
}

/// int8, symmetric, per-vector scale: `scale = max|x| / 127`. Stored as a 4-byte
/// f32 scale + `dims` i8 codes → `dims + 4` bytes/vector.
fn quant_i8(v: &[f32]) -> (f32, Vec<i8>) {
    let maxabs = v.iter().fold(0f32, |m, &x| m.max(x.abs()));
    let scale = if maxabs == 0.0 { 1.0 } else { maxabs / 127.0 };
    let codes = v
        .iter()
        .map(|&x| (x / scale).round().clamp(-127.0, 127.0) as i8)
        .collect();
    (scale, codes)
}
fn dequant_i8(scale: f32, codes: &[i8]) -> Vec<f32> {
    codes.iter().map(|&c| c as f32 * scale).collect()
}

/// binary: one sign bit per component, packed into `ceil(dims/8)` bytes.
fn quant_bin(v: &[f32]) -> Vec<u8> {
    let mut out = vec![0u8; v.len().div_ceil(8)];
    for (i, &x) in v.iter().enumerate() {
        if x >= 0.0 {
            out[i / 8] |= 1 << (i % 8);
        }
    }
    out
}
fn hamming(a: &[u8], b: &[u8]) -> u32 {
    a.iter().zip(b).map(|(&x, &y)| (x ^ y).count_ones()).sum()
}

// ── recall harness ────────────────────────────────────────────────────────────

/// Top-k doc indices by a score where *larger is nearer*.
fn topk_by<F: Fn(usize) -> f64>(n: usize, k: usize, score: F) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..n).collect();
    idx.sort_by(|&a, &b| {
        score(b)
            .partial_cmp(&score(a))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.cmp(&b))
    });
    idx.truncate(k);
    idx
}

/// `|approx-rescored top-k ∩ exact top-k| / k`, averaged over the queries.
/// `approx_for(i, qi)` is the approximate *similarity* of corpus doc `i` to query
/// `qi` (larger nearer); the candidate top-`n_window` are re-ranked with exact
/// cosine, then we keep k.
fn recall_at_k(
    corpus: &[Vec<f32>],
    exact_topk: &[Vec<usize>],
    queries: &[Vec<f32>],
    k: usize,
    n_window: usize,
    approx_for: impl Fn(usize, usize) -> f64,
) -> f64 {
    let mut sum = 0.0;
    for (qi, q) in queries.iter().enumerate() {
        let candidates = topk_by(corpus.len(), n_window, |i| approx_for(i, qi));
        // Exact rescore over the candidate shortlist (what the executor does
        // with the document's full-precision float32 vector).
        let rescored = {
            let mut c = candidates.clone();
            c.sort_by(|&a, &b| {
                cosine(&corpus[b], q)
                    .partial_cmp(&cosine(&corpus[a], q))
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.cmp(&b))
            });
            c.truncate(k);
            c
        };
        let truth = &exact_topk[qi];
        let hit = rescored.iter().filter(|i| truth.contains(i)).count();
        sum += hit as f64 / k as f64;
    }
    sum / queries.len() as f64
}

const WINDOWS: [usize; 5] = [10, 20, 40, 80, 160];

#[test]
#[ignore = "spike measurement harness; run explicitly with --ignored --nocapture"]
fn quantization_recall_and_footprint() {
    let corpus_n = 10_000;
    let queries_n = 100;
    let k = 10;
    let n_clusters = 64;
    let sigma = 0.35;

    for &dims in &[768usize, 1536] {
        let mut rng = Rng::new(0xC0FFEE ^ dims as u64);
        let centroids: Vec<Vec<f32>> = (0..n_clusters)
            .map(|_| random_unit(&mut rng, dims))
            .collect();
        let corpus: Vec<Vec<f32>> = (0..corpus_n)
            .map(|_| clustered(&mut rng, &centroids, sigma, dims))
            .collect();
        let queries: Vec<Vec<f32>> = (0..queries_n)
            .map(|_| clustered(&mut rng, &centroids, sigma, dims))
            .collect();

        // Ground truth: exact float32 top-k per query.
        let exact_topk: Vec<Vec<usize>> = queries
            .iter()
            .map(|q| topk_by(corpus.len(), k, |i| cosine(&corpus[i], q)))
            .collect();

        // Pre-quantize the corpus once per width.
        let f16_c: Vec<Vec<f32>> = corpus
            .iter()
            .map(|v| dequant_f16(&quant_f16(v), dims))
            .collect();
        let i8_c: Vec<Vec<f32>> = corpus
            .iter()
            .map(|v| {
                let (s, c) = quant_i8(v);
                dequant_i8(s, &c)
            })
            .collect();
        let bin_c: Vec<Vec<u8>> = corpus.iter().map(|v| quant_bin(v)).collect();
        let bin_q: Vec<Vec<u8>> = queries.iter().map(|v| quant_bin(v)).collect();

        println!("\n══════════════════════════════════════════════════════════════");
        println!(
            "dims={dims}  corpus={corpus_n}  queries={queries_n}  k={k}  clustered(σ={sigma}, {n_clusters} centroids)"
        );
        println!("── footprint (bytes/vector) ──");
        let f32_bytes = dims * 4;
        let f16_bytes = dims * 2;
        let i8_bytes = dims + 4;
        let bin_bytes = dims.div_ceil(8);
        println!("  float32 (Phase 1) : {f32_bytes:>6}   1.00×");
        println!(
            "  float16           : {f16_bytes:>6}   {:.2}× smaller",
            f32_bytes as f64 / f16_bytes as f64
        );
        println!(
            "  int8 (+4B scale)  : {i8_bytes:>6}   {:.2}× smaller",
            f32_bytes as f64 / i8_bytes as f64
        );
        println!(
            "  binary            : {bin_bytes:>6}   {:.2}× smaller",
            f32_bytes as f64 / bin_bytes as f64
        );

        println!("── recall@{k} vs rescore window N ──");
        print!("  {:<8}", "N");
        for n in WINDOWS {
            print!("{n:>8}");
        }
        println!();

        let row = |label: &str, f: &dyn Fn(usize) -> f64| {
            print!("  {label:<8}");
            for n in WINDOWS {
                print!("{:>8.3}", f(n));
            }
            println!();
        };
        row("float16", &|n| {
            recall_at_k(&corpus, &exact_topk, &queries, k, n, |i, qi| {
                cosine(&f16_c[i], &queries[qi])
            })
        });
        row("int8", &|n| {
            recall_at_k(&corpus, &exact_topk, &queries, k, n, |i, qi| {
                cosine(&i8_c[i], &queries[qi])
            })
        });
        row("binary", &|n| {
            // Approx similarity = -Hamming between packed corpus & query bits.
            recall_at_k(&corpus, &exact_topk, &queries, k, n, |i, qi| {
                -(hamming(&bin_c[i], &bin_q[qi]) as f64)
            })
        });

        // ── scan latency (whole-corpus distance for one query, averaged) ──
        println!("── scan latency: µs/query over {corpus_n} vectors ──");
        let mut sink = 0.0f64; // defeat dead-code elimination

        let t = Instant::now();
        for q in &queries {
            for v in &corpus {
                sink += cosine(v, q);
            }
        }
        let f32_us = t.elapsed().as_secs_f64() * 1e6 / queries_n as f64;

        let t = Instant::now();
        for q in &queries {
            for v in &f16_c {
                sink += cosine(v, q);
            }
        }
        let f16_us = t.elapsed().as_secs_f64() * 1e6 / queries_n as f64;

        let t = Instant::now();
        for q in &queries {
            for v in &i8_c {
                sink += cosine(v, q);
            }
        }
        let i8_us = t.elapsed().as_secs_f64() * 1e6 / queries_n as f64;

        let t = Instant::now();
        for qp in &bin_q {
            for v in &bin_c {
                sink += hamming(v, qp) as f64;
            }
        }
        let bin_us = t.elapsed().as_secs_f64() * 1e6 / queries_n as f64;

        println!("  float32 (direct)        : {f32_us:>8.1}");
        println!("  float16 (→f32 + cosine) : {f16_us:>8.1}");
        println!("  int8    (→f32 + cosine) : {i8_us:>8.1}");
        println!("  binary  (Hamming)       : {bin_us:>8.1}");
        println!("  (sink={sink:.3})");
    }
}
