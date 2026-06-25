//! The vector distance/similarity math, shared by the `VECTORDISTANCE` scalar
//! function and the flat-vector-index top-k node.
//!
//! Both must agree on the *exact* arithmetic — the index is only ever consulted
//! when its query's metric matches the index's, and a flat top-k must return the
//! same rows the function would over a full scan. Keeping one definition here
//! (rather than one in the function and one in the executor) is what guarantees
//! they can't drift, the same single-definition discipline the comparison and
//! arithmetic rules follow.
//!
//! Following Cosmos's `VectorDistance` semantics, the returned value's *sense*
//! depends on the metric: **`cosine` and `dotproduct` are similarities** (higher
//! is closer → `ORDER BY … DESC`), while **`euclidean` is a distance** (lower is
//! closer → `ORDER BY … ASC`). [`VectorMetric::higher_is_closer`] reports which.

/// The distance/similarity function `VECTORDISTANCE` (and a flat vector index)
/// can be built for — Cosmos's three metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMetric {
    /// Cosine similarity — higher is closer.
    Cosine,
    /// Inner (dot) product — higher is closer.
    DotProduct,
    /// Euclidean (L2) distance — lower is closer.
    Euclidean,
}

impl VectorMetric {
    /// Parse the case-insensitive metric name accepted as `VECTORDISTANCE`'s
    /// optional third argument. `None` for an unknown name (the caller decides
    /// whether that is an error).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "cosine" => Some(VectorMetric::Cosine),
            "dotproduct" => Some(VectorMetric::DotProduct),
            "euclidean" => Some(VectorMetric::Euclidean),
            _ => None,
        }
    }

    /// Whether a *larger* score means *nearer* — true for the similarity metrics
    /// (`cosine`/`dotproduct`), false for the `euclidean` distance. The flat
    /// top-k keeps the k largest scores when true and the k smallest when false,
    /// and the planner only seeks the index when the `ORDER BY` direction matches
    /// this sense (DESC for true, ASC for false).
    pub fn higher_is_closer(self) -> bool {
        match self {
            VectorMetric::Cosine | VectorMetric::DotProduct => true,
            VectorMetric::Euclidean => false,
        }
    }

    /// The metric's value between two equal-length, non-empty vectors (the caller
    /// guarantees the shapes match). The single definition both the scalar
    /// function and the index top-k call.
    pub fn measure(self, a: &[f64], b: &[f64]) -> f64 {
        match self {
            VectorMetric::DotProduct => dot(a, b),
            VectorMetric::Euclidean => a
                .iter()
                .zip(b)
                .map(|(x, y)| (x - y) * (x - y))
                .sum::<f64>()
                .sqrt(),
            VectorMetric::Cosine => {
                let denom = norm(a) * norm(b);
                // A zero-magnitude vector has no direction; define its cosine
                // similarity as 0 rather than NaN.
                if denom == 0.0 { 0.0 } else { dot(a, b) / denom }
            }
        }
    }

    /// [`measure`](Self::measure) over `f32` vectors — the stored flat-index
    /// layout — widening to `f64` so the arithmetic is bit-identical to the
    /// scalar function's (which always works in `f64`). Equal-length, non-empty
    /// vectors (the caller guarantees the shapes match).
    pub fn measure_f32(self, a: &[f32], b: &[f32]) -> f64 {
        let af: Vec<f64> = a.iter().map(|&x| x as f64).collect();
        let bf: Vec<f64> = b.iter().map(|&x| x as f64).collect();
        self.measure(&af, &bf)
    }
}

fn dot(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn norm(a: &[f64]) -> f64 {
    dot(a, a).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotproduct_is_the_inner_product() {
        // [1,2,3]·[4,5,6] = 4 + 10 + 18 = 32
        let d = VectorMetric::DotProduct.measure(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]);
        assert!((d - 32.0).abs() < 1e-9, "got {d}");
    }

    #[test]
    fn euclidean_is_l2_distance() {
        let d = VectorMetric::Euclidean.measure(&[0.0, 0.0, 0.0], &[3.0, 4.0, 0.0]);
        assert!((d - 5.0).abs() < 1e-9, "got {d}");
    }

    #[test]
    fn cosine_identical_direction_is_one() {
        let d = VectorMetric::Cosine.measure(&[1.0, 0.0, 0.0], &[2.0, 0.0, 0.0]);
        assert!((d - 1.0).abs() < 1e-9, "got {d}");
    }

    #[test]
    fn cosine_zero_vector_is_zero_not_nan() {
        let d = VectorMetric::Cosine.measure(&[0.0, 0.0], &[1.0, 1.0]);
        assert_eq!(d, 0.0);
    }

    #[test]
    fn measure_f32_matches_f64() {
        // The f32 path must give the same number as widening by hand.
        let a32 = [1.0_f32, 2.0, 3.0];
        let b32 = [4.0_f32, 5.0, 6.0];
        let a64 = [1.0_f64, 2.0, 3.0];
        let b64 = [4.0_f64, 5.0, 6.0];
        for m in [
            VectorMetric::Cosine,
            VectorMetric::DotProduct,
            VectorMetric::Euclidean,
        ] {
            assert_eq!(m.measure_f32(&a32, &b32), m.measure(&a64, &b64));
        }
    }

    #[test]
    fn parse_is_case_insensitive() {
        assert_eq!(VectorMetric::parse("CoSiNe"), Some(VectorMetric::Cosine));
        assert_eq!(
            VectorMetric::parse("dotproduct"),
            Some(VectorMetric::DotProduct)
        );
        assert_eq!(
            VectorMetric::parse("EUCLIDEAN"),
            Some(VectorMetric::Euclidean)
        );
        assert_eq!(VectorMetric::parse("manhattan"), None);
    }

    #[test]
    fn higher_is_closer_matches_metric_sense() {
        assert!(VectorMetric::Cosine.higher_is_closer());
        assert!(VectorMetric::DotProduct.higher_is_closer());
        assert!(!VectorMetric::Euclidean.higher_is_closer());
    }
}
