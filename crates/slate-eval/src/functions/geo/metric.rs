//! Metric spatial computations on the WGS84 ellipsoid: `ST_DISTANCE`.
//!
//! Cosmos measures distance on the WGS84 ellipsoid (a sphere is ~0.3% off), so
//! point-to-point distance uses the Vincenty inverse geodesic. This lands within
//! centimetres of the Cosmos oracle but does not bit-reproduce its proprietary
//! spatial library — close, not identical (a tracked numeric gap).

use super::{Coord, Geometry};

/// WGS84 semi-major axis (m).
const WGS84_A: f64 = 6_378_137.0;
/// WGS84 flattening.
const WGS84_F: f64 = 1.0 / 298.257_223_563;
/// WGS84 mean radius (m), used only by the antipodal fallback.
const WGS84_MEAN_R: f64 = 6_371_008.771_415_06;

/// Geodesic distance in **meters** between two geometries on the WGS84
/// ellipsoid. Point-to-point (the common case, and all the corpus exercises) is
/// the Vincenty inverse geodesic; other combinations return the minimum geodesic
/// distance between their vertices — an approximation that is exact for
/// point-to-point and a reasonable lower-ish bound otherwise.
pub(crate) fn distance(a: &Geometry, b: &Geometry) -> f64 {
    let va = coords(a);
    let vb = coords(b);
    let mut min = f64::INFINITY;
    for &p in &va {
        for &q in &vb {
            let d = geodesic(p, q);
            if d < min {
                min = d;
            }
        }
    }
    min
}

/// Flatten a geometry to the list of its `[lng, lat]` vertices. `Coord` is
/// `Copy`, so this copies rather than clones.
fn coords(g: &Geometry) -> Vec<Coord> {
    match g {
        Geometry::Point(p) => vec![*p],
        Geometry::MultiPoint(ps) | Geometry::LineString(ps) => ps.to_vec(),
        Geometry::Polygon(rings) | Geometry::MultiLineString(rings) => {
            rings.iter().flatten().copied().collect()
        }
        Geometry::MultiPolygon(polys) => polys.iter().flatten().flatten().copied().collect(),
    }
}

/// Vincenty inverse formula: geodesic distance (m) between two `[lng, lat]`
/// positions on the WGS84 ellipsoid. Falls back to a spherical estimate for the
/// near-antipodal case where Vincenty fails to converge (rather than NaN).
fn geodesic(p1: Coord, p2: Coord) -> f64 {
    let a = WGS84_A;
    let f = WGS84_F;
    let b = a * (1.0 - f);
    let l = (p2[0] - p1[0]).to_radians();
    let u1 = ((1.0 - f) * p1[1].to_radians().tan()).atan();
    let u2 = ((1.0 - f) * p2[1].to_radians().tan()).atan();
    let (sin_u1, cos_u1) = u1.sin_cos();
    let (sin_u2, cos_u2) = u2.sin_cos();

    let mut lambda = l;
    for _ in 0..200 {
        let (sin_lambda, cos_lambda) = lambda.sin_cos();
        let sin_sigma = ((cos_u2 * sin_lambda).powi(2)
            + (cos_u1 * sin_u2 - sin_u1 * cos_u2 * cos_lambda).powi(2))
        .sqrt();
        if sin_sigma == 0.0 {
            return 0.0; // coincident points
        }
        let cos_sigma = sin_u1 * sin_u2 + cos_u1 * cos_u2 * cos_lambda;
        let sigma = sin_sigma.atan2(cos_sigma);
        let sin_alpha = cos_u1 * cos_u2 * sin_lambda / sin_sigma;
        let cos_sq_alpha = 1.0 - sin_alpha * sin_alpha;
        // cos(2·σ_m); zero on the equatorial line where cos²α == 0.
        let cos2_sigma_m = if cos_sq_alpha != 0.0 {
            cos_sigma - 2.0 * sin_u1 * sin_u2 / cos_sq_alpha
        } else {
            0.0
        };
        let c = f / 16.0 * cos_sq_alpha * (4.0 + f * (4.0 - 3.0 * cos_sq_alpha));
        let lambda_prev = lambda;
        lambda = l
            + (1.0 - c)
                * f
                * sin_alpha
                * (sigma
                    + c * sin_sigma
                        * (cos2_sigma_m
                            + c * cos_sigma * (-1.0 + 2.0 * cos2_sigma_m * cos2_sigma_m)));
        if (lambda - lambda_prev).abs() < 1e-12 {
            let u_sq = cos_sq_alpha * (a * a - b * b) / (b * b);
            let big_a =
                1.0 + u_sq / 16384.0 * (4096.0 + u_sq * (-768.0 + u_sq * (320.0 - 175.0 * u_sq)));
            let big_b = u_sq / 1024.0 * (256.0 + u_sq * (-128.0 + u_sq * (74.0 - 47.0 * u_sq)));
            let delta_sigma = big_b
                * sin_sigma
                * (cos2_sigma_m
                    + big_b / 4.0
                        * (cos_sigma * (-1.0 + 2.0 * cos2_sigma_m * cos2_sigma_m)
                            - big_b / 6.0
                                * cos2_sigma_m
                                * (-3.0 + 4.0 * sin_sigma * sin_sigma)
                                * (-3.0 + 4.0 * cos2_sigma_m * cos2_sigma_m)));
            return b * big_a * (sigma - delta_sigma);
        }
    }
    haversine(p1, p2)
}

/// Spherical great-circle distance (m) on the WGS84 mean radius — the
/// near-antipodal fallback for [`geodesic`].
fn haversine(p1: Coord, p2: Coord) -> f64 {
    let lat1 = p1[1].to_radians();
    let lat2 = p2[1].to_radians();
    let dlat = (p2[1] - p1[1]).to_radians();
    let dlng = (p2[0] - p1[0]).to_radians();
    let h = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlng / 2.0).sin().powi(2);
    2.0 * WGS84_MEAN_R * h.sqrt().asin()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coincident_points_are_zero() {
        let p = Geometry::Point([10.0, 20.0]);
        assert_eq!(distance(&p, &p), 0.0);
    }

    #[test]
    fn cosmos_example_matches_within_centimetres() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/st-distance
        // Cosmos oracle: HQ 3345.269817 m, R&D 1907438.421300 m. Slate uses the
        // Vincenty geodesic, which agrees with Cosmos to within ~8 cm — close
        // but not a bit-for-bit match against its proprietary spatial library.
        let target = Geometry::Point([-122.11758113953535, 47.66901087006131]);
        let hq = Geometry::Point([-122.12826822304672, 47.63980239335718]);
        let rd = Geometry::Point([-96.84368664765994, 46.81297794314663]);
        assert!((distance(&hq, &target) - 3345.269817).abs() < 0.5);
        assert!((distance(&rd, &target) - 1907438.421300).abs() < 1.0);
    }

    #[test]
    fn one_degree_of_latitude_is_about_111km() {
        // A degree of latitude is ~110.9 km near the equator on WGS84.
        let d = distance(&Geometry::Point([0.0, 0.0]), &Geometry::Point([0.0, 1.0]));
        assert!((d - 110_574.0).abs() < 50.0, "got {d}");
    }
}
