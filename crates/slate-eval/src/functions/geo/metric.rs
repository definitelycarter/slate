//! Metric spatial computations on the WGS84 ellipsoid: `ST_DISTANCE` and
//! `ST_AREA`.
//!
//! Cosmos measures on the WGS84 ellipsoid (a sphere is ~0.3% off), so distance
//! uses the Vincenty inverse geodesic and area the authalic-sphere spherical
//! excess. Both land very close to the Cosmos oracle (distance within
//! centimetres, area within ~1 ppm) but do not bit-reproduce its proprietary
//! spatial library — close, not identical (tracked numeric gaps).

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

/// Area in **square meters** of a geometry on the WGS84 ellipsoid. Only the
/// areal geometries (`Polygon`, `MultiPolygon`) have a non-zero area; everything
/// else is zero, matching Cosmos.
pub(crate) fn area(g: &Geometry) -> f64 {
    match g {
        Geometry::Polygon(rings) => polygon_area(rings),
        Geometry::MultiPolygon(polys) => polys.iter().map(|p| polygon_area(p)).sum(),
        _ => 0.0,
    }
}

/// A polygon's area: exterior ring minus any holes. Ring areas are taken as
/// absolute values so winding direction (which GeoJSON does not strictly enforce
/// across all data) cannot flip a sign.
fn polygon_area(rings: &[Vec<Coord>]) -> f64 {
    let mut iter = rings.iter();
    let Some(exterior) = iter.next().map(|r| ring_area(r)) else {
        return 0.0;
    };
    let holes: f64 = iter.map(|r| ring_area(r)).sum();
    (exterior - holes).max(0.0)
}

/// Area of a single closed linear ring via spherical excess on the **authalic
/// sphere**, using authalic latitudes. The authalic mapping is equal-area, so
/// this is the WGS84 ellipsoidal area to ~1 ppm — close to Cosmos but not a
/// bit-for-bit match (a tracked numeric gap).
fn ring_area(ring: &[Coord]) -> f64 {
    let e2 = WGS84_F * (2.0 - WGS84_F);
    let e = e2.sqrt();
    let r_auth = authalic_radius(e);
    let qp = authalic_q(std::f64::consts::FRAC_PI_2, e, e2);
    let mut total = 0.0;
    for w in ring.windows(2) {
        let (lng1, lat1) = (w[0][0].to_radians(), w[0][1].to_radians());
        let (lng2, lat2) = (w[1][0].to_radians(), w[1][1].to_radians());
        // sin(authalic latitude) = q(φ) / q(π/2).
        let s1 = authalic_q(lat1, e, e2) / qp;
        let s2 = authalic_q(lat2, e, e2) / qp;
        total += (lng2 - lng1) * (2.0 + s1 + s2);
    }
    (total * r_auth * r_auth / 2.0).abs()
}

/// The authalic radius (radius of the sphere with the ellipsoid's surface area).
fn authalic_radius(e: f64) -> f64 {
    let a = WGS84_A;
    let b = a * (1.0 - WGS84_F);
    (a * a / 2.0 + b * b / 2.0 * (e.atanh() / e)).sqrt()
}

/// The authalic area function q(φ); `sin(authalic latitude) = q(φ) / q(π/2)`.
fn authalic_q(phi: f64, e: f64, e2: f64) -> f64 {
    let s = phi.sin();
    (1.0 - e2) * (s / (1.0 - e2 * s * s) - (1.0 / (2.0 * e)) * ((1.0 - e * s) / (1.0 + e * s)).ln())
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

    #[test]
    fn cosmos_example_area_within_one_ppm() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/st-area
        // Cosmos oracle: 735970283.0522614 m². The authalic-sphere area lands
        // within ~731 m² (~1 ppm) — close but not a bit-for-bit match.
        let poly = Geometry::Polygon(vec![vec![
            [31.8, -5.0],
            [32.0, -5.0],
            [32.0, -4.7],
            [31.8, -4.7],
            [31.8, -5.0],
        ]]);
        let got = area(&poly);
        assert!((got - 735_970_283.052).abs() < 1500.0, "got {got}");
    }

    #[test]
    fn hole_is_subtracted() {
        let outer = vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]];
        let hole = vec![
            [0.25, 0.25],
            [0.75, 0.25],
            [0.75, 0.75],
            [0.25, 0.75],
            [0.25, 0.25],
        ];
        let solid = area(&Geometry::Polygon(vec![outer.clone()]));
        let with_hole = area(&Geometry::Polygon(vec![outer, hole]));
        assert!(with_hole < solid);
        // The hole is a quarter of the square's side each way → 1/4 of the area.
        assert!(
            (with_hole - solid * 0.75).abs() < solid * 0.01,
            "got {with_hole}"
        );
    }

    #[test]
    fn non_areal_geometry_is_zero() {
        assert_eq!(area(&Geometry::Point([0.0, 0.0])), 0.0);
        assert_eq!(
            area(&Geometry::LineString(vec![[0.0, 0.0], [1.0, 1.0]])),
            0.0
        );
    }
}
