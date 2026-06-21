//! Spatial predicate functions: `ST_WITHIN`.
//!
//! Containment uses a planar point-in-polygon test in lng/lat space. For the
//! small, non-pole-crossing, non-antimeridian polygons Cosmos's corpus and
//! typical data use, this agrees with Cosmos's spherical predicate — the boolean
//! results are exact (unlike the metric functions, which carry a numeric gap).

use super::{Coord, Geometry, vertices};

/// Whether geometry `a` lies within geometry `b`: every vertex of `a` is
/// contained in `b`. Exact for the point-in-polygon case the corpus exercises;
/// for a line/polygon `a` it is a vertex-containment approximation.
pub(crate) fn within(a: &Geometry, b: &Geometry) -> bool {
    let pts = vertices(a);
    !pts.is_empty() && pts.iter().all(|&p| contains_point(b, p))
}

/// Whether point `p` lies inside areal geometry `b` (polygon interior minus
/// holes). Non-areal `b` contains no points.
pub(super) fn contains_point(b: &Geometry, p: Coord) -> bool {
    match b {
        Geometry::Polygon(rings) => point_in_polygon(rings, p),
        Geometry::MultiPolygon(polys) => polys.iter().any(|rings| point_in_polygon(rings, p)),
        _ => false,
    }
}

/// Point-in-polygon with holes: inside the exterior ring and outside every hole.
fn point_in_polygon(rings: &[Vec<Coord>], p: Coord) -> bool {
    let mut it = rings.iter();
    let Some(exterior) = it.next() else {
        return false;
    };
    point_in_ring(exterior, p) && !it.any(|hole| point_in_ring(hole, p))
}

/// Even-odd ray-casting test for a point in a closed linear ring (planar
/// lng/lat). A valid ring has at least four positions.
fn point_in_ring(ring: &[Coord], p: Coord) -> bool {
    if ring.len() < 3 {
        return false;
    }
    let (x, y) = (p[0], p[1]);
    let mut inside = false;
    let mut j = ring.len() - 1;
    for i in 0..ring.len() {
        let (xi, yi) = (ring[i][0], ring[i][1]);
        let (xj, yj) = (ring[j][0], ring[j][1]);
        if ((yi > y) != (yj > y)) && x < (xj - xi) * (y - yi) / (yj - yi) + xi {
            inside = !inside;
        }
        j = i;
    }
    inside
}

#[cfg(test)]
mod tests {
    use super::*;

    fn square() -> Geometry {
        Geometry::Polygon(vec![vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ]])
    }

    #[test]
    fn point_inside_is_within() {
        assert!(within(&Geometry::Point([5.0, 5.0]), &square()));
    }

    #[test]
    fn point_outside_is_not_within() {
        assert!(!within(&Geometry::Point([15.0, 5.0]), &square()));
    }

    #[test]
    fn hole_excludes_point() {
        let with_hole = Geometry::Polygon(vec![
            vec![
                [0.0, 0.0],
                [10.0, 0.0],
                [10.0, 10.0],
                [0.0, 10.0],
                [0.0, 0.0],
            ],
            vec![[3.0, 3.0], [7.0, 3.0], [7.0, 7.0], [3.0, 7.0], [3.0, 3.0]],
        ]);
        assert!(!within(&Geometry::Point([5.0, 5.0]), &with_hole));
        assert!(within(&Geometry::Point([1.0, 1.0]), &with_hole));
    }

    #[test]
    fn non_areal_container_is_false() {
        let line = Geometry::LineString(vec![[0.0, 0.0], [10.0, 10.0]]);
        assert!(!within(&Geometry::Point([5.0, 5.0]), &line));
    }
}
