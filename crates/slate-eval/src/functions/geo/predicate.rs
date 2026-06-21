//! Spatial predicate functions: `ST_WITHIN` and `ST_INTERSECTS`.
//!
//! Containment and intersection use planar point-in-polygon and segment tests in
//! lng/lat space. For the small, non-pole-crossing, non-antimeridian polygons
//! Cosmos's corpus and typical data use, this agrees with Cosmos's spherical
//! predicates — the boolean results are exact (unlike the metric functions,
//! which carry a numeric gap).

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

/// Whether two geometries share any point. Combines containment (a vertex of one
/// inside the other's polygon), coincident/boundary-touching vertices, and
/// crossing edges — enough for the point/line/polygon combinations the corpus
/// and typical data exercise, all in planar lng/lat space.
pub(crate) fn intersects(a: &Geometry, b: &Geometry) -> bool {
    let va = vertices(a);
    let vb = vertices(b);
    // Containment: a vertex of one lies in the other's polygon interior.
    if va.iter().any(|&p| contains_point(b, p)) || vb.iter().any(|&p| contains_point(a, p)) {
        return true;
    }
    // Coincident vertex (covers point/point and shared endpoints).
    if va.iter().any(|p| vb.contains(p)) {
        return true;
    }
    let ea = edges(a);
    let eb = edges(b);
    // A vertex lying on the other geometry's boundary.
    if va
        .iter()
        .any(|&p| eb.iter().any(|&(s, e)| point_on_segment(p, s, e)))
        || vb
            .iter()
            .any(|&p| ea.iter().any(|&(s, e)| point_on_segment(p, s, e)))
    {
        return true;
    }
    // Crossing edges (partial overlap with no contained vertex).
    ea.iter()
        .any(|&(p1, p2)| eb.iter().any(|&(q1, q2)| segments_cross(p1, p2, q1, q2)))
}

/// All boundary segments of a geometry (empty for points).
fn edges(g: &Geometry) -> Vec<(Coord, Coord)> {
    let mut out = Vec::new();
    match g {
        Geometry::Point(_) | Geometry::MultiPoint(_) => {}
        Geometry::LineString(line) => push_segments(&mut out, line),
        Geometry::MultiLineString(lines) => lines.iter().for_each(|l| push_segments(&mut out, l)),
        Geometry::Polygon(rings) => rings.iter().for_each(|r| push_segments(&mut out, r)),
        Geometry::MultiPolygon(polys) => {
            polys
                .iter()
                .flatten()
                .for_each(|r| push_segments(&mut out, r));
        }
    }
    out
}

fn push_segments(out: &mut Vec<(Coord, Coord)>, line: &[Coord]) {
    for w in line.windows(2) {
        out.push((w[0], w[1]));
    }
}

/// Twice the signed area of triangle abc; its sign gives the orientation of c
/// relative to the directed line a→b (zero ⇒ collinear).
fn orient(a: Coord, b: Coord, c: Coord) -> f64 {
    (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
}

/// Whether `c`, known to be collinear with a→b, lies within its bounding box.
fn in_bbox(a: Coord, b: Coord, c: Coord) -> bool {
    c[0] <= a[0].max(b[0])
        && c[0] >= a[0].min(b[0])
        && c[1] <= a[1].max(b[1])
        && c[1] >= a[1].min(b[1])
}

/// Whether point `p` lies on segment a→b.
fn point_on_segment(p: Coord, a: Coord, b: Coord) -> bool {
    orient(a, b, p) == 0.0 && in_bbox(a, b, p)
}

/// Whether segments p1→p2 and p3→p4 intersect (the CLRS orientation test,
/// including collinear endpoint-touching).
fn segments_cross(p1: Coord, p2: Coord, p3: Coord, p4: Coord) -> bool {
    let d1 = orient(p3, p4, p1);
    let d2 = orient(p3, p4, p2);
    let d3 = orient(p1, p2, p3);
    let d4 = orient(p1, p2, p4);
    if ((d1 > 0.0 && d2 < 0.0) || (d1 < 0.0 && d2 > 0.0))
        && ((d3 > 0.0 && d4 < 0.0) || (d3 < 0.0 && d4 > 0.0))
    {
        return true;
    }
    (d1 == 0.0 && in_bbox(p3, p4, p1))
        || (d2 == 0.0 && in_bbox(p3, p4, p2))
        || (d3 == 0.0 && in_bbox(p1, p2, p3))
        || (d4 == 0.0 && in_bbox(p1, p2, p4))
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

    fn shifted_square(dx: f64) -> Geometry {
        Geometry::Polygon(vec![vec![
            [dx, 0.0],
            [dx + 10.0, 0.0],
            [dx + 10.0, 10.0],
            [dx, 10.0],
            [dx, 0.0],
        ]])
    }

    #[test]
    fn overlapping_polygons_intersect() {
        // Crossing edges: the second square's left edge cuts through the first.
        assert!(intersects(&square(), &shifted_square(5.0)));
    }

    #[test]
    fn disjoint_polygons_do_not_intersect() {
        assert!(!intersects(&square(), &shifted_square(50.0)));
    }

    #[test]
    fn contained_polygon_intersects() {
        let inner = Geometry::Polygon(vec![vec![
            [2.0, 2.0],
            [3.0, 2.0],
            [3.0, 3.0],
            [2.0, 3.0],
            [2.0, 2.0],
        ]]);
        assert!(intersects(&square(), &inner));
        assert!(intersects(&inner, &square()));
    }

    #[test]
    fn point_on_line_intersects() {
        let line = Geometry::LineString(vec![[0.0, 0.0], [10.0, 0.0]]);
        assert!(intersects(&Geometry::Point([5.0, 0.0]), &line));
        assert!(!intersects(&Geometry::Point([5.0, 1.0]), &line));
    }
}
