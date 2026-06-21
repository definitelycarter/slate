//! Internal GeoJSON geometry model shared by the spatial (`ST_*`) functions.
//!
//! Cosmos spatial values are ordinary GeoJSON documents — `{"type": "Point",
//! "coordinates": [lng, lat]}`, `Polygon`, `LineString`, and their `Multi*`
//! variants — so they reach eval as a [`Bson::Document`] with a nested
//! `coordinates` array, not a dedicated BSON type. This module reads such a
//! document into [`Geometry`], applies the GeoJSON validity rules Cosmos
//! enforces (coordinate ranges, linear-ring closure, minimum ring size), and is
//! the shared base the metric ([`distance`], [`area`]) and predicate
//! ([`within`], [`intersects`]) functions build on.
//!
//! **Geodesy note.** The Cosmos oracle computes metric results (`ST_DISTANCE`,
//! `ST_AREA`) on the **WGS84 ellipsoid**, not a sphere — a mean-radius sphere is
//! ~0.3% off (5.5 km at 1907 km). So [`distance`] uses the ellipsoidal geodesic
//! (Vincenty) and [`area`] the authalic-sphere area; both are pure-Rust and
//! wasm-safe. They land within centimetres / ~1 ppm of Cosmos but do not
//! bit-reproduce its proprietary spatial library to the corpus's 10-decimal
//! comparison (tracked as a known numeric gap). The boolean predicates are exact.

use bson::Bson;

mod metric;

pub(crate) use metric::{area, distance};

/// A `[longitude, latitude]` position in degrees.
pub(crate) type Coord = [f64; 2];

/// A parsed, valid GeoJSON geometry. Polygons hold their linear rings with the
/// exterior ring first and any holes following (GeoJSON convention).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Geometry {
    Point(Coord),
    LineString(Vec<Coord>),
    Polygon(Vec<Vec<Coord>>),
    MultiPoint(Vec<Coord>),
    MultiLineString(Vec<Vec<Coord>>),
    MultiPolygon(Vec<Vec<Vec<Coord>>>),
}

// GeoJSON validity reason strings. The coordinate-range messages match Cosmos's
// `ST_ISVALIDDETAILED` output verbatim; the structural messages are best-effort
// (the corpus only exercises the range case).
const REASON_LON: &str = "Longitude values must be between -180 and 180 degrees.";
const REASON_LAT: &str = "Latitude values must be between -90 and 90 degrees.";
const REASON_INVALID: &str = "Invalid GeoJSON geometry.";
const REASON_RING_CLOSED: &str = "A Polygon ring must be closed (first and last positions equal).";
const REASON_RING_SIZE: &str = "A Polygon ring must have at least four positions.";
const REASON_LINE_SIZE: &str = "A LineString must have at least two positions.";

/// The outcome of validating a candidate GeoJSON value: valid, or invalid with a
/// Cosmos-style reason. Drives both `ST_ISVALID` (the flag) and
/// `ST_ISVALIDDETAILED` (the flag plus the reason).
pub(crate) struct Validity {
    pub valid: bool,
    pub reason: Option<&'static str>,
}

/// Validate a candidate GeoJSON value the way Cosmos's `ST_ISVALID*` do.
pub(crate) fn validity(b: &Bson) -> Validity {
    match read(b) {
        Ok(_) => Validity {
            valid: true,
            reason: None,
        },
        Err(reason) => Validity {
            valid: false,
            reason: Some(reason),
        },
    }
}

/// Parse a value into a usable [`Geometry`], or `None` if it is not a valid
/// GeoJSON geometry. The metric/predicate functions use this: invalid or
/// non-geometry input yields `Value::Undefined`, matching Cosmos.
pub(crate) fn parse(b: &Bson) -> Option<Geometry> {
    read(b).ok()
}

/// Coerce a BSON numeric (any of the three numeric types — coordinates may be
/// authored as integers, e.g. `[32, -5]`) to `f64`.
fn as_f64(b: &Bson) -> Option<f64> {
    match b {
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        _ => None,
    }
}

/// Read and fully validate a GeoJSON geometry document. `Err` carries the
/// validity reason so [`validity`] can surface it.
fn read(b: &Bson) -> Result<Geometry, &'static str> {
    let doc = b.as_document().ok_or(REASON_INVALID)?;
    let ty = doc
        .get("type")
        .and_then(Bson::as_str)
        .ok_or(REASON_INVALID)?;
    let coords = doc.get("coordinates").ok_or(REASON_INVALID)?;
    match ty {
        "Point" => Ok(Geometry::Point(read_position(coords)?)),
        "MultiPoint" => Ok(Geometry::MultiPoint(read_positions(coords)?)),
        "LineString" => Ok(Geometry::LineString(read_line(coords)?)),
        "MultiLineString" => {
            let arr = coords.as_array().ok_or(REASON_INVALID)?;
            let lines = arr.iter().map(read_line).collect::<Result<Vec<_>, _>>()?;
            Ok(Geometry::MultiLineString(lines))
        }
        "Polygon" => Ok(Geometry::Polygon(read_polygon(coords)?)),
        "MultiPolygon" => {
            let arr = coords.as_array().ok_or(REASON_INVALID)?;
            let polys = arr
                .iter()
                .map(read_polygon)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Geometry::MultiPolygon(polys))
        }
        _ => Err(REASON_INVALID),
    }
}

/// A single position: `[lng, lat]` with both in range. Extra ordinates (a GeoJSON
/// altitude) are ignored — Cosmos uses only longitude and latitude.
fn read_position(b: &Bson) -> Result<Coord, &'static str> {
    let arr = b.as_array().ok_or(REASON_INVALID)?;
    let lng = arr.first().and_then(as_f64).ok_or(REASON_INVALID)?;
    let lat = arr.get(1).and_then(as_f64).ok_or(REASON_INVALID)?;
    if !(-180.0..=180.0).contains(&lng) {
        return Err(REASON_LON);
    }
    if !(-90.0..=90.0).contains(&lat) {
        return Err(REASON_LAT);
    }
    Ok([lng, lat])
}

/// An array of positions (used by `MultiPoint`).
fn read_positions(b: &Bson) -> Result<Vec<Coord>, &'static str> {
    let arr = b.as_array().ok_or(REASON_INVALID)?;
    arr.iter().map(read_position).collect()
}

/// A `LineString`'s positions: at least two.
fn read_line(b: &Bson) -> Result<Vec<Coord>, &'static str> {
    let line = read_positions(b)?;
    if line.len() < 2 {
        return Err(REASON_LINE_SIZE);
    }
    Ok(line)
}

/// A polygon's linear rings: each ring has ≥4 positions and is closed.
fn read_polygon(b: &Bson) -> Result<Vec<Vec<Coord>>, &'static str> {
    let arr = b.as_array().ok_or(REASON_INVALID)?;
    let rings = arr.iter().map(read_ring).collect::<Result<Vec<_>, _>>()?;
    if rings.is_empty() {
        return Err(REASON_INVALID);
    }
    Ok(rings)
}

/// A single linear ring: ≥4 positions, first == last.
fn read_ring(b: &Bson) -> Result<Vec<Coord>, &'static str> {
    let ring = read_positions(b)?;
    if ring.len() < 4 {
        return Err(REASON_RING_SIZE);
    }
    if ring.first() != ring.last() {
        return Err(REASON_RING_CLOSED);
    }
    Ok(ring)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn point(lng: f64, lat: f64) -> Bson {
        Bson::Document(doc! { "type": "Point", "coordinates": [lng, lat] })
    }

    #[test]
    fn valid_point_parses() {
        let g = read(&point(-84.388, 33.756)).unwrap();
        assert_eq!(g, Geometry::Point([-84.388, 33.756]));
    }

    #[test]
    fn latitude_out_of_range_is_invalid() {
        let v = validity(&point(133.756, -184.388));
        assert!(!v.valid);
        assert_eq!(v.reason, Some(REASON_LAT));
        assert!(read(&point(133.756, -184.388)).is_err());
    }

    #[test]
    fn longitude_out_of_range_is_invalid() {
        let v = validity(&point(-200.0, 10.0));
        assert!(!v.valid);
        assert_eq!(v.reason, Some(REASON_LON));
    }

    #[test]
    fn integer_coordinates_parse() {
        let g = read(&Bson::Document(
            doc! { "type": "Point", "coordinates": [32_i32, -5_i32] },
        ));
        assert_eq!(g.ok(), Some(Geometry::Point([32.0, -5.0])));
    }

    #[test]
    fn polygon_ring_must_be_closed() {
        let open = Bson::Document(doc! {
            "type": "Polygon",
            "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0]]],
        });
        assert_eq!(validity(&open).reason, Some(REASON_RING_CLOSED));
    }

    #[test]
    fn polygon_ring_needs_four_positions() {
        let tiny = Bson::Document(doc! {
            "type": "Polygon",
            "coordinates": [[[0.0, 0.0], [1.0, 0.0], [0.0, 0.0]]],
        });
        assert_eq!(validity(&tiny).reason, Some(REASON_RING_SIZE));
    }

    #[test]
    fn closed_polygon_parses() {
        let p = Bson::Document(doc! {
            "type": "Polygon",
            "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]],
        });
        assert!(read(&p).is_ok());
        assert!(validity(&p).valid);
    }

    #[test]
    fn non_geometry_is_invalid() {
        assert!(!validity(&Bson::String("nope".into())).valid);
        assert!(read(&Bson::Int32(5)).is_err());
    }

    #[test]
    fn unknown_type_is_invalid() {
        let g = Bson::Document(doc! { "type": "Sphere", "coordinates": [0.0, 0.0] });
        assert!(!validity(&g).valid);
    }
}
