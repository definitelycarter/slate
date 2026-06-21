//! `ST_AREA(geometry)` — area in square meters of a GeoJSON geometry on the
//! WGS84 ellipsoid.
//!
//! Polygons and MultiPolygons have area (holes subtracted); other geometries are
//! zero. An undefined or invalid-geometry argument yields `Undefined`, matching
//! Cosmos.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, geo};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    let Some(g) = args[0].as_bson().and_then(geo::parse) else {
        return Ok(Value::Undefined);
    };
    Ok(Value::Defined(Bson::Double(geo::area(&g))))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::{Bson, doc};

    #[test]
    fn cosmos_example() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/st-area
        // Cosmos oracle is 735970283.0522614 m²; the authalic-sphere area is
        // within ~1 ppm.
        let poly = Bson::Document(doc! {
            "type": "Polygon",
            "coordinates": [[
                [31.8, -5.0],
                [32.0, -5.0],
                [32.0, -4.7],
                [31.8, -4.7],
                [31.8, -5.0],
            ]],
        });
        match call("ST_AREA", vec![def(poly)]).unwrap() {
            Value::Defined(Bson::Double(a)) => {
                assert!((a - 735_970_283.052).abs() < 1500.0, "got {a}")
            }
            other => panic!("expected a Double, got {other:?}"),
        }
    }

    #[test]
    fn non_geometry_is_undefined() {
        assert!(call("ST_AREA", vec![def(7)]).unwrap().is_undefined());
    }

    #[test]
    fn wrong_arity_is_error() {
        assert!(call("ST_AREA", vec![]).is_err());
    }
}
