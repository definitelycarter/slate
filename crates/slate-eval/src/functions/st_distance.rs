//! `ST_DISTANCE(geometry1, geometry2)` — distance in meters between two
//! GeoJSON geometries on the WGS84 ellipsoid.
//!
//! Either argument being undefined or not a valid geometry yields `Undefined`,
//! matching Cosmos.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, geo};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    let (Some(g1), Some(g2)) = (
        args[0].as_bson().and_then(geo::parse),
        args[1].as_bson().and_then(geo::parse),
    ) else {
        return Ok(Value::Undefined);
    };
    Ok(Value::Defined(Bson::Double(geo::distance(&g1, &g2))))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::{Bson, doc};

    fn point(lng: f64, lat: f64) -> Bson {
        Bson::Document(doc! { "type": "Point", "coordinates": [lng, lat] })
    }

    #[test]
    fn cosmos_example() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/st-distance
        // Cosmos oracle is 3345.269817 m; the Vincenty geodesic agrees to ~mm.
        let hq = point(-122.12826822304672, 47.63980239335718);
        let target = point(-122.11758113953535, 47.66901087006131);
        let got = call("ST_DISTANCE", vec![def(hq), def(target)]).unwrap();
        match got {
            Value::Defined(Bson::Double(d)) => assert!((d - 3345.269817).abs() < 0.5, "got {d}"),
            other => panic!("expected a Double, got {other:?}"),
        }
    }

    #[test]
    fn non_geometry_is_undefined() {
        assert!(
            call("ST_DISTANCE", vec![def("x"), def(point(0.0, 0.0))])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn wrong_arity_is_error() {
        assert!(call("ST_DISTANCE", vec![def(point(0.0, 0.0))]).is_err());
    }
}
