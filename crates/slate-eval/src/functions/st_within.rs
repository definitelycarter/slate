//! `ST_WITHIN(geometry1, geometry2)` — whether geometry1 is within geometry2.
//!
//! Either argument being undefined or not a valid geometry yields `Undefined`,
//! matching Cosmos.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, geo};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    let (Some(a), Some(b)) = (
        args[0].as_bson().and_then(geo::parse),
        args[1].as_bson().and_then(geo::parse),
    ) else {
        return Ok(Value::Undefined);
    };
    Ok(Value::Defined(Bson::Boolean(geo::within(&a, &b))))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::{Bson, doc};

    #[test]
    fn cosmos_example() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/st-within
        let point = Bson::Document(doc! {
            "type": "Point",
            "coordinates": [-122.12824857332558, 47.6395516675712],
        });
        let campus = Bson::Document(doc! {
            "type": "Polygon",
            "coordinates": [[
                [-122.13236581015025, 47.64606476313813],
                [-122.13221982500913, 47.633757091363975],
                [-122.11840598103835, 47.641749416109235],
                [-122.12061400629656, 47.64589264786028],
                [-122.13236581015025, 47.64606476313813],
            ]],
        });
        assert_eq!(
            call("ST_WITHIN", vec![def(point), def(campus)]).unwrap(),
            def(true)
        );
    }

    #[test]
    fn point_outside_polygon_is_false() {
        let point = Bson::Document(doc! { "type": "Point", "coordinates": [50.0, 50.0] });
        let poly = Bson::Document(doc! {
            "type": "Polygon",
            "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]],
        });
        assert_eq!(
            call("ST_WITHIN", vec![def(point), def(poly)]).unwrap(),
            def(false)
        );
    }

    #[test]
    fn non_geometry_is_undefined() {
        assert!(
            call("ST_WITHIN", vec![def("x"), def("y")])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn wrong_arity_is_error() {
        assert!(call("ST_WITHIN", vec![]).is_err());
    }
}
