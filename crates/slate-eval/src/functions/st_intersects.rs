//! `ST_INTERSECTS(geometry1, geometry2)` — whether two GeoJSON geometries
//! share any point.
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
    Ok(Value::Defined(Bson::Boolean(geo::intersects(&a, &b))))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::{Bson, doc};

    #[test]
    fn cosmos_example() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/st-intersects
        let highway = Bson::Document(doc! {
            "type": "Polygon",
            "coordinates": [[
                [-122.13693695285855, 47.64996065621003],
                [-122.1351662656516, 47.64627863318731],
                [-122.13488295569863, 47.646326350048696],
                [-122.1366182291613, 47.650016321952904],
                [-122.13693695285855, 47.64996065621003],
            ]],
        });
        let campus = Bson::Document(doc! {
            "type": "Polygon",
            "coordinates": [[
                [-122.14034847687708, 47.6494835188378],
                [-122.14014779899375, 47.64625477474044],
                [-122.13256925774829, 47.646207057813655],
                [-122.13254564858545, 47.64941990019193],
                [-122.14034847687708, 47.6494835188378],
            ]],
        });
        assert_eq!(
            call("ST_INTERSECTS", vec![def(highway), def(campus)]).unwrap(),
            def(true)
        );
    }

    #[test]
    fn disjoint_polygons_are_false() {
        let a = Bson::Document(doc! {
            "type": "Polygon",
            "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]],
        });
        let b = Bson::Document(doc! {
            "type": "Polygon",
            "coordinates": [[
                [50.0, 50.0],
                [51.0, 50.0],
                [51.0, 51.0],
                [50.0, 51.0],
                [50.0, 50.0],
            ]],
        });
        assert_eq!(
            call("ST_INTERSECTS", vec![def(a), def(b)]).unwrap(),
            def(false)
        );
    }

    #[test]
    fn non_geometry_is_undefined() {
        assert!(
            call("ST_INTERSECTS", vec![def(1), def(2)])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn wrong_arity_is_error() {
        assert!(call("ST_INTERSECTS", vec![]).is_err());
    }
}
