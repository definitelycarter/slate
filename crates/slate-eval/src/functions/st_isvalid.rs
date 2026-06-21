//! `ST_ISVALID(geometry)` — whether a value is a valid GeoJSON geometry.
//!
//! Returns a `Boolean`. An undefined argument propagates to `Undefined`; any
//! other value (including a non-geometry) is checked for GeoJSON validity.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, geo};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match args.into_iter().next() {
        Some(Value::Defined(b)) => Value::Defined(Bson::Boolean(geo::validity(&b).valid)),
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::{Bson, doc};

    #[test]
    fn cosmos_example() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/st-isvalid
        let valid = Bson::Document(doc! {
            "type": "Point",
            "coordinates": [-84.38876194345323, 33.75682784306348],
        });
        let invalid = Bson::Document(doc! {
            "type": "Point",
            "coordinates": [133.7568278430635, -184.38876194345323],
        });
        assert_eq!(call("ST_ISVALID", vec![def(valid)]).unwrap(), def(true));
        assert_eq!(call("ST_ISVALID", vec![def(invalid)]).unwrap(), def(false));
    }

    #[test]
    fn non_geometry_is_false() {
        assert_eq!(call("ST_ISVALID", vec![def("hello")]).unwrap(), def(false));
    }

    #[test]
    fn wrong_arity_is_error() {
        assert!(call("ST_ISVALID", vec![]).is_err());
    }
}
