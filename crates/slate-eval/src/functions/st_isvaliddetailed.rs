//! `ST_ISVALIDDETAILED(geometry)` — GeoJSON validity with a reason.
//!
//! Returns a document: `{ "valid": true }` when the geometry is valid, or
//! `{ "valid": false, "reason": "..." }` otherwise, matching Cosmos. An
//! undefined argument propagates to `Undefined`.

use bson::{Bson, Document};

use crate::error::Result;
use crate::value::Value;

use super::{arity, geo};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match args.into_iter().next() {
        Some(Value::Defined(b)) => {
            let v = geo::validity(&b);
            let mut doc = Document::new();
            doc.insert("valid", Bson::Boolean(v.valid));
            if let Some(reason) = v.reason {
                doc.insert("reason", Bson::String(reason.to_string()));
            }
            Value::Defined(Bson::Document(doc))
        }
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::{Bson, doc};

    #[test]
    fn cosmos_example() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/st-isvaliddetailed
        let valid = Bson::Document(doc! {
            "type": "Point",
            "coordinates": [-84.38876194345323, 33.75682784306348],
        });
        let invalid = Bson::Document(doc! {
            "type": "Point",
            "coordinates": [133.7568278430635, -184.38876194345323],
        });
        assert_eq!(
            call("ST_ISVALIDDETAILED", vec![def(valid)]).unwrap(),
            def(Bson::Document(doc! { "valid": true })),
        );
        assert_eq!(
            call("ST_ISVALIDDETAILED", vec![def(invalid)]).unwrap(),
            def(Bson::Document(doc! {
                "valid": false,
                "reason": "Latitude values must be between -90 and 90 degrees.",
            })),
        );
    }

    #[test]
    fn wrong_arity_is_error() {
        assert!(call("ST_ISVALIDDETAILED", vec![]).is_err());
    }
}
