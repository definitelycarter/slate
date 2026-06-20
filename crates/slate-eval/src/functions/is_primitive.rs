//! `IS_PRIMITIVE(expr)` — whether the value is a primitive: string, boolean,
//! number, or null. Arrays, objects, and undefined are not primitives.
//! (Type-test → boolean.)

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(Value::Defined(Bson::Boolean(matches!(
        &args[0],
        Value::Defined(
            Bson::String(_)
                | Bson::Boolean(_)
                | Bson::Int32(_)
                | Bson::Int64(_)
                | Bson::Double(_)
                | Bson::Null
        )
    ))))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::{Bson, doc};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/is-primitive
        let cases: [(Value, bool); 9] = [
            (def(true), true),
            (def(1), true),
            (def("value"), true),
            (
                def(Bson::Array(vec![
                    "green".into(),
                    "red".into(),
                    "yellow".into(),
                ])),
                false,
            ),
            (def(Bson::Null), true),
            (def(Bson::Document(doc! { "name": "Tecozow coat" })), false),
            (def("Tecozow coat"), true), // {name:...}.name
            (def(false), true),          // {onSale:false}.onSale
            (Value::Undefined, false),   // {}.category
        ];
        for (input, expected) in cases {
            assert_eq!(call("IS_PRIMITIVE", vec![input]).unwrap(), def(expected));
        }
    }
}
