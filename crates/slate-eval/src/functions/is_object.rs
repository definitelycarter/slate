//! `IS_OBJECT(expr)` — whether the value is a JSON object (document).
//! (Type-test → boolean.)

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(Value::Defined(Bson::Boolean(matches!(
        &args[0],
        Value::Defined(Bson::Document(_))
    ))))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::{Bson, doc};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/is-object
        let cases: [(Value, bool); 9] = [
            (def(true), false),
            (def(1), false),
            (def("value"), false),
            (
                def(Bson::Array(vec![
                    "green".into(),
                    "red".into(),
                    "yellow".into(),
                ])),
                false,
            ),
            (def(Bson::Null), false),
            (def(Bson::Document(doc! { "name": "Tecozow coat" })), true),
            (def("Tecozow coat"), false), // {name:...}.name
            (def(Bson::Document(doc! { "count": 0 })), true), // {quantity:{count:0}}.quantity
            (Value::Undefined, false),    // {}.category
        ];
        for (input, expected) in cases {
            assert_eq!(call("IS_OBJECT", vec![input]).unwrap(), def(expected));
        }
    }
}
