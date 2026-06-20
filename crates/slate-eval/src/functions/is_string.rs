//! `IS_STRING(expr)` — whether the value is a string. (Type-test → boolean.)

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(Value::Defined(Bson::Boolean(matches!(
        &args[0],
        Value::Defined(Bson::String(_))
    ))))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::{Bson, doc};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/is-string
        let cases: [(Value, bool); 9] = [
            (def(true), false),
            (def(1), false),
            (def("value"), true),
            (
                def(Bson::Array(vec![
                    "green".into(),
                    "red".into(),
                    "yellow".into(),
                ])),
                false,
            ),
            (def(Bson::Null), false),
            (def(Bson::Document(doc! { "name": "Tecozow coat" })), false),
            (def("Tecozow coat"), true), // {name:"Tecozow coat"}.name
            (def(false), false),         // {onSale:false}.onSale
            (Value::Undefined, false),   // {}.category
        ];
        for (input, expected) in cases {
            assert_eq!(call("IS_STRING", vec![input]).unwrap(), def(expected));
        }
    }
}
