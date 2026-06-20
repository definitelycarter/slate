//! `IS_NUMBER(expr)` — whether the value is a number. (Type-test → boolean.)

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(Value::Defined(Bson::Boolean(matches!(
        &args[0],
        Value::Defined(Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_))
    ))))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::{Bson, doc};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/is-number
        let cases: [(Value, bool); 8] = [
            (def(true), false),
            (def(1), true),
            (def("value"), false),
            (def(Bson::Null), false),
            (def(Bson::Document(doc! { "name": "Tecozow coat" })), false),
            (def("Tecozow coat"), false), // {name:...}.name
            (def(0), true),               // {quantity:0}.quantity
            (Value::Undefined, false),    // {}.category
        ];
        for (input, expected) in cases {
            assert_eq!(call("IS_NUMBER", vec![input]).unwrap(), def(expected));
        }
    }
}
