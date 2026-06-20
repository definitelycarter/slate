//! `IS_BOOL(expr)` — whether the value is a boolean. (Type-test → boolean.)

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(Value::Defined(Bson::Boolean(matches!(
        &args[0],
        Value::Defined(Bson::Boolean(_))
    ))))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::Bson;

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/is-bool
        let cases: [(Value, bool); 9] = [
            (def(true), true),
            (def(65), false),
            (def("AdventureWorks"), false),
            (def(Bson::Null), false),
            (def(Bson::Document(bson::doc! { "size": "small" })), false),
            (def(Bson::Array(vec![25344.into(), 82947.into()])), false),
            (def(Bson::Array(vec![25344.into(), 82947.into()])), false), // {skus:[...]}.skus
            (Value::Undefined, false),                                   // .size (missing)
            (Value::Undefined, false),                                   // .vendor (missing)
        ];
        for (input, expected) in cases {
            assert_eq!(call("IS_BOOL", vec![input]).unwrap(), def(expected));
        }
    }
}
