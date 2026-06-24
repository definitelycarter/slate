//! `IS_NUMBER(expr)` — whether the value is a number. (Type-test → boolean.)

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    // `Decimal128` joins the number tower like every other arm of slate's
    // single-f64 number model (see `eval::decimal_to_f64`): comparisons,
    // arithmetic, and `SUM`/`AVG` already treat a stored decimal as numeric, so
    // the type test must agree. Like `Double`, a non-finite decimal is still a
    // number — `IS_NUMBER` is a type test, not a finiteness test.
    Ok(Value::Defined(Bson::Boolean(matches!(
        &args[0],
        Value::Defined(Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_))
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

    #[test]
    fn decimal_is_a_number() {
        // A `Decimal128` shares slate's number tower (SUM/AVG/comparison treat it
        // as numeric), so the type test agrees — and, like `Double`, even a
        // non-finite decimal is still a number.
        let dec = |s: &str| Value::Defined(Bson::Decimal128(s.parse().unwrap()));
        assert_eq!(call("IS_NUMBER", vec![dec("25.50")]).unwrap(), def(true));
        assert_eq!(call("IS_NUMBER", vec![dec("0")]).unwrap(), def(true));
        assert_eq!(call("IS_NUMBER", vec![dec("Infinity")]).unwrap(), def(true));
        assert_eq!(call("IS_NUMBER", vec![dec("NaN")]).unwrap(), def(true));
    }
}
