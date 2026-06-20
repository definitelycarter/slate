//! `SQUARE(num)` — `num` multiplied by itself.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match f64_arg(&args[0]) {
        Some(f) => Value::Defined(Bson::Double(f * f)),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::Bson;

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/square
        assert_eq!(call("SQUARE", vec![def(0)]).unwrap(), def(0.0_f64));
        assert_eq!(call("SQUARE", vec![def(1)]).unwrap(), def(1.0_f64));
        assert_eq!(call("SQUARE", vec![def(2)]).unwrap(), def(4.0_f64));
        assert_eq!(call("SQUARE", vec![def(3)]).unwrap(), def(9.0_f64));
        // SQUARE(null) is undefined (dropped from the Cosmos result object).
        assert!(
            call("SQUARE", vec![def(Bson::Null)])
                .unwrap()
                .is_undefined()
        );
    }
}
