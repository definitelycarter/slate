//! `FLOOR(num)` — largest integer value less than or equal to `num`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match f64_arg(&args[0]) {
        Some(f) => Value::Defined(Bson::Double(f.floor())),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::Bson;

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/floor
        assert_eq!(call("FLOOR", vec![def(62.6_f64)]).unwrap(), def(62.0_f64));
        assert_eq!(
            call("FLOOR", vec![def(-145.12_f64)]).unwrap(),
            def(-146.0_f64)
        );
        assert_eq!(call("FLOOR", vec![def(0.2989_f64)]).unwrap(), def(0.0_f64));
        assert_eq!(call("FLOOR", vec![def(0.0_f64)]).unwrap(), def(0.0_f64));
        // FLOOR(null) is undefined (dropped from the Cosmos result object).
        assert!(call("FLOOR", vec![def(Bson::Null)]).unwrap().is_undefined());
    }
}
