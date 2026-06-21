//! `INTBITNOT(num)` — bitwise complement of an integer. A fractional or
//! non-numeric argument yields undefined.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, int_value};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match int_value(&args[0]) {
        Some(a) => Value::Defined(Bson::Int64(!a)),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        assert_eq!(call("INTBITNOT", vec![def(65)]).unwrap(), def(-66_i64));
        assert_eq!(call("INTBITNOT", vec![def(0)]).unwrap(), def(-1_i64));
        assert!(call("INTBITNOT", vec![def(0.1)]).unwrap().is_undefined());
    }
}
