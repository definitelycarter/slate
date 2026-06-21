//! `INTDIV(num1, num2)` — integer (truncating) division. Both arguments must be
//! integers; division by zero (or a fractional/non-numeric arg) yields undefined.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, int_value};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    Ok(match (int_value(&args[0]), int_value(&args[1])) {
        // checked_div is None on divide-by-zero and on i64::MIN / -1 overflow.
        (Some(a), Some(b)) => match a.checked_div(b) {
            Some(q) => Value::Defined(Bson::Int64(q)),
            None => Value::Undefined,
        },
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        assert_eq!(call("INTDIV", vec![def(10), def(2)]).unwrap(), def(5_i64));
        assert_eq!(call("INTDIV", vec![def(10), def(-2)]).unwrap(), def(-5_i64));
        assert!(
            call("INTDIV", vec![def(10), def(0)])
                .unwrap()
                .is_undefined()
        );
    }
}
