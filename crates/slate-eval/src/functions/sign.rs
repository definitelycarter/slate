//! `SIGN(num)` — the sign of `num`: `-1`, `0`, or `+1`.
//!
//! Note this is *not* `f64::signum`, which reports `+1` for `+0.0`; Cosmos
//! returns `0` for zero.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match f64_arg(&args[0]) {
        Some(f) => {
            let s = if f > 0.0 {
                1.0
            } else if f < 0.0 {
                -1.0
            } else {
                0.0
            };
            Value::Defined(Bson::Double(s))
        }
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/sign
        assert_eq!(call("SIGN", vec![def(-2)]).unwrap(), def(-1.0_f64));
        assert_eq!(call("SIGN", vec![def(-1)]).unwrap(), def(-1.0_f64));
        assert_eq!(call("SIGN", vec![def(0)]).unwrap(), def(0.0_f64));
        assert_eq!(call("SIGN", vec![def(1)]).unwrap(), def(1.0_f64));
        assert_eq!(call("SIGN", vec![def(2)]).unwrap(), def(1.0_f64));
    }
}
