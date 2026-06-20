//! `ROUND(num)` — `num` rounded to the closest integer.
//!
//! Uses midpoint rounding **away from zero** (Cosmos): a value exactly between
//! two integers rounds to the one further from 0 (`2.5` → `3`, `-2.5` → `-3`).
//! Rust's `f64::round` has exactly this tie-breaking rule.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match f64_arg(&args[0]) {
        Some(f) => Value::Defined(Bson::Double(f.round())),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/round
        assert_eq!(call("ROUND", vec![def(2.4_f64)]).unwrap(), def(2.0_f64));
        assert_eq!(call("ROUND", vec![def(2.6_f64)]).unwrap(), def(3.0_f64));
        assert_eq!(call("ROUND", vec![def(2.5_f64)]).unwrap(), def(3.0_f64));
        assert_eq!(call("ROUND", vec![def(-2.4_f64)]).unwrap(), def(-2.0_f64));
        assert_eq!(call("ROUND", vec![def(-2.6_f64)]).unwrap(), def(-3.0_f64));
    }
}
