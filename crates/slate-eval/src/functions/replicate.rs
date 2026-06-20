//! `REPLICATE(str, n)` — a string repeated `n` times.
//!
//! Matching Cosmos: the result is capped at 10,000 characters, and a negative
//! or non-finite count yields `Undefined`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg, str_arg};

const MAX_LEN: usize = 10_000;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    let (Some(s), Some(n)) = (str_arg(&args[0]), f64_arg(&args[1])) else {
        return Ok(Value::Undefined);
    };
    if !n.is_finite() || n < 0.0 {
        return Ok(Value::Undefined);
    }
    let count = n as usize;
    if s.chars().count().saturating_mul(count) > MAX_LEN {
        return Ok(Value::Undefined);
    }
    Ok(Value::Defined(Bson::String(s.repeat(count))))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_example() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/replicate
        assert_eq!(
            call("REPLICATE", vec![def("Cosmic"), def(3)]).unwrap(),
            def("CosmicCosmicCosmic")
        );
    }

    #[test]
    fn negative_or_too_long_is_undefined() {
        assert!(
            call("REPLICATE", vec![def("x"), def(-1)])
                .unwrap()
                .is_undefined()
        );
        assert!(
            call("REPLICATE", vec![def("xx"), def(9999)])
                .unwrap()
                .is_undefined()
        );
    }
}
