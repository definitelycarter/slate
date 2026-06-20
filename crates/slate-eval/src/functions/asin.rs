//! `ASIN(num)` — arcsine of `num` (result in radians).

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match f64_arg(&args[0]) {
        Some(f) => Value::Defined(Bson::Double(f.asin())),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{approx, call, def};

    #[test]
    fn cosmos_examples() {
        // asin(1) = pi/2
        approx(
            call("ASIN", vec![def(1)]).unwrap(),
            std::f64::consts::FRAC_PI_2,
        );
    }
}
