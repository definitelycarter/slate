//! `ATN2(n1, n2)` — angle (in radians) between the positive x-axis and the point
//! `(n2, n1)`; the two-argument arctangent of `n1 / n2`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    Ok(match (f64_arg(&args[0]), f64_arg(&args[1])) {
        (Some(y), Some(x)) => Value::Defined(Bson::Double(y.atan2(x))),
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{approx, call, def};

    #[test]
    fn cosmos_examples() {
        // atan2(1, 1) = pi/4
        approx(
            call("ATN2", vec![def(1), def(1)]).unwrap(),
            std::f64::consts::FRAC_PI_4,
        );
    }
}
