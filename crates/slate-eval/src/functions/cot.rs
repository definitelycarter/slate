//! `COT(num)` — trigonometric cotangent of `num` (in radians): `1 / tan(num)`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match f64_arg(&args[0]) {
        Some(f) => Value::Defined(Bson::Double(1.0 / f.tan())),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{approx, call, def};

    #[test]
    fn cosmos_examples() {
        // cot(pi/4) = 1
        approx(
            call("COT", vec![def(std::f64::consts::FRAC_PI_4)]).unwrap(),
            1.0,
        );
    }
}
