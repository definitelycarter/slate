//! `RADIANS(num)` — convert `num` degrees to radians.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match f64_arg(&args[0]) {
        Some(f) => Value::Defined(Bson::Double(f.to_radians())),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{approx, call, def};

    #[test]
    fn cosmos_examples() {
        // 180 degrees = pi radians
        approx(
            call("RADIANS", vec![def(180)]).unwrap(),
            std::f64::consts::PI,
        );
    }
}
