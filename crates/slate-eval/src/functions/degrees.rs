//! `DEGREES(num)` — convert `num` radians to degrees.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match f64_arg(&args[0]) {
        Some(f) => Value::Defined(Bson::Double(f.to_degrees())),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{approx, call, def};

    #[test]
    fn cosmos_examples() {
        // pi radians = 180 degrees
        approx(
            call("DEGREES", vec![def(std::f64::consts::PI)]).unwrap(),
            180.0,
        );
    }
}
