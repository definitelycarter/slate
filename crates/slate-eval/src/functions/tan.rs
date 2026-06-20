//! `TAN(num)` — trigonometric tangent of `num` (in radians).

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match f64_arg(&args[0]) {
        Some(f) => Value::Defined(Bson::Double(f.tan())),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{approx, call, def};

    #[test]
    fn cosmos_examples() {
        approx(call("TAN", vec![def(0)]).unwrap(), 0.0);
    }
}
