//! `LOG10(num)` — the base-10 logarithm of `num`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match f64_arg(&args[0]) {
        Some(f) => Value::Defined(Bson::Double(f.log10())),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{approx, call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/log10
        approx(call("LOG10", vec![def(5)]).unwrap(), 0.6989700043360189);
        approx(call("LOG10", vec![def(2)]).unwrap(), 0.3010299956639812);
        approx(call("LOG10", vec![def(100)]).unwrap(), 2.0);
    }
}
