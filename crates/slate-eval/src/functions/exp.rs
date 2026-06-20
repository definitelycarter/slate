//! `EXP(num)` — `e` raised to the power `num`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match f64_arg(&args[0]) {
        Some(f) => Value::Defined(Bson::Double(f.exp())),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{approx, call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/exp
        approx(call("EXP", vec![def(0)]).unwrap(), 1.0);
        approx(call("EXP", vec![def(10)]).unwrap(), 22026.465794806718);
        approx(call("EXP", vec![def(20)]).unwrap(), 485165195.4097903);
    }
}
