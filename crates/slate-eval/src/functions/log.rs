//! `LOG(num [, base])` — the logarithm of `num`.
//!
//! With one argument it is the natural logarithm; an optional second argument
//! sets the base.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity_err, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(arity_err(name, "1 or 2"));
    }
    let Some(x) = f64_arg(&args[0]) else {
        return Ok(Value::Undefined);
    };
    let result = match args.get(1) {
        Some(base_arg) => match f64_arg(base_arg) {
            Some(base) => x.log(base),
            None => return Ok(Value::Undefined),
        },
        None => x.ln(),
    };
    Ok(Value::Defined(Bson::Double(result)))
}

#[cfg(test)]
mod tests {
    use super::super::{approx, call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/log
        approx(call("LOG", vec![def(5)]).unwrap(), 1.6094379124341003);
        approx(
            call("LOG", vec![def(2), def(10)]).unwrap(),
            0.3010299956639812,
        );
    }

    #[test]
    fn wrong_arity_is_error() {
        assert!(call("LOG", vec![]).is_err());
        assert!(call("LOG", vec![def(1), def(2), def(3)]).is_err());
    }
}
