//! `INTBITRIGHTSHIFT(num, shift)` — arithmetic right-shift `num` by `shift`
//! bits. Both arguments must be integers; a fractional/non-numeric arg, a
//! negative shift, or a shift of 64+ bits yields undefined.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, int_value};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    Ok(match (int_value(&args[0]), int_value(&args[1])) {
        (Some(a), Some(b)) => match u32::try_from(b).ok().and_then(|s| a.checked_shr(s)) {
            Some(r) => Value::Defined(Bson::Int64(r)),
            None => Value::Undefined,
        },
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        assert_eq!(
            call("INTBITRIGHTSHIFT", vec![def(16), def(4)]).unwrap(),
            def(1_i64)
        );
        assert!(
            call("INTBITRIGHTSHIFT", vec![def(16), def(0.4)])
                .unwrap()
                .is_undefined()
        );
    }
}
