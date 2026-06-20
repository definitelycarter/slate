//! `ARRAY_LENGTH(arr)` — the number of elements in an array.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match &args[0] {
        Value::Defined(Bson::Array(a)) => Value::Defined(Bson::Int64(a.len() as i64)),
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::Bson;

    #[test]
    fn counts_elements() {
        let arr = def(Bson::Array(vec![Bson::Int32(1), Bson::String("a".into())]));
        assert_eq!(call("ARRAY_LENGTH", vec![arr]).unwrap(), def(2_i64));
    }

    #[test]
    fn non_array_is_undefined() {
        assert!(call("ARRAY_LENGTH", vec![def("x")]).unwrap().is_undefined());
    }
}
