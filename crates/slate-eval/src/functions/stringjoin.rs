//! `STRINGJOIN(array, separator)` — concatenate the elements of an array into a
//! string, placing the separator between each element. The first argument must be
//! an array whose elements are *all* strings, and the second a string; otherwise
//! the result is undefined. An empty array yields the empty string.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    let Some(sep) = str_arg(&args[1]) else {
        return Ok(Value::Undefined);
    };
    let Value::Defined(Bson::Array(arr)) = &args[0] else {
        return Ok(Value::Undefined);
    };
    let mut parts = Vec::with_capacity(arr.len());
    for e in arr {
        match e {
            Bson::String(s) => parts.push(s.as_str()),
            _ => return Ok(Value::Undefined),
        }
    }
    Ok(Value::Defined(Bson::String(parts.join(sep))))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::Bson;

    fn strs(items: &[&str]) -> crate::value::Value {
        def(Bson::Array(
            items.iter().map(|s| Bson::String((*s).into())).collect(),
        ))
    }

    #[test]
    fn cosmos_examples() {
        assert_eq!(
            call("STRINGJOIN", vec![strs(&["a", "b", "c"]), def("-")]).unwrap(),
            def("a-b-c")
        );
        assert_eq!(
            call("STRINGJOIN", vec![strs(&["a"]), def("-")]).unwrap(),
            def("a")
        );
        // empty array → empty string
        assert_eq!(
            call("STRINGJOIN", vec![strs(&[]), def("-")]).unwrap(),
            def("")
        );
        // empty separator
        assert_eq!(
            call("STRINGJOIN", vec![strs(&["a", "b"]), def("")]).unwrap(),
            def("ab")
        );
    }

    #[test]
    fn non_string_elements_are_undefined() {
        let mixed = def(Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]));
        assert!(
            call("STRINGJOIN", vec![mixed, def("-")])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn non_array_first_arg_is_undefined() {
        assert!(
            call("STRINGJOIN", vec![def("abc"), def("-")])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn wrong_arity_is_error() {
        assert!(call("STRINGJOIN", vec![strs(&["a"])]).is_err());
    }
}
