//! `CONCAT(str, str, …)` — concatenate two or more strings. Any non-string
//! argument yields `Undefined`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity_err, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    if args.len() < 2 {
        return Err(arity_err(name, "at least 2"));
    }
    let mut out = String::new();
    for a in &args {
        match str_arg(a) {
            Some(s) => out.push_str(s),
            None => return Ok(Value::Undefined),
        }
    }
    Ok(Value::Defined(Bson::String(out)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn concatenates() {
        assert_eq!(call("CONCAT", vec![def("a"), def("b")]).unwrap(), def("ab"));
        assert_eq!(
            call("CONCAT", vec![def("a"), def("b"), def("c")]).unwrap(),
            def("abc")
        );
    }

    #[test]
    fn non_string_arg_is_undefined() {
        assert!(
            call("CONCAT", vec![def("a"), def(1)])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn fewer_than_two_args_is_error() {
        assert!(call("CONCAT", vec![def("a")]).is_err());
    }
}
