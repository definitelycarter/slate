//! `LEFT(str, n)` — the first `n` characters of a string.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, int_arg, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    let (Some(s), Some(n)) = (str_arg(&args[0]), int_arg(&args[1])) else {
        return Ok(Value::Undefined);
    };
    let take = if n < 0 { 0 } else { n as usize };
    let out: String = s.chars().take(take).collect();
    Ok(Value::Defined(Bson::String(out)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/left
        let s = "AdventureWorks";
        assert_eq!(call("LEFT", vec![def(s), def(0)]).unwrap(), def(""));
        assert_eq!(call("LEFT", vec![def(s), def(1)]).unwrap(), def("A"));
        assert_eq!(call("LEFT", vec![def(s), def(5)]).unwrap(), def("Adven"));
        assert_eq!(
            call("LEFT", vec![def(s), def(14)]).unwrap(),
            def("AdventureWorks")
        );
        assert_eq!(
            call("LEFT", vec![def(s), def(100)]).unwrap(),
            def("AdventureWorks")
        );
    }

    #[test]
    fn non_string_is_undefined() {
        assert!(call("LEFT", vec![def(1), def(2)]).unwrap().is_undefined());
    }
}
