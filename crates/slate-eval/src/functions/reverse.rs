//! `REVERSE(str)` — the characters of a string in reverse order.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match str_arg(&args[0]) {
        Some(s) => Value::Defined(Bson::String(s.chars().rev().collect())),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/reverse
        assert_eq!(
            call("REVERSE", vec![def("AdventureWorks")]).unwrap(),
            def("skroWerutnevdA")
        );
        assert_eq!(
            call("REVERSE", vec![def("skroWerutnevdA")]).unwrap(),
            def("AdventureWorks")
        );
        // Double reverse round-trips.
        let once = call("REVERSE", vec![def("AdventureWorks")]).unwrap();
        assert_eq!(call("REVERSE", vec![once]).unwrap(), def("AdventureWorks"));
    }

    #[test]
    fn non_string_is_undefined() {
        assert!(call("REVERSE", vec![def(1)]).unwrap().is_undefined());
    }
}
