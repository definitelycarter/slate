//! `STRINGEQUALS(str1, str2 [, ignoreCase])` — whether two strings are equal.
//! An optional third argument requests a case-insensitive comparison.

use crate::error::Result;
use crate::value::Value;

use super::{arity_2_or_3, str_match};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity_2_or_3(name, &args)?;
    Ok(str_match(&args, |a, b| a == b))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/stringequals
        assert_eq!(
            call(
                "STRINGEQUALS",
                vec![def("AdventureWorks"), def("AdventureWorks")]
            )
            .unwrap(),
            def(true)
        );
        assert_eq!(
            call(
                "STRINGEQUALS",
                vec![def("AdventureWorks"), def("adventureworks")]
            )
            .unwrap(),
            def(false)
        );
        assert_eq!(
            call(
                "STRINGEQUALS",
                vec![def("AdventureWorks"), def("adventureworks"), def(true)]
            )
            .unwrap(),
            def(true)
        );
    }

    #[test]
    fn non_string_is_undefined() {
        assert!(
            call("STRINGEQUALS", vec![def("a"), def(1)])
                .unwrap()
                .is_undefined()
        );
    }
}
