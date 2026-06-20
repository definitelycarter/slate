//! `TRIM(str [, chars])` — remove leading and trailing characters.
//!
//! With one argument it strips whitespace. With a second string argument it
//! strips any character in that set from both ends (a *set*, not a substring —
//! `TRIM("-_-x-_-", "-_")` → `"x"`), matching Cosmos.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity_err, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(arity_err(name, "1 or 2"));
    }
    let Some(s) = str_arg(&args[0]) else {
        return Ok(Value::Undefined);
    };
    let out = match args.get(1) {
        Some(v) => match str_arg(v) {
            Some(set) => s.trim_matches(|c| set.contains(c)).to_string(),
            None => return Ok(Value::Undefined),
        },
        None => s.trim().to_string(),
    };
    Ok(Value::Defined(Bson::String(out)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/trim
        assert_eq!(
            call("TRIM", vec![def("   AdventureWorks")]).unwrap(),
            def("AdventureWorks")
        );
        assert_eq!(
            call("TRIM", vec![def("AdventureWorks   ")]).unwrap(),
            def("AdventureWorks")
        );
        assert_eq!(
            call("TRIM", vec![def("   AdventureWorks   ")]).unwrap(),
            def("AdventureWorks")
        );
        // Default trims only whitespace, so hyphens survive.
        assert_eq!(
            call("TRIM", vec![def("---AdventureWorks---")]).unwrap(),
            def("---AdventureWorks---")
        );
        assert_eq!(
            call("TRIM", vec![def("___AdventureWorks___"), def("_")]).unwrap(),
            def("AdventureWorks")
        );
        assert_eq!(
            call("TRIM", vec![def("---AdventureWorks---"), def("-")]).unwrap(),
            def("AdventureWorks")
        );
        // Only hyphens are in the set, so the inner spaces remain.
        assert_eq!(
            call("TRIM", vec![def("-- AdventureWorks --"), def("-")]).unwrap(),
            def(" AdventureWorks ")
        );
        // The set is the characters '-' and '_'.
        assert_eq!(
            call("TRIM", vec![def("-_-AdventureWorks-_-"), def("-_")]).unwrap(),
            def("AdventureWorks")
        );
    }

    #[test]
    fn non_string_is_undefined() {
        assert!(call("TRIM", vec![def(1)]).unwrap().is_undefined());
    }
}
