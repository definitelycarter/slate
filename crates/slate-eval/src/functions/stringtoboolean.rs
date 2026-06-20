//! `STRINGTOBOOLEAN(str)` — parse a string to a boolean. Surrounding whitespace
//! is ignored. Anything but the strings `"true"`/`"false"` (e.g. a non-string)
//! yields undefined.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match str_arg(&args[0]).map(str::trim) {
        Some("true") => Value::Defined(Bson::Boolean(true)),
        Some("false") => Value::Defined(Bson::Boolean(false)),
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        assert_eq!(
            call("STRINGTOBOOLEAN", vec![def("true")]).unwrap(),
            def(true)
        );
        assert_eq!(
            call("STRINGTOBOOLEAN", vec![def("  false  ")]).unwrap(),
            def(false)
        );
        // a non-string boolean yields undefined
        assert!(
            call("STRINGTOBOOLEAN", vec![def(true)])
                .unwrap()
                .is_undefined()
        );
    }
}
