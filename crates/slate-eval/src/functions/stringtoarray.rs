//! `STRINGTOARRAY(str)` — parse a JSON array string into an array. A non-string,
//! invalid JSON, or JSON that isn't an array yields undefined.

use crate::error::Result;
use crate::value::Value;

use super::{arity, json_to_bson, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    let Some(s) = str_arg(&args[0]) else {
        return Ok(Value::Undefined);
    };
    Ok(match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v @ serde_json::Value::Array(_)) => Value::Defined(json_to_bson(v)),
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::Bson;

    #[test]
    fn cosmos_examples() {
        assert_eq!(
            call("STRINGTOARRAY", vec![def("[]")]).unwrap(),
            def(Bson::Array(vec![]))
        );
        assert_eq!(
            call("STRINGTOARRAY", vec![def(r#"["a", "b"]"#)]).unwrap(),
            def(Bson::Array(vec![
                Bson::String("a".into()),
                Bson::String("b".into())
            ]))
        );
        // single-quoted strings are not valid JSON
        assert!(
            call("STRINGTOARRAY", vec![def("['a']")])
                .unwrap()
                .is_undefined()
        );
        // valid JSON but not an array
        assert!(
            call("STRINGTOARRAY", vec![def("5")])
                .unwrap()
                .is_undefined()
        );
    }
}
