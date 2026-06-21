//! `STRINGTOOBJECT(str)` — parse a JSON object string into an object. A
//! non-string, invalid JSON, or JSON that isn't an object yields undefined.

use crate::error::Result;
use crate::value::Value;

use super::{arity, json_to_bson, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    let Some(s) = str_arg(&args[0]) else {
        return Ok(Value::Undefined);
    };
    Ok(match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v @ serde_json::Value::Object(_)) => Value::Defined(json_to_bson(v)),
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::{Bson, doc};

    #[test]
    fn cosmos_examples() {
        assert_eq!(
            call("STRINGTOOBJECT", vec![def("{}")]).unwrap(),
            def(Bson::Document(doc! {}))
        );
        assert_eq!(
            call("STRINGTOOBJECT", vec![def(r#"{"isAvailable": true}"#)]).unwrap(),
            def(Bson::Document(doc! { "isAvailable": true }))
        );
        // single-quoted property names are not valid JSON
        assert!(
            call("STRINGTOOBJECT", vec![def("{'price': 27.55}")])
                .unwrap()
                .is_undefined()
        );
    }
}
