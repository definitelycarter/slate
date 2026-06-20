//! `OBJECTTOARRAY(obj [, keyName, valueName])` — convert an object's field/value
//! pairs into an array of two-field elements.
//!
//! Each element is `{ "k": <field>, "v": <value> }` by default; the optional
//! second and third string arguments rename those two fields. Matches Cosmos.

use bson::{Bson, Document};

use crate::error::Result;
use crate::value::Value;

use super::{arity_err, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 && args.len() != 3 {
        return Err(arity_err(name, "1 or 3"));
    }
    let (kname, vname) = if args.len() == 3 {
        match (str_arg(&args[1]), str_arg(&args[2])) {
            (Some(k), Some(v)) => (k.to_string(), v.to_string()),
            _ => return Ok(Value::Undefined),
        }
    } else {
        ("k".to_string(), "v".to_string())
    };
    let Some(Bson::Document(doc)) = args.into_iter().next().and_then(Value::into_bson) else {
        return Ok(Value::Undefined);
    };
    let mut out: Vec<Bson> = Vec::with_capacity(doc.len());
    for (key, val) in doc {
        let mut elem = Document::new();
        elem.insert(kname.as_str(), Bson::String(key));
        elem.insert(vname.as_str(), val);
        out.push(Bson::Document(elem));
    }
    Ok(Value::Defined(Bson::Array(out)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::{Bson, doc};

    #[test]
    fn cosmos_example() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/objecttoarray
        let obj = Bson::Document(doc! { "a": "12345", "b": "67890" });
        let expected = Bson::Array(vec![
            Bson::Document(doc! { "k": "a", "v": "12345" }),
            Bson::Document(doc! { "k": "b", "v": "67890" }),
        ]);
        assert_eq!(
            call("OBJECTTOARRAY", vec![def(obj)]).unwrap(),
            def(expected)
        );
    }

    #[test]
    fn custom_field_names() {
        let obj = Bson::Document(doc! { "a": "12345" });
        let expected = Bson::Array(vec![Bson::Document(doc! { "key": "a", "value": "12345" })]);
        assert_eq!(
            call("OBJECTTOARRAY", vec![def(obj), def("key"), def("value")]).unwrap(),
            def(expected)
        );
    }

    #[test]
    fn non_object_is_undefined() {
        assert!(
            call("OBJECTTOARRAY", vec![def("x")])
                .unwrap()
                .is_undefined()
        );
    }
}
