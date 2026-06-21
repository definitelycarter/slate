//! Rendering BSON result values as human-readable JSON.
//!
//! Values come back from the engine as [`bson::RawBson`]. We convert to owned
//! [`bson::Bson`] and emit relaxed extended JSON — numbers stay numbers,
//! ObjectIds render as `{"$oid": "…"}` — which reads far better than canonical
//! extJSON. Documents and arrays are pretty-printed across lines; scalars stay
//! on one line.

use bson::{Bson, RawBson};

/// Render one engine result value as a JSON string.
pub fn render_value(raw: RawBson) -> Result<String, String> {
    let bson = Bson::try_from(raw).map_err(|e| e.to_string())?;
    Ok(render_bson(bson))
}

fn render_bson(bson: Bson) -> String {
    let multiline = matches!(bson, Bson::Document(_) | Bson::Array(_));
    let json = bson.into_relaxed_extjson();
    if multiline {
        serde_json::to_string_pretty(&json).unwrap_or_else(|_| json.to_string())
    } else {
        json.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::oid::ObjectId;

    #[test]
    fn scalar_string_is_quoted_inline() {
        let out = render_value(RawBson::String("ada".to_string())).unwrap();
        assert_eq!(out, "\"ada\"");
    }

    #[test]
    fn scalar_int_is_bare() {
        let out = render_value(RawBson::Int64(42)).unwrap();
        assert_eq!(out, "42");
    }

    #[test]
    fn double_renders_as_number() {
        let out = render_value(RawBson::Double(3.5)).unwrap();
        assert_eq!(out, "3.5");
    }

    #[test]
    fn document_is_pretty_printed() {
        let bson = bson::bson!({ "name": "ada", "age": 36_i64 });
        let out = render_bson(bson);
        assert!(out.contains('\n'), "expected multiline output: {out}");
        assert!(out.contains("\"name\""));
        assert!(out.contains("\"ada\""));
    }

    #[test]
    fn objectid_uses_relaxed_extjson() {
        let oid = ObjectId::parse_str("0123456789abcdef01234567").unwrap();
        let out = render_bson(bson::bson!({ "_id": oid }));
        assert!(out.contains("$oid"), "expected $oid form: {out}");
        assert!(out.contains("0123456789abcdef01234567"));
    }
}
