//! Rendering BSON result values as human-readable JSON.
//!
//! Values come back from the engine as [`bson::RawBson`]. We convert to owned
//! [`bson::Bson`] and emit relaxed extended JSON — numbers stay numbers,
//! ObjectIds render as `{"$oid": "…"}` — which reads far better than canonical
//! extJSON. Documents and arrays are pretty-printed across lines; scalars stay
//! on one line.

use std::time::Duration;

use bson::{Bson, RawBson};

/// Render one engine result value as a JSON string.
pub fn render_value(raw: RawBson) -> Result<String, String> {
    let bson = Bson::try_from(raw).map_err(|e| e.to_string())?;
    Ok(render_bson(bson))
}

/// Format an elapsed duration for the REPL footer: milliseconds with two
/// decimals under a second, seconds with two decimals at or above one (so a
/// quick query reads `0.42ms` and a slow scan `1.80s`).
pub fn fmt_duration(elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64();
    if secs >= 1.0 {
        format!("{secs:.2}s")
    } else {
        format!("{:.2}ms", secs * 1000.0)
    }
}

/// Render an owned BSON value as a JSON string — documents and arrays
/// pretty-printed across lines, scalars inline.
pub fn render_bson(bson: Bson) -> String {
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

    #[test]
    fn sub_second_durations_render_as_millis() {
        assert_eq!(fmt_duration(Duration::from_millis(250)), "250.00ms");
        assert_eq!(fmt_duration(Duration::from_micros(420)), "0.42ms");
    }

    #[test]
    fn durations_at_or_above_a_second_render_as_seconds() {
        assert_eq!(fmt_duration(Duration::from_millis(1800)), "1.80s");
        assert_eq!(fmt_duration(Duration::from_secs(2)), "2.00s");
    }
}
