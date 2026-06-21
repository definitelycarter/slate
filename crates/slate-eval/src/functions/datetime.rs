//! CosmosDB date/time functions.
//!
//! Cosmos models an instant three ways:
//! - **DateTime**: an ISO 8601 string `YYYY-MM-DDTHH:MM:SS.fffffffZ` (UTC, 7
//!   fractional digits = 100-nanosecond precision).
//! - **Timestamp**: Unix epoch milliseconds (a number).
//! - **Ticks**: 100-nanosecond intervals since the Unix epoch (a number).
//!
//! We model every instant as `i128` ticks (100ns since 1970) and convert at the
//! edges, so the conversions are exact. `chrono` is used only for calendar math
//! and ISO parsing/formatting — never for the wall clock (the `clock` feature is
//! off), so this stays wasm-safe; `GETCURRENT*` instead read an injected `now`.

use bson::Bson;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Timelike};

use crate::error::Result;
use crate::value::Value;

use super::{arity, int_value, str_arg};

const TICKS_PER_SEC: i128 = 10_000_000;
const TICKS_PER_MS: i128 = 10_000;

// ── tick <-> ISO conversions ─────────────────────────────────────

fn ndt_to_ticks(ndt: &NaiveDateTime) -> i128 {
    let utc = ndt.and_utc();
    utc.timestamp() as i128 * TICKS_PER_SEC + utc.timestamp_subsec_nanos() as i128 / 100
}

fn parse_ticks(s: &str) -> Option<i128> {
    let s = s.trim().trim_end_matches('Z');
    let ndt = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .ok()?;
    Some(ndt_to_ticks(&ndt))
}

fn ticks_to_ndt(ticks: i128) -> Option<NaiveDateTime> {
    let secs = i64::try_from(ticks.div_euclid(TICKS_PER_SEC)).ok()?;
    let nanos = (ticks.rem_euclid(TICKS_PER_SEC) * 100) as u32;
    DateTime::from_timestamp(secs, nanos).map(|dt| dt.naive_utc())
}

/// Format ticks as the Cosmos ISO string (7 fractional digits + `Z`).
fn format_ticks(ticks: i128) -> Option<String> {
    let ndt = ticks_to_ndt(ticks)?;
    let frac = ticks.rem_euclid(TICKS_PER_SEC);
    Some(format!("{}.{frac:07}Z", ndt.format("%Y-%m-%dT%H:%M:%S")))
}

/// Ticks per fixed-duration `DateTimePart` (calendar parts — year/month — are
/// handled separately). `None` for an unknown or variable-length part.
fn fixed_part_ticks(part: &str) -> Option<i128> {
    Some(match part {
        "dd" => 86_400 * TICKS_PER_SEC,
        "hh" => 3_600 * TICKS_PER_SEC,
        "mi" => 60 * TICKS_PER_SEC,
        "ss" => TICKS_PER_SEC,
        "ms" => TICKS_PER_MS,
        "mcs" => 10,
        _ => return None,
    })
}

fn dt(ticks: i128) -> Value {
    match format_ticks(ticks) {
        Some(s) => Value::Defined(Bson::String(s)),
        None => Value::Undefined,
    }
}

// ── GETCURRENT* (clock-dependent) ────────────────────────────────

/// Whether `name` is a `GETCURRENT*` function (including the `…STATIC` forms).
/// These read an injected "now" rather than a syscall, so they're evaluated in
/// the executor (which threads `now`) rather than the pure dispatch.
pub(super) fn is_current(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "GETCURRENTDATETIME"
            | "GETCURRENTDATETIMESTATIC"
            | "GETCURRENTTIMESTAMP"
            | "GETCURRENTTIMESTAMPSTATIC"
            | "GETCURRENTTICKS"
            | "GETCURRENTTICKSSTATIC"
    )
}

/// Resolve a `GETCURRENT*` function from `now_ms`. The plain and `…STATIC` forms
/// are equivalent here (now is fixed for the transaction).
pub(super) fn current(name: &str, now_ms: i64) -> Value {
    let ticks = now_ms as i128 * TICKS_PER_MS;
    match name.to_ascii_uppercase().as_str() {
        "GETCURRENTDATETIME" | "GETCURRENTDATETIMESTATIC" => dt(ticks),
        "GETCURRENTTIMESTAMP" | "GETCURRENTTIMESTAMPSTATIC" => Value::Defined(Bson::Int64(now_ms)),
        "GETCURRENTTICKS" | "GETCURRENTTICKSSTATIC" => Value::Defined(Bson::Int64(ticks as i64)),
        _ => Value::Undefined,
    }
}

// ── functions ────────────────────────────────────────────────────

pub(super) fn to_ticks(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match str_arg(&args[0]).and_then(parse_ticks) {
        Some(t) => Value::Defined(Bson::Int64(t as i64)),
        None => Value::Undefined,
    })
}

pub(super) fn to_timestamp(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match str_arg(&args[0]).and_then(parse_ticks) {
        Some(t) => Value::Defined(Bson::Int64((t.div_euclid(TICKS_PER_MS)) as i64)),
        None => Value::Undefined,
    })
}

pub(super) fn from_timestamp(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match int_value(&args[0]) {
        Some(ms) => dt(ms as i128 * TICKS_PER_MS),
        None => Value::Undefined,
    })
}

pub(super) fn from_ticks(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match int_value(&args[0]) {
        Some(t) => dt(t as i128),
        None => Value::Undefined,
    })
}

pub(super) fn part(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    let (Some(p), Some(s)) = (str_arg(&args[0]), str_arg(&args[1])) else {
        return Ok(Value::Undefined);
    };
    let Some(ticks) = parse_ticks(s) else {
        return Ok(Value::Undefined);
    };
    let Some(ndt) = ticks_to_ndt(ticks) else {
        return Ok(Value::Undefined);
    };
    let utc = ndt.and_utc();
    let v: i64 = match p {
        "yyyy" => ndt.year() as i64,
        "mm" => ndt.month() as i64,
        "dd" => ndt.day() as i64,
        "hh" => ndt.hour() as i64,
        "mi" => ndt.minute() as i64,
        "ss" => ndt.second() as i64,
        "ms" => utc.timestamp_subsec_millis() as i64,
        "mcs" => utc.timestamp_subsec_micros() as i64,
        "ns" => utc.timestamp_subsec_nanos() as i64,
        _ => return Ok(Value::Undefined),
    };
    Ok(Value::Defined(Bson::Int64(v)))
}

pub(super) fn from_parts(name: &str, args: Vec<Value>) -> Result<Value> {
    if !(3..=7).contains(&args.len()) {
        return Err(super::arity_err(name, "3 to 7"));
    }
    let part = |i: usize, default: i64| -> Option<i64> {
        match args.get(i) {
            Some(v) => int_value(v),
            None => Some(default),
        }
    };
    let (Some(y), Some(mo), Some(d), Some(h), Some(mi), Some(s), Some(frac)) = (
        part(0, 0),
        part(1, 0),
        part(2, 0),
        part(3, 0),
        part(4, 0),
        part(5, 0),
        part(6, 0),
    ) else {
        return Ok(Value::Undefined);
    };
    let built = i32::try_from(y)
        .ok()
        .zip(u32::try_from(mo).ok())
        .zip(u32::try_from(d).ok())
        .and_then(|((y, mo), d)| NaiveDate::from_ymd_opt(y, mo, d))
        .zip(
            u32::try_from(h)
                .ok()
                .zip(u32::try_from(mi).ok())
                .zip(u32::try_from(s).ok()),
        )
        .and_then(|(date, ((h, mi), s))| date.and_hms_opt(h, mi, s));
    let Some(ndt) = built else {
        return Ok(Value::Undefined);
    };
    let frac = i128::from(frac);
    if !(0..TICKS_PER_SEC).contains(&frac) {
        return Ok(Value::Undefined);
    }
    Ok(dt(ndt_to_ticks(&ndt) + frac))
}

pub(super) fn add(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 3)?;
    let (Some(p), Some(amount), Some(s)) =
        (str_arg(&args[0]), int_value(&args[1]), str_arg(&args[2]))
    else {
        return Ok(Value::Undefined);
    };
    let Some(ticks) = parse_ticks(s) else {
        return Ok(Value::Undefined);
    };
    // Year/month are calendar-aware; the rest are fixed durations on the tick line.
    if let Some(months) = match p {
        "yyyy" => Some(amount.checked_mul(12)),
        "mm" => Some(Some(amount)),
        _ => None,
    } {
        let Some(months) = months else {
            return Ok(Value::Undefined);
        };
        let Some(ndt) = ticks_to_ndt(ticks) else {
            return Ok(Value::Undefined);
        };
        let shifted = if months >= 0 {
            u32::try_from(months)
                .ok()
                .and_then(|m| ndt.checked_add_months(chrono::Months::new(m)))
        } else {
            u32::try_from(-months)
                .ok()
                .and_then(|m| ndt.checked_sub_months(chrono::Months::new(m)))
        };
        return Ok(match shifted {
            Some(ndt) => dt(ndt_to_ticks(&ndt)),
            None => Value::Undefined,
        });
    }
    match fixed_part_ticks(p) {
        Some(size) => Ok(dt(ticks + amount as i128 * size)),
        None => Ok(Value::Undefined),
    }
}

pub(super) fn diff(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 3)?;
    let (Some(p), Some(start), Some(end)) =
        (str_arg(&args[0]), str_arg(&args[1]), str_arg(&args[2]))
    else {
        return Ok(Value::Undefined);
    };
    let (Some(st), Some(et)) = (parse_ticks(start), parse_ticks(end)) else {
        return Ok(Value::Undefined);
    };
    // Calendar parts compare components; fixed parts count boundaries crossed.
    let v: i128 = match p {
        "yyyy" | "mm" => {
            let (Some(sn), Some(en)) = (ticks_to_ndt(st), ticks_to_ndt(et)) else {
                return Ok(Value::Undefined);
            };
            let years = en.year() as i128 - sn.year() as i128;
            if p == "yyyy" {
                years
            } else {
                years * 12 + (en.month() as i128 - sn.month() as i128)
            }
        }
        _ => match fixed_part_ticks(p) {
            Some(size) => et.div_euclid(size) - st.div_euclid(size),
            None => return Ok(Value::Undefined),
        },
    };
    Ok(Value::Defined(Bson::Int64(v as i64)))
}

pub(super) fn bin(name: &str, args: Vec<Value>) -> Result<Value> {
    if !(2..=4).contains(&args.len()) {
        return Err(super::arity_err(name, "2 to 4"));
    }
    let (Some(s), Some(p)) = (str_arg(&args[0]), str_arg(&args[1])) else {
        return Ok(Value::Undefined);
    };
    let bin_size = match args.get(2) {
        Some(v) => match int_value(v) {
            Some(n) if n > 0 => n as i128,
            _ => return Ok(Value::Undefined),
        },
        None => 1,
    };
    let origin = match args.get(3) {
        Some(v) => match str_arg(v).and_then(parse_ticks) {
            Some(t) => t,
            None => return Ok(Value::Undefined),
        },
        None => 0, // Unix epoch
    };
    let (Some(ticks), Some(unit)) = (parse_ticks(s), fixed_part_ticks(p)) else {
        return Ok(Value::Undefined);
    };
    let size = unit * bin_size;
    let binned = origin + (ticks - origin).div_euclid(size) * size;
    Ok(dt(binned))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::Bson;

    fn s(v: bson::Bson) -> String {
        match v {
            Bson::String(s) => s,
            other => panic!("expected string, got {other:?}"),
        }
    }
    fn defined(v: crate::value::Value) -> Bson {
        match v {
            crate::value::Value::Defined(b) => b,
            crate::value::Value::Undefined => panic!("undefined"),
        }
    }

    #[test]
    fn ticks_and_timestamp_round_trip() {
        assert_eq!(
            call("DATETIMETOTICKS", vec![def("2015-05-19T12:00:00.0000000")]).unwrap(),
            def(14320368000000000_i64)
        );
        assert_eq!(
            call(
                "DATETIMETOTIMESTAMP",
                vec![def("2015-05-19T12:00:00.0000000")]
            )
            .unwrap(),
            def(1432036800000_i64)
        );
        assert_eq!(
            s(defined(call("TIMESTAMPTODATETIME", vec![def(0)]).unwrap())),
            "1970-01-01T00:00:00.0000000Z"
        );
        assert_eq!(
            s(defined(
                call("TICKSTODATETIME", vec![def(15973607943002652_i64)]).unwrap()
            )),
            "2020-08-13T23:19:54.3002652Z"
        );
    }

    #[test]
    fn part_extraction() {
        let dt = "2016-05-29T08:30:00.1301617";
        assert_eq!(
            call("DATETIMEPART", vec![def("yyyy"), def(dt)]).unwrap(),
            def(2016_i64)
        );
        assert_eq!(
            call("DATETIMEPART", vec![def("ms"), def(dt)]).unwrap(),
            def(130_i64)
        );
        assert_eq!(
            call("DATETIMEPART", vec![def("ns"), def(dt)]).unwrap(),
            def(130161700_i64)
        );
    }

    #[test]
    fn add_and_diff_and_bin() {
        assert_eq!(
            s(defined(
                call(
                    "DATETIMEADD",
                    vec![def("mm"), def(1), def("2020-07-03T00:00:00.0000000")]
                )
                .unwrap()
            )),
            "2020-08-03T00:00:00.0000000Z"
        );
        assert_eq!(
            call(
                "DATETIMEDIFF",
                vec![
                    def("mm"),
                    def("2018-03-05T05:00:00.0000000"),
                    def("2019-02-04T16:00:00.0000000")
                ]
            )
            .unwrap(),
            def(11_i64)
        );
        // yyyy is the calendar-year difference, not months/12.
        assert_eq!(
            call(
                "DATETIMEDIFF",
                vec![
                    def("yyyy"),
                    def("2018-03-05T05:00:00.0000000"),
                    def("2019-02-04T16:00:00.0000000")
                ]
            )
            .unwrap(),
            def(1_i64)
        );
        assert_eq!(
            s(defined(
                call(
                    "DATETIMEBIN",
                    vec![def("2021-01-08T18:35:00.0000000"), def("hh"), def(5)]
                )
                .unwrap()
            )),
            "2021-01-08T15:00:00.0000000Z"
        );
    }

    #[test]
    fn from_parts() {
        assert_eq!(
            s(defined(
                call(
                    "DATETIMEFROMPARTS",
                    vec![
                        def(2017),
                        def(4),
                        def(20),
                        def(13),
                        def(15),
                        def(20),
                        def(3456789)
                    ]
                )
                .unwrap()
            )),
            "2017-04-20T13:15:20.3456789Z"
        );
        assert!(
            call("DATETIMEFROMPARTS", vec![def(-2000), def(-1), def(-1)])
                .unwrap()
                .is_undefined()
        );
    }
}
