//! Aggregate accumulators — `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`.
//!
//! Unlike the stateless scalar [`crate::functions`], an aggregate folds a stream
//! of per-row values into one result. The executor's aggregation node evaluates
//! each aggregate's argument expression per row (via the shared evaluator), feeds
//! the resulting [`Value`] to an [`Accumulator`], then [`finalize`](Accumulator::finalize)s
//! once per group.
//!
//! Cosmos semantics, matched here:
//! - `COUNT(expr)` counts rows where `expr` is *defined* (`COUNT(1)` counts all);
//!   an empty group is `0`.
//! - `SUM`/`AVG` skip `undefined` values, but a single non-numeric *defined*
//!   value (string/bool/null/…) poisons the whole result to `undefined`. With no
//!   qualifying values the result is `undefined`.
//! - `MIN`/`MAX` skip `undefined`, order by the shared total order
//!   ([`order_bson`]), preserve the winning value's type, and have **no** poison
//!   rule. An empty group is `undefined`.
//! - `ARRAY_AGG` (synonym `COLLECT`) gathers each row's *defined* value into an
//!   array, preserving arrival order and each value's type; `undefined` values
//!   are skipped. An empty group is the empty array `[]` — not `undefined` —
//!   so a grouped query always emits an array for the group.

use bson::Bson;

use crate::eval::{decimal_to_f64, order_bson};
use crate::value::Value;

/// The aggregate functions the planner recognizes in a `SELECT`/`HAVING`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// `ARRAY_AGG` (synonym `COLLECT`) — gather the group's values into an array.
    ArrayAgg,
}

impl AggFunc {
    /// Recognize a function name (case-insensitive) as an aggregate, or `None`
    /// if it's an ordinary scalar function.
    pub fn from_name(name: &str) -> Option<AggFunc> {
        match name.to_ascii_uppercase().as_str() {
            "COUNT" => Some(AggFunc::Count),
            "SUM" => Some(AggFunc::Sum),
            "AVG" => Some(AggFunc::Avg),
            "MIN" => Some(AggFunc::Min),
            "MAX" => Some(AggFunc::Max),
            "ARRAY_AGG" | "COLLECT" => Some(AggFunc::ArrayAgg),
            _ => None,
        }
    }

    /// A fresh zero-state accumulator for this function.
    pub fn accumulator(self) -> Accumulator {
        match self {
            AggFunc::Count => Accumulator::Count(0),
            AggFunc::Sum => Accumulator::Sum {
                total: 0.0,
                any: false,
                poisoned: false,
            },
            AggFunc::Avg => Accumulator::Avg {
                total: 0.0,
                count: 0,
                poisoned: false,
            },
            AggFunc::Min => Accumulator::Extreme {
                want: std::cmp::Ordering::Less,
                cur: None,
            },
            AggFunc::Max => Accumulator::Extreme {
                want: std::cmp::Ordering::Greater,
                cur: None,
            },
            AggFunc::ArrayAgg => Accumulator::ArrayAgg(Vec::new()),
        }
    }
}

/// A running aggregate over a stream of per-row [`Value`]s.
pub enum Accumulator {
    Count(i64),
    /// `total` is the running sum; `any` records whether a numeric value was
    /// seen; `poisoned` records a non-numeric defined value.
    Sum {
        total: f64,
        any: bool,
        poisoned: bool,
    },
    Avg {
        total: f64,
        count: i64,
        poisoned: bool,
    },
    /// `MIN` (`want = Less`) or `MAX` (`want = Greater`): keep the current value
    /// when a new one orders `want` relative to it.
    Extreme {
        want: std::cmp::Ordering,
        cur: Option<Bson>,
    },
    /// `ARRAY_AGG`/`COLLECT` — the group's defined values in arrival order.
    ArrayAgg(Vec<Bson>),
}

impl Accumulator {
    /// Fold one row's value into the running aggregate.
    pub fn accumulate(&mut self, value: Value) {
        match self {
            Accumulator::Count(n) => {
                if !value.is_undefined() {
                    *n += 1;
                }
            }
            Accumulator::Sum {
                total,
                any,
                poisoned,
            } => match value {
                Value::Undefined => {}
                Value::Defined(b) => match num_f64(&b) {
                    Some(f) => {
                        *total += f;
                        *any = true;
                    }
                    None => *poisoned = true,
                },
            },
            Accumulator::Avg {
                total,
                count,
                poisoned,
            } => match value {
                Value::Undefined => {}
                Value::Defined(b) => match num_f64(&b) {
                    Some(f) => {
                        *total += f;
                        *count += 1;
                    }
                    None => *poisoned = true,
                },
            },
            Accumulator::Extreme { want, cur } => {
                if let Value::Defined(b) = value {
                    let replace = match cur {
                        None => true,
                        Some(current) => order_bson(&b, current) == *want,
                    };
                    if replace {
                        *cur = Some(b);
                    }
                }
            }
            Accumulator::ArrayAgg(items) => {
                // Skip undefined (a row that didn't produce a value), gather the
                // rest in arrival order with their type preserved.
                if let Value::Defined(b) = value {
                    items.push(b);
                }
            }
        }
    }

    /// Collapse the running state into the aggregate's result value.
    pub fn finalize(self) -> Value {
        match self {
            Accumulator::Count(n) => Value::Defined(Bson::Int64(n)),
            Accumulator::Sum {
                total,
                any,
                poisoned,
            } => {
                if poisoned || !any {
                    Value::Undefined
                } else {
                    Value::Defined(Bson::Double(total))
                }
            }
            Accumulator::Avg {
                total,
                count,
                poisoned,
            } => {
                if poisoned || count == 0 {
                    Value::Undefined
                } else {
                    Value::Defined(Bson::Double(total / count as f64))
                }
            }
            Accumulator::Extreme { cur, .. } => match cur {
                Some(b) => Value::Defined(b),
                None => Value::Undefined,
            },
            // Always an array — an empty group is `[]`, never undefined — so the
            // group row carries the slot (an undefined slot would be omitted).
            Accumulator::ArrayAgg(items) => Value::Defined(Bson::Array(items)),
        }
    }
}

/// Numeric view of a BSON value as `f64`, or `None` for any non-numeric type.
fn num_f64(b: &Bson) -> Option<f64> {
    match b {
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        Bson::Decimal128(d) => decimal_to_f64(d),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(b: impl Into<Bson>) -> Value {
        Value::Defined(b.into())
    }

    fn run(func: AggFunc, values: Vec<Value>) -> Value {
        let mut acc = func.accumulator();
        for v in values {
            acc.accumulate(v);
        }
        acc.finalize()
    }

    #[test]
    fn count_counts_defined_values() {
        // COUNT(1) counts every row; COUNT(expr) skips undefined.
        assert_eq!(
            run(AggFunc::Count, vec![def(1), def(1), def(1)]),
            def(3_i64)
        );
        assert_eq!(
            run(AggFunc::Count, vec![def("a"), Value::Undefined, def("b")]),
            def(2_i64)
        );
        assert_eq!(run(AggFunc::Count, vec![]), def(0_i64));
    }

    #[test]
    fn sum_matches_cosmos() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/sum  → 617
        assert_eq!(
            run(
                AggFunc::Sum,
                vec![def(0), def(230), def(14), def(232), def(141)]
            ),
            def(617.0_f64)
        );
        // Undefined values are skipped.
        assert_eq!(
            run(AggFunc::Sum, vec![def(10), Value::Undefined, def(5)]),
            def(15.0_f64)
        );
        // A single non-numeric defined value poisons the whole result.
        assert!(run(AggFunc::Sum, vec![def(10), def("x")]).is_undefined());
        assert!(run(AggFunc::Sum, vec![def(10), Value::Defined(Bson::Null)]).is_undefined());
        // No qualifying values → undefined.
        assert!(run(AggFunc::Sum, vec![]).is_undefined());
        assert!(run(AggFunc::Sum, vec![Value::Undefined]).is_undefined());
    }

    #[test]
    fn avg_matches_cosmos() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/avg  → 101.5
        assert_eq!(run(AggFunc::Avg, vec![def(98), def(105)]), def(101.5_f64));
        assert_eq!(
            run(AggFunc::Avg, vec![def(2), Value::Undefined, def(4)]),
            def(3.0_f64)
        );
        assert!(run(AggFunc::Avg, vec![def(2), def(true)]).is_undefined());
        assert!(run(AggFunc::Avg, vec![]).is_undefined());
    }

    #[test]
    fn min_max_order_and_preserve_type() {
        assert_eq!(
            run(
                AggFunc::Min,
                vec![def(27.6_f64), def(71.76_f64), def(40.0_f64)]
            ),
            def(27.6_f64)
        );
        assert_eq!(
            run(
                AggFunc::Max,
                vec![def(27.6_f64), def(71.76_f64), def(40.0_f64)]
            ),
            def(71.76_f64)
        );
        // Undefined is skipped; the type of the winner is preserved.
        assert_eq!(
            run(AggFunc::Min, vec![Value::Undefined, def(5_i32), def(9_i32)]),
            def(5_i32)
        );
        // No poison rule — MIN/MAX range over mixed types via the total order.
        assert!(run(AggFunc::Min, vec![]).is_undefined());
        assert!(run(AggFunc::Max, vec![Value::Undefined]).is_undefined());
    }

    #[test]
    fn array_agg_gathers_defined_values_in_order() {
        // COLLECT is the synonym, so both names resolve to ArrayAgg.
        assert_eq!(AggFunc::from_name("ARRAY_AGG"), Some(AggFunc::ArrayAgg));
        assert_eq!(AggFunc::from_name("collect"), Some(AggFunc::ArrayAgg));

        // Values are gathered in arrival order, with their type preserved.
        assert_eq!(
            run(AggFunc::ArrayAgg, vec![def("a"), def(2_i32), def(3.5_f64)]),
            def(Bson::Array(vec![
                Bson::String("a".into()),
                Bson::Int32(2),
                Bson::Double(3.5),
            ]))
        );
    }

    #[test]
    fn array_agg_skips_undefined() {
        // Undefined rows (a missing field) are dropped, not collected as null.
        assert_eq!(
            run(
                AggFunc::ArrayAgg,
                vec![def(1_i32), Value::Undefined, def(2_i32)]
            ),
            def(Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]))
        );
        // A real null *is* a value and is kept (distinct from undefined).
        assert_eq!(
            run(
                AggFunc::ArrayAgg,
                vec![Value::Defined(Bson::Null), def(1_i32)]
            ),
            def(Bson::Array(vec![Bson::Null, Bson::Int32(1)]))
        );
    }

    #[test]
    fn array_agg_empty_group_is_empty_array() {
        // An empty group is `[]`, not undefined — so the group row keeps the slot.
        assert_eq!(run(AggFunc::ArrayAgg, vec![]), def(Bson::Array(vec![])));
        assert_eq!(
            run(AggFunc::ArrayAgg, vec![Value::Undefined]),
            def(Bson::Array(vec![]))
        );
    }
}
