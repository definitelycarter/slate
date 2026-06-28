//! The `Aggregate` node — `GROUP BY` and aggregate functions.
//!
//! A *blocking* transform: it buffers the source, groups rows by the group-key
//! expressions (no keys → one group over the whole input), folds each
//! aggregate's [`Accumulator`] per group, and emits one **environment** row per
//! group of the form `{ $key0: …, $agg0: … }`. With no group keys and an empty
//! input it still emits a single row, so `COUNT` is `0`. The downstream
//! `Project` reads the `$keyN`/`$aggN` slots.

use bson::{Document, RawBson, RawDocumentBuf};
use slate_eval::agg::{Accumulator, AggFunc};
use slate_eval::eval::order_values;
use slate_eval::raweval;
use slate_eval::{EvalError, Value};
use slate_planner::{AggregateExpr, GroupKey, RowBinding};

use super::env::with_row_env;
use crate::budget;
use crate::{ExecEnv, ExecError, ValueIter};

/// Buffer `source`, group by `group_keys`, accumulate `aggregates`, emit one
/// environment row per group.
pub(crate) fn execute<'a>(
    group_keys: Vec<GroupKey>,
    aggregates: Vec<AggregateExpr>,
    binding: RowBinding,
    source: ValueIter<'a>,
    env: ExecEnv<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    // Resolve each aggregate's function name once (the planner only routes real
    // aggregates here, so an unknown name is an internal error).
    let funcs: Vec<AggFunc> = aggregates
        .iter()
        .map(|a| {
            AggFunc::from_name(&a.func).ok_or_else(|| EvalError {
                message: format!("not an aggregate function: {}", a.func),
            })
        })
        .collect::<Result<_, _>>()?;

    // Each group is its key tuple plus one accumulator per aggregate.
    let mut groups: Vec<(Vec<Value>, Vec<Accumulator>)> = Vec::new();
    // OOM guard (Resource Limits RFC, B): cap the *rows consumed*, which bounds
    // both the group vector and the per-group accumulator buffers (e.g. an
    // ARRAY_AGG folding every row into a single group) — the one number that
    // bounds the aggregate's total materialization.
    let mut consumed = 0usize;

    for item in source {
        let Some(row) = item? else { continue };
        consumed += 1;
        budget::check_cap(consumed, env.materialization_cap, "GroupBy")?;
        let (keys, args) = with_row_env(&row, &binding, &env, |renv| {
            let mut keys = Vec::with_capacity(group_keys.len());
            for gk in &group_keys {
                keys.push(raweval::eval(&gk.expr, renv)?.into_value()?);
            }
            let mut args = Vec::with_capacity(aggregates.len());
            for agg in &aggregates {
                args.push(raweval::eval(&agg.arg, renv)?.into_value()?);
            }
            Ok((keys, args))
        })?;

        let idx = match groups.iter().position(|(k, _)| keys_eq(k, &keys)) {
            Some(i) => i,
            None => {
                groups.push((keys, funcs.iter().map(|f| f.accumulator()).collect()));
                groups.len() - 1
            }
        };
        for (acc, value) in groups[idx].1.iter_mut().zip(args) {
            acc.accumulate(value);
        }
    }

    // A bare aggregate (no group keys) over an empty input still yields one row.
    if group_keys.is_empty() && groups.is_empty() {
        groups.push((Vec::new(), funcs.iter().map(|f| f.accumulator()).collect()));
    }

    let mut out: Vec<RawBson> = Vec::with_capacity(groups.len());
    for (keys, accs) in groups {
        // Build the environment row; undefined slot values are omitted, matching
        // how undefined is dropped from documents everywhere else.
        let mut doc = Document::new();
        for (gk, key) in group_keys.iter().zip(keys) {
            if let Value::Defined(b) = key {
                doc.insert(gk.slot.as_str(), b);
            }
        }
        for (agg, acc) in aggregates.iter().zip(accs) {
            if let Value::Defined(b) = acc.finalize() {
                doc.insert(agg.slot.as_str(), b);
            }
        }
        let raw = RawDocumentBuf::try_from(&doc).map_err(|e| EvalError {
            message: format!("could not build aggregate row: {e}"),
        })?;
        out.push(RawBson::Document(raw));
    }

    Ok(Box::new(out.into_iter().map(|v| Ok(Some(v)))))
}

/// Two group-key tuples are equal when each pair compares equal under the shared
/// total order (so numeric types coerce and undefined groups with undefined).
fn keys_eq(a: &[Value], b: &[Value]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| order_values(x, y) == std::cmp::Ordering::Equal)
}
