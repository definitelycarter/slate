//! Validation: the semantic checks Cosmos applies before a query is planned.
//!
//! [`validate_bindings`] rejects unqualified identifiers (every property
//! reference must be bound, like `c.x`); [`validate_grouping`] enforces the
//! `GROUP BY` / aggregate column rule. Both run before [`lower`](crate::lower)
//! and surface a [`PlanError`]. The aggregate-detection helpers live here too,
//! shared with lowering's group/aggregate rewrite.

use slate_ast::{Expression, FromSource, Query, SelectClause};

/// A query the planner accepts syntactically but cannot plan.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanError {
    pub message: String,
}

/// Reject unqualified identifiers (Cosmos requires every property reference to be
/// bound — `SELECT id FROM c` is invalid; it must be `c.id`). An identifier is
/// valid only if it names a binding in scope (the `FROM`/`JOIN` aliases, plus the
/// enclosing aliases inside a subquery) or one of the special value words.
pub fn validate_bindings(query: &Query) -> Result<(), PlanError> {
    check_query(query, &[])
}

/// `undefined`/`NaN`/`Infinity` are value words, not bound identifiers — Cosmos
/// accepts them anywhere a value is expected.
fn is_special_ident(name: &str) -> bool {
    matches!(name, "undefined" | "NaN" | "Infinity")
}

fn check_query(query: &Query, outer: &[&str]) -> Result<(), PlanError> {
    let mut scope: Vec<&str> = outer.to_vec();
    if let Some(from) = &query.from {
        match &from.source {
            FromSource::ImplicitContainer { alias } => scope.push(alias),
            // The array is evaluated before its alias is bound.
            FromSource::Array { alias, array } => {
                check_expr(array, &scope)?;
                scope.push(alias);
            }
            // `base` names the container root (valid by construction); only the
            // bound `alias` enters scope for the rest of the query.
            FromSource::Subroot { alias, .. } => scope.push(alias),
        }
        for join in &from.joins {
            check_expr(&join.array, &scope)?;
            scope.push(&join.alias);
        }
    }
    match &query.select {
        SelectClause::Star => {}
        SelectClause::Value(e) => check_expr(e, &scope)?,
        SelectClause::Projections(items) => {
            for it in items {
                check_expr(&it.expr, &scope)?;
            }
        }
    }
    if let Some(f) = &query.filter {
        check_expr(f, &scope)?;
    }
    for k in &query.group_by {
        check_expr(k, &scope)?;
    }
    for o in &query.order_by {
        check_expr(&o.expr, &scope)?;
    }
    Ok(())
}

fn check_expr(expr: &Expression, scope: &[&str]) -> Result<(), PlanError> {
    match expr {
        Expression::Identifier(name) => {
            if scope.contains(&name.as_str()) || is_special_ident(name) || name.starts_with('$') {
                Ok(())
            } else {
                Err(PlanError {
                    message: format!(
                        "unqualified identifier `{name}` — Cosmos requires a bound path \
                         (did you mean an alias like `c.{name}`?)"
                    ),
                })
            }
        }
        Expression::Subquery { query, .. } => check_query(query, scope),
        Expression::Member { base, .. } => check_expr(base, scope),
        Expression::Index { base, index } => {
            check_expr(base, scope)?;
            check_expr(index, scope)
        }
        Expression::Unary { expr, .. } => check_expr(expr, scope),
        Expression::Binary { lhs, rhs, .. } => {
            check_expr(lhs, scope)?;
            check_expr(rhs, scope)
        }
        Expression::Function { args, .. } => {
            for a in args {
                check_expr(a, scope)?;
            }
            Ok(())
        }
        Expression::Object(fields) => {
            for (_, v) in fields {
                check_expr(v, scope)?;
            }
            Ok(())
        }
        Expression::Array(items) => {
            for i in items {
                check_expr(i, scope)?;
            }
            Ok(())
        }
        Expression::PathGet { base, .. } => check_expr(base, scope),
        Expression::MultikeyEq { base, value, .. } => {
            check_expr(base, scope)?;
            check_expr(value, scope)
        }
        // Literals, pre-converted values, and `@parameter`s bind no identifier.
        Expression::Literal(_) | Expression::Value(_) | Expression::Parameter(_) => Ok(()),
    }
}

/// Enforce the `GROUP BY`/aggregate column rule (Cosmos): when a query groups or
/// aggregates, every projected — and `ORDER BY` — expression must be built from
/// the group keys, aggregates, and constants. A bare row reference (a column
/// neither grouped nor inside an aggregate) is rejected, as is `SELECT *`.
pub fn validate_grouping(query: &Query) -> Result<(), PlanError> {
    let grouping = !query.group_by.is_empty()
        || select_has_aggregate(&query.select)
        || query.order_by.iter().any(|i| contains_aggregate(&i.expr));
    if !grouping {
        return Ok(());
    }

    // FROM-less aggregate (e.g. `SELECT COUNT(1)`) has no row bindings.
    let mut bindings: Vec<&str> = Vec::new();
    if let Some(from) = &query.from {
        match &from.source {
            FromSource::ImplicitContainer { alias }
            | FromSource::Array { alias, .. }
            | FromSource::Subroot { alias, .. } => bindings.push(alias.as_str()),
        }
        for join in &from.joins {
            bindings.push(join.alias.as_str());
        }
    }

    match &query.select {
        SelectClause::Star => {
            return Err(PlanError {
                message: "SELECT * is not allowed with GROUP BY or aggregates".into(),
            });
        }
        SelectClause::Value(e) => check_grounded(e, &query.group_by, &bindings)?,
        SelectClause::Projections(items) => {
            for it in items {
                check_grounded(&it.expr, &query.group_by, &bindings)?;
            }
        }
    }
    for item in &query.order_by {
        check_grounded(&item.expr, &query.group_by, &bindings)?;
    }
    Ok(())
}

/// Whether the projection contains an aggregate call.
fn select_has_aggregate(select: &SelectClause) -> bool {
    match select {
        SelectClause::Value(e) => contains_aggregate(e),
        SelectClause::Projections(items) => items.iter().any(|it| contains_aggregate(&it.expr)),
        SelectClause::Star => false,
    }
}

/// Check one expression against the grouping rule: it's fine if it equals a
/// group key, is an aggregate call (its arguments are evaluated per row, so we
/// don't descend), or is built only from constants and such. A reference to a
/// binding (the `FROM`/`JOIN` alias) that isn't a group key is the violation.
fn check_grounded(
    expr: &Expression,
    group_keys: &[Expression],
    bindings: &[&str],
) -> Result<(), PlanError> {
    if group_keys.iter().any(|k| k == expr) {
        return Ok(());
    }
    match expr {
        Expression::Function { name, .. } if is_aggregate_name(name) => Ok(()),
        // A subquery is self-contained (its inner correlation is its own scope),
        // like an aggregate — it's a valid grouped projection.
        Expression::Subquery { .. } => Ok(()),
        Expression::Identifier(name) if bindings.contains(&name.as_str()) => Err(PlanError {
            message: format!("'{name}' must appear in GROUP BY or be used in an aggregate"),
        }),
        Expression::Identifier(_)
        | Expression::Literal(_)
        | Expression::Value(_)
        | Expression::Parameter(_) => Ok(()),
        Expression::Member { base, .. } | Expression::PathGet { base, .. } => {
            check_grounded(base, group_keys, bindings)
        }
        Expression::Index { base, index } => {
            check_grounded(base, group_keys, bindings)?;
            check_grounded(index, group_keys, bindings)
        }
        Expression::Unary { expr, .. } => check_grounded(expr, group_keys, bindings),
        Expression::Binary { lhs, rhs, .. } => {
            check_grounded(lhs, group_keys, bindings)?;
            check_grounded(rhs, group_keys, bindings)
        }
        Expression::MultikeyEq { base, value, .. } => {
            check_grounded(base, group_keys, bindings)?;
            check_grounded(value, group_keys, bindings)
        }
        Expression::Function { args, .. } => {
            for a in args {
                check_grounded(a, group_keys, bindings)?;
            }
            Ok(())
        }
        Expression::Object(fields) => {
            for (_, v) in fields {
                check_grounded(v, group_keys, bindings)?;
            }
            Ok(())
        }
        Expression::Array(items) => {
            for i in items {
                check_grounded(i, group_keys, bindings)?;
            }
            Ok(())
        }
    }
}

/// Whether `name` (case-insensitive) is an aggregate function.
pub(crate) fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
    )
}

/// Read-only check for whether `expr` mentions any aggregate, so lowering can
/// skip the (allocating) rewrite for the common non-aggregate projection.
pub(crate) fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::Function { name, args } => {
            is_aggregate_name(name) || args.iter().any(contains_aggregate)
        }
        Expression::Binary { lhs, rhs, .. } => contains_aggregate(lhs) || contains_aggregate(rhs),
        Expression::Unary { expr, .. } => contains_aggregate(expr),
        Expression::Member { base, .. } => contains_aggregate(base),
        Expression::Index { base, index } => contains_aggregate(base) || contains_aggregate(index),
        Expression::Object(fields) => fields.iter().any(|(_, v)| contains_aggregate(v)),
        Expression::Array(items) => items.iter().any(contains_aggregate),
        _ => false,
    }
}
