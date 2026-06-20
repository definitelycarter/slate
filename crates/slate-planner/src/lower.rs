//! Lowering: a parsed [`slate_ast::Query`] into an executable [`Plan`].
//!
//! The query's `FROM` clause supplies only the alias — the container is chosen
//! by the caller (matching Cosmos) and passed in as `container`, along with its
//! index metadata ([`CollectionMeta`]) so the planner can choose an index.
//!
//! Pipeline shape:
//!
//! ```text
//! FROM c            → <source> → Bind(c)       (source = Scan, or an index path)
//! JOIN t IN <arr>   → Unwind(t, <arr>)
//! <residual WHERE>  → Filter(e)                (the non-sargable remainder)
//! ORDER BY ...      → Sort(...)
//! SELECT VALUE <e>  → Project(e)
//! OFFSET/LIMIT      → Limit(...)
//! ```
//!
//! ## Sargability
//!
//! The `WHERE` predicate is split: conjuncts of the form `c.<indexed-field>
//! <cmp> <literal>` are *pushed* into an `IndexScan` (wrapped by `KeyLookup` to
//! fetch documents), and primary-key equality becomes a direct `KeyLookup`.
//! Everything else stays a residual `Filter`. A predicate that wraps the field
//! in a computation (`UPPER(c.x) = ...`, `c.x + 1 = ...`) is not sargable and
//! falls through to the residual.

use bson::{Bson, RawBson, RawDocumentBuf};
use slate_ast::{
    BinOp, FromClause, FromSource, Literal, OrderByItem, Query, ScalarExpr, SelectClause,
    SubqueryKind,
};

use crate::plan::{
    AggregateExpr, CollectionRef, GroupKey, IndexScanRange, LogicalOp, Node, Plan, RowBinding,
    ScanDirection,
};

/// Index metadata for the queried collection, used to choose a scan source.
#[derive(Debug, Clone, Default)]
pub struct CollectionMeta {
    /// Indexed field paths (e.g. `"age"`, `"address.city"`).
    pub indexes: Vec<String>,
    /// Primary-key field path (e.g. `"_id"`).
    pub pk_path: String,
}

/// Lower `query` into a plan that reads from `container`.
pub fn lower(query: Query, container: CollectionRef, meta: &CollectionMeta) -> Plan {
    Plan::Query(lower_query(query, container, meta, &[]))
}

/// A subquery used directly as a FROM/JOIN iteration source is *multi-value*:
/// it yields the set of rows to unwind, so it must reduce as an array regardless
/// of the parser's default (a parenthesized `(SELECT …)` is tagged scalar).
fn as_iteration_source(expr: ScalarExpr) -> ScalarExpr {
    match expr {
        ScalarExpr::Subquery { query, .. } => ScalarExpr::Subquery {
            query,
            kind: SubqueryKind::Array,
        },
        other => other,
    }
}

/// A subquery pulled out of an expression: its result binds to `slot`, computed
/// by running `subplan` per outer row and reducing by `kind`.
struct SubquerySpec {
    slot: String,
    kind: SubqueryKind,
    subplan: Node,
}

/// Lower a query (outer or a subquery's inner query) into a [`Node`] subtree.
/// The outer query reads its container via a `Scan`/index source; a subquery's
/// `FROM x IN <array>` reads an in-document array via `Unwind` over a
/// [`Node::CurrentRow`] (the correlated outer row).
fn lower_query(
    query: Query,
    container: CollectionRef,
    meta: &CollectionMeta,
    outer: &[&str],
) -> Node {
    let Query {
        select,
        from,
        filter,
        group_by,
        order_by,
        offset,
        limit,
    } = query;

    // Query-wide counter for subquery slot names (`$subN`), shared across the
    // FROM/JOIN sources and the projection so every slot is unique.
    let mut next_slot = 0usize;

    // The base source: a container scan/index path, or — for a subquery — an
    // `Unwind` of the correlated array over the outer row (`CurrentRow`), or —
    // for a FROM-less query — a single empty environment row evaluated once. The
    // array and FROM-less sources already yield an environment row; the container
    // source doesn't until a `Bind`.
    let (alias, mut node, residual, mut is_env, joins) = match from {
        // FROM-less (`SELECT VALUE 1`): one empty row, no bindings. `WHERE` (if
        // any) filters that single row; `SELECT *` is rejected by the front-end.
        None => (
            String::new(),
            Node::Values(vec![RawBson::Document(RawDocumentBuf::new())]),
            filter,
            true,
            Vec::new(),
        ),
        Some(FromClause { source, joins }) => match source {
            // A subquery whose `FROM` names an enclosing alias is *item-scoped*:
            // it iterates that single bound value, which the outer row already
            // carries — so the source is just `CurrentRow`, not a container
            // re-scan. (Top-level `FROM c` has an empty outer scope → scan.)
            FromSource::ImplicitContainer { alias } if outer.contains(&alias.as_str()) => {
                (alias, Node::CurrentRow, filter, true, joins)
            }
            FromSource::ImplicitContainer { alias } => {
                let (source, residual) = plan_source(filter, &alias, &container, meta);
                (alias, source, residual, false, joins)
            }
            FromSource::Array { alias, array } => {
                // The array expression may itself be a subquery (a nested
                // `FROM x IN (SELECT …)`), multi-value here; extract it over the
                // correlated row.
                let mut subs = Vec::new();
                let array = extract_subqueries(
                    as_iteration_source(array),
                    &container,
                    meta,
                    &mut subs,
                    outer,
                    &mut next_slot,
                );
                let mut src = Node::CurrentRow;
                for spec in subs {
                    src = Node::Subquery {
                        slot: spec.slot,
                        kind: spec.kind,
                        subplan: Box::new(spec.subplan),
                        source: Box::new(src),
                    };
                }
                let source = Node::Unwind {
                    alias: alias.clone(),
                    array,
                    source: Box::new(src),
                };
                (alias, source, filter, true, joins)
            }
        },
    };

    // GROUP BY keys, each bound to a `$keyN` slot in the aggregation output.
    let group_keys: Vec<GroupKey> = group_by
        .into_iter()
        .enumerate()
        .map(|(i, expr)| GroupKey {
            slot: format!("$key{i}"),
            expr,
        })
        .collect();

    // Pull subqueries out of the projection and the residual WHERE into `$subN`
    // slots, lowering each inner query to its own subtree. Done before the
    // group/aggregate rewrite so a subquery inside an aggregate's argument
    // becomes a slot the aggregate then reads.
    let value_expr = select.into_value_expr(&alias);
    let mut subqueries: Vec<SubquerySpec> = Vec::new();
    // Bindings a nested subquery can reference: the enclosing scope plus this
    // query's own alias and joins. Borrows `alias`/`joins`, used only here
    // (before either is moved below).
    let mut scope: Vec<&str> = outer.to_vec();
    if !alias.is_empty() {
        scope.push(&alias);
    }
    for j in &joins {
        scope.push(&j.alias);
    }
    let value_expr = extract_subqueries(
        value_expr,
        &container,
        meta,
        &mut subqueries,
        &scope,
        &mut next_slot,
    );
    let residual = residual
        .map(|p| extract_subqueries(p, &container, meta, &mut subqueries, &scope, &mut next_slot));

    // Resolve the projection: a whole sub-expression equal to a group key
    // becomes its `$keyN` slot, and each `AGG(arg)` becomes a `$aggN` slot. The
    // read-only pre-check keeps `find` and non-aggregate SQL allocation-free.
    let aggregating = !group_keys.is_empty()
        || contains_aggregate(&value_expr)
        || order_by.iter().any(|item| contains_aggregate(&item.expr));
    let (project_expr, order_by, aggregates) = if aggregating {
        let mut aggregates = Vec::new();
        let project_expr = rewrite_projection(value_expr, &group_keys, &mut aggregates);
        // ORDER BY runs after aggregation, so its keys reference the group keys
        // and aggregates — rewrite them into the same `$keyN`/`$aggN` slots (an
        // aggregate appearing only in ORDER BY is still computed by the node).
        let order_by: Vec<OrderByItem> = order_by
            .into_iter()
            .map(|item| OrderByItem {
                expr: rewrite_projection(item.expr, &group_keys, &mut aggregates),
                direction: item.direction,
            })
            .collect();
        (project_expr, order_by, aggregates)
    } else {
        (value_expr, order_by, Vec::new())
    };

    // JOIN ... IN — each `Unwind` extends the environment. The first join (or a
    // subquery below) forces the environment shape via `Bind`.
    if !joins.is_empty() {
        if !is_env {
            node = Node::Bind {
                alias: alias.clone(),
                source: Box::new(node),
            };
            is_env = true;
        }
        // A join's array can be a subquery (`JOIN j IN (SELECT …)`) and can
        // reference the FROM alias and earlier join aliases, so the visible scope
        // grows as we go. Any subquery is applied just before this join's unwind.
        let mut jscope: Vec<String> = outer.iter().map(|s| s.to_string()).collect();
        if !alias.is_empty() {
            jscope.push(alias.clone());
        }
        for join in joins {
            let scope_refs: Vec<&str> = jscope.iter().map(String::as_str).collect();
            let mut subs = Vec::new();
            let array = extract_subqueries(
                as_iteration_source(join.array),
                &container,
                meta,
                &mut subs,
                &scope_refs,
                &mut next_slot,
            );
            for spec in subs {
                node = Node::Subquery {
                    slot: spec.slot,
                    kind: spec.kind,
                    subplan: Box::new(spec.subplan),
                    source: Box::new(node),
                };
            }
            node = Node::Unwind {
                alias: join.alias.clone(),
                array,
                source: Box::new(node),
            };
            jscope.push(join.alias);
        }
    }

    // Correlated-apply nodes for the extracted subqueries — each augments the
    // row with its `$subN` slot, so the environment shape is required.
    if !subqueries.is_empty() {
        if !is_env {
            node = Node::Bind {
                alias: alias.clone(),
                source: Box::new(node),
            };
            is_env = true;
        }
        for spec in subqueries {
            node = Node::Subquery {
                slot: spec.slot,
                kind: spec.kind,
                subplan: Box::new(spec.subplan),
                source: Box::new(node),
            };
        }
    }

    let binding = if is_env {
        RowBinding::Env
    } else {
        RowBinding::Alias(alias)
    };

    // Residual WHERE  →  Filter
    if let Some(predicate) = residual {
        node = Node::Filter {
            predicate,
            binding: binding.clone(),
            source: Box::new(node),
        };
    }

    if aggregating {
        // Aggregation collapses rows into one per group; ORDER BY then sorts the
        // group rows, and the projection (rewritten to read the `$keyN`/`$aggN`
        // slots) shapes each. Both bind as `Env` over the aggregate output rows.
        node = Node::Aggregate {
            group_keys,
            aggregates,
            binding,
            source: Box::new(node),
        };
        if !order_by.is_empty() {
            node = Node::Sort {
                keys: order_by,
                binding: RowBinding::Env,
                source: Box::new(node),
            };
        }
        node = Node::Project {
            expr: project_expr,
            binding: RowBinding::Env,
            source: Box::new(node),
        };
    } else {
        // ORDER BY  →  Sort (before projection — keys reference the row environment)
        if !order_by.is_empty() {
            node = Node::Sort {
                keys: order_by,
                binding: binding.clone(),
                source: Box::new(node),
            };
        }

        // SELECT ...  →  Project (resolved above)
        node = Node::Project {
            expr: project_expr,
            binding,
            source: Box::new(node),
        };
    }

    // OFFSET / LIMIT  →  Limit (after projection — on result rows)
    if offset.is_some() || limit.is_some() {
        node = Node::Limit {
            skip: offset.unwrap_or(0) as usize,
            take: limit.map(|n| n as usize),
            source: Box::new(node),
        };
    }

    node
}

/// Pull subqueries out of `expr`: each `(SELECT …)`/`EXISTS`/`ARRAY` becomes a
/// reference to a fresh `$subN` slot and is recorded in `out` with its inner
/// query lowered to a subtree. Every other sub-expression is rebuilt unchanged.
fn extract_subqueries(
    expr: ScalarExpr,
    container: &CollectionRef,
    meta: &CollectionMeta,
    out: &mut Vec<SubquerySpec>,
    outer: &[&str],
    next: &mut usize,
) -> ScalarExpr {
    match expr {
        ScalarExpr::Subquery { query, kind } => {
            let subplan = lower_query(*query, container.clone(), meta, outer);
            // A query-wide counter keeps slot names unique across every position
            // (projection, WHERE, JOIN sources), so a later subquery can't shadow
            // an earlier slot in the row environment.
            let slot = format!("$sub{next}");
            *next += 1;
            out.push(SubquerySpec {
                slot: slot.clone(),
                kind,
                subplan,
            });
            ScalarExpr::Identifier(slot)
        }
        ScalarExpr::Binary { op, lhs, rhs } => ScalarExpr::Binary {
            op,
            lhs: Box::new(extract_subqueries(*lhs, container, meta, out, outer, next)),
            rhs: Box::new(extract_subqueries(*rhs, container, meta, out, outer, next)),
        },
        ScalarExpr::Unary { op, expr } => ScalarExpr::Unary {
            op,
            expr: Box::new(extract_subqueries(*expr, container, meta, out, outer, next)),
        },
        ScalarExpr::Member { base, field } => ScalarExpr::Member {
            base: Box::new(extract_subqueries(*base, container, meta, out, outer, next)),
            field,
        },
        ScalarExpr::Index { base, index } => ScalarExpr::Index {
            base: Box::new(extract_subqueries(*base, container, meta, out, outer, next)),
            index: Box::new(extract_subqueries(
                *index, container, meta, out, outer, next,
            )),
        },
        ScalarExpr::Function { name, args } => ScalarExpr::Function {
            name,
            args: args
                .into_iter()
                .map(|a| extract_subqueries(a, container, meta, out, outer, next))
                .collect(),
        },
        ScalarExpr::Object(fields) => ScalarExpr::Object(
            fields
                .into_iter()
                .map(|(k, v)| (k, extract_subqueries(v, container, meta, out, outer, next)))
                .collect(),
        ),
        ScalarExpr::Array(items) => ScalarExpr::Array(
            items
                .into_iter()
                .map(|i| extract_subqueries(i, container, meta, out, outer, next))
                .collect(),
        ),
        // Leaves and Mongo-only constructs hold no SQL subqueries.
        other => other,
    }
}

// ── Aggregation ─────────────────────────────────────────────────

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

fn check_expr(expr: &ScalarExpr, scope: &[&str]) -> Result<(), PlanError> {
    match expr {
        ScalarExpr::Identifier(name) => {
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
        ScalarExpr::Subquery { query, .. } => check_query(query, scope),
        ScalarExpr::Member { base, .. } => check_expr(base, scope),
        ScalarExpr::Index { base, index } => {
            check_expr(base, scope)?;
            check_expr(index, scope)
        }
        ScalarExpr::Unary { expr, .. } => check_expr(expr, scope),
        ScalarExpr::Binary { lhs, rhs, .. } => {
            check_expr(lhs, scope)?;
            check_expr(rhs, scope)
        }
        ScalarExpr::Function { args, .. } => {
            for a in args {
                check_expr(a, scope)?;
            }
            Ok(())
        }
        ScalarExpr::Object(fields) => {
            for (_, v) in fields {
                check_expr(v, scope)?;
            }
            Ok(())
        }
        ScalarExpr::Array(items) => {
            for i in items {
                check_expr(i, scope)?;
            }
            Ok(())
        }
        ScalarExpr::PathGet { base, .. } => check_expr(base, scope),
        ScalarExpr::MultikeyEq { base, value, .. } => {
            check_expr(base, scope)?;
            check_expr(value, scope)
        }
        // Literals, pre-converted values, and `@parameter`s bind no identifier.
        ScalarExpr::Literal(_) | ScalarExpr::Value(_) | ScalarExpr::Parameter(_) => Ok(()),
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
            FromSource::ImplicitContainer { alias } | FromSource::Array { alias, .. } => {
                bindings.push(alias.as_str())
            }
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
    expr: &ScalarExpr,
    group_keys: &[ScalarExpr],
    bindings: &[&str],
) -> Result<(), PlanError> {
    if group_keys.iter().any(|k| k == expr) {
        return Ok(());
    }
    match expr {
        ScalarExpr::Function { name, .. } if is_aggregate_name(name) => Ok(()),
        // A subquery is self-contained (its inner correlation is its own scope),
        // like an aggregate — it's a valid grouped projection.
        ScalarExpr::Subquery { .. } => Ok(()),
        ScalarExpr::Identifier(name) if bindings.contains(&name.as_str()) => Err(PlanError {
            message: format!("'{name}' must appear in GROUP BY or be used in an aggregate"),
        }),
        ScalarExpr::Identifier(_)
        | ScalarExpr::Literal(_)
        | ScalarExpr::Value(_)
        | ScalarExpr::Parameter(_) => Ok(()),
        ScalarExpr::Member { base, .. } | ScalarExpr::PathGet { base, .. } => {
            check_grounded(base, group_keys, bindings)
        }
        ScalarExpr::Index { base, index } => {
            check_grounded(base, group_keys, bindings)?;
            check_grounded(index, group_keys, bindings)
        }
        ScalarExpr::Unary { expr, .. } => check_grounded(expr, group_keys, bindings),
        ScalarExpr::Binary { lhs, rhs, .. } => {
            check_grounded(lhs, group_keys, bindings)?;
            check_grounded(rhs, group_keys, bindings)
        }
        ScalarExpr::MultikeyEq { base, value, .. } => {
            check_grounded(base, group_keys, bindings)?;
            check_grounded(value, group_keys, bindings)
        }
        ScalarExpr::Function { args, .. } => {
            for a in args {
                check_grounded(a, group_keys, bindings)?;
            }
            Ok(())
        }
        ScalarExpr::Object(fields) => {
            for (_, v) in fields {
                check_grounded(v, group_keys, bindings)?;
            }
            Ok(())
        }
        ScalarExpr::Array(items) => {
            for i in items {
                check_grounded(i, group_keys, bindings)?;
            }
            Ok(())
        }
    }
}

/// Whether `name` (case-insensitive) is an aggregate function.
fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
    )
}

/// Read-only check for whether `expr` mentions any aggregate, so lowering can
/// skip the (allocating) rewrite for the common non-aggregate projection.
fn contains_aggregate(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::Function { name, args } => {
            is_aggregate_name(name) || args.iter().any(contains_aggregate)
        }
        ScalarExpr::Binary { lhs, rhs, .. } => contains_aggregate(lhs) || contains_aggregate(rhs),
        ScalarExpr::Unary { expr, .. } => contains_aggregate(expr),
        ScalarExpr::Member { base, .. } => contains_aggregate(base),
        ScalarExpr::Index { base, index } => contains_aggregate(base) || contains_aggregate(index),
        ScalarExpr::Object(fields) => fields.iter().any(|(_, v)| contains_aggregate(v)),
        ScalarExpr::Array(items) => items.iter().any(contains_aggregate),
        _ => false,
    }
}

/// Rewrite a projection for aggregation: a whole sub-expression equal to a
/// group key becomes a reference to its `$keyN` slot; each `AGG(arg)` becomes a
/// `$aggN` slot recorded in `out`; every other sub-expression is rebuilt
/// unchanged. Aggregate arguments aren't re-scanned — aggregates don't nest, and
/// they're evaluated per-row inside the aggregation node, not per-group.
fn rewrite_projection(
    expr: ScalarExpr,
    group_keys: &[GroupKey],
    out: &mut Vec<AggregateExpr>,
) -> ScalarExpr {
    // A whole sub-expression that matches a group key reads from its slot.
    if let Some(key) = group_keys.iter().find(|k| k.expr == expr) {
        return ScalarExpr::Identifier(key.slot.clone());
    }
    match expr {
        ScalarExpr::Function { name, args } if is_aggregate_name(&name) => {
            let arg = args
                .into_iter()
                .next()
                .unwrap_or(ScalarExpr::Literal(Literal::Int(1)));
            let slot = format!("$agg{}", out.len());
            out.push(AggregateExpr {
                func: name,
                arg,
                slot: slot.clone(),
            });
            ScalarExpr::Identifier(slot)
        }
        ScalarExpr::Function { name, args } => ScalarExpr::Function {
            name,
            args: args
                .into_iter()
                .map(|a| rewrite_projection(a, group_keys, out))
                .collect(),
        },
        ScalarExpr::Binary { op, lhs, rhs } => ScalarExpr::Binary {
            op,
            lhs: Box::new(rewrite_projection(*lhs, group_keys, out)),
            rhs: Box::new(rewrite_projection(*rhs, group_keys, out)),
        },
        ScalarExpr::Unary { op, expr } => ScalarExpr::Unary {
            op,
            expr: Box::new(rewrite_projection(*expr, group_keys, out)),
        },
        ScalarExpr::Member { base, field } => ScalarExpr::Member {
            base: Box::new(rewrite_projection(*base, group_keys, out)),
            field,
        },
        ScalarExpr::Index { base, index } => ScalarExpr::Index {
            base: Box::new(rewrite_projection(*base, group_keys, out)),
            index: Box::new(rewrite_projection(*index, group_keys, out)),
        },
        ScalarExpr::Object(fields) => ScalarExpr::Object(
            fields
                .into_iter()
                .map(|(k, v)| (k, rewrite_projection(v, group_keys, out)))
                .collect(),
        ),
        ScalarExpr::Array(items) => ScalarExpr::Array(
            items
                .into_iter()
                .map(|i| rewrite_projection(i, group_keys, out))
                .collect(),
        ),
        // Leaves and Mongo-only constructs — no SQL aggregates nested inside.
        other => other,
    }
}

// ── Sargability ─────────────────────────────────────────────────

/// Decide the scan source and the residual (non-pushed) predicate.
///
/// - A top-level `OR` whose every branch is indexable → `IndexMerge(Or)` (with
///   the full predicate kept as a residual recheck).
/// - Otherwise the predicate is treated as a conjunction: pk equality wins
///   (direct key lookup); else each indexed field's atoms become one
///   `IndexScan` (range bounds combined), and `OR` sub-groups become
///   `IndexMerge(Or)` — all intersected via `IndexMerge(And)` when more than
///   one applies. Consumed atoms leave the residual; the rest stay a `Filter`.
fn plan_source(
    filter: Option<ScalarExpr>,
    alias: &str,
    container: &CollectionRef,
    meta: &CollectionMeta,
) -> (Node, Option<ScalarExpr>) {
    let Some(expr) = filter else {
        return (scan(container), None);
    };

    // Top-level OR → IndexMerge(Or) when fully indexable; recheck the full OR.
    // A whole-filter Mongo implicit-equality (`f = v OR ARRAY_CONTAINS(f, v)`)
    // is *not* a real disjunction — it is sargable as an equality on `f`, so it
    // falls through to the conjunction path below rather than taking this route.
    if matches!(expr, ScalarExpr::Binary { op: BinOp::Or, .. })
        && as_mongo_eq(&expr, alias).is_none()
    {
        return match index_source_for(&expr, alias, container, meta) {
            Some(ids) => (key_lookup(container, ids), Some(expr)),
            None => (scan(container), Some(expr)),
        };
    }

    let mut conjuncts = Vec::new();
    flatten_and(expr, &mut conjuncts);

    // Priority 1: primary-key equality (a plain `Eq` or the Mongo idiom) →
    // direct point lookup. The pk is never an array, so the `ARRAY_CONTAINS`
    // branch is always false and the equality is exact (safe to consume).
    for i in 0..conjuncts.len() {
        if let Some(value) = pk_eq_value(&conjuncts[i], alias, &meta.pk_path)
            && let Some(id) = bson_to_raw(&value)
        {
            let source = Node::KeyLookup {
                collection: container.clone(),
                source: Box::new(Node::Values(vec![id])),
            };
            return (source, residual_excluding(conjuncts, &[i]));
        }
    }

    // Collect ID sources from conjuncts, tracking which conjuncts are consumed.
    let mut sources: Vec<Node> = Vec::new();
    let mut consumed: Vec<usize> = Vec::new();

    // Per indexed field: combine its atoms into a single IndexScan.
    for field in &meta.indexes {
        if let Some((scan, used)) = field_index_scan(&conjuncts, &consumed, alias, container, field)
        {
            sources.push(scan);
            consumed.extend(used);
        }
    }

    // Mongo implicit-equality on a scalar-indexed field → an index Eq lookup,
    // and the conjunct is *consumed* (no residual recheck). A scalar index holds
    // only scalar entries — an array-valued field produces none (see
    // `index_record::extract_all`) — so every candidate the `Eq` returns already
    // has `field == value`; the idiom's recheck (`field = v OR ARRAY_CONTAINS`)
    // is therefore always true over the candidate set and is pure per-row
    // overhead. (Array-valued documents are absent from a scalar index whether
    // or not the recheck runs, so consuming it changes no results — pinned by
    // the v1↔v2 differential `diff_fuzz`/`parity_audit`.)
    //
    // Multikey (`.[]`) equality is handled separately below and stays a recheck.
    for (i, conjunct) in conjuncts.iter().enumerate() {
        if consumed.contains(&i) {
            continue;
        }
        if let Some((field, value)) = as_mongo_eq(conjunct, alias)
            && meta.indexes.contains(&field)
        {
            sources.push(index_scan(container, &field, IndexScanRange::Eq(value)));
            consumed.push(i);
        }
    }

    // Explicit multikey equality on an indexed `.[]` path → index Eq lookup
    // (kept as a residual recheck, like the Mongo idiom above). The index name
    // is the verbatim `.[]` path the predicate carries.
    for conjunct in &conjuncts {
        if let Some((field, value)) = as_multikey_eq(conjunct, alias)
            && meta.indexes.contains(&field)
        {
            sources.push(index_scan(container, &field, IndexScanRange::Eq(value)));
        }
    }

    // OR sub-groups that are fully indexable become IndexMerge(Or) inputs.
    // The conjunct is NOT consumed: it stays as a residual recheck, because an
    // index merge can over-return (e.g. a range bound against a field holding
    // mixed numeric types). The index narrows the candidate set; the recheck
    // keeps the result precise — matching how the top-level-OR path behaves.
    for (i, conjunct) in conjuncts.iter().enumerate() {
        if consumed.contains(&i) {
            continue;
        }
        if as_mongo_eq(conjunct, alias).is_some() {
            continue; // already handled above
        }
        if matches!(conjunct, ScalarExpr::Binary { op: BinOp::Or, .. })
            && let Some(ids) = index_source_for(conjunct, alias, container, meta)
        {
            sources.push(ids);
        }
    }

    let residual = residual_excluding(conjuncts, &consumed);
    match merge_sources(container, LogicalOp::And, sources) {
        Some(ids) => (key_lookup(container, ids), residual),
        None => (scan(container), residual),
    }
}

/// Build one `IndexScan` covering all of `field`'s atoms (`Eq` wins; otherwise
/// range bounds are combined). Returns the scan and the consumed conjunct
/// indices, or `None` if `field` has no usable atom here.
fn field_index_scan(
    conjuncts: &[ScalarExpr],
    consumed: &[usize],
    alias: &str,
    container: &CollectionRef,
    field: &str,
) -> Option<(Node, Vec<usize>)> {
    let mut eq: Option<(Bson, usize)> = None;
    let mut lower: Option<(Bson, bool, usize)> = None;
    let mut upper: Option<(Bson, bool, usize)> = None;

    for (i, conjunct) in conjuncts.iter().enumerate() {
        if consumed.contains(&i) {
            continue;
        }
        let Some((f, op, value)) = as_atom(conjunct, alias) else {
            continue;
        };
        if f != field {
            continue;
        }
        match op {
            BinOp::Eq if eq.is_none() => eq = Some((value, i)),
            BinOp::Gt if lower.is_none() => lower = Some((value, false, i)),
            BinOp::Gte if lower.is_none() => lower = Some((value, true, i)),
            BinOp::Lt if upper.is_none() => upper = Some((value, false, i)),
            BinOp::Lte if upper.is_none() => upper = Some((value, true, i)),
            _ => {}
        }
    }

    if let Some((value, i)) = eq {
        // Eq is the most selective; leave any range atoms to the residual.
        return Some((
            index_scan(container, field, IndexScanRange::Eq(value)),
            vec![i],
        ));
    }
    if lower.is_none() && upper.is_none() {
        return None;
    }
    let mut used = Vec::new();
    let lo = lower.map(|(v, incl, i)| {
        used.push(i);
        (v, incl)
    });
    let hi = upper.map(|(v, incl, i)| {
        used.push(i);
        (v, incl)
    });
    Some((
        index_scan(
            container,
            field,
            IndexScanRange::Range {
                lower: lo,
                upper: hi,
            },
        ),
        used,
    ))
}

/// Build an ID-yielding source for a single predicate, or `None` if it is not
/// fully indexable: an atom on an indexed field → `IndexScan`; an `OR` whose
/// every branch is indexable → `IndexMerge(Or)`.
fn index_source_for(
    expr: &ScalarExpr,
    alias: &str,
    container: &CollectionRef,
    meta: &CollectionMeta,
) -> Option<Node> {
    // The Mongo implicit-equality idiom (`field = lit OR ARRAY_CONTAINS(field,
    // lit)`) is itself an `Or`. Recognize it as a single indexed Eq *before*
    // treating a generic `Or` as a disjunction to merge — otherwise flattening
    // would split it and expose the lone, un-indexable `ARRAY_CONTAINS` branch,
    // poisoning the whole merge (so an OR/IN of `{field: value}` fell back to a
    // full scan).
    if let Some((field, value)) = as_mongo_eq(expr, alias) {
        return meta
            .indexes
            .contains(&field)
            .then(|| index_scan(container, &field, IndexScanRange::Eq(value)));
    }

    if matches!(expr, ScalarExpr::Binary { op: BinOp::Or, .. }) {
        let mut branches = Vec::new();
        collect_or(expr, alias, &mut branches);
        let mut sources = Vec::with_capacity(branches.len());
        for branch in branches {
            sources.push(index_source_for(branch, alias, container, meta)?);
        }
        return merge_sources(container, LogicalOp::Or, sources);
    }

    let (field, op, value) = as_atom(expr, alias)?;
    if !meta.indexes.contains(&field) {
        return None;
    }
    let range = match op {
        BinOp::Eq => IndexScanRange::Eq(value),
        BinOp::Gt => IndexScanRange::Range {
            lower: Some((value, false)),
            upper: None,
        },
        BinOp::Gte => IndexScanRange::Range {
            lower: Some((value, true)),
            upper: None,
        },
        BinOp::Lt => IndexScanRange::Range {
            lower: None,
            upper: Some((value, false)),
        },
        BinOp::Lte => IndexScanRange::Range {
            lower: None,
            upper: Some((value, true)),
        },
        _ => return None,
    };
    Some(index_scan(container, &field, range))
}

/// Fold ID sources into an `IndexMerge` tree (`None` if empty, the source
/// itself if a single one).
fn merge_sources(
    container: &CollectionRef,
    logical: LogicalOp,
    sources: Vec<Node>,
) -> Option<Node> {
    let mut iter = sources.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, node| Node::IndexMerge {
        collection: container.clone(),
        logical,
        lhs: Box::new(acc),
        rhs: Box::new(node),
    }))
}

/// Flatten an `Or` into its branches, but treat a Mongo implicit-equality idiom
/// as one indivisible branch (its inner `Eq OR ARRAY_CONTAINS` must not be split
/// — `index_source_for` recognizes the whole idiom as an indexed Eq).
fn collect_or<'a>(expr: &'a ScalarExpr, alias: &str, out: &mut Vec<&'a ScalarExpr>) {
    if as_mongo_eq(expr, alias).is_some() {
        out.push(expr);
        return;
    }
    if let ScalarExpr::Binary {
        op: BinOp::Or,
        lhs,
        rhs,
    } = expr
    {
        collect_or(lhs, alias, out);
        collect_or(rhs, alias, out);
    } else {
        out.push(expr);
    }
}

fn scan(container: &CollectionRef) -> Node {
    Node::Scan {
        collection: container.clone(),
    }
}

fn index_scan(container: &CollectionRef, field: &str, range: IndexScanRange) -> Node {
    Node::IndexScan {
        collection: container.clone(),
        field: field.to_string(),
        range,
        direction: ScanDirection::Forward,
        limit: None,
    }
}

/// Wrap an ID-yielding source in a `KeyLookup` to fetch the documents.
fn key_lookup(container: &CollectionRef, ids: Node) -> Node {
    Node::KeyLookup {
        collection: container.clone(),
        source: Box::new(ids),
    }
}

/// Interpret a conjunct as `alias.<path> <cmp> <literal>` (either operand
/// order), returning the field path, comparison op, and literal value.
fn as_atom(expr: &ScalarExpr, alias: &str) -> Option<(String, BinOp, Bson)> {
    let ScalarExpr::Binary { op, lhs, rhs } = expr else {
        return None;
    };
    if !is_comparison(*op) {
        return None;
    }
    if let (Some(path), Some(lit)) = (path_of(lhs.as_ref(), alias), as_literal(rhs.as_ref())) {
        Some((path, *op, lit))
    } else if let (Some(lit), Some(path)) = (as_literal(lhs.as_ref()), path_of(rhs.as_ref(), alias))
    {
        Some((path, flip(*op), lit))
    } else {
        None
    }
}

/// Recognize the Mongo implicit-equality idiom the find front-end emits:
/// `alias.field = lit OR ARRAY_CONTAINS(alias.field, lit)` (same field, same
/// literal). Returns the field path and value — it is sargable as an equality
/// on `field`, because an index/pk lookup for `lit` finds both the
/// scalar-equal and the array-containing documents.
fn as_mongo_eq(expr: &ScalarExpr, alias: &str) -> Option<(String, Bson)> {
    let ScalarExpr::Binary {
        op: BinOp::Or,
        lhs,
        rhs,
    } = expr
    else {
        return None;
    };
    // lhs: `field = lit`
    let (eq_field, BinOp::Eq, eq_val) = as_atom(lhs.as_ref(), alias)? else {
        return None;
    };
    // rhs: `ARRAY_CONTAINS(field, lit)` over the same field and value
    let (ac_field, ac_val) = as_array_contains(rhs.as_ref(), alias)?;
    if ac_field == eq_field && ac_val == eq_val {
        Some((eq_field, eq_val))
    } else {
        None
    }
}

/// Interpret `ARRAY_CONTAINS(alias.<path>, <literal>)`, returning the field
/// path and the literal value.
fn as_array_contains(expr: &ScalarExpr, alias: &str) -> Option<(String, Bson)> {
    let ScalarExpr::Function { name, args } = expr else {
        return None;
    };
    if !name.eq_ignore_ascii_case("ARRAY_CONTAINS") || args.len() != 2 {
        return None;
    }
    Some((path_of(&args[0], alias)?, as_literal(&args[1])?))
}

/// Recognize a [`ScalarExpr::MultikeyEq`] on `alias` — explicit multikey
/// equality the find front-end emits for a `.[]` path. Returns the verbatim
/// `.[]` path (which is also the index name) and the literal value, so it can
/// be matched to a multikey index.
fn as_multikey_eq(expr: &ScalarExpr, alias: &str) -> Option<(String, Bson)> {
    let ScalarExpr::MultikeyEq {
        base,
        index_path,
        value,
    } = expr
    else {
        return None;
    };
    if !matches!(base.as_ref(), ScalarExpr::Identifier(a) if a == alias) {
        return None;
    }
    Some((index_path.clone(), as_literal(value)?))
}

/// The value of a primary-key equality on `pk` — a plain `Eq` atom or the Mongo
/// idiom (whose `ARRAY_CONTAINS` branch is vacuous for a non-array pk).
fn pk_eq_value(expr: &ScalarExpr, alias: &str, pk: &str) -> Option<Bson> {
    if let Some((field, BinOp::Eq, value)) = as_atom(expr, alias)
        && field == pk
    {
        return Some(value);
    }
    match as_mongo_eq(expr, alias) {
        Some((field, value)) if field == pk => Some(value),
        _ => None,
    }
}

/// The dotted field path of an `alias.a.b.c` access (`None` for bare `alias`,
/// computed expressions, or a different root).
fn path_of(expr: &ScalarExpr, alias: &str) -> Option<String> {
    match expr {
        ScalarExpr::Member { base, field } => match base.as_ref() {
            ScalarExpr::Identifier(a) if a == alias => Some(field.clone()),
            other => path_of(other, alias).map(|p| format!("{p}.{field}")),
        },
        _ => None,
    }
}

fn as_literal(expr: &ScalarExpr) -> Option<Bson> {
    match expr {
        ScalarExpr::Literal(lit) => Some(literal_to_bson(lit)),
        // A materialized value preserves its exact BSON type — important here,
        // since an index bound must match the stored key's numeric type.
        ScalarExpr::Value(b) => Some(b.clone()),
        _ => None,
    }
}

fn literal_to_bson(lit: &Literal) -> Bson {
    match lit {
        Literal::Null => Bson::Null,
        Literal::Bool(b) => Bson::Boolean(*b),
        Literal::Int(i) => Bson::Int64(*i),
        Literal::Float(f) => Bson::Double(*f),
        Literal::Str(s) => Bson::String(s.clone()),
    }
}

fn bson_to_raw(value: &Bson) -> Option<RawBson> {
    match RawBson::try_from(value.clone()) {
        Ok(raw) => Some(raw),
        Err(_) => None, // unrepresentable → skip the optimization
    }
}

fn is_comparison(op: BinOp) -> bool {
    matches!(
        op,
        BinOp::Eq | BinOp::Gt | BinOp::Gte | BinOp::Lt | BinOp::Lte
    )
}

/// Flip a comparison so the field is on the left (`40 < c.age` → `c.age > 40`).
fn flip(op: BinOp) -> BinOp {
    match op {
        BinOp::Gt => BinOp::Lt,
        BinOp::Gte => BinOp::Lte,
        BinOp::Lt => BinOp::Gt,
        BinOp::Lte => BinOp::Gte,
        other => other,
    }
}

fn flatten_and(expr: ScalarExpr, out: &mut Vec<ScalarExpr>) {
    match expr {
        ScalarExpr::Binary {
            op: BinOp::And,
            lhs,
            rhs,
        } => {
            flatten_and(*lhs, out);
            flatten_and(*rhs, out);
        }
        other => out.push(other),
    }
}

/// Rebuild an `AND` from the conjuncts not in `used`, or `None` if none remain.
fn residual_excluding(conjuncts: Vec<ScalarExpr>, used: &[usize]) -> Option<ScalarExpr> {
    let remaining: Vec<ScalarExpr> = conjuncts
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !used.contains(i))
        .map(|(_, c)| c)
        .collect();
    rebuild_and(remaining)
}

fn rebuild_and(conjuncts: Vec<ScalarExpr>) -> Option<ScalarExpr> {
    let mut iter = conjuncts.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, e| ScalarExpr::Binary {
        op: BinOp::And,
        lhs: Box::new(acc),
        rhs: Box::new(e),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn container() -> CollectionRef {
        CollectionRef {
            cf: "default".into(),
            collection: "people".into(),
        }
    }

    fn no_index() -> CollectionMeta {
        CollectionMeta {
            indexes: vec![],
            pk_path: "_id".into(),
        }
    }

    fn age_indexed() -> CollectionMeta {
        CollectionMeta {
            indexes: vec!["age".into()],
            pk_path: "_id".into(),
        }
    }

    fn lower_with(sql: &str, meta: &CollectionMeta) -> Node {
        let Plan::Query(node) = lower(slate_sql::parse(sql).unwrap(), container(), meta) else {
            panic!("lower always produces a Query");
        };
        node
    }

    fn lower_sql(sql: &str) -> Node {
        lower_with(sql, &no_index())
    }

    // ── Structural lowering (no index) ──────────────────────────

    #[test]
    fn minimal_is_project_scan() {
        // No joins → no Bind wrapper; the alias binds the bare scan rows.
        match lower_sql("SELECT VALUE c FROM c") {
            Node::Project {
                source, binding, ..
            } => {
                assert_eq!(binding, RowBinding::Alias("c".into()));
                assert!(matches!(*source, Node::Scan { .. }));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn full_pipeline_nesting() {
        // Limit(Project(Sort(Filter(Scan)))) — no Bind without joins.
        let node = lower_sql(
            "SELECT VALUE c.name FROM c WHERE c.age > 40 ORDER BY c.age OFFSET 1 LIMIT 2",
        );
        let Node::Limit { skip, take, source } = node else {
            panic!("expected Limit");
        };
        assert_eq!(skip, 1);
        assert_eq!(take, Some(2));
        let Node::Project { source, .. } = *source else {
            panic!("expected Project");
        };
        let Node::Sort { source, .. } = *source else {
            panic!("expected Sort");
        };
        let Node::Filter { source, .. } = *source else {
            panic!("expected Filter");
        };
        assert!(matches!(*source, Node::Scan { .. }));
    }

    #[test]
    fn join_lowers_to_unwind() {
        let Node::Project { source, .. } = lower_sql("SELECT VALUE t FROM c JOIN t IN c.tags")
        else {
            panic!("expected Project");
        };
        let Node::Unwind { alias, source, .. } = *source else {
            panic!("expected Unwind");
        };
        assert_eq!(alias, "t");
        assert!(matches!(*source, Node::Bind { .. }));
    }

    // ── Sargability ─────────────────────────────────────────────

    /// Unwrap `Project(<source>)` — or `Project(Filter(<source>))` — and return
    /// the scan/index source. With no joins there is no `Bind` wrapper.
    fn source_under_bind(node: Node) -> Node {
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        match *source {
            Node::Filter { source, .. } => *source,
            other => other,
        }
    }

    #[test]
    fn eq_on_indexed_field_uses_index() {
        let node = lower_with("SELECT VALUE c FROM c WHERE c.age = 41", &age_indexed());
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(
                *source,
                Node::IndexScan {
                    range: IndexScanRange::Eq(_),
                    ..
                }
            )),
            other => panic!("expected KeyLookup(IndexScan), got {other:?}"),
        }
    }

    #[test]
    fn range_on_indexed_field_uses_index() {
        let node = lower_with("SELECT VALUE c FROM c WHERE 40 < c.age", &age_indexed());
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(
                *source,
                Node::IndexScan {
                    range: IndexScanRange::Range { .. },
                    ..
                }
            )),
            other => panic!("expected KeyLookup(IndexScan range), got {other:?}"),
        }
    }

    #[test]
    fn pk_equality_uses_direct_key_lookup() {
        let node = lower_with(r#"SELECT VALUE c FROM c WHERE c._id = "2""#, &age_indexed());
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(*source, Node::Values(_))),
            other => panic!("expected KeyLookup(Values), got {other:?}"),
        }
    }

    #[test]
    fn non_indexed_predicate_falls_back_to_scan() {
        // name is not indexed → Scan + residual Filter
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE c.name = "ada""#,
            &age_indexed(),
        );
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        let Node::Filter { source, .. } = *source else {
            panic!("expected residual Filter");
        };
        assert!(matches!(*source, Node::Scan { .. }));
    }

    #[test]
    fn partial_pushdown_keeps_residual() {
        // c.age > 40 pushed to index; c.name = "x" stays a residual Filter.
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE c.age > 40 AND c.name = "x""#,
            &age_indexed(),
        );
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        // Residual Filter present...
        let Node::Filter { source, .. } = *source else {
            panic!("expected residual Filter");
        };
        // ...and the source is the index path.
        assert!(matches!(*source, Node::KeyLookup { .. }));
    }

    #[test]
    fn computed_field_is_not_sargable() {
        // c.age + 1 = 42 wraps the field → not sargable → Scan + Filter.
        let node = lower_with("SELECT VALUE c FROM c WHERE c.age + 1 = 42", &age_indexed());
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        let Node::Filter { source, .. } = *source else {
            panic!("expected Filter");
        };
        assert!(matches!(*source, Node::Scan { .. }));
    }

    // ── OR / multi-index ────────────────────────────────────────

    fn age_status_indexed() -> CollectionMeta {
        CollectionMeta {
            indexes: vec!["age".into(), "status".into()],
            pk_path: "_id".into(),
        }
    }

    #[test]
    fn or_of_indexed_atoms_uses_index_merge_or() {
        let node = lower_with(
            "SELECT VALUE c FROM c WHERE c.age = 41 OR c.age = 44",
            &age_indexed(),
        );
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(
                *source,
                Node::IndexMerge {
                    logical: LogicalOp::Or,
                    ..
                }
            )),
            other => panic!("expected KeyLookup(IndexMerge Or), got {other:?}"),
        }
    }

    #[test]
    fn or_of_implicit_equality_idioms_uses_index_merge_or() {
        // The Mongo `{field: v}` form lowers to `field = v OR ARRAY_CONTAINS(
        // field, v)`. An OR/IN of those must still merge indexes — the regression
        // was that flattening the outer OR split each idiom and exposed the lone,
        // un-indexable `ARRAY_CONTAINS` branch, forcing a full scan.
        let node = lower_with(
            "SELECT VALUE c FROM c WHERE (c.age = 41 OR ARRAY_CONTAINS(c.age, 41)) \
             OR (c.age = 44 OR ARRAY_CONTAINS(c.age, 44))",
            &age_indexed(),
        );
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(
                *source,
                Node::IndexMerge {
                    logical: LogicalOp::Or,
                    ..
                }
            )),
            other => panic!("expected KeyLookup(IndexMerge Or), got {other:?}"),
        }
    }

    #[test]
    fn and_of_two_indexed_fields_uses_index_merge_and() {
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE c.age = 41 AND c.status = "active""#,
            &age_status_indexed(),
        );
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(
                *source,
                Node::IndexMerge {
                    logical: LogicalOp::And,
                    ..
                }
            )),
            other => panic!("expected KeyLookup(IndexMerge And), got {other:?}"),
        }
    }

    #[test]
    fn range_bounds_on_one_field_combine_into_one_scan() {
        let node = lower_with(
            "SELECT VALUE c FROM c WHERE c.age > 40 AND c.age < 50",
            &age_indexed(),
        );
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => match *source {
                Node::IndexScan {
                    range: IndexScanRange::Range { lower, upper },
                    ..
                } => {
                    assert!(lower.is_some() && upper.is_some());
                }
                other => panic!("expected single IndexScan range, got {other:?}"),
            },
            other => panic!("expected KeyLookup(IndexScan), got {other:?}"),
        }
    }

    #[test]
    fn aggregate_select_lowers_to_aggregate_node() {
        // SELECT VALUE COUNT(1) FROM c  →  Project(Aggregate{ one agg, no keys })
        let node = lower_sql("SELECT VALUE COUNT(1) FROM c");
        let Node::Project {
            source, binding, ..
        } = node
        else {
            panic!("expected Project at the root, got {node:?}");
        };
        assert_eq!(binding, RowBinding::Env);
        match *source {
            Node::Aggregate {
                aggregates,
                group_keys,
                ..
            } => {
                assert_eq!(aggregates.len(), 1);
                assert_eq!(aggregates[0].func, "COUNT");
                assert!(group_keys.is_empty());
            }
            other => panic!("expected Aggregate under Project, got {other:?}"),
        }
    }

    #[test]
    fn non_aggregate_select_has_no_aggregate_node() {
        // A plain projection must not introduce an Aggregate node.
        let node = lower_sql("SELECT VALUE c.name FROM c");
        assert!(matches!(node, Node::Project { .. }));
        let Node::Project { source, .. } = node else {
            unreachable!()
        };
        assert!(!matches!(*source, Node::Aggregate { .. }));
    }

    #[test]
    fn group_by_lowers_to_aggregate_with_keys() {
        let node = lower_sql("SELECT c.kind, COUNT(1) FROM c GROUP BY c.kind");
        let Node::Project { source, .. } = node else {
            panic!("expected Project at the root, got {node:?}");
        };
        match *source {
            Node::Aggregate {
                group_keys,
                aggregates,
                ..
            } => {
                assert_eq!(group_keys.len(), 1);
                assert_eq!(aggregates.len(), 1);
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    fn parse(sql: &str) -> slate_ast::Query {
        slate_sql::parse(sql).unwrap()
    }

    #[test]
    fn validate_grouping_allows_keys_and_aggregates() {
        assert!(
            validate_grouping(&parse("SELECT c.kind, COUNT(1) FROM c GROUP BY c.kind")).is_ok()
        );
        // Expressions built over a group key are fine.
        assert!(validate_grouping(&parse("SELECT VALUE c.kind FROM c GROUP BY c.kind")).is_ok());
    }

    #[test]
    fn validate_grouping_rejects_ungrouped_column() {
        // `c.other` is neither a group key nor inside an aggregate.
        assert!(
            validate_grouping(&parse("SELECT c.kind, c.other FROM c GROUP BY c.kind")).is_err()
        );
    }

    #[test]
    fn validate_grouping_rejects_bare_column_with_aggregate() {
        // An aggregate with a bare ungrouped column (and no GROUP BY) is invalid.
        assert!(validate_grouping(&parse("SELECT c.name, COUNT(1) FROM c")).is_err());
    }

    #[test]
    fn validate_grouping_rejects_select_star_with_group_by() {
        assert!(validate_grouping(&parse("SELECT * FROM c GROUP BY c.kind")).is_err());
    }

    #[test]
    fn validate_grouping_ignores_plain_queries() {
        // No grouping or aggregates → nothing to validate.
        assert!(validate_grouping(&parse("SELECT c.a, c.b FROM c")).is_ok());
    }

    #[test]
    fn group_by_without_aggregate_still_lowers_to_aggregate() {
        // Distinct-groups query: a group key, no aggregate functions.
        let node = lower_sql("SELECT VALUE c.kind FROM c GROUP BY c.kind");
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        match *source {
            Node::Aggregate {
                group_keys,
                aggregates,
                ..
            } => {
                assert_eq!(group_keys.len(), 1);
                assert!(aggregates.is_empty());
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn in_list_uses_index_merge_or() {
        // `IN (…)` desugars to an OR of equalities, so it indexes like one — the
        // planner needs no IN-specific handling.
        let node = lower_with(
            "SELECT VALUE c FROM c WHERE c.age IN (41, 44)",
            &age_indexed(),
        );
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(
                *source,
                Node::IndexMerge {
                    logical: LogicalOp::Or,
                    ..
                }
            )),
            other => panic!("expected KeyLookup(IndexMerge Or), got {other:?}"),
        }
    }

    #[test]
    fn between_uses_range_index_scan() {
        // `BETWEEN lo AND hi` desugars to `>= lo AND <= hi`, a single range scan.
        let node = lower_with(
            "SELECT VALUE c FROM c WHERE c.age BETWEEN 40 AND 50",
            &age_indexed(),
        );
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => match *source {
                Node::IndexScan {
                    range: IndexScanRange::Range { lower, upper },
                    ..
                } => assert!(lower.is_some() && upper.is_some()),
                other => panic!("expected single IndexScan range, got {other:?}"),
            },
            other => panic!("expected KeyLookup(IndexScan), got {other:?}"),
        }
    }

    #[test]
    fn or_with_non_indexed_branch_falls_back_to_scan() {
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE c.age = 41 OR c.name = "x""#,
            &age_indexed(),
        );
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        let Node::Filter { source, .. } = *source else {
            panic!("expected Filter");
        };
        assert!(matches!(*source, Node::Scan { .. }));
    }

    // ── Mongo implicit-equality idiom (`f = v OR ARRAY_CONTAINS(f, v)`) ──

    #[test]
    fn mongo_eq_on_pk_uses_point_lookup() {
        // The find front-end's `{_id: "2"}`. Must be a direct pk lookup, not a
        // scan — and the (vacuous) idiom is consumed, so no residual Filter.
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE c._id = "2" OR ARRAY_CONTAINS(c._id, "2")"#,
            &age_indexed(),
        );
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(*source, Node::Values(_))),
            other => panic!("expected KeyLookup(Values), got {other:?}"),
        }
    }

    #[test]
    fn mongo_eq_on_indexed_field_uses_index_and_consumes_recheck() {
        // `{age: 41}` on a scalar-indexed field → index Eq, with the idiom
        // *consumed* (no residual Filter): a scalar index returns only docs
        // whose `age` already equals 41, so the `OR ARRAY_CONTAINS` recheck is
        // always true over the candidates and is dropped.
        let node = lower_with(
            "SELECT VALUE c FROM c WHERE c.age = 41 OR ARRAY_CONTAINS(c.age, 41)",
            &age_indexed(),
        );
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        match *source {
            Node::KeyLookup { source, .. } => assert!(matches!(
                *source,
                Node::IndexScan {
                    range: IndexScanRange::Eq(_),
                    ..
                }
            )),
            other => panic!("expected KeyLookup(IndexScan) with no residual Filter, got {other:?}"),
        }
    }

    #[test]
    fn mongo_eq_on_unindexed_field_scans() {
        // `{name: "x"}` — name not indexed → scan + residual recheck.
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE c.name = "x" OR ARRAY_CONTAINS(c.name, "x")"#,
            &age_indexed(),
        );
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        let Node::Filter { source, .. } = *source else {
            panic!("expected Filter");
        };
        assert!(matches!(*source, Node::Scan { .. }));
    }
}
