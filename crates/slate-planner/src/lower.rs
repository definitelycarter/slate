//! Lowering: a parsed [`slate_ast::Query`] into an executable [`Plan`] subtree.
//!
//! The query's `FROM` clause supplies only the alias — the container is chosen
//! by the caller (matching Cosmos) and passed in as `container`, along with its
//! index metadata ([`CollectionMeta`](crate::sargable::CollectionMeta)).
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
//! Index selection (which conjuncts are pushed into a scan) lives in
//! [`crate::sargable`]; semantic validation in [`crate::validate`].

use bson::{Bson, RawBson, RawDocumentBuf};
use slate_ast::{
    Expression, FromClause, FromSource, Join, Literal, OrderByItem, Query, SortDirection,
    SubqueryKind,
};

use crate::plan::{AggregateExpr, CollectionRef, GroupKey, Node, Plan, RowBinding, VectorMetric};
use crate::sargable::{CollectionMeta, plan_source, scan};
use crate::validate::{contains_aggregate, is_aggregate_name};

/// Lower `query` into a plan that reads from `container`.
pub fn lower(query: Query, container: CollectionRef, meta: &CollectionMeta) -> Plan {
    let node = lower_query(query, container, meta, &[]);
    Plan::Query(crate::covering::apply(node, meta))
}

/// A subquery used directly as a FROM/JOIN iteration source is *multi-value*:
/// it yields the set of rows to unwind, so it must reduce as an array regardless
/// of the parser's default (a parenthesized `(SELECT …)` is tagged scalar).
fn as_iteration_source(expr: Expression) -> Expression {
    match expr {
        Expression::Subquery { query, .. } => Expression::Subquery {
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

/// The base of the pipeline produced from the `FROM` clause: the source `node`
/// and the `alias` it binds, the `residual` WHERE not pushed into that source,
/// whether the node already yields an environment row (`is_env`), and any
/// `joins` still to unwind.
struct BaseSource {
    alias: String,
    node: Node,
    residual: Option<Expression>,
    is_env: bool,
    joins: Vec<Join>,
}

/// Force the row-environment shape: if `node` isn't already an environment row,
/// wrap it in a `Bind` of `alias` (the env-only nodes — `Unwind`, `Subquery` —
/// need it). A no-op once the environment exists.
fn ensure_env(node: Node, is_env: &mut bool, alias: &str) -> Node {
    if *is_env {
        node
    } else {
        *is_env = true;
        Node::Bind {
            alias: alias.to_string(),
            source: Box::new(node),
        }
    }
}

/// Lower a query (outer or a subquery's inner query) into a [`Node`] subtree.
/// The outer query reads its container via a `Scan`/index source; a subquery's
/// `FROM x IN <array>` reads an in-document array via `Unwind` over a
/// [`Node::CurrentRow`] (the correlated outer row).
pub(crate) fn lower_query(
    query: Query,
    container: CollectionRef,
    meta: &CollectionMeta,
    outer: &[&str],
) -> Node {
    let Query {
        select,
        distinct,
        from,
        filter,
        group_by,
        having,
        order_by,
        offset,
        limit,
    } = query;

    // Whether the query carries a `WHERE` at all. A constraining `WHERE` is the
    // *pre-filter* of a vector-index kNN: its candidate doc-ids must shrink the
    // set *before* the top-k (a global top-k then filtered would under-return),
    // so the kNN recogniser routes this candidate pipeline into `VectorTopK`'s
    // `source`. Captured before `filter` is moved into the sargability pass.
    let had_where = filter.is_some();

    // Desugar `DOCUMENTID(x)` → `x.<pk>` in the `WHERE` up front, before the
    // sargability pass consumes it — so `WHERE DOCUMENTID(c) = v` is recognised
    // as pk-equality (a point lookup) just like `WHERE c.<pk> = v`. The other
    // clauses are desugared further below, after the FROM source is built.
    let pk = meta.pk_path.as_str();
    let filter = filter.map(|f| desugar_pk(f, pk));

    // Query-wide counter for subquery slot names (`$subN`), shared across the
    // FROM/JOIN sources and the projection so every slot is unique.
    let mut next_slot = 0usize;

    // The base source: a container scan/index path, or — for a subquery — an
    // `Unwind` of the correlated array over the outer row (`CurrentRow`), or —
    // for a FROM-less query — a single empty environment row evaluated once. The
    // array and FROM-less sources already yield an environment row; the container
    // source doesn't until a `Bind`.
    let BaseSource {
        alias,
        mut node,
        residual,
        mut is_env,
        joins,
    } = match from {
        // FROM-less (`SELECT VALUE 1`): one empty row, no bindings. `WHERE` (if
        // any) filters that single row; `SELECT *` is rejected by the front-end.
        None => BaseSource {
            alias: String::new(),
            node: Node::Values(vec![RawBson::Document(RawDocumentBuf::new())]),
            residual: filter,
            is_env: true,
            joins: Vec::new(),
        },
        Some(FromClause { source, joins }) => match source {
            // A subquery whose `FROM` names an enclosing alias is *item-scoped*:
            // it iterates that single bound value, which the outer row already
            // carries — so the source is just `CurrentRow`, not a container
            // re-scan. (Top-level `FROM c` has an empty outer scope → scan.)
            FromSource::ImplicitContainer { alias } if outer.contains(&alias.as_str()) => {
                BaseSource {
                    alias,
                    node: Node::CurrentRow,
                    residual: filter,
                    is_env: true,
                    joins,
                }
            }
            FromSource::ImplicitContainer { alias } => {
                let (source, residual) = plan_source(filter, &alias, &container, meta);
                BaseSource {
                    alias,
                    node: source,
                    residual,
                    is_env: false,
                    joins,
                }
            }
            // `FROM base.path alias` — scan the container bound to `base`, then
            // navigate to `base.path` (a Project, which drops rows where the path
            // is undefined) and rebind that bare sub-value to `alias`. The WHERE
            // references `alias`, not container fields, so nothing is pushed into
            // the scan — the full filter stays residual.
            FromSource::Subroot { base, path, alias } => {
                let navigated = Node::Project {
                    expr: member_chain(&base, &path),
                    binding: RowBinding::Env,
                    source: Box::new(Node::Bind {
                        alias: base,
                        source: Box::new(scan(&container)),
                    }),
                };
                BaseSource {
                    alias,
                    node: navigated,
                    residual: filter,
                    is_env: false,
                    joins,
                }
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
                BaseSource {
                    alias,
                    node: source,
                    residual: filter,
                    is_env: true,
                    joins,
                }
            }
        },
    };

    // Desugar `DOCUMENTID(c)` → `c.<pk>` in the remaining clauses (the `WHERE`
    // was already desugared above for sargability). Done before group keys are
    // formed, so `GROUP BY DOCUMENTID(c)` matches the same desugared form a
    // `SELECT DOCUMENTID(c)` produces and folds into one `$keyN` slot. The
    // `residual` is the not-pushed remainder of the (already-desugared) WHERE, so
    // it needs no further desugaring.
    let group_by: Vec<Expression> = group_by.into_iter().map(|e| desugar_pk(e, pk)).collect();
    let having = having.map(|h| desugar_pk(h, pk));
    let order_by: Vec<OrderByItem> = order_by
        .into_iter()
        .map(|item| OrderByItem {
            expr: desugar_pk(item.expr, pk),
            direction: item.direction,
        })
        .collect();

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
    let value_expr = desugar_pk(select.into_value_expr(&alias), pk);
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
        || having.as_ref().is_some_and(contains_aggregate)
        || order_by.iter().any(|item| contains_aggregate(&item.expr));
    let (project_expr, having, order_by, aggregates) = if aggregating {
        let mut aggregates = Vec::new();
        let project_expr = rewrite_projection(value_expr, &group_keys, &mut aggregates);
        // HAVING runs after aggregation, so — like ORDER BY — its references to
        // the group keys and aggregates rewrite into the same `$keyN`/`$aggN`
        // slots (an aggregate appearing only in HAVING is still computed).
        let having = having.map(|h| rewrite_projection(h, &group_keys, &mut aggregates));
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
        (project_expr, having, order_by, aggregates)
    } else {
        // A `HAVING` with no grouping/aggregates is not reachable here — the
        // validator requires HAVING to imply grouping — so `having` is `None`.
        (value_expr, None, order_by, Vec::new())
    };

    // JOIN ... IN — each `Unwind` extends the environment. The first join (or a
    // subquery below) forces the environment shape via `Bind`.
    if !joins.is_empty() {
        node = ensure_env(node, &mut is_env, &alias);
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
        node = ensure_env(node, &mut is_env, &alias);
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
        // Aggregation collapses rows into one per group; HAVING then filters those
        // group rows, ORDER BY sorts them, and the projection (rewritten to read
        // the `$keyN`/`$aggN` slots) shapes each. All bind as `Env` over the
        // aggregate output rows.
        node = Node::Aggregate {
            group_keys,
            aggregates,
            binding,
            source: Box::new(node),
        };
        // HAVING — a post-aggregation `Filter` over the group rows, before ORDER
        // BY (so a group dropped by HAVING never reaches the sort or projection).
        if let Some(predicate) = having {
            node = Node::Filter {
                predicate,
                binding: RowBinding::Env,
                source: Box::new(node),
            };
        }
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
        // kNN seek: `ORDER BY VECTORDISTANCE(c.<field>, <q>[, <m>]) LIMIT k` over
        // a matching flat vector index lowers to a `VectorTopK` source +
        // `KeyLookup`, *in place of* `Sort`+`Limit` (the LIMIT folds into `k`).
        // `recognize_vector_topk` decides; `None` keeps the `Sort`+`Limit`
        // fallback (correct, just a full scan).
        match recognize_vector_topk(&order_by, limit, offset, &binding, meta) {
            Some(knn) => {
                // The pre-filter: a constraining WHERE makes the candidate pipeline
                // (`node`) the top-k's `source`, so the filter shrinks the set
                // *before* the top-k. No WHERE → scan the whole field (drop `node`,
                // a bare scan).
                let source = had_where.then(|| Box::new(node));
                let topk = Node::VectorTopK {
                    collection: container.clone(),
                    field: knn.field,
                    query_vector: knn.query_vector,
                    metric: knn.metric,
                    dtype: knn.dtype,
                    k: knn.k,
                    source,
                };
                // The top-k emits exactly the k nearest doc-ids in nearest-first
                // order, so the LIMIT is already honoured — fetch + project, with
                // no `Sort` and no trailing `Limit`.
                node = Node::KeyLookup {
                    collection: container.clone(),
                    source: Box::new(topk),
                };
                node = Node::Project {
                    expr: project_expr,
                    binding,
                    source: Box::new(node),
                };
                if distinct {
                    node = Node::Distinct {
                        source: Box::new(node),
                        flatten: false,
                    };
                }
                return node;
            }
            None => {
                // ORDER BY  →  Sort (before projection — keys reference the env)
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
        }
    }

    // SELECT DISTINCT  →  dedup the projected rows (whole-value, no array
    // flattening — that's Mongo `distinct`'s multikey behaviour, not SQL's).
    if distinct {
        node = Node::Distinct {
            source: Box::new(node),
            flatten: false,
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

/// The recognised pieces of a kNN seek — the small data the caller needs to
/// build a [`Node::VectorTopK`] (it owns the `node` that becomes the pre-filter
/// `source`, so the recogniser returns no `Node`).
struct RecognizedKnn {
    field: String,
    query_vector: Expression,
    metric: VectorMetric,
    dtype: crate::plan::VectorDataType,
    k: usize,
}

/// Recognise `ORDER BY VECTORDISTANCE(c.<field>, <q>[, <m>]) LIMIT k` as a flat
/// vector-index seek, returning the pieces to build a [`Node::VectorTopK`], or
/// `None` so the caller builds the correct `Sort`+`Limit` fallback (also correct,
/// just a full scan). Recognises only when **all** hold:
///
/// - the *sole* `ORDER BY` key is `VECTORDISTANCE(<alias>.<field>, <q>[, <m>])`;
/// - `<field>` has a flat vector index on the collection (`meta.vector_indexes`);
/// - the metric arg, if present, matches the index's metric (a literal string);
///   absent, the index's metric is used;
/// - the `ORDER BY` direction is the metric's nearest-first sense — `ASC` for
///   euclidean (lower = nearer), `DESC` for cosine/dotproduct (higher = nearer);
/// - there is a `LIMIT k` and no `OFFSET` (a non-zero offset over a top-k is left
///   to the generic path; `k` alone is the clean seek).
fn recognize_vector_topk(
    order_by: &[OrderByItem],
    limit: Option<u64>,
    offset: Option<u64>,
    binding: &RowBinding,
    meta: &CollectionMeta,
) -> Option<RecognizedKnn> {
    // Need exactly one ORDER BY key, a LIMIT, and no OFFSET.
    let ([item], Some(k), None) = (order_by, limit, offset) else {
        return None;
    };
    let k = k as usize;

    // The alias the row binds to — `VECTORDISTANCE(c.…)` references it. An `Env`
    // binding (joins/aggregates/subqueries) has no single alias to strip, so the
    // kNN seek isn't attempted there.
    let RowBinding::Alias(alias) = binding else {
        return None;
    };

    // The sort key must be a VECTORDISTANCE(...) call.
    let Expression::Function { name, args } = &item.expr else {
        return None;
    };
    if !name.eq_ignore_ascii_case("VECTORDISTANCE") || !(args.len() == 2 || args.len() == 3) {
        return None;
    }

    // arg0 must be `<alias>.<field>` (possibly dotted) → the indexed field path.
    let field = member_path(&args[0], alias)?;

    // The field must have a flat vector index on this collection.
    let index = meta.vector_indexes.iter().find(|v| v.field == field)?;

    // arg2 (the metric), when present, must be a string literal that names a
    // metric matching the index's. Absent → use the index's metric.
    let metric = index.metric;
    if let Some(arg) = args.get(2) {
        match metric_literal(arg) {
            Some(m) if m == metric => {}
            // Present but mismatched (or not a recognisable metric string) →
            // fall back; the function would still measure by the named metric, so
            // the index (built for a different one) can't serve it.
            _ => return None,
        }
    }

    // The ORDER BY direction must be the metric's nearest-first sense.
    let wants_desc = metric.higher_is_closer();
    let dir_ok = match item.direction {
        SortDirection::Desc => wants_desc,
        SortDirection::Asc => !wants_desc,
    };
    if !dir_ok {
        return None;
    }

    Some(RecognizedKnn {
        field,
        // The query vector is arg1, evaluated once by the executor.
        query_vector: args[1].clone(),
        metric,
        // The width decides whether the executor rescores the shortlist.
        dtype: index.dtype,
        k,
    })
}

/// Flatten an `<alias>.<a>.<b>…` member chain rooted at `alias` into the dotted
/// field path `"a.b…"` (the path inside a `VECTORDISTANCE` call). `None` for any
/// other shape (a bare identifier, a different root, an array index, a function).
fn member_path(expr: &Expression, alias: &str) -> Option<String> {
    match expr {
        Expression::Member { base, field } => match base.as_ref() {
            Expression::Identifier(root) if root == alias => Some(field.clone()),
            Expression::Member { .. } => {
                let prefix = member_path(base, alias)?;
                Some(format!("{prefix}.{field}"))
            }
            _ => None,
        },
        _ => None,
    }
}

/// The [`VectorMetric`] named by a string-literal metric argument, or `None` for
/// a non-string or unrecognised metric. Accepts both the SQL form
/// (`Literal::Str`) and the Mongo front-end form (`Value(Bson::String)`).
fn metric_literal(expr: &Expression) -> Option<VectorMetric> {
    let s = match expr {
        Expression::Literal(Literal::Str(s)) => s.as_str(),
        Expression::Value(Bson::String(s)) => s.as_str(),
        _ => return None,
    };
    match s.to_ascii_lowercase().as_str() {
        "cosine" => Some(VectorMetric::Cosine),
        "dotproduct" => Some(VectorMetric::DotProduct),
        "euclidean" => Some(VectorMetric::Euclidean),
        _ => None,
    }
}

/// Pull subqueries out of `expr`: each `(SELECT …)`/`EXISTS`/`ARRAY` becomes a
/// reference to a fresh `$subN` slot and is recorded in `out` with its inner
/// query lowered to a subtree. Every other sub-expression is rebuilt unchanged.
fn extract_subqueries(
    expr: Expression,
    container: &CollectionRef,
    meta: &CollectionMeta,
    out: &mut Vec<SubquerySpec>,
    outer: &[&str],
    next: &mut usize,
) -> Expression {
    match expr {
        Expression::Subquery { query, kind } => {
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
            Expression::Identifier(slot)
        }
        Expression::Binary { op, lhs, rhs } => Expression::Binary {
            op,
            lhs: Box::new(extract_subqueries(*lhs, container, meta, out, outer, next)),
            rhs: Box::new(extract_subqueries(*rhs, container, meta, out, outer, next)),
        },
        Expression::Unary { op, expr } => Expression::Unary {
            op,
            expr: Box::new(extract_subqueries(*expr, container, meta, out, outer, next)),
        },
        Expression::Member { base, field } => Expression::Member {
            base: Box::new(extract_subqueries(*base, container, meta, out, outer, next)),
            field,
        },
        Expression::Index { base, index } => Expression::Index {
            base: Box::new(extract_subqueries(*base, container, meta, out, outer, next)),
            index: Box::new(extract_subqueries(
                *index, container, meta, out, outer, next,
            )),
        },
        Expression::Function { name, args } => Expression::Function {
            name,
            args: args
                .into_iter()
                .map(|a| extract_subqueries(a, container, meta, out, outer, next))
                .collect(),
        },
        Expression::Object(fields) => Expression::Object(
            fields
                .into_iter()
                .map(|(k, v)| (k, extract_subqueries(v, container, meta, out, outer, next)))
                .collect(),
        ),
        Expression::Array(items) => Expression::Array(
            items
                .into_iter()
                .map(|i| extract_subqueries(i, container, meta, out, outer, next))
                .collect(),
        ),
        // Leaves and Mongo-only constructs hold no SQL subqueries.
        other => other,
    }
}

/// Rewrite a projection for aggregation: a whole sub-expression equal to a
/// group key becomes a reference to its `$keyN` slot; each `AGG(arg)` becomes a
/// `$aggN` slot recorded in `out`; every other sub-expression is rebuilt
/// unchanged. Aggregate arguments aren't re-scanned — aggregates don't nest, and
/// they're evaluated per-row inside the aggregation node, not per-group.
fn rewrite_projection(
    expr: Expression,
    group_keys: &[GroupKey],
    out: &mut Vec<AggregateExpr>,
) -> Expression {
    // A whole sub-expression that matches a group key reads from its slot.
    if let Some(key) = group_keys.iter().find(|k| k.expr == expr) {
        return Expression::Identifier(key.slot.clone());
    }
    match expr {
        Expression::Function { name, args } if is_aggregate_name(&name) => {
            let arg = args
                .into_iter()
                .next()
                .unwrap_or(Expression::Literal(Literal::Int(1)));
            let slot = format!("$agg{}", out.len());
            out.push(AggregateExpr {
                func: name,
                arg,
                slot: slot.clone(),
            });
            Expression::Identifier(slot)
        }
        Expression::Function { name, args } => Expression::Function {
            name,
            args: args
                .into_iter()
                .map(|a| rewrite_projection(a, group_keys, out))
                .collect(),
        },
        Expression::Binary { op, lhs, rhs } => Expression::Binary {
            op,
            lhs: Box::new(rewrite_projection(*lhs, group_keys, out)),
            rhs: Box::new(rewrite_projection(*rhs, group_keys, out)),
        },
        Expression::Unary { op, expr } => Expression::Unary {
            op,
            expr: Box::new(rewrite_projection(*expr, group_keys, out)),
        },
        Expression::Member { base, field } => Expression::Member {
            base: Box::new(rewrite_projection(*base, group_keys, out)),
            field,
        },
        Expression::Index { base, index } => Expression::Index {
            base: Box::new(rewrite_projection(*base, group_keys, out)),
            index: Box::new(rewrite_projection(*index, group_keys, out)),
        },
        Expression::Object(fields) => Expression::Object(
            fields
                .into_iter()
                .map(|(k, v)| (k, rewrite_projection(v, group_keys, out)))
                .collect(),
        ),
        Expression::Array(items) => Expression::Array(
            items
                .into_iter()
                .map(|i| rewrite_projection(i, group_keys, out))
                .collect(),
        ),
        // Leaves and Mongo-only constructs — no SQL aggregates nested inside.
        other => other,
    }
}

/// Desugar `DOCUMENTID(<expr>)` into `<expr>.<pk_path>`, recursively, so it reads
/// the configured primary-key field of the bound document (Cosmos's `DOCUMENTID`,
/// which returns the document's id). The pk path is a catalog fact known only at
/// plan time, so this rewrite happens here rather than in the evaluator (which
/// has no catalog). A non-document argument's member access yields undefined,
/// matching Cosmos. Arity ≠ 1 is left untouched — it falls through to the
/// evaluator as an "unknown function"/arity error.
fn desugar_pk(expr: Expression, pk_path: &str) -> Expression {
    match expr {
        Expression::Function { name, mut args }
            if name.eq_ignore_ascii_case("DOCUMENTID") && args.len() == 1 =>
        {
            // `arg` itself may contain a nested DOCUMENTID — desugar it first.
            let arg = desugar_pk(args.remove(0), pk_path);
            member_chain_expr(arg, pk_path)
        }
        Expression::Function { name, args } => Expression::Function {
            name,
            args: args.into_iter().map(|a| desugar_pk(a, pk_path)).collect(),
        },
        Expression::Binary { op, lhs, rhs } => Expression::Binary {
            op,
            lhs: Box::new(desugar_pk(*lhs, pk_path)),
            rhs: Box::new(desugar_pk(*rhs, pk_path)),
        },
        Expression::Unary { op, expr } => Expression::Unary {
            op,
            expr: Box::new(desugar_pk(*expr, pk_path)),
        },
        Expression::Member { base, field } => Expression::Member {
            base: Box::new(desugar_pk(*base, pk_path)),
            field,
        },
        Expression::Index { base, index } => Expression::Index {
            base: Box::new(desugar_pk(*base, pk_path)),
            index: Box::new(desugar_pk(*index, pk_path)),
        },
        Expression::Object(fields) => Expression::Object(
            fields
                .into_iter()
                .map(|(k, v)| (k, desugar_pk(v, pk_path)))
                .collect(),
        ),
        Expression::Array(items) => {
            Expression::Array(items.into_iter().map(|i| desugar_pk(i, pk_path)).collect())
        }
        // A subquery's own lowering desugars it with the (same) pk path, so it is
        // left intact here; leaves and Mongo-only constructs hold no DOCUMENTID.
        other => other,
    }
}

/// Build the member-access chain `base.seg0.seg1…` over an already-built base
/// expression — `pk_path` may be a dotted path (e.g. `"meta.id"`).
fn member_chain_expr(base: Expression, pk_path: &str) -> Expression {
    let mut expr = base;
    for seg in pk_path.split('.') {
        expr = Expression::Member {
            base: Box::new(expr),
            field: seg.to_string(),
        };
    }
    expr
}

/// Build the member-access chain `base.seg0.seg1…` as a scalar expression — the
/// path a subroot `FROM base.path alias` navigates on each document.
fn member_chain(base: &str, path: &[String]) -> Expression {
    let mut expr = Expression::Identifier(base.to_string());
    for seg in path {
        expr = Expression::Member {
            base: Box::new(expr),
            field: seg.clone(),
        };
    }
    expr
}

#[cfg(test)]
mod tests {
    use super::*;
    // Index range/merge enums and grouping validation moved to sibling modules.
    use crate::plan::{IndexScanRange, LogicalOp};
    use crate::validate::validate_grouping;

    fn container() -> CollectionRef {
        CollectionRef {
            cf: "default".into(),
            collection: "people".into(),
        }
    }

    fn no_index() -> CollectionMeta {
        CollectionMeta {
            indexes: vec![],
            compound_indexes: Vec::new(),
            vector_indexes: Vec::new(),
            pk_path: "_id".into(),
        }
    }

    fn age_indexed() -> CollectionMeta {
        CollectionMeta {
            indexes: vec!["age".into()],
            compound_indexes: Vec::new(),
            vector_indexes: Vec::new(),
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

    #[test]
    fn from_subroot_navigates_then_binds() {
        // `FROM c.address e` scans the container, navigates to `c.address`, and
        // binds that sub-value to `e`: Project(e) → Project(navigation) → Bind(c).
        let Node::Project {
            source, binding, ..
        } = lower_sql("SELECT VALUE e FROM c.address e")
        else {
            panic!("expected Project");
        };
        assert_eq!(binding, RowBinding::Alias("e".into()));
        let Node::Project { source: inner, .. } = *source else {
            panic!("expected the navigation Project");
        };
        assert!(matches!(*inner, Node::Bind { .. }));
    }

    #[test]
    fn projection_subquery_emits_subquery_node() {
        // A subquery in the projection is extracted into a `$sub` slot computed by
        // a correlated-apply `Subquery` node above the (env-forced) source.
        let node = lower_sql("SELECT VALUE ARRAY(SELECT VALUE t FROM t IN c.tags) FROM c");
        let Node::Project {
            source, binding, ..
        } = node
        else {
            panic!("expected Project");
        };
        // The extracted subquery forces the environment shape.
        assert_eq!(binding, RowBinding::Env);
        assert!(matches!(*source, Node::Subquery { .. }));
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
            compound_indexes: Vec::new(),
            vector_indexes: Vec::new(),
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
    fn and_of_two_indexed_equalities_uses_index_intersect() {
        // All-equality AND → the galloping skip-merge (Index Intersection RFC),
        // not the left-associative IndexMerge(And) fold.
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE c.age = 41 AND c.status = "active""#,
            &age_status_indexed(),
        );
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => match *source {
                Node::IndexIntersect { parts, .. } => {
                    let fields: Vec<&str> = parts.iter().map(|p| p.field.as_str()).collect();
                    assert_eq!(fields, vec!["age", "status"]);
                }
                other => panic!("expected KeyLookup(IndexIntersect), got {other:?}"),
            },
            other => panic!("expected KeyLookup, got {other:?}"),
        }
    }

    #[test]
    fn and_of_eq_and_range_keeps_index_merge_and() {
        // A non-equality part (the range) disqualifies the doc-id skip-merge, so
        // the intersection falls back to today's hash IndexMerge(And).
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE c.age > 40 AND c.status = "active""#,
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
    fn validate_grouping_allows_having_over_aggregates_and_keys() {
        // HAVING over an aggregate and a group key is fine.
        assert!(
            validate_grouping(&parse(
                "SELECT c.kind FROM c GROUP BY c.kind HAVING COUNT(1) > 1 AND c.kind != \"x\""
            ))
            .is_ok()
        );
    }

    #[test]
    fn validate_grouping_rejects_ungrouped_column_in_having() {
        // `c.other` in HAVING is neither a group key nor inside an aggregate.
        assert!(
            validate_grouping(&parse(
                "SELECT c.kind FROM c GROUP BY c.kind HAVING c.other > 1"
            ))
            .is_err()
        );
    }

    #[test]
    fn validate_grouping_having_without_group_by_implies_grouping() {
        // A bare HAVING (no GROUP BY) still triggers the grouping rule, so an
        // ungrouped SELECT column is rejected.
        assert!(
            validate_grouping(&parse("SELECT VALUE c.name FROM c HAVING COUNT(1) > 1")).is_err()
        );
        // …and an aggregate-only projection is accepted.
        assert!(
            validate_grouping(&parse("SELECT VALUE COUNT(1) FROM c HAVING COUNT(1) > 1")).is_ok()
        );
    }

    #[test]
    fn having_lowers_to_filter_above_aggregate() {
        // HAVING is a `Filter` between the `Aggregate` and the `Project`, reading
        // the rewritten `$keyN`/`$aggN` slots.
        let node =
            lower_sql("SELECT c.kind, COUNT(1) AS n FROM c GROUP BY c.kind HAVING COUNT(1) > 1");
        let Node::Project { source, .. } = node else {
            panic!("expected Project at the root, got {node:?}");
        };
        let Node::Filter {
            predicate,
            binding,
            source,
        } = *source
        else {
            panic!("expected a HAVING Filter under Project");
        };
        assert_eq!(binding, RowBinding::Env);
        // The COUNT in the HAVING predicate is rewritten to read an `$agg` slot,
        // not re-evaluated per group.
        let Expression::Binary { lhs, .. } = predicate else {
            panic!("expected a comparison predicate");
        };
        assert!(matches!(*lhs, Expression::Identifier(ref s) if s.starts_with("$agg")));
        assert!(matches!(*source, Node::Aggregate { .. }));
    }

    #[test]
    fn having_only_aggregate_is_still_computed() {
        // An aggregate that appears *only* in HAVING (not the SELECT) is still
        // registered on the Aggregate node so the Filter can read it.
        let node = lower_sql("SELECT VALUE c.kind FROM c GROUP BY c.kind HAVING COUNT(1) > 1");
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        let Node::Filter { source, .. } = *source else {
            panic!("expected HAVING Filter");
        };
        let Node::Aggregate { aggregates, .. } = *source else {
            panic!("expected Aggregate");
        };
        assert_eq!(aggregates.len(), 1);
        assert_eq!(aggregates[0].func, "COUNT");
    }

    #[test]
    fn having_sits_below_sort() {
        // Pipeline: Project <- Sort <- Filter(HAVING) <- Aggregate.
        let node = lower_sql(
            "SELECT c.kind, COUNT(1) AS n FROM c GROUP BY c.kind \
             HAVING COUNT(1) > 1 ORDER BY COUNT(1) DESC",
        );
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        let Node::Sort { source, .. } = *source else {
            panic!("expected Sort under Project");
        };
        let Node::Filter { source, .. } = *source else {
            panic!("expected HAVING Filter under Sort");
        };
        assert!(matches!(*source, Node::Aggregate { .. }));
    }

    #[test]
    fn array_agg_lowers_to_aggregate() {
        let node = lower_sql("SELECT c.kind, ARRAY_AGG(c.name) AS names FROM c GROUP BY c.kind");
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        let Node::Aggregate { aggregates, .. } = *source else {
            panic!("expected Aggregate");
        };
        assert_eq!(aggregates.len(), 1);
        assert_eq!(aggregates[0].func, "ARRAY_AGG");
    }

    #[test]
    fn documentid_desugars_to_pk_member_access() {
        // DOCUMENTID(c) → c._id (the configured pk path), so the projection is a
        // plain member access — no Aggregate node, no DOCUMENTID function call.
        let node = lower_sql("SELECT VALUE DOCUMENTID(c) FROM c");
        let Node::Project { expr, .. } = node else {
            panic!("expected Project");
        };
        match expr {
            Expression::Member { base, field } => {
                assert_eq!(field, "_id");
                assert!(matches!(*base, Expression::Identifier(ref a) if a == "c"));
            }
            other => panic!("expected `c._id` member access, got {other:?}"),
        }
    }

    #[test]
    fn documentid_in_where_desugars() {
        // DOCUMENTID in a WHERE is desugared too; over the pk it plans as a point
        // lookup (the pk-equality fast path), proving the rewrite reached the
        // residual/sargability split.
        let node = lower_with(
            r#"SELECT VALUE c FROM c WHERE DOCUMENTID(c) = "2""#,
            &age_indexed(),
        );
        match source_under_bind(node) {
            Node::KeyLookup { source, .. } => assert!(matches!(*source, Node::Values(_))),
            other => panic!("expected KeyLookup(Values) point read, got {other:?}"),
        }
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

    // ── Vector kNN recognition (VectorTopK) ─────────────────────

    /// Meta with a flat vector index on `embedding` of the given metric (plus an
    /// `age` secondary index, to exercise a WHERE pre-filter that pushes down).
    fn vector_indexed(metric: VectorMetric) -> CollectionMeta {
        CollectionMeta {
            indexes: vec!["age".into()],
            compound_indexes: Vec::new(),
            vector_indexes: vec![crate::sargable::VectorIndexMeta {
                field: "embedding".into(),
                metric,
                dtype: crate::plan::VectorDataType::Float32,
            }],
            pk_path: "_id".into(),
        }
    }

    /// Pull the `VectorTopK` out of a lowered `Project(KeyLookup(VectorTopK))`,
    /// or `None` if the plan didn't take the kNN-seek shape (a fallback).
    fn vector_topk_of(node: Node) -> Option<Node> {
        let Node::Project { source, .. } = node else {
            return None;
        };
        let Node::KeyLookup { source, .. } = *source else {
            return None;
        };
        match *source {
            n @ Node::VectorTopK { .. } => Some(n),
            _ => None,
        }
    }

    #[test]
    fn knn_no_where_emits_vector_topk_whole_field() {
        // cosine → DESC is nearest-first; no WHERE → `source` is None.
        let node = lower_with(
            "SELECT VALUE c FROM c ORDER BY VECTORDISTANCE(c.embedding, [1.0,0.0]) DESC LIMIT 5",
            &vector_indexed(VectorMetric::Cosine),
        );
        match vector_topk_of(node) {
            Some(Node::VectorTopK {
                field,
                metric,
                k,
                source,
                ..
            }) => {
                assert_eq!(field, "embedding");
                assert_eq!(metric, VectorMetric::Cosine);
                assert_eq!(k, 5);
                assert!(source.is_none(), "no WHERE → no pre-filter source");
            }
            other => panic!("expected VectorTopK, got {other:?}"),
        }
    }

    #[test]
    fn knn_carries_the_index_dtype() {
        // The recogniser is metric-/dtype-agnostic, but it must thread the index's
        // stored width onto the node so the executor knows whether to rescore.
        let mut meta = vector_indexed(VectorMetric::Cosine);
        meta.vector_indexes[0].dtype = crate::plan::VectorDataType::Float16;
        let node = lower_with(
            "SELECT VALUE c FROM c ORDER BY VECTORDISTANCE(c.embedding, [1.0,0.0]) DESC LIMIT 5",
            &meta,
        );
        match vector_topk_of(node) {
            Some(Node::VectorTopK { dtype, .. }) => {
                assert_eq!(dtype, crate::plan::VectorDataType::Float16);
            }
            other => panic!("expected VectorTopK, got {other:?}"),
        }
    }

    #[test]
    fn knn_with_where_carries_prefilter_source() {
        // CORRECTNESS TRAP 1: a constraining WHERE becomes the candidate `source`
        // so the filter shrinks the set *before* the top-k. The source is the
        // pushed-down candidate pipeline (here an index path on `age`).
        let node = lower_with(
            "SELECT VALUE c FROM c WHERE c.age = 40 \
             ORDER BY VECTORDISTANCE(c.embedding, [1.0,0.0]) DESC LIMIT 3",
            &vector_indexed(VectorMetric::Cosine),
        );
        match vector_topk_of(node) {
            Some(Node::VectorTopK { source, k, .. }) => {
                assert_eq!(k, 3);
                assert!(
                    source.is_some(),
                    "a WHERE must hand its candidate ids to the top-k as `source`"
                );
            }
            other => panic!("expected VectorTopK with a pre-filter source, got {other:?}"),
        }
    }

    #[test]
    fn knn_euclidean_asc_is_recognized() {
        // euclidean is a distance → ASC is nearest-first.
        let node = lower_with(
            "SELECT VALUE c FROM c ORDER BY VECTORDISTANCE(c.embedding, [1.0,0.0]) ASC LIMIT 2",
            &vector_indexed(VectorMetric::Euclidean),
        );
        assert!(
            matches!(vector_topk_of(node), Some(Node::VectorTopK { metric, .. }) if metric == VectorMetric::Euclidean)
        );
    }

    #[test]
    fn knn_explicit_metric_arg_matching_index_is_recognized() {
        let node = lower_with(
            "SELECT VALUE c FROM c \
             ORDER BY VECTORDISTANCE(c.embedding, [1.0,0.0], 'dotproduct') DESC LIMIT 4",
            &vector_indexed(VectorMetric::DotProduct),
        );
        assert!(
            matches!(vector_topk_of(node), Some(Node::VectorTopK { metric, .. }) if metric == VectorMetric::DotProduct)
        );
    }

    #[test]
    fn knn_wrong_direction_falls_back_to_sort_limit() {
        // CORRECTNESS TRAP 2: cosine wants DESC; an ASC order is *not* the
        // nearest-first sense → must NOT use the index. Fall back to Sort+Limit.
        let node = lower_with(
            "SELECT VALUE c FROM c ORDER BY VECTORDISTANCE(c.embedding, [1.0,0.0]) ASC LIMIT 5",
            &vector_indexed(VectorMetric::Cosine),
        );
        assert!(vector_topk_of(node.clone()).is_none(), "must not seek");
        // The fallback is a correct Sort + Limit over the projected rows.
        assert_plan_has_sort_and_limit(node);
    }

    #[test]
    fn knn_wrong_metric_arg_falls_back() {
        // CORRECTNESS TRAP 2: the call names 'euclidean' but the index is cosine —
        // the index can't serve a different metric → fall back.
        let node = lower_with(
            "SELECT VALUE c FROM c \
             ORDER BY VECTORDISTANCE(c.embedding, [1.0,0.0], 'euclidean') DESC LIMIT 5",
            &vector_indexed(VectorMetric::Cosine),
        );
        assert!(vector_topk_of(node.clone()).is_none());
        assert_plan_has_sort_and_limit(node);
    }

    #[test]
    fn knn_non_indexed_field_falls_back() {
        // `other` has no vector index → fall back to the full Sort+Limit.
        let node = lower_with(
            "SELECT VALUE c FROM c ORDER BY VECTORDISTANCE(c.other, [1.0,0.0]) DESC LIMIT 5",
            &vector_indexed(VectorMetric::Cosine),
        );
        assert!(vector_topk_of(node.clone()).is_none());
        assert_plan_has_sort_and_limit(node);
    }

    #[test]
    fn knn_without_limit_falls_back() {
        // No LIMIT → no k → the seek isn't applicable; a plain Sort.
        let node = lower_with(
            "SELECT VALUE c FROM c ORDER BY VECTORDISTANCE(c.embedding, [1.0,0.0]) DESC",
            &vector_indexed(VectorMetric::Cosine),
        );
        assert!(vector_topk_of(node.clone()).is_none());
        let Node::Project { source, .. } = node else {
            panic!("expected Project");
        };
        assert!(
            matches!(*source, Node::Sort { .. }),
            "fallback keeps the Sort"
        );
    }

    #[test]
    fn knn_with_offset_falls_back() {
        // A non-zero OFFSET over a top-k is left to the generic path.
        let node = lower_with(
            "SELECT VALUE c FROM c ORDER BY VECTORDISTANCE(c.embedding, [1.0,0.0]) DESC \
             OFFSET 2 LIMIT 5",
            &vector_indexed(VectorMetric::Cosine),
        );
        assert!(vector_topk_of(node.clone()).is_none());
        assert_plan_has_sort_and_limit(node);
    }

    #[test]
    fn knn_two_order_keys_falls_back() {
        // The VECTORDISTANCE key must be the *sole* ORDER BY key.
        let node = lower_with(
            "SELECT VALUE c FROM c \
             ORDER BY VECTORDISTANCE(c.embedding, [1.0,0.0]) DESC, c.age ASC LIMIT 5",
            &vector_indexed(VectorMetric::Cosine),
        );
        assert!(vector_topk_of(node).is_none());
    }

    /// Assert the plan fell back to the generic `Sort` + `Limit` shape:
    /// `Limit(Project(Sort(...)))`.
    fn assert_plan_has_sort_and_limit(node: Node) {
        let Node::Limit { source, .. } = node else {
            panic!("expected a trailing Limit, got {node:?}");
        };
        let Node::Project { source, .. } = *source else {
            panic!("expected Project under Limit");
        };
        assert!(
            matches!(*source, Node::Sort { .. }),
            "expected a Sort under the Project in the fallback"
        );
    }
}
