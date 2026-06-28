//! The planner front door: lower a [`Statement`] into a [`Plan`].
//!
//! [`plan`] is the single fallible entry point both query surfaces go through.
//! It owns *all* plan shaping — validating a read, lowering it (sargability,
//! index choice), and, for writes, wrapping the matched-document source with the
//! collection's validators and before/after triggers. The caller supplies only
//! the catalog state ([`PlanContext`]: the target collection, its index
//! metadata, and its resolved hooks); it builds no `Node`/`Plan` trees itself.

use std::collections::HashMap;

use slate_ast::{Assignment, Expression, OrderByItem, SortDirection, Statement};

use crate::lower::lower_query;
use crate::plan::{CollectionRef, Node, Plan, RowBinding};
use crate::sargable::CollectionMeta;
use crate::validate::{PlanError, validate_bindings, validate_grouping, validate_udfs};

/// The catalog state a [`Statement`] is planned against: the target collection,
/// the index metadata used to choose a scan source, and the collection's
/// validators / triggers (resolved by the caller, who owns the catalog). A
/// statement plans identically given the same context, wherever it runs.
#[derive(Debug, Clone)]
pub struct PlanContext {
    /// The container the statement reads from / writes to.
    pub container: CollectionRef,
    /// Index/pk metadata for choosing a scan source.
    pub meta: CollectionMeta,
    /// Schema validator *bindings* to gate writes through (empty = none): an
    /// ordered list of `(validator_name, native_function_name)`, resolved from the
    /// catalog by the caller. The executor looks up each native name in the live
    /// validator bag at fire time (a dangling binding aborts the write, fail-safe).
    pub validators: Vec<(String, String)>,
    /// Before/after-mutation trigger *bindings* to fire (empty = none): an
    /// ordered list of `(trigger_name, native_function_name)`, resolved from the
    /// catalog by the caller. The same set is attached with a per-action label
    /// (`inserting`/`inserted`, …); the executor resolves each native name in the
    /// live trigger bag at fire time (a dangling binding aborts the write,
    /// fail-safe) and the trigger body branches on the action.
    pub triggers: Vec<(String, String)>,
    /// The collection's UDF bindings (`query_name -> native_name`), resolved
    /// from the catalog. The planner checks that every `udf.NAME` reference is
    /// bound — a dangling reference is a plan-build error — exactly as it
    /// resolves triggers/validators through this context. The native name is
    /// looked up later, in the executor's bag.
    pub udfs: HashMap<String, String>,
}

/// Lower a statement into an executable [`Plan`] against `ctx`.
///
/// Reads are validated then lowered; writes lower their matched-document source,
/// wrap it with validators + before-triggers, build the mutation plan, and wrap
/// that with after-triggers — the order the engine requires.
pub fn plan(stmt: Statement, ctx: &PlanContext) -> Result<Plan, PlanError> {
    match stmt {
        Statement::Query(query) => {
            // The read surface is user-authored (SQL text / a find request), so
            // validate it before lowering — the two checks Cosmos applies.
            validate_bindings(&query)?;
            validate_grouping(&query)?;
            validate_udfs(&query, &ctx.udfs)?;
            let node = lower_query(query, ctx.container.clone(), &ctx.meta, &[]);
            // Covering applies only to reads: a single-field or compound index
            // scan whose query touches only indexed components and the pk skips
            // the document fetch (RFC Part B). Writes lower through `write_source`
            // and never reach here, so a write always sees the real documents.
            Ok(Plan::Query(crate::covering::apply(node, &ctx.meta)))
        }

        Statement::Insert { docs } => {
            let source = wrap_before(ctx, "inserting", Node::Values(docs));
            let plan = Plan::Insert {
                collection: ctx.container.clone(),
                source,
            };
            Ok(wrap_after(ctx, "inserted", plan))
        }

        Statement::Upsert { docs, mode } => {
            // Upsert decides insert-vs-update per document at runtime, so it fires
            // the inserting/updating hooks internally rather than via wrappers.
            Ok(Plan::Upsert {
                collection: ctx.container.clone(),
                mode,
                triggers: ctx.triggers.clone(),
                source: Node::Values(docs),
            })
        }

        Statement::Update { query, assignments } => {
            // The primary key is immutable — reject any assignment targeting it.
            // (This is the check that used to live in `parse_mutation`; it needs
            // the catalog's pk path, which only exists here at plan time.)
            reject_pk_mutation(&assignments, &ctx.meta.pk_path)?;
            let source = wrap_before(ctx, "updating", write_source(query, ctx));
            let plan = Plan::Update {
                collection: ctx.container.clone(),
                assignments,
                source,
            };
            Ok(wrap_after(ctx, "updated", plan))
        }

        Statement::Replace { query, replacement } => {
            let source = wrap_before(ctx, "updating", write_source(query, ctx));
            let plan = Plan::Replace {
                collection: ctx.container.clone(),
                replacement,
                source,
            };
            Ok(wrap_after(ctx, "updated", plan))
        }

        Statement::Delete { query } => {
            // Delete performs no write, so it runs before-triggers but no validators.
            let source = wrap_before_triggers(ctx, "deleting", write_source(query, ctx));
            let plan = Plan::Delete {
                collection: ctx.container.clone(),
                source,
            };
            Ok(wrap_after(ctx, "deleted", plan))
        }

        Statement::Distinct {
            alias,
            field,
            predicate,
            sort,
            skip,
            take,
        } => Ok(Plan::Query(build_distinct(
            ctx, &alias, &field, predicate, sort, skip, take,
        ))),
    }
}

/// The read node selecting the documents a write targets: the find `query`
/// lowered to its `Scan → [Filter] → Project(c)` source tree.
fn write_source(query: slate_ast::Query, ctx: &PlanContext) -> Node {
    lower_query(query, ctx.container.clone(), &ctx.meta, &[])
}

/// Reject any assignment whose target's first path segment is the primary key —
/// the pk is immutable. (`a.b` targeting the pk root is rejected too.)
fn reject_pk_mutation(assignments: &[Assignment], pk_path: &str) -> Result<(), PlanError> {
    for a in assignments {
        if a.path.first().map(String::as_str) == Some(pk_path) {
            return Err(PlanError {
                message: format!("cannot mutate primary key field '{pk_path}'"),
            });
        }
    }
    Ok(())
}

/// Wrap a write source with the collection's validators then before-triggers
/// (`Trigger(before) → Validate → source`).
fn wrap_before(ctx: &PlanContext, action: &str, source: Node) -> Node {
    let node = if ctx.validators.is_empty() {
        source
    } else {
        Node::Validate {
            validators: ctx.validators.clone(),
            source: Box::new(source),
        }
    };
    wrap_before_triggers(ctx, action, node)
}

/// Wrap a write source with the collection's before-triggers only (no
/// validators) — used by delete, which performs no write.
fn wrap_before_triggers(ctx: &PlanContext, action: &str, source: Node) -> Node {
    if ctx.triggers.is_empty() {
        source
    } else {
        Node::Trigger {
            cf: ctx.container.cf.clone(),
            action: action.to_string(),
            triggers: ctx.triggers.clone(),
            source: Box::new(source),
        }
    }
}

/// Wrap a finished write plan with the collection's after-triggers.
fn wrap_after(ctx: &PlanContext, action: &str, plan: Plan) -> Plan {
    if ctx.triggers.is_empty() {
        plan
    } else {
        Plan::Trigger {
            cf: ctx.container.cf.clone(),
            action: action.to_string(),
            triggers: ctx.triggers.clone(),
            plan: Box::new(plan),
        }
    }
}

/// Build the Mongo `distinct` pipeline: `Scan → [Filter] → Project(path) →
/// Distinct(flatten) → [Sort] → [Limit]`. The projected path uses array-
/// distributing [`Expression::PathGet`] and `Distinct` flattens one level, so an
/// array field's elements are the distinct values (Mongo semantics, not SQL's).
fn build_distinct(
    ctx: &PlanContext,
    alias: &str,
    field: &str,
    predicate: Option<Expression>,
    sort: Option<SortDirection>,
    skip: Option<u64>,
    take: Option<u64>,
) -> Node {
    let binding = RowBinding::Alias(alias.to_string());
    let mut node = Node::Scan {
        collection: ctx.container.clone(),
    };
    if let Some(predicate) = predicate {
        node = Node::Filter {
            predicate,
            binding: binding.clone(),
            source: Box::new(node),
        };
    }
    node = Node::Project {
        expr: Expression::PathGet {
            base: Box::new(Expression::Identifier(alias.to_string())),
            path: field.split('.').map(str::to_string).collect(),
        },
        binding: binding.clone(),
        source: Box::new(node),
    };
    node = Node::Distinct {
        source: Box::new(node),
        flatten: true,
    };
    if let Some(direction) = sort {
        // The distinct values are the bare rows (bound to `alias`), so ordering
        // by `alias` sorts the values themselves.
        node = Node::Sort {
            keys: vec![OrderByItem {
                expr: Expression::Identifier(alias.to_string()),
                direction,
            }],
            binding,
            source: Box::new(node),
        };
    }
    if skip.is_some() || take.is_some() {
        node = Node::Limit {
            skip: skip.unwrap_or(0) as usize,
            take: take.map(|n| n as usize),
            source: Box::new(node),
        };
    }
    node
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::UpsertMode;
    use bson::RawBson;

    fn ctx() -> PlanContext {
        PlanContext {
            container: CollectionRef {
                cf: "default".into(),
                collection: "people".into(),
            },
            meta: CollectionMeta {
                indexes: vec!["age".into()],
                compound_indexes: Vec::new(),
                vector_indexes: Vec::new(),
                pk_path: "_id".into(),
            },
            validators: Vec::new(),
            triggers: Vec::new(),
            udfs: HashMap::new(),
        }
    }

    fn query(sql: &str) -> Statement {
        Statement::Query(slate_sql::parse(sql).unwrap())
    }

    #[test]
    fn unbound_udf_is_a_plan_error() {
        // `udf.tax` with no binding in the context → rejected at plan build,
        // before execution.
        let err = plan(query("SELECT VALUE udf.tax(c.x) FROM c"), &ctx()).unwrap_err();
        assert!(
            err.message.contains("udf.tax") && err.message.contains("not bound"),
            "got: {}",
            err.message
        );
    }

    #[test]
    fn bound_udf_plans() {
        // With the binding present, planning succeeds; the native name is
        // resolved later, in the executor's bag.
        let mut c = ctx();
        c.udfs.insert("tax".to_string(), "compute_tax".to_string());
        assert!(plan(query("SELECT VALUE udf.tax(c.x) FROM c"), &c).is_ok());
    }

    #[test]
    fn query_validates_then_lowers() {
        let Plan::Query(Node::Project { .. }) =
            plan(query("SELECT VALUE c FROM c"), &ctx()).unwrap()
        else {
            panic!("expected a lowered read query");
        };
    }

    #[test]
    fn query_rejects_unqualified_identifier() {
        // `id` is not a bound path — validation must fail (not panic / mis-plan).
        assert!(plan(query("SELECT VALUE id FROM c"), &ctx()).is_err());
    }

    #[test]
    fn query_rejects_ungrouped_column() {
        assert!(
            plan(
                query("SELECT c.kind, c.other FROM c GROUP BY c.kind"),
                &ctx()
            )
            .is_err()
        );
    }

    #[test]
    fn insert_with_no_hooks_is_bare() {
        // No validators/triggers configured → no wrapper nodes.
        let docs = vec![RawBson::Int32(1)];
        match plan(Statement::Insert { docs }, &ctx()).unwrap() {
            Plan::Insert { source, .. } => assert!(matches!(source, Node::Values(_))),
            other => panic!("expected bare Insert, got {other:?}"),
        }
    }

    #[test]
    fn delete_lowers_filter_into_source() {
        // The find query selecting the targets becomes the Delete source tree,
        // sargably planned: a pk equality lowers to a KeyLookup point read.
        let q = slate_sql::parse(r#"SELECT VALUE c FROM c WHERE c._id = "2""#).unwrap();
        match plan(Statement::Delete { query: q }, &ctx()).unwrap() {
            Plan::Delete { source, .. } => {
                let Node::Project { source, .. } = source else {
                    panic!("expected Project source");
                };
                assert!(matches!(*source, Node::KeyLookup { .. }));
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn distinct_builds_flattening_pipeline() {
        let stmt = Statement::Distinct {
            alias: "c".into(),
            field: "tags".into(),
            predicate: None,
            sort: None,
            skip: None,
            take: None,
        };
        let Plan::Query(Node::Distinct { source, flatten }) = plan(stmt, &ctx()).unwrap() else {
            panic!("expected a Distinct pipeline");
        };
        assert!(flatten, "Mongo distinct flattens arrays one level");
        assert!(matches!(*source, Node::Project { .. }));
    }

    #[test]
    fn upsert_carries_mode_and_hooks() {
        let docs = vec![RawBson::Int32(1)];
        match plan(
            Statement::Upsert {
                docs,
                mode: UpsertMode::Merge,
            },
            &ctx(),
        )
        .unwrap()
        {
            Plan::Upsert { mode, .. } => assert_eq!(mode, UpsertMode::Merge),
            other => panic!("expected Upsert, got {other:?}"),
        }
    }
}
