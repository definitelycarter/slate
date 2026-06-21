//! The planner front door: lower a [`Statement`] into a [`Plan`].
//!
//! [`plan`] is the single fallible entry point both query surfaces go through.
//! It owns *all* plan shaping — validating a read, lowering it (sargability,
//! index choice), and, for writes, wrapping the matched-document source with the
//! collection's validators and before/after triggers. The caller supplies only
//! the catalog state ([`PlanContext`]: the target collection, its index
//! metadata, and its resolved hooks); it builds no `Node`/`Plan` trees itself.

use slate_ast::{OrderByItem, ScalarExpr, SortDirection, Statement};
use slate_vm::ResolvedHook;

use crate::lower::lower_query;
use crate::plan::{CollectionRef, Node, Plan, RowBinding};
use crate::sargable::CollectionMeta;
use crate::validate::{PlanError, validate_bindings, validate_grouping};

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
    /// Schema validators to gate writes through (empty = none).
    pub validators: Vec<ResolvedHook>,
    /// Before/after-mutation triggers to fire (empty = none). The same set is
    /// attached with a per-action label (`inserting`/`inserted`, …); the
    /// executor fires only the hooks registered for that action.
    pub triggers: Vec<ResolvedHook>,
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
            Ok(Plan::Query(lower_query(
                query,
                ctx.container.clone(),
                &ctx.meta,
                &[],
            )))
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
                hooks: ctx.triggers.clone(),
                source: Node::Values(docs),
            })
        }

        Statement::Update { query, mutation } => {
            let source = wrap_before(ctx, "updating", write_source(query, ctx));
            let plan = Plan::Update {
                collection: ctx.container.clone(),
                mutation,
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
            hooks: ctx.triggers.clone(),
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
            hooks: ctx.triggers.clone(),
            plan: Box::new(plan),
        }
    }
}

/// Build the Mongo `distinct` pipeline: `Scan → [Filter] → Project(path) →
/// Distinct(flatten) → [Sort] → [Limit]`. The projected path uses array-
/// distributing [`ScalarExpr::PathGet`] and `Distinct` flattens one level, so an
/// array field's elements are the distinct values (Mongo semantics, not SQL's).
fn build_distinct(
    ctx: &PlanContext,
    alias: &str,
    field: &str,
    predicate: Option<ScalarExpr>,
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
        expr: ScalarExpr::PathGet {
            base: Box::new(ScalarExpr::Identifier(alias.to_string())),
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
                expr: ScalarExpr::Identifier(alias.to_string()),
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
                pk_path: "_id".into(),
            },
            validators: Vec::new(),
            triggers: Vec::new(),
        }
    }

    fn query(sql: &str) -> Statement {
        Statement::Query(slate_sql::parse(sql).unwrap())
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
