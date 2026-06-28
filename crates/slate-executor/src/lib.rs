//! `slate-executor` — the **v2** physical execution engine.
//!
//! Consumes a [`slate_planner::Plan`] and streams its results against a
//! transaction. This is the *physical* half of the v2 rebuild; `slate-planner`
//! makes the decisions, this crate runs them. It runs alongside (and does not
//! touch) v1's executor in `slate-db`.
//!
//! Pull-based, mirroring v1's `RawIter`: each [`Node`] is executed by a small
//! per-node function in [`nodes`] (see that module for the source/transform
//! contract). [`Executor::execute`] is the dispatcher — it recurses children,
//! then hands each node executor its input. Node *setup* is fallible (a `Scan`
//! resolves a collection and opens a store iterator), so the dispatcher returns
//! `Result<ValueIter, ExecError>`; per-row errors then flow on the stream.
//!
//! The stream item is `Result<Option<RawBson>, ExecError>`. The `Option` is the
//! *undefined* channel: `Some(v)` is a value, `None` is undefined and is
//! dropped at the output boundary ([`collect`]). Carrying raw `RawBson` keeps
//! the `find` path zero-copy — values are not re-parsed as they flow.
//!
//! ## Expression evaluation
//!
//! Like toydb, the planner embeds expressions and the executor *calls*
//! evaluation — the evaluator lives with the expression type (in `slate-eval`,
//! over the `slate-ast` types), so there is exactly one of it across find and
//! SQL. `Project`/`Filter` use the **raw** evaluator, walking row bytes
//! zero-copy rather than decoding each row to `Bson`.

mod analyze;
mod env;
mod error;
mod nodes;
mod trace;
pub mod watch;

#[cfg(feature = "bench-internals")]
pub mod bench;

use std::rc::Rc;

use bson::RawBson;
use bson::raw::CString;
use slate_engine::{Catalog, EngineTransaction};
use slate_eval::EvalError;
use slate_planner::{Node, Plan, PlanStats};

use analyze::Counting;
pub use env::ExecEnv;
pub use error::ExecError;
pub use watch::{CapturedEvent, ChangeEvent, Compiled, CompiledWatch, WatchSink};

/// Map the planner's [`VectorMetric`](slate_planner::VectorMetric) onto
/// [`slate_eval::VectorMetric`], which the vector-distance math is keyed on — the
/// planner can't name the eval type, so the metric crosses the boundary here, the
/// physical-execution side of the same map `slate-db` does for the catalog. The
/// shared eval math is what keeps the top-k node bit-identical to the scalar
/// `VECTORDISTANCE`.
fn map_vector_metric(metric: slate_planner::VectorMetric) -> slate_eval::VectorMetric {
    match metric {
        slate_planner::VectorMetric::Cosine => slate_eval::VectorMetric::Cosine,
        slate_planner::VectorMetric::DotProduct => slate_eval::VectorMetric::DotProduct,
        slate_planner::VectorMetric::Euclidean => slate_eval::VectorMetric::Euclidean,
    }
}

/// A streaming sequence of optionally-undefined raw values.
///
/// The `'a` lifetime ties a stream to the transaction it reads from.
pub type ValueIter<'a> = Box<dyn Iterator<Item = Result<Option<RawBson>, ExecError>> + 'a>;

/// Executes plans against a transaction, with an optional scripting pool for
/// validators/triggers and an optional `@`-parameter document for SQL queries.
pub struct Executor<'a, T> {
    /// The engine transaction the query reads and (for writes) mutates — the
    /// data handle the source/mutation nodes run against. Held *beside* `env`,
    /// not inside it: together they are the query's execution context, but the
    /// transaction is not an *evaluator* input, so it stays a peer rather than
    /// living in the capability bundle (mirroring how the per-row `RowEnv` holds
    /// no transaction). Keeping it here is also why [`ExecEnv`] needs no engine
    /// type parameter.
    txn: &'a T,
    /// The per-query evaluator capabilities — pool, params, rand, and watch sink
    /// — bundled so a new capability is one field rather than a re-thread through
    /// every layer (see [`ExecEnv`]).
    env: ExecEnv<'a>,
    /// `EXPLAIN ANALYZE` collector, set *only* by
    /// [`execute_analyze`](Self::execute_analyze). When `None` (the normal path),
    /// `execute_node` builds no counting wrappers and pays nothing — the whole
    /// instrumentation is off. When `Some`, each node's output is wrapped in
    /// [`Counting`] keyed by its pre-order index (tracked in `analyze_index`).
    ///
    /// Instrumentation is *not* a per-query evaluator input, so it stays on the
    /// `Executor` rather than the [`ExecEnv`] bundle.
    analyze: Option<Rc<PlanStats>>,
    /// Running pre-order node index for the analyze walk. `Cell` because
    /// `execute_node` is `&self`; it advances once per node, in the same order
    /// the renderer and `PlanStats::for_plan` walk the tree.
    analyze_index: std::cell::Cell<usize>,
}

impl<'a, T: EngineTransaction + Catalog> Executor<'a, T> {
    /// Construct an executor from a transaction and a fully-built [`ExecEnv`]
    /// capability bundle — the two halves of the execution context. This is the
    /// seam call sites migrate to; the other constructors are thin sugar over
    /// it, building the bundle a capability at a time.
    pub fn with_env(txn: &'a T, env: ExecEnv<'a>) -> Self {
        Self {
            txn,
            env,
            analyze: None,
            analyze_index: std::cell::Cell::new(0),
        }
    }

    /// Construct an executor with an empty [`ExecEnv`] (no pool, params, rand,
    /// or watch). The capabilities are attached on the env via
    /// [`with_env`](Self::with_env); the db layer's `Transaction::exec_env`
    /// builds the populated bundle.
    pub fn new(txn: &'a T) -> Self {
        Self::with_env(txn, ExecEnv::new())
    }

    /// Execute a plan into a streaming iterator. For write plans, the mutations
    /// happen as the stream is consumed (drain it to apply them).
    pub fn execute(&self, plan: Plan) -> Result<ValueIter<'a>, ExecError> {
        trace::trace_scope!("execute", plan = plan_kind(&plan));
        trace::trace_event!(plan = plan_kind(&plan), "execute plan");
        match plan {
            Plan::Query(node) => self.execute_node(node, None),

            Plan::Insert { collection, source } => {
                let handle = self
                    .txn
                    .collection(&collection.cf, &collection.collection)?;
                let source = self.execute_node(source, None)?;
                nodes::insert::execute(self.txn, handle, source, self.env.watch.clone())
            }

            Plan::Delete { collection, source } => {
                let handle = self
                    .txn
                    .collection(&collection.cf, &collection.collection)?;
                let source = self.execute_node(source, None)?;
                nodes::delete::execute(self.txn, handle, source, self.env.watch.clone())
            }

            Plan::Replace {
                collection,
                replacement,
                source,
            } => {
                let handle = self
                    .txn
                    .collection(&collection.cf, &collection.collection)?;
                let source = self.execute_node(source, None)?;
                nodes::replace::execute(
                    self.txn,
                    handle,
                    replacement,
                    source,
                    self.env.watch.clone(),
                )
            }

            Plan::Update {
                collection,
                assignments,
                source,
            } => {
                let handle = self
                    .txn
                    .collection(&collection.cf, &collection.collection)?;
                let source = self.execute_node(source, None)?;
                nodes::mutate::execute(
                    self.txn,
                    handle,
                    assignments,
                    source,
                    self.env.watch.clone(),
                )
            }

            Plan::Trigger {
                cf,
                action,
                hooks,
                plan,
            } => {
                let inner = self.execute(*plan)?;
                nodes::trigger::execute(self.txn, self.env.pool, cf, action, hooks, inner)
            }

            Plan::Upsert {
                collection,
                mode,
                hooks,
                source,
            } => {
                let handle = self
                    .txn
                    .collection(&collection.cf, &collection.collection)?;
                let source = self.execute_node(source, None)?;
                nodes::upsert::execute(
                    self.txn,
                    self.env.pool,
                    hooks,
                    handle,
                    mode,
                    source,
                    self.env.watch.clone(),
                )
            }
        }
    }

    /// Execute a plan and collect the **defined** results, dropping undefined
    /// rows. The typical output boundary for a query.
    pub fn execute_collect(&self, plan: Plan) -> Result<Vec<RawBson>, ExecError> {
        collect(self.execute(plan)?)
    }

    /// Execute a plan with `EXPLAIN ANALYZE` instrumentation, returning the
    /// defined results and a [`PlanStats`] of per-node row counts.
    ///
    /// This is the *only* path that builds counting wrappers; the plain
    /// [`execute`](Self::execute) never reads `stats`, so normal queries pay
    /// nothing for instrumentation. The returned `stats` is sized to one counter
    /// per node in `plan` and keyed by pre-order index — pass it to
    /// [`Plan::explain_analyze`](slate_planner::Plan::explain_analyze) (clone the
    /// plan first if you need it for rendering, since execution consumes it).
    pub fn execute_analyze(
        mut self,
        plan: Plan,
    ) -> Result<(Vec<RawBson>, Rc<PlanStats>), ExecError> {
        let stats = Rc::new(PlanStats::for_plan(&plan));
        self.analyze = Some(Rc::clone(&stats));
        self.analyze_index.set(0);
        let rows = collect(self.execute(plan)?)?;
        Ok((rows, stats))
    }

    /// Dispatch a node to its per-node executor, recursing into children first.
    ///
    /// `current` is the outer row supplied by an enclosing [`Node::Subquery`],
    /// threaded down so a [`Node::CurrentRow`] leaf can yield it (correlation);
    /// it's `None` outside any subquery.
    fn execute_node(
        &self,
        node: Node,
        current: Option<&RawBson>,
    ) -> Result<ValueIter<'a>, ExecError> {
        // Claim this node's pre-order index *before* recursing children, so the
        // index walk matches the renderer and `PlanStats::for_plan`. Off the
        // analyze path this is a no-op (and `index` is unused, hence the wrap).
        let index = if self.analyze.is_some() {
            let i = self.analyze_index.get();
            self.analyze_index.set(i + 1);
            i
        } else {
            0
        };
        let iter: ValueIter<'a> = match node {
            Node::Values(values) => nodes::values::execute(values),

            Node::Scan { collection } => nodes::scan::execute(self.txn, &collection)?,

            Node::IndexScan {
                collection,
                field,
                range,
                direction,
                limit,
                covering,
            } => nodes::index_scan::execute(
                self.txn,
                &collection,
                field,
                &range,
                direction,
                limit,
                covering,
            )?,

            Node::CompoundIndexScan {
                collection,
                field,
                range,
                direction,
                limit,
                covering,
            } => nodes::compound_index_scan::execute(
                self.txn,
                &collection,
                field,
                &range,
                direction,
                limit,
                covering,
            )?,

            Node::KeyLookup { collection, source } => {
                let source = self.execute_node(*source, current)?;
                nodes::key_lookup::execute(self.txn, &collection, source)?
            }

            Node::VectorTopK {
                collection,
                field,
                query_vector,
                metric,
                k,
                source,
            } => {
                // The optional pre-filter sub-plan yields the candidate doc-ids
                // (rule 1); `None` scans the whole field.
                let source = source.map(|s| self.execute_node(*s, current)).transpose()?;
                nodes::vector_topk::execute(
                    self.txn,
                    &collection,
                    field,
                    query_vector,
                    map_vector_metric(metric),
                    k,
                    source,
                    self.env.clone(),
                )?
            }

            Node::IndexMerge {
                collection,
                logical,
                lhs,
                rhs,
            } => {
                let left = self.execute_node(*lhs, current)?;
                let right = self.execute_node(*rhs, current)?;
                nodes::index_merge::execute(self.txn, &collection, logical, left, right)?
            }

            Node::Bind { alias, source } => {
                let source = self.execute_node(*source, current)?;
                nodes::bind::execute(alias, source)
            }

            Node::Unwind {
                alias,
                array,
                source,
            } => {
                let source = self.execute_node(*source, current)?;
                nodes::unwind::execute(alias, array, source, self.env.clone())
            }

            Node::Project {
                expr,
                binding,
                source,
            } => {
                let source = self.execute_node(*source, current)?;
                nodes::project::execute(expr, binding, source, self.env.clone())
            }

            Node::Filter {
                predicate,
                binding,
                source,
            } => {
                let source = self.execute_node(*source, current)?;
                nodes::filter::execute(predicate, binding, source, self.env.clone())
            }

            Node::Sort {
                keys,
                binding,
                source,
            } => {
                let source = self.execute_node(*source, current)?;
                nodes::sort::execute(keys, binding, source, self.env.clone())?
            }

            Node::Limit { skip, take, source } => {
                let source = self.execute_node(*source, current)?;
                nodes::limit::execute(skip, take, source)
            }

            Node::Distinct { source, flatten } => {
                let source = self.execute_node(*source, current)?;
                nodes::distinct::execute(source, flatten)
            }

            Node::Aggregate {
                group_keys,
                aggregates,
                binding,
                source,
            } => {
                let source = self.execute_node(*source, current)?;
                nodes::aggregate::execute(
                    group_keys,
                    aggregates,
                    binding,
                    source,
                    self.env.clone(),
                )?
            }

            Node::Trigger {
                cf,
                action,
                hooks,
                source,
            } => {
                let source = self.execute_node(*source, current)?;
                nodes::trigger::execute(self.txn, self.env.pool, cf, action, hooks, source)?
            }

            Node::Validate { validators, source } => {
                let source = self.execute_node(*source, current)?;
                nodes::validate::execute(self.env.validator, validators, source)?
            }

            // The single outer row fed in by an enclosing `Subquery`.
            Node::CurrentRow => match current {
                Some(row) => Box::new(std::iter::once(Ok(Some(row.clone())))),
                None => Box::new(std::iter::once(Err(EvalError {
                    message: "CurrentRow evaluated outside a subquery".into(),
                }
                .into()))),
            },

            // Correlated apply: for each outer row, run the subplan with that row
            // fed in via `CurrentRow`, reduce by `kind`, and attach to the slot.
            // Blocking (eager) so the result stream borrows only the txn, not the
            // executor. The subplan is cloned per row because re-running it
            // consumes a fresh `Node`; this is the nested-loop baseline that
            // decorrelation would optimize.
            Node::Subquery {
                slot,
                kind,
                subplan,
                source,
            } => {
                let source = self.execute_node(*source, current)?;
                // The slot name is stable across every outer row, so validate it
                // into a `CString` once here rather than rebuilding it per row in
                // `augment`.
                let key = CString::try_from(slot.as_str()).map_err(|e| EvalError {
                    message: format!("invalid subquery slot '{slot}': {e}"),
                })?;
                // In analyze mode the subplan re-runs per outer row, which would
                // advance the pre-order index counter once per run. Snapshot the
                // counter at the subplan's base and restore it before each run so
                // the subplan's nodes keep their single stable indices and their
                // counts accumulate across runs.
                let subplan_base = self.analyze_index.get();
                let mut out: Vec<RawBson> = Vec::new();
                for item in source {
                    let Some(row) = item? else { continue };
                    if self.analyze.is_some() {
                        self.analyze_index.set(subplan_base);
                    }
                    let sub = self.execute_node((*subplan).clone(), Some(&row))?;
                    let value = nodes::subquery::reduce(sub, kind)?;
                    out.push(nodes::subquery::augment(row, &key, value)?);
                }
                Box::new(out.into_iter().map(|v| Ok(Some(v))))
            }
        };

        // On the analyze path, wrap the node's output so each emitted row bumps
        // this node's pre-order counter. Off it, return the iterator untouched —
        // zero added cost for normal execution.
        Ok(match &self.analyze {
            Some(stats) => Box::new(Counting::new(iter, Rc::clone(stats), index)),
            None => iter,
        })
    }
}

/// A stable, allocation-free label for a plan's top-level kind, for tracing
/// fields. Only built when the `trace` feature is on (otherwise the call sites
/// vanish and this would be dead code).
#[cfg(feature = "trace")]
fn plan_kind(plan: &Plan) -> &'static str {
    match plan {
        Plan::Query(_) => "query",
        Plan::Insert { .. } => "insert",
        Plan::Delete { .. } => "delete",
        Plan::Update { .. } => "update",
        Plan::Replace { .. } => "replace",
        Plan::Trigger { .. } => "trigger",
        Plan::Upsert { .. } => "upsert",
    }
}

/// Drain a stream into its defined values, dropping undefined rows.
pub fn collect(iter: ValueIter<'_>) -> Result<Vec<RawBson>, ExecError> {
    let mut out = Vec::new();
    for item in iter {
        if let Some(value) = item? {
            out.push(value);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod end_to_end {
    //! parse → lower → execute against a seeded engine — the full SQL loop.

    use crate::Executor;
    use crate::nodes::test_support::{people_ref, seeded_people};
    use bson::{RawBson, rawdoc};
    use slate_engine::{Catalog, DEFAULT_CF, Engine};
    use slate_planner::CollectionMeta;

    fn run(sql: &str) -> Vec<RawBson> {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        // Index metadata from the catalog drives sargability (age is indexed).
        let handle = txn.collection(DEFAULT_CF, "people").unwrap();
        let mut indexes = Vec::new();
        let mut compound_indexes = Vec::new();
        for identity in handle.indexes() {
            let components = slate_engine::split_index_fields(identity);
            if components.len() > 1 {
                compound_indexes.push((identity.clone(), components));
            } else {
                indexes.push(identity.clone());
            }
        }
        let meta = CollectionMeta {
            indexes,
            compound_indexes,
            vector_indexes: Vec::new(),
            pk_path: handle.pk_path().to_string(),
        };
        let plan = slate_planner::lower(slate_sql::parse(sql).unwrap(), people_ref(), &meta);
        Executor::new(&txn).execute_collect(plan).unwrap()
    }

    /// Convert a `bson!([...])` array into the `RawBson::Array` an executor emits.
    fn raw_array(b: bson::Bson) -> RawBson {
        RawBson::try_from(b).unwrap()
    }

    /// Lower a SQL string, run it under analyze, and render the annotated tree.
    fn analyze(sql: &str) -> String {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let handle = txn.collection(DEFAULT_CF, "people").unwrap();
        let meta = CollectionMeta {
            indexes: handle.indexes().to_vec(),
            compound_indexes: Vec::new(),
            vector_indexes: Vec::new(),
            pk_path: handle.pk_path().to_string(),
        };
        let plan = slate_planner::lower(slate_sql::parse(sql).unwrap(), people_ref(), &meta);
        let render_plan = plan.clone();
        let (_rows, stats) = Executor::new(&txn).execute_analyze(plan).unwrap();
        render_plan.explain_analyze(&stats)
    }

    #[test]
    fn explain_analyze_counts_rows_through_a_scan_filter() {
        // people: ada/36, alan/41, grace/44. WHERE c.age > 40 keeps 2 of 3.
        // `age` is indexed, so this could lower to an index path — assert on the
        // counts that must hold regardless of plan shape: the Scan/source sees 3
        // (or fewer via index seek) and the result is 2.
        let rendered = analyze("SELECT VALUE c.name FROM c WHERE c.age = 41");
        // Exactly one person is 41 (alan); the rendered tree must report rows=1
        // at the root projection.
        let root = rendered.lines().next().unwrap();
        assert!(root.contains("rows=1"), "root line was: {root}");
    }

    #[test]
    fn explain_analyze_full_scan_examines_all_rows() {
        // No predicate → a full scan of all 3 people, projected to names.
        let rendered = analyze("SELECT VALUE c.name FROM c");
        // The source scan emits all 3 documents.
        assert!(
            rendered.contains("rows=3"),
            "expected a node reporting rows=3; got:\n{rendered}"
        );
        // And the root projection emits 3 too.
        let root = rendered.lines().next().unwrap();
        assert!(root.contains("rows=3"), "root line was: {root}");
    }

    #[test]
    fn explain_analyze_filter_examined_exceeds_emitted() {
        // A residual filter that drops rows: examined should exceed rows on the
        // Filter line. Force a scan+filter (not an index seek) with a non-sargable
        // predicate over the unindexed `name`.
        let rendered = analyze("SELECT VALUE c.name FROM c WHERE c.name = \"grace\"");
        let filter_line = rendered
            .lines()
            .find(|l| l.trim_start().starts_with("Filter"))
            .expect("expected a Filter line");
        // Filter keeps 1 of the 3 it examines.
        assert!(
            filter_line.contains("rows=1") && filter_line.contains("examined=3"),
            "filter line was: {filter_line}"
        );
    }

    #[test]
    fn where_order_limit_project() {
        // people: ada/36, alan/41, grace/44
        let out = run("SELECT VALUE c.name FROM c WHERE c.age > 40 ORDER BY c.age DESC LIMIT 1");
        assert_eq!(out, vec![RawBson::String("grace".into())]);
    }

    #[test]
    fn object_projection() {
        let out = run(r#"SELECT VALUE { "n": c.name, "a": c.age } FROM c WHERE c.name = "ada""#);
        assert_eq!(
            out,
            vec![RawBson::Document(rawdoc! { "n": "ada", "a": 36 })]
        );
    }

    #[test]
    fn full_scan_ordered() {
        let out = run("SELECT VALUE c.name FROM c ORDER BY c.age ASC");
        assert_eq!(
            out,
            vec![
                RawBson::String("ada".into()),
                RawBson::String("alan".into()),
                RawBson::String("grace".into()),
            ]
        );
    }

    #[test]
    fn or_predicate_via_index_merge() {
        // age is indexed → OR lowers to IndexMerge(Or) + residual recheck.
        let out =
            run("SELECT VALUE c.name FROM c WHERE c.age = 36 OR c.age = 44 ORDER BY c.age ASC");
        assert_eq!(
            out,
            vec![
                RawBson::String("ada".into()),
                RawBson::String("grace".into())
            ]
        );
    }

    // ── Aggregates (no GROUP BY) ────────────────────────────────

    #[test]
    fn aggregate_count_all() {
        // people: ada/36, alan/41, grace/44
        assert_eq!(run("SELECT VALUE COUNT(1) FROM c"), vec![RawBson::Int64(3)]);
    }

    #[test]
    fn aggregate_count_with_filter() {
        assert_eq!(
            run("SELECT VALUE COUNT(1) FROM c WHERE c.age > 40"),
            vec![RawBson::Int64(2)]
        );
    }

    #[test]
    fn aggregate_count_empty_is_zero() {
        // A bare aggregate over zero matching rows still emits one row.
        assert_eq!(
            run("SELECT VALUE COUNT(1) FROM c WHERE c.age > 100"),
            vec![RawBson::Int64(0)]
        );
    }

    #[test]
    fn aggregate_min_max_preserve_type() {
        // ages are Int32 → MIN/MAX return the actual value, type preserved.
        assert_eq!(
            run("SELECT VALUE MIN(c.age) FROM c"),
            vec![RawBson::Int32(36)]
        );
        assert_eq!(
            run("SELECT VALUE MAX(c.age) FROM c"),
            vec![RawBson::Int32(44)]
        );
    }

    #[test]
    fn aggregate_sum_is_double() {
        // 36 + 41 + 44 = 121, returned as a Double (the all-double convention).
        assert_eq!(
            run("SELECT VALUE SUM(c.age) FROM c"),
            vec![RawBson::Double(121.0)]
        );
    }

    #[test]
    fn aggregate_tabular_projection() {
        // SELECT COUNT(1) AS n  →  [{ "n": 3 }]
        assert_eq!(
            run("SELECT COUNT(1) AS n FROM c"),
            vec![RawBson::Document(rawdoc! { "n": 3_i64 })]
        );
    }

    // ── GROUP BY ────────────────────────────────────────────────

    #[test]
    fn group_by_produces_one_row_per_group() {
        // Group by a boolean expression → two groups (ada is under 41; alan and
        // grace are 41+). One row per group, in discovery order.
        let out = run("SELECT c.age >= 41 AS senior, COUNT(1) AS n FROM c GROUP BY c.age >= 41");
        assert_eq!(
            out,
            vec![
                RawBson::Document(rawdoc! { "senior": false, "n": 1_i64 }),
                RawBson::Document(rawdoc! { "senior": true, "n": 2_i64 }),
            ]
        );
    }

    #[test]
    fn group_by_without_aggregate_is_distinct_groups() {
        // GROUP BY with no aggregate yields the distinct group values.
        let out = run("SELECT VALUE c.age >= 41 FROM c GROUP BY c.age >= 41");
        assert_eq!(out, vec![RawBson::Boolean(false), RawBson::Boolean(true)]);
    }

    #[test]
    fn group_by_with_order_by_sorts_the_groups() {
        // Two groups: under-41 (ada → n=1) and 41+ (alan, grace → n=2). Ordering
        // by the count descending puts the larger group first — the sort runs
        // after aggregation, over the group rows.
        let out = run("SELECT c.age >= 41 AS senior, COUNT(1) AS n FROM c \
             GROUP BY c.age >= 41 ORDER BY COUNT(1) DESC");
        assert_eq!(
            out,
            vec![
                RawBson::Document(rawdoc! { "senior": true, "n": 2_i64 }),
                RawBson::Document(rawdoc! { "senior": false, "n": 1_i64 }),
            ]
        );
    }

    // ── HAVING ──────────────────────────────────────────────────

    #[test]
    fn having_filters_groups_by_aggregate() {
        // Two groups: under-41 (ada → n=1) and 41+ (alan, grace → n=2).
        // HAVING COUNT(1) > 1 drops the single-member group.
        let out = run("SELECT c.age >= 41 AS senior, COUNT(1) AS n FROM c \
             GROUP BY c.age >= 41 HAVING COUNT(1) > 1");
        assert_eq!(
            out,
            vec![RawBson::Document(rawdoc! { "senior": true, "n": 2_i64 })]
        );
    }

    #[test]
    fn having_can_reference_a_group_key() {
        // HAVING over the group key itself: keep only the senior group.
        let out = run("SELECT c.age >= 41 AS senior, COUNT(1) AS n FROM c \
             GROUP BY c.age >= 41 HAVING c.age >= 41");
        assert_eq!(
            out,
            vec![RawBson::Document(rawdoc! { "senior": true, "n": 2_i64 })]
        );
    }

    #[test]
    fn having_aggregate_not_in_select() {
        // SUM(age) only appears in HAVING — it must still be computed. Senior
        // group's age sum is 41 + 44 = 85 > 80; the junior group's is 36.
        let out = run("SELECT VALUE c.age >= 41 FROM c \
             GROUP BY c.age >= 41 HAVING SUM(c.age) > 80");
        assert_eq!(out, vec![RawBson::Boolean(true)]);
    }

    #[test]
    fn having_with_order_by_runs_before_sort() {
        // Both groups survive HAVING COUNT(1) >= 1; ORDER BY then sorts them.
        let out = run("SELECT c.age >= 41 AS senior, COUNT(1) AS n FROM c \
             GROUP BY c.age >= 41 HAVING COUNT(1) >= 1 ORDER BY COUNT(1) DESC");
        assert_eq!(
            out,
            vec![
                RawBson::Document(rawdoc! { "senior": true, "n": 2_i64 }),
                RawBson::Document(rawdoc! { "senior": false, "n": 1_i64 }),
            ]
        );
    }

    #[test]
    fn having_dropping_all_groups_yields_no_rows() {
        let out = run("SELECT c.age >= 41 AS senior, COUNT(1) AS n FROM c \
             GROUP BY c.age >= 41 HAVING COUNT(1) > 100");
        assert!(out.is_empty());
    }

    // ── ARRAY_AGG / COLLECT ─────────────────────────────────────

    #[test]
    fn array_agg_gathers_group_values() {
        // Group by senior-ness; gather each group's names into an array.
        // ada is junior; alan, grace are senior (scan order is by _id 1,2,3).
        let out = run(
            "SELECT c.age >= 41 AS senior, ARRAY_AGG(c.name) AS names FROM c \
             GROUP BY c.age >= 41",
        );
        assert_eq!(
            out,
            vec![
                RawBson::Document(rawdoc! { "senior": false, "names": ["ada"] }),
                RawBson::Document(rawdoc! { "senior": true, "names": ["alan", "grace"] }),
            ]
        );
    }

    #[test]
    fn collect_is_a_synonym_for_array_agg() {
        let out = run("SELECT VALUE COLLECT(c.name) FROM c");
        assert_eq!(out, vec![raw_array(bson::bson!(["ada", "alan", "grace"]))]);
    }

    #[test]
    fn array_agg_over_empty_set_is_empty_array() {
        // A bare ARRAY_AGG over no matching rows still emits one row: `[]`.
        let out = run("SELECT VALUE ARRAY_AGG(c.name) FROM c WHERE c.age > 100");
        assert_eq!(out, vec![raw_array(bson::bson!([]))]);
    }

    // ── DOCUMENTID ──────────────────────────────────────────────

    #[test]
    fn documentid_returns_the_pk() {
        // DOCUMENTID(c) → c._id for the people fixture (pk path `_id`).
        let out = run("SELECT VALUE DOCUMENTID(c) FROM c ORDER BY c.age ASC");
        assert_eq!(
            out,
            vec![
                RawBson::String("1".into()),
                RawBson::String("2".into()),
                RawBson::String("3".into()),
            ]
        );
    }

    #[test]
    fn documentid_in_where_point_reads() {
        // DOCUMENTID(c) = "2" selects exactly alan.
        let out = run(r#"SELECT VALUE c.name FROM c WHERE DOCUMENTID(c) = "2""#);
        assert_eq!(out, vec![RawBson::String("alan".into())]);
    }

    #[test]
    fn documentid_grouped_with_count() {
        // GROUP BY DOCUMENTID(c) — one group per document, COUNT(1) = 1 each.
        let out = run(
            "SELECT DOCUMENTID(c) AS id, COUNT(1) AS n FROM c GROUP BY DOCUMENTID(c) ORDER BY DOCUMENTID(c) ASC",
        );
        assert_eq!(
            out,
            vec![
                RawBson::Document(rawdoc! { "id": "1", "n": 1_i64 }),
                RawBson::Document(rawdoc! { "id": "2", "n": 1_i64 }),
                RawBson::Document(rawdoc! { "id": "3", "n": 1_i64 }),
            ]
        );
    }
}

#[cfg(test)]
mod hooks {
    use crate::nodes::test_support::seeded_people;
    use crate::{ExecEnv, ExecError, Executor};
    use bson::{RawBson, rawdoc};
    use slate_engine::{DEFAULT_CF, Engine};
    use slate_planner::{Node, Plan};
    use slate_validator::{ValidatorBag, ValidatorCtx, Verdict};
    use slate_vm::pool::{RuntimeRegistry, VmPool};
    use slate_vm::{LuaScriptRuntime, ResolvedHook, RuntimeKind};
    use std::sync::Arc;

    const LUA_TAG: u8 = 0x01;

    fn lua_pool() -> VmPool {
        let mut reg = RuntimeRegistry::new();
        reg.register(RuntimeKind::Lua, Arc::new(LuaScriptRuntime::new()));
        VmPool::new(reg)
    }

    fn hook(name: &str, src: &str) -> ResolvedHook {
        ResolvedHook {
            name: name.into(),
            runtime: LUA_TAG,
            source: src.as_bytes().to_vec(),
            source_hash: 0,
        }
    }

    fn doc() -> RawBson {
        RawBson::Document(rawdoc! { "_id": "1", "age": 50 })
    }

    // A native validate plan: bind the validator named `v` to the native
    // function `func`, which the executor resolves against the supplied bag.
    fn validate_plan(func: &str) -> Plan {
        Plan::Query(Node::Validate {
            validators: vec![("v".to_string(), func.to_string())],
            source: Box::new(Node::Values(vec![doc()])),
        })
    }

    #[test]
    fn validator_passes_document_through() {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let bag = ValidatorBag::new();
        bag.register("pass", |_: &ValidatorCtx<'_>| Ok(Verdict::Accept));
        let out = Executor::with_env(&txn, ExecEnv::new().with_validator(Some(&bag)))
            .execute_collect(validate_plan("pass"))
            .unwrap();
        assert_eq!(out, vec![doc()]);
    }

    #[test]
    fn validator_rejection_is_an_error() {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let bag = ValidatorBag::new();
        bag.register("reject", |_: &ValidatorCtx<'_>| Ok(Verdict::reject("nope")));
        let err = Executor::with_env(&txn, ExecEnv::new().with_validator(Some(&bag)))
            .execute_collect(validate_plan("reject"))
            .unwrap_err();
        assert!(matches!(err, ExecError::Validation(_)), "got {err:?}");
    }

    #[test]
    fn dangling_validator_aborts_the_write() {
        // The binding points at `absent`, which is not in the bag — a dangling
        // validator aborts the write (fail-safe), rather than silently passing.
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let bag = ValidatorBag::new();
        let err = Executor::with_env(&txn, ExecEnv::new().with_validator(Some(&bag)))
            .execute_collect(validate_plan("absent"))
            .unwrap_err();
        assert!(matches!(err, ExecError::Validation(_)), "got {err:?}");
    }

    #[test]
    fn validator_panic_aborts_the_write() {
        // A panicking validator is caught at the seam and aborts the write.
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let bag = ValidatorBag::new();
        bag.register("boom", |_: &ValidatorCtx<'_>| panic!("kaboom"));
        let err = Executor::with_env(&txn, ExecEnv::new().with_validator(Some(&bag)))
            .execute_collect(validate_plan("boom"))
            .unwrap_err();
        assert!(matches!(err, ExecError::Validation(_)), "got {err:?}");
    }

    #[test]
    fn no_bag_skips_validation() {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        // No validator bag attached → validation skipped → document passes,
        // even though the plan names a (here unregistered) validator.
        let out = Executor::new(&txn)
            .execute_collect(validate_plan("reject"))
            .unwrap();
        assert_eq!(out, vec![doc()]);
    }

    #[test]
    fn trigger_script_runs_and_errors_propagate() {
        let engine = seeded_people();
        let txn = engine.begin(false).unwrap();
        let pool = lua_pool();
        let plan = Plan::Query(Node::Trigger {
            cf: DEFAULT_CF.into(),
            action: "inserted".into(),
            hooks: vec![hook("t", "return function(ctx, event) error('boom') end")],
            source: Box::new(Node::Values(vec![doc()])),
        });
        let err = Executor::with_env(&txn, ExecEnv::new().with_pool(Some(&pool)))
            .execute_collect(plan)
            .unwrap_err();
        assert!(matches!(err, ExecError::Vm(_)), "got {err:?}");
    }
}

#[cfg(test)]
mod write_path {
    use crate::Executor;
    use crate::nodes::test_support::{people_ref, pred, seeded_people, sv};
    use bson::{RawBson, rawdoc};
    use slate_engine::{Engine, EngineTransaction, KvEngine};
    use slate_planner::{Node, Plan, RowBinding, UpsertMode};
    use slate_store::MemoryStore;

    /// Find the document with `_id` in a scan result.
    fn find_id<'a>(docs: &'a [RawBson], id: &str) -> &'a bson::RawDocument {
        docs.iter()
            .find_map(|v| match v {
                RawBson::Document(d) if d.get_str("_id").map(|x| x == id).unwrap_or(false) => {
                    Some(d.as_ref())
                }
                _ => None,
            })
            .expect("document not found")
    }

    fn scan_all(engine: &KvEngine<MemoryStore>) -> Vec<RawBson> {
        let txn = engine.begin(true).unwrap();
        Executor::new(&txn)
            .execute_collect(Plan::Query(Node::Scan {
                collection: people_ref(),
            }))
            .unwrap()
    }

    /// Run a write plan in a write txn, commit, return the affected documents.
    fn write(engine: &KvEngine<MemoryStore>, plan: Plan) -> Vec<RawBson> {
        let txn = engine.begin(false).unwrap();
        let out = Executor::new(&txn).execute_collect(plan).unwrap();
        txn.commit().unwrap();
        out
    }

    /// `Scan → Filter(pred) → Project(c)` — bare docs matching `pred`, in
    /// single-binding (`Alias`) mode (no `Bind`, matching what the planner now
    /// emits for a join-free query).
    fn matched_docs(pred_src: &str) -> Node {
        Node::Project {
            expr: sv("c"),
            binding: RowBinding::Alias("c".into()),
            source: Box::new(Node::Filter {
                predicate: pred(pred_src),
                binding: RowBinding::Alias("c".into()),
                source: Box::new(Node::Scan {
                    collection: people_ref(),
                }),
            }),
        }
    }

    #[test]
    fn insert_adds_document() {
        let engine = seeded_people();
        let affected = write(
            &engine,
            Plan::Insert {
                collection: people_ref(),
                source: Node::Values(vec![RawBson::Document(
                    rawdoc! { "_id": "4", "name": "kay", "age": 50 },
                )]),
            },
        );
        assert_eq!(affected.len(), 1);
        assert_eq!(scan_all(&engine).len(), 4);
    }

    #[test]
    fn insert_generates_pk_when_missing() {
        let engine = seeded_people();
        let affected = write(
            &engine,
            Plan::Insert {
                collection: people_ref(),
                source: Node::Values(vec![RawBson::Document(rawdoc! { "name": "nopk" })]),
            },
        );
        let RawBson::Document(doc) = &affected[0] else {
            panic!("expected a document");
        };
        assert!(doc.get("_id").unwrap().is_some());
        assert_eq!(scan_all(&engine).len(), 4);
    }

    #[test]
    fn delete_by_id() {
        let engine = seeded_people();
        write(
            &engine,
            Plan::Delete {
                collection: people_ref(),
                source: Node::Values(vec![RawBson::Document(rawdoc! { "_id": "2" })]),
            },
        );
        assert_eq!(scan_all(&engine).len(), 2);
    }

    #[test]
    fn delete_by_predicate() {
        // DELETE WHERE c.age > 40 → removes alan(41) and grace(44), leaves ada.
        let engine = seeded_people();
        write(
            &engine,
            Plan::Delete {
                collection: people_ref(),
                source: matched_docs("c.age > 40"),
            },
        );
        let remaining = scan_all(&engine);
        assert_eq!(remaining.len(), 1);
        let RawBson::Document(doc) = &remaining[0] else {
            panic!("expected a document");
        };
        assert_eq!(doc.get_str("name").unwrap(), "ada");
    }

    #[test]
    fn update_mutates_matching_documents() {
        // UPDATE SET age = c.age + 1 WHERE c.age > 40 → alan 41→42, grace 44→45.
        use slate_ast::{Assignment, BinOp, Expression};
        let engine = seeded_people();
        write(
            &engine,
            Plan::Update {
                collection: people_ref(),
                assignments: vec![Assignment {
                    path: vec!["age".into()],
                    value: Expression::Binary {
                        op: BinOp::Add,
                        lhs: Box::new(Expression::Member {
                            base: Box::new(Expression::Identifier("c".into())),
                            field: "age".into(),
                        }),
                        rhs: Box::new(Expression::Value(bson::Bson::Int32(1))),
                    },
                }],
                source: matched_docs("c.age > 40"),
            },
        );
        // `Int + Int` normalizes to Int64, so read either width.
        let mut ages: Vec<i64> = scan_all(&engine)
            .iter()
            .filter_map(|v| match v {
                RawBson::Document(d) => match d.get("age") {
                    Ok(Some(bson::RawBsonRef::Int32(i))) => Some(i as i64),
                    Ok(Some(bson::RawBsonRef::Int64(i))) => Some(i),
                    _ => None,
                },
                _ => None,
            })
            .collect();
        ages.sort_unstable();
        assert_eq!(ages, vec![36, 42, 45]); // ada unchanged, alan/grace +1
    }

    fn upsert(mode: UpsertMode, doc: bson::raw::RawDocumentBuf) -> Plan {
        Plan::Upsert {
            collection: people_ref(),
            mode,
            hooks: vec![],
            source: Node::Values(vec![RawBson::Document(doc)]),
        }
    }

    #[test]
    fn upsert_inserts_when_absent() {
        let engine = seeded_people();
        write(
            &engine,
            upsert(
                UpsertMode::Replace,
                rawdoc! { "_id": "4", "name": "kay", "age": 50 },
            ),
        );
        assert_eq!(scan_all(&engine).len(), 4);
    }

    #[test]
    fn upsert_replace_overwrites_existing() {
        let engine = seeded_people();
        write(
            &engine,
            upsert(UpsertMode::Replace, rawdoc! { "_id": "2", "name": "bob" }),
        );
        let all = scan_all(&engine);
        assert_eq!(all.len(), 3); // updated, not inserted
        let doc = find_id(&all, "2");
        assert_eq!(doc.get_str("name").unwrap(), "bob");
        assert!(doc.get("age").unwrap().is_none()); // Replace drops old fields
    }

    #[test]
    fn upsert_merge_combines_with_existing() {
        let engine = seeded_people();
        write(
            &engine,
            upsert(UpsertMode::Merge, rawdoc! { "_id": "2", "city": "nyc" }),
        );
        let all = scan_all(&engine);
        assert_eq!(all.len(), 3);
        let doc = find_id(&all, "2");
        assert_eq!(doc.get_str("name").unwrap(), "alan"); // preserved
        assert_eq!(doc.get_i32("age").unwrap(), 41); // preserved
        assert_eq!(doc.get_str("city").unwrap(), "nyc"); // merged in
    }

    #[test]
    fn replace_overwrites_preserving_pk() {
        let engine = seeded_people();
        write(
            &engine,
            Plan::Replace {
                collection: people_ref(),
                replacement: rawdoc! { "name": "bob", "age": 99 },
                source: Node::Values(vec![RawBson::Document(rawdoc! { "_id": "2" })]),
            },
        );
        let all = scan_all(&engine);
        assert_eq!(all.len(), 3); // replaced, not inserted
        let bob = all.iter().find(|v| match v {
            RawBson::Document(d) => d.get_str("name").map(|n| n == "bob").unwrap_or(false),
            _ => false,
        });
        let Some(RawBson::Document(doc)) = bob else {
            panic!("expected replaced doc");
        };
        assert_eq!(doc.get_str("_id").unwrap(), "2"); // pk preserved
        assert_eq!(doc.get_i32("age").unwrap(), 99);
    }
}

#[cfg(test)]
mod exec_env {
    //! The [`ExecEnv`] bundle — migration step 1. Each capability attached to
    //! the bundle reaches expression evaluation through `Executor::with_env`,
    //! the `None` default stays zero-cost, and the `Executor::with_*` sugar
    //! delegates to the bundle faithfully.

    use crate::nodes::test_support::{people_ref, pred, seeded_people};
    use crate::watch::{CompiledWatch, compile_filter};
    use crate::{ChangeEvent, ExecEnv, Executor, WatchSink};
    use bson::{RawBson, RawDocumentBuf, rawdoc};
    use slate_engine::{Catalog, DEFAULT_CF, Engine};
    use slate_planner::{CollectionMeta, Node, Plan};
    use std::rc::Rc;

    /// The `people` fixture's collection metadata (only `age` is indexed), which
    /// the lowerer needs to plan the query.
    fn people_meta<T: Catalog>(txn: &T) -> CollectionMeta {
        let handle = txn.collection(DEFAULT_CF, "people").unwrap();
        CollectionMeta {
            indexes: handle.indexes().to_vec(),
            compound_indexes: Vec::new(),
            vector_indexes: Vec::new(),
            pk_path: handle.pk_path().to_string(),
        }
    }

    /// Lower a SQL string against the `people` fixture.
    fn people_plan<T: Catalog>(txn: &T, sql: &str) -> Plan {
        slate_planner::lower(
            slate_sql::parse(sql).unwrap(),
            people_ref(),
            &people_meta(txn),
        )
    }

    #[test]
    fn params_attached_to_env_reach_at_param_eval() {
        // `@who` resolves from the params document carried on the bundle.
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let plan = people_plan(&txn, "SELECT VALUE c.name FROM c WHERE c.name = @who");
        let params: Option<Rc<RawDocumentBuf>> = Some(Rc::new(rawdoc! { "who": "alan" }));
        let out = Executor::with_env(&txn, ExecEnv::new().with_params(params))
            .execute_collect(plan)
            .unwrap();
        assert_eq!(out, vec![RawBson::String("alan".into())]);
    }

    #[test]
    fn rand_attached_to_env_feeds_the_rand_function() {
        // A fixed source on the bundle reaches `RAND()`: one draw per person.
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let plan = people_plan(&txn, "SELECT VALUE RAND() FROM c");
        let source: Rc<dyn Fn() -> f64> = Rc::new(|| 0.42);
        let out = Executor::with_env(&txn, ExecEnv::new().with_rand(Some(source)))
            .execute_collect(plan)
            .unwrap();
        assert_eq!(
            out,
            vec![
                RawBson::Double(0.42),
                RawBson::Double(0.42),
                RawBson::Double(0.42),
            ]
        );
    }

    #[test]
    fn absent_rand_leaves_rand_undefined() {
        // The `None` default stays zero-cost: `RAND()` is undefined, so every
        // projected value is dropped at the output boundary.
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let plan = people_plan(&txn, "SELECT VALUE RAND() FROM c");
        let out = Executor::with_env(&txn, ExecEnv::new())
            .execute_collect(plan)
            .unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn watch_attached_to_env_captures_a_matching_write() {
        // The watch sink threads through the bundle into the insert node, which
        // captures the matching new document as an `Insert`.
        let engine = seeded_people();
        let txn = engine.begin(false).unwrap();
        let filter = compile_filter(&pred("c.age > 40"), "c");
        let sink = Rc::new(WatchSink::new(vec![(
            DEFAULT_CF.into(),
            "people".into(),
            vec![CompiledWatch {
                handle_id: 1,
                alias: "c".into(),
                filter,
            }],
        )]));
        let insert = Plan::Insert {
            collection: people_ref(),
            source: Node::Values(vec![RawBson::Document(
                rawdoc! { "_id": "9", "name": "kay", "age": 50 },
            )]),
        };
        Executor::with_env(&txn, ExecEnv::new().with_watch(Some(Rc::clone(&sink))))
            .execute_collect(insert)
            .unwrap();

        let captured = sink.take();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].handle_id, 1);
        assert!(matches!(captured[0].event, ChangeEvent::Insert { .. }));
    }
}
