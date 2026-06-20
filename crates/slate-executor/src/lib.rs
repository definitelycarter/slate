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

mod error;
mod nodes;

use std::rc::Rc;

use bson::{RawBson, RawDocumentBuf};
use slate_engine::{Catalog, EngineTransaction};
use slate_planner::{Node, Plan};
use slate_vm::pool::VmPool;

pub use error::ExecError;

/// A streaming sequence of optionally-undefined raw values.
///
/// The `'a` lifetime ties a stream to the transaction it reads from.
pub type ValueIter<'a> = Box<dyn Iterator<Item = Result<Option<RawBson>, ExecError>> + 'a>;

/// Executes plans against a transaction, with an optional scripting pool for
/// validators/triggers and an optional `@`-parameter document for SQL queries.
pub struct Executor<'a, T> {
    txn: &'a T,
    pool: Option<&'a VmPool>,
    /// Query `@`-parameters, shared (by `Rc`) into each evaluating node so they
    /// outlive this executor — the result stream borrows only the transaction.
    params: Option<Rc<RawDocumentBuf>>,
}

impl<'a, T: EngineTransaction + Catalog> Executor<'a, T> {
    /// Construct an executor with no scripting pool (validators/triggers are
    /// skipped if encountered).
    pub fn new(txn: &'a T) -> Self {
        Self {
            txn,
            pool: None,
            params: None,
        }
    }

    /// Construct an executor with a scripting pool for validators/triggers.
    pub fn with_pool(txn: &'a T, pool: Option<&'a VmPool>) -> Self {
        Self {
            txn,
            pool,
            params: None,
        }
    }

    /// Construct an executor with a scripting pool and a document of query
    /// `@`-parameters visible to expression evaluation.
    pub fn with_pool_and_params(
        txn: &'a T,
        pool: Option<&'a VmPool>,
        params: Option<Rc<RawDocumentBuf>>,
    ) -> Self {
        Self { txn, pool, params }
    }

    /// Execute a plan into a streaming iterator. For write plans, the mutations
    /// happen as the stream is consumed (drain it to apply them).
    pub fn execute(&self, plan: Plan) -> Result<ValueIter<'a>, ExecError> {
        match plan {
            Plan::Query(node) => self.execute_node(node),

            Plan::Insert { collection, source } => {
                let handle = self
                    .txn
                    .collection(&collection.cf, &collection.collection)?;
                let source = self.execute_node(source)?;
                nodes::insert::execute(self.txn, handle, source)
            }

            Plan::Delete { collection, source } => {
                let handle = self
                    .txn
                    .collection(&collection.cf, &collection.collection)?;
                let source = self.execute_node(source)?;
                nodes::delete::execute(self.txn, handle, source)
            }

            Plan::Replace {
                collection,
                replacement,
                source,
            } => {
                let handle = self
                    .txn
                    .collection(&collection.cf, &collection.collection)?;
                let source = self.execute_node(source)?;
                nodes::replace::execute(self.txn, handle, replacement, source)
            }

            Plan::Update {
                collection,
                mutation,
                source,
            } => {
                let handle = self
                    .txn
                    .collection(&collection.cf, &collection.collection)?;
                let source = self.execute_node(source)?;
                nodes::mutate::execute(self.txn, handle, mutation, source)
            }

            Plan::Trigger {
                cf,
                action,
                hooks,
                plan,
            } => {
                let inner = self.execute(*plan)?;
                nodes::trigger::execute(self.txn, self.pool, cf, action, hooks, inner)
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
                let source = self.execute_node(source)?;
                nodes::upsert::execute(self.txn, self.pool, hooks, handle, mode, source)
            }
        }
    }

    /// Execute a plan and collect the **defined** results, dropping undefined
    /// rows. The typical output boundary for a query.
    pub fn execute_collect(&self, plan: Plan) -> Result<Vec<RawBson>, ExecError> {
        collect(self.execute(plan)?)
    }

    /// Dispatch a node to its per-node executor, recursing into children first.
    fn execute_node(&self, node: Node) -> Result<ValueIter<'a>, ExecError> {
        Ok(match node {
            Node::Values(values) => nodes::values::execute(values),

            Node::Scan { collection } => nodes::scan::execute(self.txn, &collection)?,

            Node::IndexScan {
                collection,
                field,
                range,
                direction,
                limit,
            } => {
                nodes::index_scan::execute(self.txn, &collection, field, &range, direction, limit)?
            }

            Node::KeyLookup { collection, source } => {
                let source = self.execute_node(*source)?;
                nodes::key_lookup::execute(self.txn, &collection, source)?
            }

            Node::IndexMerge {
                collection,
                logical,
                lhs,
                rhs,
            } => {
                let left = self.execute_node(*lhs)?;
                let right = self.execute_node(*rhs)?;
                nodes::index_merge::execute(self.txn, &collection, logical, left, right)?
            }

            Node::Bind { alias, source } => {
                let source = self.execute_node(*source)?;
                nodes::bind::execute(alias, source)
            }

            Node::Unwind {
                alias,
                array,
                source,
            } => {
                let source = self.execute_node(*source)?;
                nodes::unwind::execute(alias, array, source, self.params.clone())
            }

            Node::Project {
                expr,
                binding,
                source,
            } => {
                let source = self.execute_node(*source)?;
                nodes::project::execute(expr, binding, source, self.params.clone())
            }

            Node::Filter {
                predicate,
                binding,
                source,
            } => {
                let source = self.execute_node(*source)?;
                nodes::filter::execute(predicate, binding, source, self.params.clone())
            }

            Node::Sort {
                keys,
                binding,
                source,
            } => {
                let source = self.execute_node(*source)?;
                nodes::sort::execute(keys, binding, source, self.params.clone())?
            }

            Node::Limit { skip, take, source } => {
                let source = self.execute_node(*source)?;
                nodes::limit::execute(skip, take, source)
            }

            Node::Distinct { source } => {
                let source = self.execute_node(*source)?;
                nodes::distinct::execute(source)
            }

            Node::Trigger {
                cf,
                action,
                hooks,
                source,
            } => {
                let source = self.execute_node(*source)?;
                nodes::trigger::execute(self.txn, self.pool, cf, action, hooks, source)?
            }

            Node::Validate { validators, source } => {
                let source = self.execute_node(*source)?;
                nodes::validate::execute(self.pool, validators, source)?
            }
        })
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
        let meta = CollectionMeta {
            indexes: handle.indexes().to_vec(),
            pk_path: handle.pk_path().to_string(),
        };
        let plan = slate_planner::lower(slate_sql::parse(sql).unwrap(), people_ref(), &meta);
        Executor::new(&txn).execute_collect(plan).unwrap()
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
}

#[cfg(test)]
mod hooks {
    use crate::nodes::test_support::seeded_people;
    use crate::{ExecError, Executor};
    use bson::{RawBson, rawdoc};
    use slate_engine::{DEFAULT_CF, Engine};
    use slate_planner::{Node, Plan};
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

    fn validate_plan(src: &str) -> Plan {
        Plan::Query(Node::Validate {
            validators: vec![hook("v", src)],
            source: Box::new(Node::Values(vec![doc()])),
        })
    }

    #[test]
    fn validator_passes_document_through() {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let pool = lua_pool();
        let out = Executor::with_pool(&txn, Some(&pool))
            .execute_collect(validate_plan(
                "return function(ctx, event) return { ok = true } end",
            ))
            .unwrap();
        assert_eq!(out, vec![doc()]);
    }

    #[test]
    fn validator_rejection_is_an_error() {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let pool = lua_pool();
        let err = Executor::with_pool(&txn, Some(&pool))
            .execute_collect(validate_plan(
                "return function(ctx, event) return { ok = false, reason = 'nope' } end",
            ))
            .unwrap_err();
        assert!(matches!(err, ExecError::Validation(_)), "got {err:?}");
    }

    #[test]
    fn no_pool_skips_validation() {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        // A rejecting validator, but no pool → skipped → document passes.
        let out = Executor::new(&txn)
            .execute_collect(validate_plan(
                "return function(ctx, event) return { ok = false } end",
            ))
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
        let err = Executor::with_pool(&txn, Some(&pool))
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
        // UPDATE SET age += 1 WHERE c.age > 40 → alan 41→42, grace 44→45.
        use slate_mutation::{FieldMutation, Mutation, MutationOp};
        let engine = seeded_people();
        write(
            &engine,
            Plan::Update {
                collection: people_ref(),
                mutation: Mutation {
                    ops: vec![FieldMutation {
                        field: "age".into(),
                        op: MutationOp::Inc(bson::Bson::Int32(1)),
                    }],
                },
                source: matched_docs("c.age > 40"),
            },
        );
        let mut ages: Vec<i32> = scan_all(&engine)
            .iter()
            .filter_map(|v| match v {
                RawBson::Document(d) => d.get_i32("age").ok(),
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
