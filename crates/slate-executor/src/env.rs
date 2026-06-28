//! The per-query execution capabilities — the evaluator's inputs, bundled.
//!
//! Every per-query capability the [`Executor`](crate::Executor) feeds to
//! expression evaluation — the SQL `@`-parameters, the `RAND()` source, the
//! injected clock reading, the watch-capture sink, and the live UDF / validator
//! / trigger bags — was threaded as a *separate* `Executor` field and
//! constructor argument. Adding the next scoped capability meant re-threading
//! every layer again.
//!
//! [`ExecEnv`] groups those capabilities into one value, so a new capability is a
//! single field rather than a re-thread. The handles are the same `&`/`Rc` the
//! `Executor` held before, so it is allocation-neutral. The db layer builds the
//! populated bundle in one place — `Transaction::exec_env` — and hands it to
//! [`Executor::with_env`](crate::Executor::with_env).
//!
//! The **transaction is not part of this bundle.** It is the data handle the
//! source/mutation nodes run *against*, not an *evaluator* input — so the
//! `Executor` holds it as a peer of the env (together they are the query's
//! execution context), exactly as the per-row [`RowEnv`](slate_eval::raweval)
//! also carries no transaction. Keeping the txn out is also why this type needs
//! no engine type parameter — only the `'a` of the borrowed bags.
//!
//! This is the per-*query* env, distinct from the per-*row* helpers in
//! [`nodes::env`](crate::nodes::env): those build the leaf `RowEnv` for a single
//! row out of the capabilities this bundle carries. The eval nodes hold a
//! (cheaply cloned) `ExecEnv` and derive a `RowEnv` per row — see
//! [`row_env`](crate::nodes::env::row_env).

use std::collections::HashMap;
use std::rc::Rc;

use bson::RawDocumentBuf;
use slate_eval::raweval::UdfCtx;
use slate_trigger::TriggerBag;
use slate_udf::UdfBag;
use slate_validator::ValidatorBag;

use crate::nodes::env::Rand;
use crate::watch::WatchSink;

/// The capabilities an [`Executor`](crate::Executor) feeds to expression
/// evaluation for one query.
///
/// An absent capability stays `None` and costs nothing — the empty/`None` state
/// is the zero-cost default, attached only when the query needs it.
///
/// `'a` ties the borrowed bags (UDF/validator/trigger) to the transaction they
/// live alongside; the remaining capabilities are `Rc`/owned and outlive nothing
/// narrower.
///
/// `Clone` is a handful of `Rc` refcount bumps plus a `Copy` of the borrowed
/// bag references — no allocation, no deep copy. The eval nodes each take an owned clone
/// (so a node's result stream can outlive the executor, borrowing only the txn),
/// exactly as they previously cloned the `params`/`rand` handles individually.
#[derive(Default, Clone)]
pub struct ExecEnv<'a> {
    /// Query `@`-parameters, shared (by `Rc`) into each evaluating node so they
    /// outlive this executor. `None` means the query had no parameters.
    pub(crate) params: Option<Rc<RawDocumentBuf>>,
    /// Random source backing `RAND()`, shared (by `Rc`) into each evaluating
    /// node like `params`. `None` makes `RAND()` undefined. The closure owns its
    /// PRNG state, so the env only ever calls it.
    pub(crate) rand: Rand,
    /// Change-detection sink for watch queries, threaded (by `Rc`) into the
    /// mutation nodes so they buffer matching before/after documents as the
    /// write stream drains. `None` when no watches are registered.
    pub(crate) watch: Option<Rc<WatchSink>>,
    /// Injected clock reading (epoch milliseconds), captured once at txn begin,
    /// backing the SQL `GETCURRENT*` functions. A *static* per-query value
    /// (every `GETCURRENT*` in a query sees the same instant), so unlike `rand`
    /// it is a plain value, not a callable. `None` makes the clock functions
    /// undefined.
    pub(crate) clock: Option<i64>,
    /// The live, database-scoped UDF bag, borrowed from the transaction for the
    /// life of the query (treated like `pool`). Consulted once per query, at
    /// `compile`, to bake each `udf.*` reference into a resolved handle; per-row
    /// eval then just calls it. `None` (the default) makes any `udf.*` reference
    /// an unregistered error.
    pub(crate) udf: Option<&'a UdfBag>,
    /// The query collection's UDF bindings (`query_name -> native_name`),
    /// resolved from the cached catalog snapshot and shared (`Rc`) into each
    /// evaluating node. `compile` consults it *before* the bag. `None` when the
    /// collection has no bindings, so any `udf.*` reference there is unbound.
    pub(crate) udf_bindings: Option<Rc<HashMap<String, String>>>,
    /// The live, database-scoped validator bag, borrowed from the transaction for
    /// the life of the query (treated like `udf`). The `Validate` node resolves
    /// each bound validator's native name against it at fire time. `None` (the
    /// default) skips validation — the write path always attaches it.
    pub(crate) validator: Option<&'a ValidatorBag>,
    /// The live, database-scoped trigger bag, borrowed from the transaction for
    /// the life of the query (treated like `validator`). The `Trigger` and
    /// `Upsert` nodes resolve each bound trigger's native name against it once per
    /// query, at fire time. `None` (the default) skips triggers — the write path
    /// always attaches it.
    pub(crate) trigger: Option<&'a TriggerBag>,
}

impl<'a> ExecEnv<'a> {
    /// An empty bundle: no pool, params, rand, or watch. Each capability is
    /// absent (and zero-cost) until attached with a `with_*` builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach the query's `@`-parameter document, shared by `Rc` into each
    /// evaluating node.
    pub fn with_params(mut self, params: Option<Rc<RawDocumentBuf>>) -> Self {
        self.params = params;
        self
    }

    /// Attach the random source backing `RAND()`. `None` (the default) makes
    /// `RAND()` undefined. The closure owns its PRNG state; the env only calls
    /// it.
    pub fn with_rand(mut self, rand: Option<Rc<dyn Fn() -> f64>>) -> Self {
        self.rand = rand;
        self
    }

    /// Attach a change-detection sink so the mutation nodes buffer before/after
    /// documents for registered watch queries. `None` (the default) disables
    /// capture at zero cost.
    pub fn with_watch(mut self, watch: Option<Rc<WatchSink>>) -> Self {
        self.watch = watch;
        self
    }

    /// Attach the injected clock reading (epoch ms) backing the SQL `GETCURRENT*`
    /// functions. `None` (the default) makes them undefined.
    pub fn with_clock(mut self, clock: Option<i64>) -> Self {
        self.clock = clock;
        self
    }

    /// Attach the live UDF bag, consulted at `compile` to resolve `udf.*` calls.
    /// `None` (the default) leaves every `udf.*` reference unresolved.
    pub fn with_udf(mut self, udf: Option<&'a UdfBag>) -> Self {
        self.udf = udf;
        self
    }

    /// Attach the collection's UDF bindings (`query_name -> native_name`), shared
    /// by `Rc`. `None` (the default) leaves every `udf.*` reference unbound.
    pub fn with_udf_bindings(mut self, bindings: Option<Rc<HashMap<String, String>>>) -> Self {
        self.udf_bindings = bindings;
        self
    }

    /// Attach the live validator bag, consulted by the `Validate` node to resolve
    /// bound validators. `None` (the default) skips validation.
    pub fn with_validator(mut self, validator: Option<&'a ValidatorBag>) -> Self {
        self.validator = validator;
        self
    }

    /// Attach the live trigger bag, consulted by the `Trigger`/`Upsert` nodes to
    /// resolve bound triggers. `None` (the default) skips triggers.
    pub fn with_trigger(mut self, trigger: Option<&'a TriggerBag>) -> Self {
        self.trigger = trigger;
        self
    }

    /// Bundle the binding map and the bag into the [`UdfCtx`] `compile` consumes.
    pub(crate) fn udf_ctx(&self) -> UdfCtx<'_> {
        UdfCtx {
            bindings: self.udf_bindings.as_deref(),
            bag: self.udf,
        }
    }
}
