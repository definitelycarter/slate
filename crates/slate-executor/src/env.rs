//! The per-query execution capabilities — the evaluator's inputs, bundled.
//!
//! Every per-query capability the [`Executor`](crate::Executor) feeds to
//! expression evaluation — an optional scripting pool for validators/triggers,
//! the SQL `@`-parameters (which also carry the injected `$now` clock value),
//! the `RAND()` source, and the watch-capture sink — was threaded as a
//! *separate* `Executor` field and constructor argument. Adding the next scoped
//! capability (a UDF resolver, then triggers/validators) meant re-threading
//! every layer again.
//!
//! [`ExecEnv`] groups those capabilities into one value, so a new capability is a
//! single field rather than a re-thread. This is migration **step 1** of the
//! [execution-context RFC](../../../book/src/rfcs/execution-context.md): a
//! mechanical regrouping with no behaviour change. The handles are the same
//! `&`/`Rc` the `Executor` held before, so it is allocation-neutral. The
//! `Executor`'s `with_*` builders remain as thin sugar over this type so call
//! sites migrate incrementally.
//!
//! The **transaction is not part of this bundle.** It is the data handle the
//! source/mutation nodes run *against*, not an *evaluator* input — so the
//! `Executor` holds it as a peer of the env (together they are the query's
//! execution context), exactly as the per-row [`RowEnv`](slate_eval::raweval)
//! also carries no transaction. Keeping the txn out is also why this type needs
//! no engine type parameter — only the `'a` of the borrowed pool.
//!
//! This is the per-*query* env, distinct from the per-*row* helpers in
//! [`nodes::env`](crate::nodes::env): those build the leaf `RowEnv` for a single
//! row out of the capabilities this bundle carries. The eval nodes hold a
//! (cheaply cloned) `ExecEnv` and derive a `RowEnv` per row — see
//! [`row_env`](crate::nodes::env::row_env).

use std::rc::Rc;

use bson::RawDocumentBuf;
use slate_vm::pool::VmPool;

use crate::nodes::env::Rand;
use crate::watch::WatchSink;

/// The capabilities an [`Executor`](crate::Executor) feeds to expression
/// evaluation for one query.
///
/// An absent capability stays `None` and costs nothing — the empty/`None` state
/// is the zero-cost default, attached only when the query needs it.
///
/// `'a` ties the borrowed pool to the transaction it lives alongside; the
/// remaining capabilities are `Rc`/owned and outlive nothing narrower.
///
/// `Clone` is a handful of `Rc` refcount bumps plus a `Copy` of the borrowed
/// pool — no allocation, no deep copy. The eval nodes each take an owned clone
/// (so a node's result stream can outlive the executor, borrowing only the txn),
/// exactly as they previously cloned the `params`/`rand` handles individually.
#[derive(Default, Clone)]
pub struct ExecEnv<'a> {
    /// Scripting pool backing validators/triggers. `None` skips them.
    pub(crate) pool: Option<&'a VmPool>,
    /// Query `@`-parameters, shared (by `Rc`) into each evaluating node so they
    /// outlive this executor. Also the carrier for the injected `$now` clock
    /// value (the db layer folds it into this document). `None` means the query
    /// had no parameters.
    pub(crate) params: Option<Rc<RawDocumentBuf>>,
    /// Random source backing `RAND()`, shared (by `Rc`) into each evaluating
    /// node like `params`. `None` makes `RAND()` undefined. The closure owns its
    /// PRNG state, so the env only ever calls it.
    pub(crate) rand: Rand,
    /// Change-detection sink for watch queries, threaded (by `Rc`) into the
    /// mutation nodes so they buffer matching before/after documents as the
    /// write stream drains. `None` when no watches are registered.
    pub(crate) watch: Option<Rc<WatchSink>>,
}

impl<'a> ExecEnv<'a> {
    /// An empty bundle: no pool, params, rand, or watch. Each capability is
    /// absent (and zero-cost) until attached with a `with_*` builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a scripting pool for validators/triggers. `None` (the default)
    /// skips them.
    pub fn with_pool(mut self, pool: Option<&'a VmPool>) -> Self {
        self.pool = pool;
        self
    }

    /// Attach the query's `@`-parameter document (also the carrier for the
    /// injected `$now` clock), shared by `Rc` into each evaluating node.
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
}
