//! Watch queries — in-process change detection (the db-layer half).
//!
//! The executor (`slate_executor::watch`) owns *detection*: it evaluates each
//! watch's compiled filter against the before/after states inside the mutation
//! nodes and buffers the recast [`ChangeEvent`]s. This module owns the
//! *registry, the callbacks, and the commit-time emit*:
//!
//! - [`WatchRegistry`] — an [`ArcSwap`] of registered watches, read lock-free on
//!   the write path and **snapshotted at `begin()`** so an in-flight transaction
//!   sees a frozen set (mirroring [`HookRegistry`](crate::hooks::HookRegistry)).
//! - [`WatchHandle`] — a droppable registration that unregisters on `Drop` (or
//!   an explicit [`unwatch`](WatchHandle::unwatch)).
//! - [`emit`] — at commit, drains the executor's buffer, coalesces repeated
//!   changes to the same document into a single net change per watch, and fires
//!   each callback with its batch, isolated by `catch_unwind`.
//!
//! A watch is **ephemeral**: it holds a live closure and is registered at
//! runtime via [`Database::watch`](crate::Database::watch), not persisted in the
//! catalog like triggers/validators.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use arc_swap::ArcSwap;
use bson::{Bson, RawDocument};
use slate_ast::{Expression, FromSource, SelectClause};
use slate_executor::watch::{Compiled, CompiledWatch, WatchSink};
use slate_executor::{CapturedEvent, ChangeEvent};

use crate::error::DbError;

/// The synthetic alias a BSON (`find`-style) watch filter binds to. A
/// `translate_filter` predicate roots every field path at this alias
/// (`slate_query::ALIAS`), so a BSON watch compiles its filter against it —
/// exactly as `find` does.
const BSON_ALIAS: &str = slate_query::ALIAS;

/// A watch callback: fired once per commit with the ordered batch of net
/// changes for that watch. `Send + Sync` so the (future) async dispatcher can
/// reuse the same registry; the single-writer guarantee means it is never
/// invoked concurrently, so `Fn` (+ interior mutability) suffices.
pub type WatchCallback = Arc<dyn Fn(&[ChangeEvent]) + Send + Sync>;

/// One registered watch. Cheap to clone (every field is shared or small), so the
/// copy-on-write [`WatchSnapshot`] can rebuild on each register/unregister.
#[derive(Clone)]
struct Watch {
    handle_id: u64,
    cf: String,
    collection: String,
    /// The `FROM` alias the SQL filter was compiled against (the document binds
    /// to it during evaluation).
    alias: String,
    /// The compiled `WHERE` predicate, shared with the per-transaction sink.
    filter: Arc<Compiled>,
    callback: WatchCallback,
}

/// A frozen view of all registered watches at a point in time.
///
/// Captured at `begin()` so a transaction's capture *and* emit see a consistent
/// set even if a watch is registered or dropped mid-transaction.
pub struct WatchSnapshot {
    watches: Vec<Watch>,
}

impl WatchSnapshot {
    fn empty() -> Self {
        Self {
            watches: Vec::new(),
        }
    }

    /// Whether any watch is registered (lets `begin` skip building a sink).
    pub fn is_empty(&self) -> bool {
        self.watches.is_empty()
    }

    fn with_added(&self, watch: Watch) -> Self {
        let mut watches = self.watches.clone();
        watches.push(watch);
        Self { watches }
    }

    fn without(&self, handle_id: u64) -> Self {
        Self {
            watches: self
                .watches
                .iter()
                .filter(|w| w.handle_id != handle_id)
                .cloned()
                .collect(),
        }
    }

    /// Build the executor-facing sink for one transaction: the compiled filters
    /// grouped by `(cf, collection)` plus an empty capture buffer. `None` when
    /// no watches are registered (the common case — zero write-path overhead).
    ///
    /// Runs once per write transaction's `begin`, only when watches exist. The
    /// filter trees are shared (`Arc::clone`); the small alias/cf/collection
    /// strings are cloned to own them in the sink — off the per-row hot path.
    pub fn build_sink(&self) -> Option<Rc<WatchSink>> {
        if self.watches.is_empty() {
            return None;
        }
        let mut groups: Vec<(String, String, Vec<CompiledWatch>)> = Vec::new();
        for w in &self.watches {
            let compiled = CompiledWatch {
                handle_id: w.handle_id,
                alias: w.alias.clone(),
                filter: Arc::clone(&w.filter),
            };
            match groups
                .iter_mut()
                .find(|(cf, coll, _)| cf == &w.cf && coll == &w.collection)
            {
                Some((_, _, watches)) => watches.push(compiled),
                None => groups.push((w.cf.clone(), w.collection.clone(), vec![compiled])),
            }
        }
        Some(Rc::new(WatchSink::new(groups)))
    }

    /// The callback registered for `handle_id`, if it still exists in this
    /// snapshot.
    fn callback_for(&self, handle_id: u64) -> Option<&WatchCallback> {
        self.watches
            .iter()
            .find(|w| w.handle_id == handle_id)
            .map(|w| &w.callback)
    }
}

/// Lock-free, swappable registry of watch registrations.
///
/// Lives on [`Database`](crate::Database) behind an `Arc` so a [`WatchHandle`]
/// can outlive any borrow and unregister itself on `Drop`. Reads
/// ([`snapshot`](Self::snapshot)) are lock-free; registrations rebuild the
/// snapshot copy-on-write via [`ArcSwap::rcu`].
pub struct WatchRegistry {
    inner: ArcSwap<WatchSnapshot>,
    next_id: AtomicU64,
}

impl Default for WatchRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchRegistry {
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(WatchSnapshot::empty()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Snapshot the current set of watches (frozen for one transaction).
    pub fn snapshot(&self) -> Arc<WatchSnapshot> {
        self.inner.load_full()
    }

    /// Register a watch and return its stable handle id.
    fn register(
        &self,
        cf: String,
        collection: String,
        alias: String,
        filter: Arc<Compiled>,
        callback: WatchCallback,
    ) -> u64 {
        let handle_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let watch = Watch {
            handle_id,
            cf,
            collection,
            alias,
            filter,
            callback,
        };
        // `rcu` retries its closure on contention, so it borrows `watch` and
        // clones a fresh copy into each attempt.
        self.inner
            .rcu(|cur| Arc::new(cur.with_added(watch.clone())));
        handle_id
    }

    /// Remove a watch by handle id (idempotent — a missing id is a no-op).
    fn unregister(&self, handle_id: u64) {
        self.inner.rcu(|cur| Arc::new(cur.without(handle_id)));
    }
}

/// A live watch registration. Dropping it (or calling
/// [`unwatch`](Self::unwatch)) removes the watch from the registry; it holds no
/// collection lock, so a `drop_collection` proceeds independently and a watch on
/// a dropped collection simply goes cold.
#[must_use = "dropping the handle immediately unregisters the watch; keep it alive to keep watching"]
pub struct WatchHandle {
    registry: Arc<WatchRegistry>,
    handle_id: u64,
}

impl WatchHandle {
    /// The registry-assigned id (stable for this registration).
    pub fn id(&self) -> u64 {
        self.handle_id
    }

    /// Explicitly unregister the watch. Equivalent to dropping the handle.
    pub fn unwatch(self) {
        // `self` drops here, running `Drop`.
    }
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        self.registry.unregister(self.handle_id);
    }
}

// ── Cursor (pull) delivery ───────────────────────────────────────

/// The default number of per-commit batches a [`WatchStream`] buffers before a
/// slow consumer is declared *lagged*.
pub(crate) const DEFAULT_STREAM_CAPACITY: usize = 1024;

/// The shared, bounded buffer behind a [`WatchStream`]: the writer-side emit
/// closure pushes per-commit batches into `queue`; the consumer drains them.
///
/// Overflow is **non-blocking, lag-and-drop** (RFC decision 3): if the queue is
/// full when a batch arrives, the batch is dropped and `lagged` is set rather
/// than blocking the writer — a stalled consumer must never freeze writes in an
/// embedded DB. The consumer observes the flag via [`WatchStream::lagged`] and
/// re-snapshots from current state (events are a convenience; state is the
/// truth).
struct Subscription {
    inner: Mutex<SubState>,
    /// Signalled when a batch is pushed, the stream is closed, or lag is set, so
    /// a blocked [`WatchStream::next_blocking`] wakes.
    ready: Condvar,
    capacity: usize,
}

struct SubState {
    queue: VecDeque<Vec<ChangeEvent>>,
    /// Set when a push overflowed the bounded queue; the consumer reads and
    /// clears it to learn it missed events.
    lagged: bool,
    /// Set when the owning [`WatchStream`] is dropped, so the writer-side push
    /// becomes a cheap no-op and any blocked consumer unblocks.
    closed: bool,
}

impl Subscription {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(SubState {
                queue: VecDeque::new(),
                lagged: false,
                closed: false,
            }),
            ready: Condvar::new(),
            capacity,
        })
    }

    /// Push one commit's batch (writer side, in the emit closure). Never blocks:
    /// on overflow it drops the batch and marks the subscription lagged. A
    /// poisoned lock (a panicked consumer) is treated as closed — drop silently.
    fn push(&self, batch: Vec<ChangeEvent>) {
        let Ok(mut state) = self.inner.lock() else {
            return;
        };
        if state.closed {
            return;
        }
        if state.queue.len() >= self.capacity {
            state.lagged = true;
        } else {
            state.queue.push_back(batch);
        }
        drop(state);
        self.ready.notify_one();
    }

    /// Mark closed and wake any blocked consumer (called when the stream drops).
    fn close(&self) {
        if let Ok(mut state) = self.inner.lock() {
            state.closed = true;
        }
        self.ready.notify_all();
    }
}

/// A long-lived **subscription cursor** over a watch's change events — the pull
/// (cursor) delivery shape, the FFI-friendly counterpart to the push (callback)
/// `watch`/`watch_query`. It outlives any single commit: the writer pushes each
/// committed batch into a bounded buffer, and the consumer drains it on its own
/// thread.
///
/// Created by [`Database::stream`](crate::Database::stream) (BSON filter) and
/// [`Database::stream_query`](crate::Database::stream_query) (SQL filter). It
/// owns the underlying [`WatchHandle`], so dropping the stream unregisters the
/// watch and releases the buffer.
///
/// **Lag (non-blocking overflow):** if the consumer falls behind and the bounded
/// buffer fills, further batches are *dropped* and the stream is marked
/// [`lagged`](Self::lagged) rather than blocking the writer. A consumer that
/// sees `lagged()` should re-snapshot current state (e.g. via `find`) and resume.
#[must_use = "dropping the stream unregisters the watch; keep it to keep receiving"]
pub struct WatchStream {
    /// Kept alive to hold the registration; its `Drop` unregisters the watch.
    _handle: WatchHandle,
    sub: Arc<Subscription>,
}

impl WatchStream {
    /// Pop the next buffered commit batch without blocking. `None` means the
    /// buffer is currently empty (more may arrive later) — check
    /// [`lagged`](Self::lagged) to learn whether batches were dropped meanwhile.
    pub fn try_next(&self) -> Option<Vec<ChangeEvent>> {
        let mut state = self.sub.inner.lock().ok()?;
        state.queue.pop_front()
    }

    /// Block until the next batch is available, then return it. Returns `None`
    /// only when the stream is closing and the buffer is drained (after the
    /// handle's registry is gone there will be no further writes to this
    /// subscription). Intended for a dedicated consumer thread.
    pub fn next_blocking(&self) -> Option<Vec<ChangeEvent>> {
        let mut state = match self.sub.inner.lock() {
            Ok(state) => state,
            Err(_) => return None,
        };
        loop {
            if let Some(batch) = state.queue.pop_front() {
                return Some(batch);
            }
            if state.closed {
                return None;
            }
            state = match self.sub.ready.wait(state) {
                Ok(state) => state,
                Err(_) => return None,
            };
        }
    }

    /// Whether the consumer has fallen behind and batches were dropped since the
    /// last check. Reading **clears** the flag, so a single `true` reports one or
    /// more drops; the consumer should re-snapshot current state on observing it.
    pub fn lagged(&self) -> bool {
        match self.sub.inner.lock() {
            Ok(mut state) => std::mem::replace(&mut state.lagged, false),
            Err(_) => false,
        }
    }
}

impl Drop for WatchStream {
    fn drop(&mut self) {
        // Wake any consumer blocked in `next_blocking` and stop the writer-side
        // push from buffering further. The `_handle` drops right after,
        // unregistering the watch.
        self.sub.close();
    }
}

impl WatchRegistry {
    /// Register a streaming (pull-delivery) watch from a SQL filter string.
    /// Backs the watch with a bounded [`Subscription`]; the registered callback
    /// pushes each commit batch into it (non-blocking, lag-drop on overflow).
    pub(crate) fn stream_sql(
        registry: &Arc<WatchRegistry>,
        cf: &str,
        collection: &str,
        sql_filter: &str,
        capacity: usize,
    ) -> Result<WatchStream, DbError> {
        let (alias, filter_expr) = parse_watch_filter(sql_filter)?;
        Ok(Self::stream_from(
            registry,
            cf,
            collection,
            alias,
            filter_expr,
            capacity,
        ))
    }

    /// Register a streaming (pull-delivery) watch from a BSON filter document.
    pub(crate) fn stream_bson(
        registry: &Arc<WatchRegistry>,
        cf: &str,
        collection: &str,
        filter: &RawDocument,
        capacity: usize,
    ) -> Result<WatchStream, DbError> {
        let (alias, filter_expr) = bson_watch_filter(filter)?;
        Ok(Self::stream_from(
            registry,
            cf,
            collection,
            alias,
            filter_expr,
            capacity,
        ))
    }

    /// Shared tail of the two `stream_*` front-ends: build the bounded
    /// subscription, register a watch whose callback pushes into it, and bundle
    /// the handle + buffer into a [`WatchStream`]. This is the *only* place the
    /// cursor delivery diverges from the callback delivery — both reuse
    /// [`register_compiled`](Self::register_compiled).
    fn stream_from(
        registry: &Arc<WatchRegistry>,
        cf: &str,
        collection: &str,
        alias: String,
        filter_expr: Expression,
        capacity: usize,
    ) -> WatchStream {
        let sub = Subscription::new(capacity.max(1));
        let push_target = Arc::clone(&sub);
        // The cursor's "callback" simply forwards the per-commit batch into the
        // bounded buffer. Identical detection/registry path as `watch`; only the
        // delivery closure differs.
        let callback: WatchCallback = Arc::new(move |events: &[ChangeEvent]| {
            push_target.push(events.to_vec());
        });
        let handle =
            Self::register_compiled(registry, cf, collection, alias, filter_expr, callback);
        WatchStream {
            _handle: handle,
            sub,
        }
    }
}

/// Parse and validate a watch's SQL `SELECT`, returning its `FROM` alias and the
/// `WHERE` predicate (a literal `true` when there is no `WHERE`).
///
/// Only the per-row-meaningful clauses apply to a watch: the `WHERE` filter and
/// an identity projection. Set operations (`ORDER BY` / `GROUP BY` / `HAVING` /
/// aggregates / `LIMIT` / `OFFSET` / `DISTINCT`) and joins are *set* semantics,
/// not per-document, so they are rejected at registration.
pub(crate) fn parse_watch_filter(sql: &str) -> Result<(String, Expression), DbError> {
    let query = slate_sql::parse(sql)?;

    let reject = |what: &str| {
        Err(DbError::InvalidQuery(format!(
            "watch filter cannot use {what}; only WHERE applies to a watch"
        )))
    };
    if query.distinct {
        return reject("SELECT DISTINCT");
    }
    if !query.group_by.is_empty() {
        return reject("GROUP BY");
    }
    if query.having.is_some() {
        return reject("HAVING");
    }
    if !query.order_by.is_empty() {
        return reject("ORDER BY");
    }
    if query.limit.is_some() {
        return reject("LIMIT");
    }
    if query.offset.is_some() {
        return reject("OFFSET");
    }
    if !query.parameter_names().is_empty() {
        return Err(DbError::InvalidQuery(
            "watch filter cannot reference @parameters".into(),
        ));
    }

    let from = query.from.ok_or_else(|| {
        DbError::InvalidQuery("watch filter requires a FROM clause (e.g. SELECT * FROM c)".into())
    })?;
    if !from.joins.is_empty() {
        return reject("JOIN");
    }
    let alias = match from.source {
        FromSource::ImplicitContainer { alias } => alias,
        _ => {
            return Err(DbError::InvalidQuery(
                "watch filter FROM must be a plain container alias".into(),
            ));
        }
    };

    // The event carries the whole document, so only the identity projection
    // (`SELECT *` or `SELECT VALUE <alias>`) is meaningful in v1; a shaping
    // projection is deferred.
    match &query.select {
        SelectClause::Star => {}
        SelectClause::Value(Expression::Identifier(id)) if *id == alias => {}
        _ => {
            return Err(DbError::InvalidQuery(
                "watch projections are not yet supported; use SELECT * or SELECT VALUE <alias>"
                    .into(),
            ));
        }
    }

    let filter = query
        .filter
        .unwrap_or(Expression::Value(Bson::Boolean(true)));
    Ok((alias, filter))
}

/// Translate a BSON (`find`-style) filter document into a watch's `(alias,
/// WHERE predicate)`, reusing `slate_query::translate_filter` so a BSON watch
/// can never drift from `find` semantics. An empty filter (`{}`) is match-all.
///
/// The BSON surface is **filter-only**: there is no projection, sort, or limit
/// to reject (unlike the SQL front-end), so this is a thin wrapper over the
/// same translator `find` uses.
pub(crate) fn bson_watch_filter(filter: &RawDocument) -> Result<(String, Expression), DbError> {
    let filter =
        slate_query::translate_filter(filter)?.unwrap_or(Expression::Value(Bson::Boolean(true)));
    Ok((BSON_ALIAS.to_string(), filter))
}

impl WatchRegistry {
    /// The registration core shared by all four front-ends: compile an
    /// already-extracted `(alias, WHERE predicate)` and register it against
    /// `(cf, collection)` with a delivery `callback`, returning a droppable
    /// handle. The BSON-vs-SQL front-ends differ only in how they produce
    /// `(alias, filter_expr)`; the callback-vs-cursor delivery differs only in
    /// what the `callback` closure does (invoke a user fn vs push to a
    /// subscription buffer). The `registry` arc is the same one stored on the
    /// database, so the handle can unregister itself.
    pub(crate) fn register_compiled(
        registry: &Arc<WatchRegistry>,
        cf: &str,
        collection: &str,
        alias: String,
        filter_expr: Expression,
        callback: WatchCallback,
    ) -> WatchHandle {
        let filter = slate_executor::watch::compile_filter(&filter_expr, &alias);
        let handle_id = registry.register(
            cf.to_string(),
            collection.to_string(),
            alias,
            filter,
            callback,
        );
        WatchHandle {
            registry: Arc::clone(registry),
            handle_id,
        }
    }

    /// Register a watch from a SQL `SELECT` filter string (the `watch_query` /
    /// `stream_query` front-end). Parses + validates the SELECT, then funnels
    /// through [`register_compiled`](Self::register_compiled).
    pub(crate) fn watch_sql(
        registry: &Arc<WatchRegistry>,
        cf: &str,
        collection: &str,
        sql_filter: &str,
        callback: WatchCallback,
    ) -> Result<WatchHandle, DbError> {
        let (alias, filter_expr) = parse_watch_filter(sql_filter)?;
        Ok(Self::register_compiled(
            registry,
            cf,
            collection,
            alias,
            filter_expr,
            callback,
        ))
    }

    /// Register a watch from a BSON (`find`-style) filter document (the `watch` /
    /// `stream` front-end). Translates the filter, then funnels through
    /// [`register_compiled`](Self::register_compiled).
    pub(crate) fn watch_bson(
        registry: &Arc<WatchRegistry>,
        cf: &str,
        collection: &str,
        filter: &RawDocument,
        callback: WatchCallback,
    ) -> Result<WatchHandle, DbError> {
        let (alias, filter_expr) = bson_watch_filter(filter)?;
        Ok(Self::register_compiled(
            registry,
            cf,
            collection,
            alias,
            filter_expr,
            callback,
        ))
    }
}

/// Fire watch callbacks for a committed transaction.
///
/// Drains the sink's buffer, coalesces repeated changes to the same document
/// into one net change per watch (last-write-wins, preserving first-seen order),
/// and invokes each watch's callback with its non-empty batch. Each invocation
/// is isolated with `catch_unwind` — the commit has already succeeded, so a
/// panicking callback must not poison anything.
pub(crate) fn emit(snapshot: &WatchSnapshot, sink: &WatchSink) {
    if sink.is_empty() {
        return;
    }
    for (handle_id, events) in coalesce(sink.take()) {
        if events.is_empty() {
            continue;
        }
        let Some(callback) = snapshot.callback_for(handle_id) else {
            // Registered but already-dropped within this txn — its callback Arc
            // may be gone from the snapshot; nothing to deliver to.
            continue;
        };
        let callback: &dyn Fn(&[ChangeEvent]) = callback.as_ref();
        if catch_unwind(AssertUnwindSafe(|| callback(&events))).is_err() {
            crate::trace::trace_event!("watch callback panicked; isolated and continuing");
        }
    }
}

/// The net membership state of one document across a commit: the state at the
/// start (`before`, set once from the first event) and the latest state
/// (`after`, updated by every event). `None` means "not in the watched set".
struct State {
    before: Option<bson::RawDocumentBuf>,
    after: Option<bson::RawDocumentBuf>,
}

impl State {
    fn first(event: ChangeEvent) -> Self {
        match event {
            ChangeEvent::Insert { doc } => State {
                before: None,
                after: Some(doc),
            },
            ChangeEvent::Update { old, new } => State {
                before: Some(old),
                after: Some(new),
            },
            ChangeEvent::Delete { doc } => State {
                before: Some(doc),
                after: None,
            },
        }
    }

    /// Fold a later event for the same document: only its resulting (`after`)
    /// state matters; the original `before` is preserved.
    fn apply(&mut self, event: ChangeEvent) {
        self.after = match event {
            ChangeEvent::Insert { doc } => Some(doc),
            ChangeEvent::Update { new, .. } => Some(new),
            ChangeEvent::Delete { .. } => None,
        };
    }

    /// The single net change, re-cast against the watch's set boundary, or
    /// `None` if the document ended where it began (in-and-out within the commit).
    fn into_net(self) -> Option<ChangeEvent> {
        match (self.before, self.after) {
            (Some(old), Some(new)) => Some(ChangeEvent::Update { old, new }),
            (None, Some(doc)) => Some(ChangeEvent::Insert { doc }),
            (Some(doc), None) => Some(ChangeEvent::Delete { doc }),
            (None, None) => None,
        }
    }
}

/// Per-handle accumulator preserving first-seen document order.
struct Acc {
    order: Vec<Vec<u8>>,
    states: HashMap<Vec<u8>, State>,
}

impl Acc {
    fn new() -> Self {
        Self {
            order: Vec::new(),
            states: HashMap::new(),
        }
    }
}

/// Group captured events by watch and coalesce per document into one net change.
fn coalesce(captured: Vec<CapturedEvent>) -> Vec<(u64, Vec<ChangeEvent>)> {
    let mut by_handle: HashMap<u64, Acc> = HashMap::new();
    for CapturedEvent {
        handle_id,
        pk,
        event,
    } in captured
    {
        let acc = by_handle.entry(handle_id).or_insert_with(Acc::new);
        match acc.states.get_mut(&pk) {
            Some(state) => state.apply(event),
            None => {
                // First sighting of this document: record its order, then key
                // the state by the same pk. Cloning the small key once (commit
                // time, off the per-row path) keeps both the order list and the
                // map owning their own copy.
                acc.order.push(pk.clone());
                acc.states.insert(pk, State::first(event));
            }
        }
    }

    let mut out = Vec::with_capacity(by_handle.len());
    for (handle_id, mut acc) in by_handle {
        let mut events = Vec::with_capacity(acc.order.len());
        for pk in &acc.order {
            if let Some(state) = acc.states.remove(pk)
                && let Some(net) = state.into_net()
            {
                events.push(net);
            }
        }
        out.push((handle_id, events));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::rawdoc;

    fn cap(handle_id: u64, pk: &str, event: ChangeEvent) -> CapturedEvent {
        CapturedEvent {
            handle_id,
            pk: pk.as_bytes().to_vec(),
            event,
        }
    }

    #[test]
    fn coalesce_collapses_repeated_updates_to_one_net_change() {
        let captured = vec![
            cap(
                1,
                "a",
                ChangeEvent::Update {
                    old: rawdoc! { "_id": "a", "t": 1 },
                    new: rawdoc! { "_id": "a", "t": 2 },
                },
            ),
            cap(
                1,
                "a",
                ChangeEvent::Update {
                    old: rawdoc! { "_id": "a", "t": 2 },
                    new: rawdoc! { "_id": "a", "t": 3 },
                },
            ),
        ];
        let out = coalesce(captured);
        assert_eq!(out.len(), 1);
        let (handle_id, events) = &out[0];
        assert_eq!(*handle_id, 1);
        assert_eq!(events.len(), 1);
        // Net: the *first* old (1) and the *last* new (3).
        match &events[0] {
            ChangeEvent::Update { old, new } => {
                assert_eq!(old.get_i32("t").unwrap(), 1);
                assert_eq!(new.get_i32("t").unwrap(), 3);
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn coalesce_insert_then_delete_nets_to_nothing() {
        let captured = vec![
            cap(
                1,
                "a",
                ChangeEvent::Insert {
                    doc: rawdoc! { "_id": "a" },
                },
            ),
            cap(
                1,
                "a",
                ChangeEvent::Delete {
                    doc: rawdoc! { "_id": "a" },
                },
            ),
        ];
        let out = coalesce(captured);
        assert_eq!(out.len(), 1);
        assert!(
            out[0].1.is_empty(),
            "in-and-out within a commit nets to nothing"
        );
    }

    #[test]
    fn coalesce_preserves_first_seen_document_order() {
        let captured = vec![
            cap(
                1,
                "b",
                ChangeEvent::Insert {
                    doc: rawdoc! { "_id": "b" },
                },
            ),
            cap(
                1,
                "a",
                ChangeEvent::Insert {
                    doc: rawdoc! { "_id": "a" },
                },
            ),
        ];
        let out = coalesce(captured);
        let events = &out[0].1;
        assert_eq!(events.len(), 2);
        match (&events[0], &events[1]) {
            (ChangeEvent::Insert { doc: d0 }, ChangeEvent::Insert { doc: d1 }) => {
                assert_eq!(d0.get_str("_id").unwrap(), "b");
                assert_eq!(d1.get_str("_id").unwrap(), "a");
            }
            _ => panic!("expected two inserts in first-seen order"),
        }
    }

    #[test]
    fn parse_rejects_set_operations() {
        assert!(parse_watch_filter("SELECT VALUE c FROM c ORDER BY c.x DESC").is_err());
        assert!(parse_watch_filter("SELECT VALUE c FROM c GROUP BY c.x").is_err());
        assert!(parse_watch_filter("SELECT VALUE c FROM c LIMIT 5").is_err());
        assert!(parse_watch_filter("SELECT c.x FROM c").is_err());
        assert!(parse_watch_filter("SELECT * FROM c WHERE c.x > @p").is_err());
    }

    #[test]
    fn parse_accepts_where_and_match_all() {
        let (alias, _) = parse_watch_filter("SELECT * FROM c WHERE c.x > 1").unwrap();
        assert_eq!(alias, "c");
        // No WHERE → match-all.
        assert!(parse_watch_filter("SELECT * FROM c").is_ok());
        // SELECT VALUE <alias> is the identity projection, also accepted.
        assert!(parse_watch_filter("SELECT VALUE c FROM c WHERE c.x > 1").is_ok());
    }
}
