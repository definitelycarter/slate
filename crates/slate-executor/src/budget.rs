//! Cooperative resource-limit checks for the executor (Resource Limits RFC).
//!
//! The query **deadline** (part A) lives here. A [`Deadline`] bundles a live
//! clock reader and the absolute instant past which a query must abort; a
//! [`Ticker`] folds the check into a source node's *existing* per-row closure,
//! reading the clock once every [`DEADLINE_CHECK_INTERVAL`] rows and yielding
//! [`ExecError::Timeout`](crate::ExecError::Timeout) when the budget is spent.
//!
//! The spike (`tasks/resource-limits-spike.md`) showed this is hot-path noise
//! *only* when folded in like this — wrapping the source in a *separate* adapter
//! iterator instead costs an extra `Box<dyn Iterator>` vtable hop per row
//! (~+8%). So the source nodes call [`Ticker::tick`] inside the closure they
//! already build, never an outer layer.

use std::rc::Rc;

use crate::ExecError;

/// Rows a source node scans between clock reads. A power of two so the periodic
/// check is a cheap mask, not a modulo. Tuned in the spike so the read amortizes
/// to noise (~one read per 56 µs of scan work) while keeping the deadline tight.
pub(crate) const DEADLINE_CHECK_INTERVAL: u64 = 1024;

/// A cooperative query deadline: the absolute wall-clock instant (epoch millis)
/// past which execution must abort, plus the live clock to read elapsed time.
///
/// Built by the db layer (`Transaction::exec_env`) from the injected clock and
/// the configured `Duration`, and carried on the [`ExecEnv`](crate::ExecEnv).
/// The clock is the same injectable source the engine and `GETCURRENT*` use, so
/// the check is wasm-safe (no syscall in the executor).
pub struct Deadline {
    clock: Rc<dyn Fn() -> i64>,
    at: i64,
}

impl Deadline {
    /// A deadline that fires once the clock reads strictly past `at` (epoch ms).
    pub fn new(clock: Rc<dyn Fn() -> i64>, at: i64) -> Self {
        Self { clock, at }
    }

    /// Whether the deadline has elapsed — one clock read.
    #[inline]
    fn exceeded(&self) -> bool {
        (self.clock)() > self.at
    }
}

/// Folds the periodic deadline check into a source node's per-row closure. Holds
/// the (optional) deadline and a row counter; [`tick`](Self::tick) is called once
/// per scanned row.
///
/// With no deadline it is a single predictable branch — the zero-cost default.
/// With one it reads the clock every [`DEADLINE_CHECK_INTERVAL`] rows. The
/// counter starts at 0, so the first row is also a check: an already-expired
/// deadline trips immediately, with no dependence on the collection being larger
/// than the interval.
pub(crate) struct Ticker {
    deadline: Option<Rc<Deadline>>,
    n: u64,
}

impl Ticker {
    pub(crate) fn new(deadline: Option<Rc<Deadline>>) -> Self {
        Self { deadline, n: 0 }
    }

    /// Account for one scanned row; `Err(Timeout)` once the deadline elapses.
    #[inline]
    pub(crate) fn tick(&mut self) -> Result<(), ExecError> {
        if let Some(deadline) = &self.deadline {
            if self.n & (DEADLINE_CHECK_INTERVAL - 1) == 0 && deadline.exceeded() {
                return Err(ExecError::Timeout);
            }
            self.n += 1;
        }
        Ok(())
    }
}

/// The materialization cap (Resource Limits RFC, B): the OOM guard on the
/// blocking nodes. `Err(LimitExceeded)` once a node's buffered-row count `count`
/// exceeds `cap`; `None` cap is unbounded — the zero-cost default. `node` names
/// the blocking node for the message. Folded into each node's existing buffering
/// loop (the spike showed a per-row counter + compare is hot-path noise).
#[inline]
pub(crate) fn check_cap(count: usize, cap: Option<usize>, node: &str) -> Result<(), ExecError> {
    if let Some(cap) = cap
        && count > cap
    {
        return Err(ExecError::LimitExceeded(format!(
            "{node} exceeded the materialization cap of {cap} rows"
        )));
    }
    Ok(())
}
