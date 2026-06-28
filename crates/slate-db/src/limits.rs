//! Per-query resource limits (Resource Limits & Safety Valves RFC, A + B).
//!
//! Limits are configured as a database-wide default via
//! [`DatabaseBuilder::with_limits`](crate::DatabaseBuilder::with_limits) /
//! [`with_deadline`](crate::DatabaseBuilder::with_deadline), with an optional
//! per-query override on the `find` / `query` / `distinct` builders
//! (`.deadline(..)`). A field left `None` on an override inherits the database
//! default; the merge is [`QueryLimits::or`].

use std::time::Duration;

/// The resource limits applied to one query — the safety valves that keep an
/// embedded query from taking down its host.
///
/// `Default` (all `None`) is unbounded. As a per-query override, each set field
/// wins and each `None` field inherits the database-wide default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryLimits {
    /// Maximum wall-clock time a query may run before aborting with
    /// [`DbError::Timeout`](crate::DbError::Timeout) (RFC part A). Enforced
    /// *cooperatively* — checked between rows in the executor's source nodes — so
    /// it is a deadline, not a preemptive interrupt: a call blocked entirely off-
    /// CPU in a backend syscall is not interrupted mid-syscall. `None` is
    /// unbounded.
    pub deadline: Option<Duration>,
}

impl QueryLimits {
    /// Per-query override merge: each field set on `self` wins; a field left
    /// `None` falls back to `default` (the database-wide setting).
    pub(crate) fn or(self, default: QueryLimits) -> QueryLimits {
        QueryLimits {
            deadline: self.deadline.or(default.deadline),
        }
    }
}
