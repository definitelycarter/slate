//! `EXPLAIN ANALYZE` support — per-node row counters collected during a run.
//!
//! The executor builds a chain of boxed iterators, one per plan node. To attach
//! *actuals* (rows emitted per node) without touching any per-node executor —
//! and without costing the normal execute path a thing — the analyze dispatcher
//! wraps each node's *output* iterator in a [`Counting`] adapter that bumps a
//! counter keyed by the node's pre-order index. The normal
//! [`Executor::execute`](crate::Executor::execute) builds no wrappers and reads
//! no [`PlanStats`], so plain queries are unaffected.
//!
//! "Rows examined" is not stored: it is a node's child's emitted count, which the
//! [`explain_analyze`](slate_planner::Plan::explain_analyze) renderer derives
//! from the same flat counter vector.

use std::rc::Rc;

use bson::RawBson;
use slate_planner::PlanStats;

use crate::ExecError;

/// Iterator adapter that counts every row a node emits into `stats` at `index`.
///
/// Counts items the node *yields* downstream: a value (`Some`) and an undefined
/// (`None`) both count as emitted (the node produced a row either way); errors do
/// not. This matches what flows into the parent, so a parent reading this node's
/// count as its "examined" sees exactly the rows it had to process.
pub(crate) struct Counting<'a> {
    inner: crate::ValueIter<'a>,
    stats: Rc<PlanStats>,
    index: usize,
}

impl<'a> Counting<'a> {
    pub(crate) fn new(inner: crate::ValueIter<'a>, stats: Rc<PlanStats>, index: usize) -> Self {
        Self {
            inner,
            stats,
            index,
        }
    }
}

impl Iterator for Counting<'_> {
    type Item = Result<Option<RawBson>, ExecError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if matches!(item, Some(Ok(_))) {
            self.stats.record_emit(self.index);
        }
        item
    }
}
