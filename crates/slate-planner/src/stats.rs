//! Per-node execution counters for `EXPLAIN ANALYZE`.
//!
//! [`Plan::explain`] renders the *logical* tree — the shape the planner settled
//! on. This module adds the *actuals*: how many rows each node emitted when the
//! plan actually ran. The counters live in a flat [`PlanStats`] vector indexed by
//! a node's **pre-order position** in the tree (the same order [`explain`] walks
//! and emits lines), so the executor can bump a counter by index without holding
//! a reference to the node, and the renderer can pair each line with its count.
//!
//! [`explain`]: crate::explain
//!
//! ## Why pre-order indexing
//!
//! The executor consumes the [`Node`](crate::Node) tree by value as it builds its
//! iterator chain, so it can't key stats by node identity. Both the executor's
//! analyze walk and [`Plan::node_count`]/the annotated renderer visit nodes in
//! the *same* pre-order (a node, then its children left-to-right), so an index
//! assigned during execution lines up with the index assigned during rendering.
//! As long as the two walks stay in lock-step, the mapping is exact.
//!
//! ## Cost
//!
//! [`PlanStats`] is only ever constructed on the explicit `explain_analyze` path.
//! The normal execute path never touches it, so plain queries pay nothing for it.

use std::cell::Cell;

use crate::plan::{Node, Plan};

/// Per-node execution counters, indexed by a node's pre-order position.
///
/// `Cell<u64>` (not atomics) because execution is single-threaded per query —
/// the counters are bumped from the iterator chain on the same thread that
/// renders them. Interior mutability lets the counting iterators hold a shared
/// `&PlanStats` while still incrementing.
#[derive(Debug, Default)]
pub struct PlanStats {
    /// Rows *emitted* by each node, indexed by pre-order position. A node's
    /// "rows examined" is its child's emitted count (what flowed in), which the
    /// renderer derives — so one counter per node suffices.
    emitted: Vec<Cell<u64>>,
}

impl PlanStats {
    /// Allocate a zeroed counter for every node in `plan`.
    pub fn for_plan(plan: &Plan) -> Self {
        Self {
            emitted: (0..plan.node_count()).map(|_| Cell::new(0)).collect(),
        }
    }

    /// Record that the node at pre-order `index` emitted one row. Out-of-range
    /// indices are ignored (defensive — the two walks should always agree).
    #[inline]
    pub fn record_emit(&self, index: usize) {
        if let Some(cell) = self.emitted.get(index) {
            cell.set(cell.get() + 1);
        }
    }

    /// Rows emitted by the node at pre-order `index` (0 if out of range).
    pub fn emitted(&self, index: usize) -> u64 {
        self.emitted.get(index).map_or(0, Cell::get)
    }

    /// Number of nodes tracked.
    pub fn len(&self) -> usize {
        self.emitted.len()
    }

    /// Whether any nodes are tracked.
    pub fn is_empty(&self) -> bool {
        self.emitted.is_empty()
    }
}

impl Plan {
    /// Count the nodes in this plan's tree (pre-order), so a [`PlanStats`] can be
    /// sized to one counter per node.
    pub fn node_count(&self) -> usize {
        match self {
            Plan::Query(node) => count_node(node),
            Plan::Insert { source, .. }
            | Plan::Delete { source, .. }
            | Plan::Update { source, .. }
            | Plan::Replace { source, .. }
            | Plan::Upsert { source, .. } => count_node(source),
            Plan::Trigger { plan, .. } => plan.node_count(),
        }
    }
}

/// Count `node` and all its descendants. Mirrors the executor's analyze walk and
/// the annotated renderer's walk — keep all three in pre-order lock-step.
fn count_node(node: &Node) -> usize {
    1 + match node {
        Node::Values(_) | Node::Scan { .. } | Node::IndexScan { .. } | Node::CurrentRow => 0,

        Node::KeyLookup { source, .. }
        | Node::Bind { source, .. }
        | Node::Unwind { source, .. }
        | Node::Project { source, .. }
        | Node::Filter { source, .. }
        | Node::Sort { source, .. }
        | Node::Limit { source, .. }
        | Node::Distinct { source, .. }
        | Node::Aggregate { source, .. }
        | Node::Trigger { source, .. }
        | Node::Validate { source, .. } => count_node(source),

        Node::IndexMerge { lhs, rhs, .. } => count_node(lhs) + count_node(rhs),

        // `Subquery` visits `source` then `subplan`, matching the renderer.
        Node::Subquery {
            source, subplan, ..
        } => count_node(source) + count_node(subplan),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{CollectionRef, Node, Plan, RowBinding};
    use slate_ast::Expression;

    fn cref() -> CollectionRef {
        CollectionRef {
            cf: "default".into(),
            collection: "users".into(),
        }
    }

    #[test]
    fn node_count_matches_tree_size() {
        // Project <- Filter <- Scan  →  3 nodes.
        let plan = Plan::Query(Node::Project {
            expr: Expression::Identifier("c".into()),
            binding: RowBinding::Alias("c".into()),
            source: Box::new(Node::Filter {
                predicate: Expression::Identifier("c".into()),
                binding: RowBinding::Alias("c".into()),
                source: Box::new(Node::Scan { collection: cref() }),
            }),
        });
        assert_eq!(plan.node_count(), 3);
    }

    #[test]
    fn index_merge_counts_both_arms() {
        let plan = Plan::Query(Node::IndexMerge {
            collection: cref(),
            logical: crate::plan::LogicalOp::Or,
            lhs: Box::new(Node::Scan { collection: cref() }),
            rhs: Box::new(Node::Scan { collection: cref() }),
        });
        assert_eq!(plan.node_count(), 3); // merge + two scans
    }

    #[test]
    fn record_and_read_back() {
        let plan = Plan::Query(Node::Scan { collection: cref() });
        let stats = PlanStats::for_plan(&plan);
        assert_eq!(stats.len(), 1);
        stats.record_emit(0);
        stats.record_emit(0);
        assert_eq!(stats.emitted(0), 2);
        // Out-of-range is a harmless no-op.
        stats.record_emit(99);
        assert_eq!(stats.emitted(99), 0);
    }
}
