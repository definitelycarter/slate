//! Executor error type.

use std::fmt;

use slate_engine::EngineError;
use slate_eval::EvalError;
use slate_rawbson::RawMergeError;

/// An error raised while executing a plan.
#[derive(Debug)]
pub enum ExecError {
    Eval(EvalError),
    Engine(EngineError),
    Mutation(RawMergeError),
    /// A validator rejected a document, errored, or is bound to an unregistered
    /// function.
    Validation(String),
    /// A trigger errored, panicked, or is bound to an unregistered function.
    Trigger(String),
    /// A structurally invalid plan node reached execution — an invariant the
    /// planner upholds, surfaced (rather than panicking) when a directly-built
    /// IR violates it (e.g. an `IndexIntersect` with fewer than two parts).
    InvalidPlan(String),
    /// The query's cooperative deadline elapsed mid-execution (Resource Limits
    /// RFC, A). Raised by a source node's between-rows check, so it aborts the
    /// stream rather than running to completion. Distinct from
    /// [`LimitExceeded`](Self::LimitExceeded) so a caller can tell "too slow"
    /// from "too big".
    Timeout,
    /// A materializing node ([`Sort`]/[`IndexMerge`]/[`Distinct`]/[`Aggregate`])
    /// buffered more rows than the configured materialization cap (Resource
    /// Limits RFC, B) — the OOM guard. The message names the node and the cap.
    ///
    /// [`Sort`]: slate_planner::Node::Sort
    /// [`IndexMerge`]: slate_planner::Node::IndexMerge
    /// [`Distinct`]: slate_planner::Node::Distinct
    /// [`Aggregate`]: slate_planner::Node::Aggregate
    LimitExceeded(String),
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecError::Eval(e) => write!(f, "evaluation error: {e}"),
            ExecError::Engine(e) => write!(f, "engine error: {e}"),
            ExecError::Mutation(e) => write!(f, "mutation error: {e}"),
            ExecError::Validation(m) => write!(f, "validation failed: {m}"),
            ExecError::Trigger(m) => write!(f, "trigger failed: {m}"),
            ExecError::InvalidPlan(m) => write!(f, "invalid plan: {m}"),
            ExecError::Timeout => write!(f, "query exceeded its time deadline"),
            ExecError::LimitExceeded(m) => write!(f, "resource limit exceeded: {m}"),
        }
    }
}

impl std::error::Error for ExecError {}

impl From<RawMergeError> for ExecError {
    fn from(e: RawMergeError) -> Self {
        ExecError::Mutation(e)
    }
}

impl From<EvalError> for ExecError {
    fn from(e: EvalError) -> Self {
        ExecError::Eval(e)
    }
}

impl From<EngineError> for ExecError {
    fn from(e: EngineError) -> Self {
        ExecError::Engine(e)
    }
}
