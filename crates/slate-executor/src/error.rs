//! Executor error type.

use std::fmt;

use slate_engine::EngineError;
use slate_eval::EvalError;
use slate_mutation::MutationError;
use slate_vm::VmError;

/// An error raised while executing a plan.
#[derive(Debug)]
pub enum ExecError {
    Eval(EvalError),
    Engine(EngineError),
    Mutation(MutationError),
    /// A script (validator/trigger) runtime error.
    Vm(VmError),
    /// A validator rejected a document.
    Validation(String),
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecError::Eval(e) => write!(f, "evaluation error: {e}"),
            ExecError::Engine(e) => write!(f, "engine error: {e}"),
            ExecError::Mutation(e) => write!(f, "mutation error: {e}"),
            ExecError::Vm(e) => write!(f, "script error: {e}"),
            ExecError::Validation(m) => write!(f, "validation failed: {m}"),
        }
    }
}

impl std::error::Error for ExecError {}

impl From<MutationError> for ExecError {
    fn from(e: MutationError) -> Self {
        ExecError::Mutation(e)
    }
}

impl From<VmError> for ExecError {
    fn from(e: VmError) -> Self {
        ExecError::Vm(e)
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
