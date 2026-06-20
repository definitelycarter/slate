//! Aggregate functions — **planned, not yet wired into [`crate::exec`]**.
//!
//! Cosmos aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) and `GROUP BY`
//! change the execution model: unlike scalar functions, which map a single row
//! to a single value, aggregates fold the *entire* row stream into one (or, with
//! `GROUP BY`, one-per-group) result. Wiring them requires an accumulation phase
//! in the executor and a grouping key in [`slate_ast::Query`].
//!
//! This module fixes the name/identity surface now so that phase can be added
//! without reshaping the AST or the scalar-function path. The variants are not
//! referenced by the executor yet.

/// A recognized aggregate function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFn {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateFn {
    /// Resolve a (case-insensitive) function name to an aggregate, if it is one.
    ///
    /// Used later by the parser/planner to decide whether a `Function` node is
    /// scalar (dispatched via [`slate_eval::functions`]) or aggregate (handled by a
    /// future grouping phase).
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "COUNT" => Some(AggregateFn::Count),
            "SUM" => Some(AggregateFn::Sum),
            "AVG" => Some(AggregateFn::Avg),
            "MIN" => Some(AggregateFn::Min),
            "MAX" => Some(AggregateFn::Max),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_aggregates_case_insensitively() {
        assert_eq!(AggregateFn::from_name("count"), Some(AggregateFn::Count));
        assert_eq!(AggregateFn::from_name("AVG"), Some(AggregateFn::Avg));
        assert_eq!(AggregateFn::from_name("upper"), None);
    }
}
