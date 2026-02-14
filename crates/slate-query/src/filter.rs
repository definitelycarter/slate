use crate::operator::Operator;
use crate::value::QueryValue;

#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    pub field: String,
    pub operator: Operator,
    pub value: QueryValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalOp {
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterNode {
    Condition(Filter),
    Group(FilterGroup),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilterGroup {
    pub logical: LogicalOp,
    pub children: Vec<FilterNode>,
}
