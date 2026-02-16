use serde::{Deserialize, Serialize};

use crate::operator::Operator;
use crate::value::QueryValue;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Filter {
    pub field: String,
    pub operator: Operator,
    pub value: QueryValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalOp {
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterNode {
    Condition(Filter),
    Group(FilterGroup),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterGroup {
    pub logical: LogicalOp,
    pub children: Vec<FilterNode>,
}
