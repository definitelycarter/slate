use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operator {
    Eq,
    IContains,
    IStartsWith,
    IEndsWith,
    Gt,
    Gte,
    Lt,
    Lte,
    IsNull,
}
