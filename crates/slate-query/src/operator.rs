use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operator {
    Eq,
    #[serde(rename = "icontains")]
    IContains,
    #[serde(rename = "istartswith")]
    IStartsWith,
    #[serde(rename = "iendswith")]
    IEndsWith,
    Gt,
    Gte,
    Lt,
    Lte,
    #[serde(rename = "isnull")]
    IsNull,
}
