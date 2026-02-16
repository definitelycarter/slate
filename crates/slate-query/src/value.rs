use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Date(i64),
    Null,
}
