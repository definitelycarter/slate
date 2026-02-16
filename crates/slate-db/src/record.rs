use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::cell::Cell;

/// A record as returned by queries — columns with their cell values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub id: String,
    pub cells: HashMap<String, Cell>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Date(i64),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
}
