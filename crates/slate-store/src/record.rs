use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub id: String,
    pub fields: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Date(i64),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
}
