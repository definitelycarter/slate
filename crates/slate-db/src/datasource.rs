use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Datasource {
    pub id: String,
    pub name: String,
    pub fields: Vec<FieldDef>,
    pub partition: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    pub field_type: FieldType,
    /// When true, an index is maintained for this field on writes.
    #[serde(default)]
    pub indexed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    String,
    Int,
    Float,
    Bool,
    Date,
    List(Box<FieldType>),
    Map(Vec<FieldDef>),
}
