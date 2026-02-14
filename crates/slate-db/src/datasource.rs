#[derive(Debug, Clone, PartialEq)]
pub struct Datasource {
    pub id: String,
    pub name: String,
    pub fields: Vec<FieldDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    pub name: String,
    pub field_type: FieldType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    String,
    Int,
    Float,
    Bool,
    Date,
    List(Box<FieldType>),
    Map(Vec<FieldDef>),
}
