use slate_store::{Record, Transaction, Value};

use crate::datasource::{Datasource, FieldDef, FieldType};
use crate::error::DbError;

const CATALOG_PREFIX: &str = "__ds__";

fn catalog_key(id: &str) -> String {
    format!("{CATALOG_PREFIX}{id}")
}

pub struct Catalog;

impl Catalog {
    pub fn save<T: Transaction>(
        &self,
        txn: &mut T,
        datasource: &Datasource,
    ) -> Result<(), DbError> {
        let record = datasource_to_record(datasource);
        txn.insert(record)?;
        Ok(())
    }

    pub fn get<T: Transaction>(&self, txn: &T, id: &str) -> Result<Option<Datasource>, DbError> {
        let key = catalog_key(id);
        match txn.get_by_id(&key)? {
            Some(record) => Ok(Some(record_to_datasource(&record)?)),
            None => Ok(None),
        }
    }

    pub fn list<T: Transaction>(&self, txn: &T) -> Result<Vec<Datasource>, DbError> {
        let iter = txn.scan_prefix(CATALOG_PREFIX)?;
        let mut datasources = Vec::new();
        for result in iter {
            let record = result?;
            datasources.push(record_to_datasource(&record)?);
        }
        Ok(datasources)
    }

    pub fn delete<T: Transaction>(&self, txn: &mut T, id: &str) -> Result<(), DbError> {
        let key = catalog_key(id);
        txn.delete(&key)?;
        Ok(())
    }
}

fn datasource_to_record(ds: &Datasource) -> Record {
    let mut fields = std::collections::HashMap::new();
    fields.insert("name".to_string(), Value::String(ds.name.clone()));
    fields.insert(
        "fields".to_string(),
        Value::List(ds.fields.iter().map(field_def_to_value).collect()),
    );
    Record {
        id: catalog_key(&ds.id),
        fields,
    }
}

fn field_def_to_value(field: &FieldDef) -> Value {
    let mut map = std::collections::HashMap::new();
    map.insert("name".to_string(), Value::String(field.name.clone()));
    map.insert("type".to_string(), field_type_to_value(&field.field_type));
    Value::Map(map)
}

fn field_type_to_value(ft: &FieldType) -> Value {
    match ft {
        FieldType::String => Value::String("string".to_string()),
        FieldType::Int => Value::String("int".to_string()),
        FieldType::Float => Value::String("float".to_string()),
        FieldType::Bool => Value::String("bool".to_string()),
        FieldType::Date => Value::String("date".to_string()),
        FieldType::List(inner) => {
            let mut map = std::collections::HashMap::new();
            map.insert("type".to_string(), Value::String("list".to_string()));
            map.insert("inner".to_string(), field_type_to_value(inner));
            Value::Map(map)
        }
        FieldType::Map(fields) => {
            let mut map = std::collections::HashMap::new();
            map.insert("type".to_string(), Value::String("map".to_string()));
            map.insert(
                "fields".to_string(),
                Value::List(fields.iter().map(field_def_to_value).collect()),
            );
            Value::Map(map)
        }
    }
}

fn record_to_datasource(record: &Record) -> Result<Datasource, DbError> {
    let id = record
        .id
        .strip_prefix(CATALOG_PREFIX)
        .ok_or_else(|| DbError::InvalidQuery("invalid catalog key".to_string()))?
        .to_string();

    let name = match record.fields.get("name") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(DbError::InvalidQuery("missing datasource name".to_string())),
    };

    let fields = match record.fields.get("fields") {
        Some(Value::List(items)) => items
            .iter()
            .map(value_to_field_def)
            .collect::<Result<Vec<_>, _>>()?,
        _ => {
            return Err(DbError::InvalidQuery(
                "missing datasource fields".to_string(),
            ));
        }
    };

    Ok(Datasource { id, name, fields })
}

fn value_to_field_def(value: &Value) -> Result<FieldDef, DbError> {
    match value {
        Value::Map(map) => {
            let name = match map.get("name") {
                Some(Value::String(s)) => s.clone(),
                _ => return Err(DbError::InvalidQuery("missing field name".to_string())),
            };
            let field_type = match map.get("type") {
                Some(v) => value_to_field_type(v)?,
                _ => return Err(DbError::InvalidQuery("missing field type".to_string())),
            };
            Ok(FieldDef { name, field_type })
        }
        _ => Err(DbError::InvalidQuery(
            "expected map for field def".to_string(),
        )),
    }
}

fn value_to_field_type(value: &Value) -> Result<FieldType, DbError> {
    match value {
        Value::String(s) => match s.as_str() {
            "string" => Ok(FieldType::String),
            "int" => Ok(FieldType::Int),
            "float" => Ok(FieldType::Float),
            "bool" => Ok(FieldType::Bool),
            "date" => Ok(FieldType::Date),
            other => Err(DbError::InvalidQuery(format!(
                "unknown field type: {other}"
            ))),
        },
        Value::Map(map) => {
            let type_name = match map.get("type") {
                Some(Value::String(s)) => s.as_str(),
                _ => {
                    return Err(DbError::InvalidQuery(
                        "missing type in complex field".to_string(),
                    ));
                }
            };
            match type_name {
                "list" => {
                    let inner = match map.get("inner") {
                        Some(v) => value_to_field_type(v)?,
                        _ => {
                            return Err(DbError::InvalidQuery(
                                "missing inner type for list".to_string(),
                            ));
                        }
                    };
                    Ok(FieldType::List(Box::new(inner)))
                }
                "map" => {
                    let fields = match map.get("fields") {
                        Some(Value::List(items)) => items
                            .iter()
                            .map(value_to_field_def)
                            .collect::<Result<Vec<_>, _>>()?,
                        _ => {
                            return Err(DbError::InvalidQuery(
                                "missing fields for map".to_string(),
                            ));
                        }
                    };
                    Ok(FieldType::Map(fields))
                }
                other => Err(DbError::InvalidQuery(format!(
                    "unknown complex type: {other}"
                ))),
            }
        }
        _ => Err(DbError::InvalidQuery(
            "invalid field type value".to_string(),
        )),
    }
}
