use serde::{Deserialize, Serialize};
use slate_db::Datasource;
use slate_query::Query;
use slate_store::Record;

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    // Data
    Insert(Record),
    InsertBatch(Vec<Record>),
    Delete(String),
    GetById(String),
    Query { prefixes: Vec<String>, query: Query },
    // Catalog (global)
    SaveDatasource(Datasource),
    GetDatasource(String),
    ListDatasources,
    DeleteDatasource(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Record(Option<Record>),
    Records(Vec<Record>),
    Datasource(Option<Datasource>),
    Datasources(Vec<Datasource>),
    Error(String),
}
