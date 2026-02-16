use serde::{Deserialize, Serialize};
use slate_db::Datasource;
use slate_query::Query;

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    // Data
    WriteRecord {
        datasource_id: String,
        record_id: String,
        doc: bson::Document,
    },
    WriteBatch {
        datasource_id: String,
        writes: Vec<(String, bson::Document)>,
    },
    DeleteRecord {
        datasource_id: String,
        record_id: String,
    },
    GetById {
        datasource_id: String,
        record_id: String,
        columns: Option<Vec<String>>,
    },
    Query {
        datasource_id: String,
        query: Query,
    },
    // Catalog (global)
    SaveDatasource(Datasource),
    GetDatasource(String),
    ListDatasources,
    DeleteDatasource(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Record(Option<bson::Document>),
    Records(Vec<bson::Document>),
    Datasource(Option<Datasource>),
    Datasources(Vec<Datasource>),
    Error(String),
}
