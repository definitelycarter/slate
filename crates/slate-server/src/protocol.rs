use serde::{Deserialize, Serialize};
use slate_db::{CellWrite, Datasource, Record};
use slate_query::Query;

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    // Data
    WriteCells {
        datasource_id: String,
        record_id: String,
        cells: Vec<CellWrite>,
    },
    WriteBatch {
        datasource_id: String,
        writes: Vec<(String, Vec<CellWrite>)>,
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
    Record(Option<Record>),
    Records(Vec<Record>),
    Datasource(Option<Datasource>),
    Datasources(Vec<Datasource>),
    Error(String),
}
