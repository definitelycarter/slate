use std::sync::Arc;

use slate_db::Database;
use slate_store::RocksStore;

use crate::protocol::{Request, Response};

pub struct Session {
    db: Arc<Database<RocksStore>>,
}

impl Session {
    pub fn new(db: Arc<Database<RocksStore>>) -> Self {
        Self { db }
    }

    pub fn handle(&self, request: Request) -> Response {
        match request {
            Request::WriteRecord {
                datasource_id,
                record_id,
                doc,
            } => self.write(|txn| {
                txn.write_record(&datasource_id, &record_id, doc)?;
                Ok(Response::Ok)
            }),
            Request::WriteBatch {
                datasource_id,
                writes,
            } => self.write(|txn| {
                txn.write_batch(&datasource_id, writes)?;
                Ok(Response::Ok)
            }),
            Request::DeleteRecord {
                datasource_id,
                record_id,
            } => self.write(|txn| {
                txn.delete_record(&datasource_id, &record_id)?;
                Ok(Response::Ok)
            }),
            Request::GetById {
                datasource_id,
                record_id,
                columns,
            } => self.read(|txn| {
                let col_refs: Vec<&str>;
                let cols = match &columns {
                    Some(c) => {
                        col_refs = c.iter().map(|s| s.as_str()).collect();
                        Some(col_refs.as_slice())
                    }
                    None => None,
                };
                let record = txn.get_by_id(&datasource_id, &record_id, cols)?;
                Ok(Response::Record(record))
            }),
            Request::Query {
                datasource_id,
                query,
            } => self.read(|txn| {
                let records = txn.query(&datasource_id, &query)?;
                Ok(Response::Records(records))
            }),
            Request::SaveDatasource(ds) => self.write(|txn| {
                txn.save_datasource(&ds)?;
                Ok(Response::Ok)
            }),
            Request::GetDatasource(id) => self.read(|txn| {
                let ds = txn.get_datasource(&id)?;
                Ok(Response::Datasource(ds))
            }),
            Request::ListDatasources => self.read(|txn| {
                let list = txn.list_datasources()?;
                Ok(Response::Datasources(list))
            }),
            Request::DeleteDatasource(id) => self.write(|txn| {
                txn.delete_datasource(&id)?;
                Ok(Response::Ok)
            }),
        }
    }

    fn read<F>(&self, f: F) -> Response
    where
        F: FnOnce(
            &slate_db::DatabaseTransaction<'_, RocksStore>,
        ) -> Result<Response, slate_db::DbError>,
    {
        match self.db.begin(true) {
            Ok(txn) => match f(&txn) {
                Ok(response) => response,
                Err(e) => Response::Error(e.to_string()),
            },
            Err(e) => Response::Error(e.to_string()),
        }
    }

    fn write<F>(&self, f: F) -> Response
    where
        F: FnOnce(
            &mut slate_db::DatabaseTransaction<'_, RocksStore>,
        ) -> Result<Response, slate_db::DbError>,
    {
        match self.db.begin(false) {
            Ok(mut txn) => match f(&mut txn) {
                Ok(response) => match txn.commit() {
                    Ok(()) => response,
                    Err(e) => Response::Error(e.to_string()),
                },
                Err(e) => Response::Error(e.to_string()),
            },
            Err(e) => Response::Error(e.to_string()),
        }
    }
}
