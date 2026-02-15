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
            Request::Insert(record) => self.write(|txn| {
                txn.insert(record)?;
                Ok(Response::Ok)
            }),
            Request::InsertBatch(records) => self.write(|txn| {
                txn.insert_batch(records)?;
                Ok(Response::Ok)
            }),
            Request::Delete(id) => self.write(|txn| {
                txn.delete(&id)?;
                Ok(Response::Ok)
            }),
            Request::GetById(id) => self.read(|txn| {
                let record = txn.get_by_id(&id)?;
                Ok(Response::Record(record))
            }),
            Request::Query(query) => self.read(|txn| {
                let records = txn.query(&query)?;
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
