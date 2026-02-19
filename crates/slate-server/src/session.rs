use std::sync::Arc;

use slate_db::Database;
use slate_store::Store;

use crate::protocol::{Request, Response};

pub struct Session<S: Store> {
    db: Arc<Database<S>>,
}

impl<S: Store> Session<S> {
    pub fn new(db: Arc<Database<S>>) -> Self {
        Self { db }
    }

    pub fn handle(&self, request: Request) -> Response {
        match request {
            Request::InsertOne { collection, doc } => self.write(|txn| {
                let result = txn.insert_one(&collection, doc)?;
                Ok(Response::Insert(result))
            }),
            Request::InsertMany { collection, docs } => self.write(|txn| {
                let results = txn.insert_many(&collection, docs)?;
                Ok(Response::Inserts(results))
            }),
            Request::Find { collection, query } => self.read(|txn| {
                let records = txn.find(&collection, &query)?;
                Ok(Response::Records(records))
            }),
            Request::FindOne { collection, query } => self.read(|txn| {
                let record = txn.find_one(&collection, &query)?;
                Ok(Response::Record(record))
            }),
            Request::FindById {
                collection,
                id,
                columns,
            } => self.read(|txn| {
                let cols: Option<Vec<&str>> = columns
                    .as_ref()
                    .map(|c| c.iter().map(|s| s.as_str()).collect());
                let record = txn.find_by_id(&collection, &id, cols.as_deref())?;
                Ok(Response::Record(record))
            }),
            Request::UpdateOne {
                collection,
                filter,
                update,
                upsert,
            } => self.write(|txn| {
                let result = txn.update_one(&collection, &filter, update, upsert)?;
                Ok(Response::Update(result))
            }),
            Request::UpdateMany {
                collection,
                filter,
                update,
            } => self.write(|txn| {
                let result = txn.update_many(&collection, &filter, update)?;
                Ok(Response::Update(result))
            }),
            Request::ReplaceOne {
                collection,
                filter,
                doc,
            } => self.write(|txn| {
                let result = txn.replace_one(&collection, &filter, doc)?;
                Ok(Response::Update(result))
            }),
            Request::DeleteOne { collection, filter } => self.write(|txn| {
                let result = txn.delete_one(&collection, &filter)?;
                Ok(Response::Delete(result))
            }),
            Request::DeleteMany { collection, filter } => self.write(|txn| {
                let result = txn.delete_many(&collection, &filter)?;
                Ok(Response::Delete(result))
            }),
            Request::Count { collection, filter } => self.read(|txn| {
                let count = txn.count(&collection, filter.as_ref())?;
                Ok(Response::Count(count))
            }),
            Request::CreateIndex { collection, field } => self.write(|txn| {
                txn.create_index(&collection, &field)?;
                Ok(Response::Ok)
            }),
            Request::DropIndex { collection, field } => self.write(|txn| {
                txn.drop_index(&collection, &field)?;
                Ok(Response::Ok)
            }),
            Request::ListIndexes { collection } => self.read(|txn| {
                let indexes = txn.list_indexes(&collection)?;
                Ok(Response::Indexes(indexes))
            }),
            Request::CreateCollection { config } => self.write(|txn| {
                txn.create_collection(&config)?;
                Ok(Response::Ok)
            }),
            Request::ListCollections => self.read(|txn| {
                let collections = txn.list_collections()?;
                Ok(Response::Collections(collections))
            }),
            Request::DropCollection { collection } => self.write(|txn| {
                txn.drop_collection(&collection)?;
                Ok(Response::Ok)
            }),
            Request::Distinct { collection, query } => self.read(|txn| {
                let values = txn.distinct(&collection, &query)?;
                Ok(Response::Values(values))
            }),
        }
    }

    fn read<F>(&self, f: F) -> Response
    where
        F: FnOnce(&mut slate_db::DatabaseTransaction<'_, S>) -> Result<Response, slate_db::DbError>,
    {
        match self.db.begin(true) {
            Ok(mut txn) => match f(&mut txn) {
                Ok(response) => response,
                Err(e) => Response::Error(e.to_string()),
            },
            Err(e) => Response::Error(e.to_string()),
        }
    }

    fn write<F>(&self, f: F) -> Response
    where
        F: FnOnce(&mut slate_db::DatabaseTransaction<'_, S>) -> Result<Response, slate_db::DbError>,
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
