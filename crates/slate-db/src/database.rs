use slate_query::Query;
use slate_store::{Record, Store, Transaction};

use crate::catalog::Catalog;
use crate::datasource::Datasource;
use crate::error::DbError;
use crate::exec;

pub struct Database<S: Store> {
    store: S,
    catalog: Catalog,
}

impl<S: Store> Database<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            catalog: Catalog,
        }
    }

    pub fn begin(&self, read_only: bool) -> Result<DatabaseTransaction<'_, S>, DbError> {
        let txn = self.store.begin(read_only)?;
        Ok(DatabaseTransaction {
            txn,
            catalog: &self.catalog,
        })
    }
}

pub struct DatabaseTransaction<'db, S: Store + 'db> {
    txn: S::Txn<'db>,
    catalog: &'db Catalog,
}

impl<'db, S: Store + 'db> DatabaseTransaction<'db, S> {
    // Catalog operations

    pub fn save_datasource(&mut self, datasource: &Datasource) -> Result<(), DbError> {
        self.catalog.save(&mut self.txn, datasource)
    }

    pub fn get_datasource(&self, id: &str) -> Result<Option<Datasource>, DbError> {
        self.catalog.get(&self.txn, id)
    }

    pub fn list_datasources(&self) -> Result<Vec<Datasource>, DbError> {
        self.catalog.list(&self.txn)
    }

    pub fn delete_datasource(&mut self, id: &str) -> Result<(), DbError> {
        self.catalog.delete(&mut self.txn, id)
    }

    // Data operations

    pub fn insert(&mut self, record: Record) -> Result<(), DbError> {
        self.txn.insert(record)?;
        Ok(())
    }

    pub fn insert_batch(&mut self, records: Vec<Record>) -> Result<(), DbError> {
        for record in records {
            self.txn.insert(record)?;
        }
        Ok(())
    }

    pub fn delete(&mut self, id: &str) -> Result<(), DbError> {
        self.txn.delete(id)?;
        Ok(())
    }

    pub fn get_by_id(&self, id: &str) -> Result<Option<Record>, DbError> {
        let record = self.txn.get_by_id(id)?;
        Ok(record)
    }

    // Query

    pub fn query(&self, query: &Query) -> Result<Vec<Record>, DbError> {
        let iter = self.txn.scan()?;
        exec::execute(iter, query)
    }

    // Lifecycle

    pub fn commit(self) -> Result<(), DbError> {
        self.txn.commit()?;
        Ok(())
    }

    pub fn rollback(self) -> Result<(), DbError> {
        self.txn.rollback()?;
        Ok(())
    }
}
