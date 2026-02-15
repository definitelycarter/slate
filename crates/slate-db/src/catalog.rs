use slate_store::Transaction;

use crate::datasource::Datasource;
use crate::error::DbError;

const SYS_CF: &str = "_sys";
const CATALOG_PREFIX: &[u8] = b"__ds__:";

fn catalog_key(id: &str) -> Vec<u8> {
    let mut key = CATALOG_PREFIX.to_vec();
    key.extend_from_slice(id.as_bytes());
    key
}

pub struct Catalog;

impl Catalog {
    pub fn save<T: Transaction>(
        &self,
        txn: &mut T,
        datasource: &Datasource,
    ) -> Result<(), DbError> {
        let key = catalog_key(&datasource.id);
        let value = bincode::serialize(datasource)?;
        txn.put(SYS_CF, &key, &value)?;
        Ok(())
    }

    pub fn get<T: Transaction>(&self, txn: &T, id: &str) -> Result<Option<Datasource>, DbError> {
        let key = catalog_key(id);
        match txn.get(SYS_CF, &key)? {
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    pub fn list<T: Transaction>(&self, txn: &T) -> Result<Vec<Datasource>, DbError> {
        let iter = txn.scan_prefix(SYS_CF, CATALOG_PREFIX)?;
        let mut datasources = Vec::new();
        for result in iter {
            let (_key, value) = result?;
            datasources.push(bincode::deserialize(&value)?);
        }
        Ok(datasources)
    }

    pub fn delete<T: Transaction>(&self, txn: &mut T, id: &str) -> Result<(), DbError> {
        let key = catalog_key(id);
        txn.delete(SYS_CF, &key)?;
        Ok(())
    }
}
