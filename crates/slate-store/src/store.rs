use crate::error::StoreError;
use crate::record::Record;

pub trait Store {
    type Txn<'a>: Transaction
    where
        Self: 'a;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, StoreError>;
}

pub trait Transaction {
    type Iter: Iterator<Item = Result<Record, StoreError>>;

    // Reads
    fn get_by_id(&self, id: &str) -> Result<Option<Record>, StoreError>;
    fn scan(&self) -> Result<Self::Iter, StoreError>;

    // Writes
    fn insert(&mut self, record: Record) -> Result<(), StoreError>;
    fn delete(&mut self, id: &str) -> Result<(), StoreError>;

    // Lifecycle
    fn commit(self) -> Result<(), StoreError>;
    fn rollback(self) -> Result<(), StoreError>;
}
