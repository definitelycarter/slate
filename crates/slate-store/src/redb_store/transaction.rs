use std::borrow::Cow;

use redb::{Database, ReadableTable, TableDefinition};

use crate::error::StoreError;
use crate::store::Transaction;

enum Inner {
    Read(redb::ReadTransaction),
    Write(redb::WriteTransaction),
    Consumed,
}

pub struct RedbTransaction<'db> {
    inner: Inner,
    #[allow(dead_code)]
    db: &'db Database,
    read_only: bool,
}

impl<'db> RedbTransaction<'db> {
    pub fn new(db: &'db Database, read_only: bool) -> Result<Self, StoreError> {
        let inner = if read_only {
            Inner::Read(
                db.begin_read()
                    .map_err(|e| StoreError::Storage(e.to_string()))?,
            )
        } else {
            Inner::Write(
                db.begin_write()
                    .map_err(|e| StoreError::Storage(e.to_string()))?,
            )
        };
        Ok(Self {
            inner,
            db,
            read_only,
        })
    }

    fn check_writable(&self) -> Result<(), StoreError> {
        if self.read_only {
            return Err(StoreError::ReadOnly);
        }
        Ok(())
    }

    fn collect_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let cf = cf.to_string();
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&cf);

        let mut upper = prefix.to_vec();
        let has_upper = if let Some(last) = upper.last_mut() {
            *last = last.wrapping_add(1);
            true
        } else {
            false
        };

        match &self.inner {
            Inner::Read(txn) => {
                let table = txn
                    .open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                collect_from_readable(&table, prefix, &upper, has_upper, reverse)
            }
            Inner::Write(txn) => {
                let table = txn
                    .open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                collect_from_readable(&table, prefix, &upper, has_upper, reverse)
            }
            Inner::Consumed => Err(StoreError::TransactionConsumed),
        }
    }
}

impl<'db> Transaction for RedbTransaction<'db> {
    type Cf = String;

    fn cf(&mut self, name: &str) -> Result<Self::Cf, StoreError> {
        // Validate the table exists by attempting to open it.
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(name);
        match &self.inner {
            Inner::Read(txn) => {
                txn.open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
            }
            Inner::Write(txn) => {
                txn.open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
            }
            Inner::Consumed => return Err(StoreError::TransactionConsumed),
        }
        Ok(name.to_string())
    }

    fn get<'c>(&self, cf: &'c Self::Cf, key: &[u8]) -> Result<Option<Cow<'c, [u8]>>, StoreError> {
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(cf);
        match &self.inner {
            Inner::Read(txn) => {
                let table = txn
                    .open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                let value = table
                    .get(key)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                Ok(value.map(|v| Cow::Owned(v.value().to_vec())))
            }
            Inner::Write(txn) => {
                let table = txn
                    .open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                let value = table
                    .get(key)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                Ok(value.map(|v| Cow::Owned(v.value().to_vec())))
            }
            Inner::Consumed => Err(StoreError::TransactionConsumed),
        }
    }

    fn multi_get<'c>(
        &self,
        cf: &'c Self::Cf,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Cow<'c, [u8]>>>, StoreError> {
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(cf);
        match &self.inner {
            Inner::Read(txn) => {
                let table = txn
                    .open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                keys.iter()
                    .map(|key| {
                        let value = table
                            .get(*key)
                            .map_err(|e| StoreError::Storage(e.to_string()))?;
                        Ok(value.map(|v| Cow::Owned(v.value().to_vec())))
                    })
                    .collect()
            }
            Inner::Write(txn) => {
                let table = txn
                    .open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                keys.iter()
                    .map(|key| {
                        let value = table
                            .get(*key)
                            .map_err(|e| StoreError::Storage(e.to_string()))?;
                        Ok(value.map(|v| Cow::Owned(v.value().to_vec())))
                    })
                    .collect()
            }
            Inner::Consumed => Err(StoreError::TransactionConsumed),
        }
    }

    fn scan_prefix<'c>(
        &'c self,
        cf: &'c Self::Cf,
        prefix: &[u8],
    ) -> Result<
        Box<dyn Iterator<Item = Result<(Cow<'c, [u8]>, Cow<'c, [u8]>), StoreError>> + 'c>,
        StoreError,
    > {
        let entries = self.collect_prefix(cf, prefix, false)?;
        Ok(Box::new(
            entries
                .into_iter()
                .map(|(k, v)| Ok((Cow::Owned(k), Cow::Owned(v)))),
        ))
    }

    fn scan_prefix_rev<'c>(
        &'c self,
        cf: &'c Self::Cf,
        prefix: &[u8],
    ) -> Result<
        Box<dyn Iterator<Item = Result<(Cow<'c, [u8]>, Cow<'c, [u8]>), StoreError>> + 'c>,
        StoreError,
    > {
        let entries = self.collect_prefix(cf, prefix, true)?;
        Ok(Box::new(
            entries
                .into_iter()
                .map(|(k, v)| Ok((Cow::Owned(k), Cow::Owned(v)))),
        ))
    }

    fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        let cf = cf.to_string();
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&cf);
        match &self.inner {
            Inner::Write(txn) => {
                let mut table = txn
                    .open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                table
                    .insert(key, value)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                Ok(())
            }
            Inner::Consumed => Err(StoreError::TransactionConsumed),
            _ => unreachable!(),
        }
    }

    fn put_batch(&mut self, cf: &str, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError> {
        self.check_writable()?;
        let cf = cf.to_string();
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&cf);
        match &self.inner {
            Inner::Write(txn) => {
                let mut table = txn
                    .open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                for (key, value) in entries {
                    table
                        .insert(*key, *value)
                        .map_err(|e| StoreError::Storage(e.to_string()))?;
                }
                Ok(())
            }
            Inner::Consumed => Err(StoreError::TransactionConsumed),
            _ => unreachable!(),
        }
    }

    fn delete(&mut self, cf: &str, key: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        let cf = cf.to_string();
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&cf);
        match &self.inner {
            Inner::Write(txn) => {
                let mut table = txn
                    .open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                table
                    .remove(key)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                Ok(())
            }
            Inner::Consumed => Err(StoreError::TransactionConsumed),
            _ => unreachable!(),
        }
    }

    fn create_cf(&mut self, name: &str) -> Result<(), StoreError> {
        self.check_writable()?;
        let name = name.to_string();
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&name);
        match &self.inner {
            Inner::Write(txn) => {
                txn.open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                Ok(())
            }
            Inner::Consumed => Err(StoreError::TransactionConsumed),
            _ => unreachable!(),
        }
    }

    fn commit(mut self) -> Result<(), StoreError> {
        let inner = std::mem::replace(&mut self.inner, Inner::Consumed);
        match inner {
            Inner::Write(txn) => txn.commit().map_err(|e| StoreError::Storage(e.to_string())),
            Inner::Read(_) => Ok(()),
            Inner::Consumed => Err(StoreError::TransactionConsumed),
        }
    }

    fn rollback(mut self) -> Result<(), StoreError> {
        let inner = std::mem::replace(&mut self.inner, Inner::Consumed);
        match inner {
            Inner::Write(txn) => {
                txn.abort()
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                Ok(())
            }
            Inner::Read(_) => Ok(()),
            Inner::Consumed => Err(StoreError::TransactionConsumed),
        }
    }
}

fn collect_from_readable<T: ReadableTable<&'static [u8], &'static [u8]>>(
    table: &T,
    prefix: &[u8],
    upper: &[u8],
    has_upper: bool,
    reverse: bool,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
    let range = if has_upper {
        table.range::<&[u8]>(prefix..upper)
    } else {
        table.range::<&[u8]>(..)
    }
    .map_err(|e| StoreError::Storage(e.to_string()))?;

    let entries: Vec<(Vec<u8>, Vec<u8>)> = if reverse {
        range
            .rev()
            .map(|entry| {
                let (k, v) = entry.unwrap();
                (k.value().to_vec(), v.value().to_vec())
            })
            .collect()
    } else {
        range
            .map(|entry| {
                let (k, v) = entry.unwrap();
                (k.value().to_vec(), v.value().to_vec())
            })
            .collect()
    };

    Ok(entries)
}
