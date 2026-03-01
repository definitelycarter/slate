use redb::{Database, ReadableTable, TableDefinition};

use crate::error::StoreError;
use crate::store::{increment_prefix, Transaction};

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

    /// Eagerly collect prefix entries for write transactions (where lazy iteration
    /// isn't possible due to table handle lifetime constraints).
    fn collect_prefix_write(
        txn: &redb::WriteTransaction,
        cf: &str,
        prefix: &[u8],
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let cf = cf.to_string();
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&cf);
        let upper = increment_prefix(prefix);

        let table = txn
            .open_table(def)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        collect_from_readable(&table, prefix, upper.as_deref(), reverse)
    }
}

impl<'db> Transaction for RedbTransaction<'db> {
    type Cf = String;

    fn cf(&self, name: &str) -> Result<Self::Cf, StoreError> {
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

    fn get(&self, cf: &Self::Cf, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(cf);
        match &self.inner {
            Inner::Read(txn) => {
                let table = txn
                    .open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                let value = table
                    .get(key)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                Ok(value.map(|v| v.value().to_vec()))
            }
            Inner::Write(txn) => {
                let table = txn
                    .open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                let value = table
                    .get(key)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                Ok(value.map(|v| v.value().to_vec()))
            }
            Inner::Consumed => Err(StoreError::TransactionConsumed),
        }
    }

    fn multi_get(&self, cf: &Self::Cf, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, StoreError> {
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
                        Ok(value.map(|v| v.value().to_vec()))
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
                        Ok(value.map(|v| v.value().to_vec()))
                    })
                    .collect()
            }
            Inner::Consumed => Err(StoreError::TransactionConsumed),
        }
    }

    fn scan_prefix<'a>(
        &'a self,
        cf: &Self::Cf,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'a>, StoreError>
    {
        match &self.inner {
            Inner::Read(txn) => {
                // ReadOnlyTable::range() returns Range<'static> — owned, ref-counted.
                let cf_str = cf.to_string();
                let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&cf_str);
                let table = txn
                    .open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                let range = table
                    .range::<&[u8]>(prefix..)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                let prefix_owned = prefix.to_vec();
                Ok(Box::new(
                    range
                        .take_while(move |entry| match entry {
                            Ok((k, _)) => k.value().starts_with(&prefix_owned),
                            Err(_) => true,
                        })
                        .map(|entry| {
                            let (k, v) =
                                entry.map_err(|e| StoreError::Storage(e.to_string()))?;
                            Ok((k.value().to_vec(), v.value().to_vec()))
                        }),
                ))
            }
            Inner::Write(txn) => {
                let entries = Self::collect_prefix_write(txn, cf, prefix, false)?;
                Ok(Box::new(entries.into_iter().map(Ok)))
            }
            Inner::Consumed => Err(StoreError::TransactionConsumed),
        }
    }

    fn scan_prefix_rev<'a>(
        &'a self,
        cf: &Self::Cf,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'a>, StoreError>
    {
        match &self.inner {
            Inner::Read(txn) => {
                let cf_str = cf.to_string();
                let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&cf_str);
                let table = txn
                    .open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                let upper = increment_prefix(prefix);
                let range = if let Some(ref upper) = upper {
                    table.range::<&[u8]>(prefix..upper.as_slice())
                } else {
                    table.range::<&[u8]>(prefix..)
                }
                .map_err(|e| StoreError::Storage(e.to_string()))?;
                Ok(Box::new(range.rev().map(|entry| {
                    let (k, v) = entry.map_err(|e| StoreError::Storage(e.to_string()))?;
                    Ok((k.value().to_vec(), v.value().to_vec()))
                })))
            }
            Inner::Write(txn) => {
                let entries = Self::collect_prefix_write(txn, cf, prefix, true)?;
                Ok(Box::new(entries.into_iter().map(Ok)))
            }
            Inner::Consumed => Err(StoreError::TransactionConsumed),
        }
    }

    fn put(&self, cf: &Self::Cf, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(cf);
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

    fn put_batch(&self, cf: &Self::Cf, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError> {
        self.check_writable()?;
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(cf);
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

    fn delete(&self, cf: &Self::Cf, key: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(cf);
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

    fn delete_batch(&self, cf: &Self::Cf, keys: &[&[u8]]) -> Result<(), StoreError> {
        self.check_writable()?;
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(cf);
        match &self.inner {
            Inner::Write(txn) => {
                let mut table = txn
                    .open_table(def)
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
                for key in keys {
                    table
                        .remove(*key)
                        .map_err(|e| StoreError::Storage(e.to_string()))?;
                }
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

    fn drop_cf(&mut self, name: &str) -> Result<(), StoreError> {
        self.check_writable()?;
        let name = name.to_string();
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&name);
        match &self.inner {
            Inner::Write(txn) => {
                txn.delete_table(def)
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
    upper: Option<&[u8]>,
    reverse: bool,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
    let range = if let Some(upper) = upper {
        table.range::<&[u8]>(prefix..upper)
    } else {
        table.range::<&[u8]>(prefix..)
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
