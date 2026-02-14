use rocksdb::{IteratorMode, OptimisticTransactionDB};

use crate::error::StoreError;
use crate::record::{Record, Value};
use crate::store::Transaction;

pub struct RocksTransaction<'db> {
    txn: Option<rocksdb::Transaction<'db, OptimisticTransactionDB>>,
    read_only: bool,
}

impl<'db> RocksTransaction<'db> {
    pub fn new(db: &'db OptimisticTransactionDB, read_only: bool) -> Result<Self, StoreError> {
        let txn = db.transaction();
        Ok(Self {
            txn: Some(txn),
            read_only,
        })
    }

    fn txn(&self) -> Result<&rocksdb::Transaction<'db, OptimisticTransactionDB>, StoreError> {
        self.txn.as_ref().ok_or(StoreError::TransactionConsumed)
    }

    fn check_writable(&self) -> Result<(), StoreError> {
        if self.read_only {
            return Err(StoreError::ReadOnly);
        }
        Ok(())
    }

    fn serialize_record(record: &Record) -> Vec<u8> {
        let mut buf = Vec::new();
        let field_count = record.fields.len() as u32;
        buf.extend_from_slice(&field_count.to_le_bytes());
        for (key, value) in &record.fields {
            let key_bytes = key.as_bytes();
            buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(key_bytes);
            Self::serialize_value(&mut buf, value);
        }
        buf
    }

    fn serialize_value(buf: &mut Vec<u8>, value: &Value) {
        match value {
            Value::String(s) => {
                buf.push(0);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Int(n) => {
                buf.push(1);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Value::Float(f) => {
                buf.push(2);
                buf.extend_from_slice(&f.to_le_bytes());
            }
            Value::Bool(b) => {
                buf.push(3);
                buf.push(if *b { 1 } else { 0 });
            }
            Value::Date(d) => {
                buf.push(4);
                buf.extend_from_slice(&d.to_le_bytes());
            }
            Value::List(items) => {
                buf.push(5);
                buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
                for item in items {
                    Self::serialize_value(buf, item);
                }
            }
            Value::Map(map) => {
                buf.push(6);
                buf.extend_from_slice(&(map.len() as u32).to_le_bytes());
                for (key, value) in map {
                    let key_bytes = key.as_bytes();
                    buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(key_bytes);
                    Self::serialize_value(buf, value);
                }
            }
        }
    }

    fn deserialize_record(id: &str, data: &[u8]) -> Result<Record, StoreError> {
        let mut cursor = 0;
        let field_count = u32::from_le_bytes(data[cursor..cursor + 4].try_into()?) as usize;
        cursor += 4;

        let mut fields = std::collections::HashMap::with_capacity(field_count);
        for _ in 0..field_count {
            let key_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into()?) as usize;
            cursor += 4;
            let key = std::str::from_utf8(&data[cursor..cursor + key_len])?.to_string();
            cursor += key_len;
            let (value, new_cursor) = Self::deserialize_value(data, cursor)?;
            cursor = new_cursor;
            fields.insert(key, value);
        }

        Ok(Record {
            id: id.to_string(),
            fields,
        })
    }

    fn deserialize_value(data: &[u8], mut cursor: usize) -> Result<(Value, usize), StoreError> {
        let tag = data[cursor];
        cursor += 1;
        match tag {
            0 => {
                let len = u32::from_le_bytes(data[cursor..cursor + 4].try_into()?) as usize;
                cursor += 4;
                let s = std::str::from_utf8(&data[cursor..cursor + len])?.to_string();
                cursor += len;
                Ok((Value::String(s), cursor))
            }
            1 => {
                let n = i64::from_le_bytes(data[cursor..cursor + 8].try_into()?);
                cursor += 8;
                Ok((Value::Int(n), cursor))
            }
            2 => {
                let f = f64::from_le_bytes(data[cursor..cursor + 8].try_into()?);
                cursor += 8;
                Ok((Value::Float(f), cursor))
            }
            3 => {
                let b = data[cursor] != 0;
                cursor += 1;
                Ok((Value::Bool(b), cursor))
            }
            4 => {
                let d = i64::from_le_bytes(data[cursor..cursor + 8].try_into()?);
                cursor += 8;
                Ok((Value::Date(d), cursor))
            }
            5 => {
                let len = u32::from_le_bytes(data[cursor..cursor + 4].try_into()?) as usize;
                cursor += 4;
                let mut items = Vec::with_capacity(len);
                for _ in 0..len {
                    let (value, new_cursor) = Self::deserialize_value(data, cursor)?;
                    cursor = new_cursor;
                    items.push(value);
                }
                Ok((Value::List(items), cursor))
            }
            6 => {
                let len = u32::from_le_bytes(data[cursor..cursor + 4].try_into()?) as usize;
                cursor += 4;
                let mut map = std::collections::HashMap::with_capacity(len);
                for _ in 0..len {
                    let key_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into()?) as usize;
                    cursor += 4;
                    let key = std::str::from_utf8(&data[cursor..cursor + key_len])?.to_string();
                    cursor += key_len;
                    let (value, new_cursor) = Self::deserialize_value(data, cursor)?;
                    cursor = new_cursor;
                    map.insert(key, value);
                }
                Ok((Value::Map(map), cursor))
            }
            _ => Err(StoreError::Serialization(format!(
                "unknown value tag: {tag}"
            ))),
        }
    }
}

pub struct RocksIterator {
    records: std::vec::IntoIter<Result<Record, StoreError>>,
}

impl Iterator for RocksIterator {
    type Item = Result<Record, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.records.next()
    }
}

impl<'db> Transaction for RocksTransaction<'db> {
    type Iter = RocksIterator;

    fn get_by_id(&self, id: &str) -> Result<Option<Record>, StoreError> {
        let data = self
            .txn()?
            .get(id.as_bytes())
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        match data {
            Some(data) => Ok(Some(Self::deserialize_record(id, &data)?)),
            None => Ok(None),
        }
    }

    fn scan(&self) -> Result<Self::Iter, StoreError> {
        let iter = self.txn()?.iterator(IteratorMode::Start);
        let mut records = Vec::new();
        for item in iter {
            let (key, value) = item.map_err(|e| StoreError::Storage(e.to_string()))?;
            let id = std::str::from_utf8(&key)?.to_string();
            records.push(Self::deserialize_record(&id, &value));
        }
        Ok(RocksIterator {
            records: records.into_iter(),
        })
    }

    fn insert(&mut self, record: Record) -> Result<(), StoreError> {
        self.check_writable()?;
        let data = Self::serialize_record(&record);
        self.txn()?
            .put(record.id.as_bytes(), &data)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }

    fn delete(&mut self, id: &str) -> Result<(), StoreError> {
        self.check_writable()?;
        self.txn()?
            .delete(id.as_bytes())
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }

    fn commit(mut self) -> Result<(), StoreError> {
        let txn = self.txn.take().ok_or(StoreError::TransactionConsumed)?;
        txn.commit()
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }

    fn rollback(mut self) -> Result<(), StoreError> {
        let txn = self.txn.take().ok_or(StoreError::TransactionConsumed)?;
        txn.rollback()
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }
}
