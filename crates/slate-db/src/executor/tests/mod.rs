use super::*;
use crate::expression::{Expression, LogicalOp};
use crate::planner::plan::{IndexScanRange, Node, Plan, ScanDirection};
use bson::raw::RawDocumentBuf;
use bson::rawdoc;
use slate_engine::{
    Catalog, CollectionHandle, CreateCollectionOptions, EngineError, EngineTransaction,
    FunctionEntry, FunctionKind, IndexEntry, IndexRange,
};
use slate_query::{Sort, SortDirection};
use std::cell::RefCell;

mod mutation;
mod range_scan;
mod read_path;
mod store;
mod upsert;

// ── NoopTransaction ─────────────────────────────────────────
//
// Panics on all store operations. Used for Tier 1 tests where
// no node touches the transaction (pure read-path over Values).

struct NoopTransaction;

impl EngineTransaction for NoopTransaction {
    type Cf = ();

    fn get(
        &self,
        _handle: &CollectionHandle<Self::Cf>,
        _doc_id: &bson::raw::RawBsonRef<'_>,
    ) -> Result<Option<RawDocumentBuf>, EngineError> {
        panic!("NoopTransaction::get called");
    }

    fn put(
        &self,
        _handle: &CollectionHandle<Self::Cf>,
        _doc: &bson::RawDocument,
    ) -> Result<(), EngineError> {
        panic!("NoopTransaction::put called");
    }

    fn put_nx(
        &self,
        _handle: &CollectionHandle<Self::Cf>,
        _doc: &bson::RawDocument,
    ) -> Result<(), EngineError> {
        panic!("NoopTransaction::put_nx called");
    }

    fn delete(
        &self,
        _handle: &CollectionHandle<Self::Cf>,
        _doc_id: &bson::raw::RawBsonRef<'_>,
    ) -> Result<(), EngineError> {
        panic!("NoopTransaction::delete called");
    }

    fn scan<'a>(
        &'a self,
        _handle: &CollectionHandle<Self::Cf>,
    ) -> Result<
        Box<dyn Iterator<Item = Result<RawDocumentBuf, EngineError>> + 'a>,
        EngineError,
    > {
        panic!("NoopTransaction::scan called");
    }

    fn scan_index<'a>(
        &'a self,
        _handle: &CollectionHandle<Self::Cf>,
        _field: &str,
        _range: IndexRange<'_>,
        _reverse: bool,
    ) -> Result<Box<dyn Iterator<Item = Result<IndexEntry, EngineError>> + 'a>, EngineError>
    {
        panic!("NoopTransaction::scan_index called");
    }

    fn purge(&self, _handle: &CollectionHandle<Self::Cf>) -> Result<u64, EngineError> {
        panic!("NoopTransaction::purge called");
    }

    fn purge_before(&self, _handle: &CollectionHandle<Self::Cf>, _as_of_millis: i64) -> Result<u64, EngineError> {
        panic!("NoopTransaction::purge_before called");
    }

    fn commit(self) -> Result<(), EngineError> {
        Ok(())
    }

    fn rollback(self) -> Result<(), EngineError> {
        Ok(())
    }
}

impl Catalog for NoopTransaction {
    fn collection(&self, _: &str, _: &str) -> Result<CollectionHandle<()>, EngineError> {
        panic!("NoopTransaction::collection called");
    }
    fn list_collections(&self, _: Option<&str>) -> Result<Vec<CollectionHandle<()>>, EngineError> {
        panic!("NoopTransaction::list_collections called");
    }
    fn create_collection(&mut self, _: &str, _: &str, _: &CreateCollectionOptions) -> Result<(), EngineError> {
        panic!("NoopTransaction::create_collection called");
    }
    fn drop_collection(&mut self, _: &str, _: &str) -> Result<(), EngineError> {
        panic!("NoopTransaction::drop_collection called");
    }
    fn create_index(&mut self, _: &str, _: &str, _: &str) -> Result<(), EngineError> {
        panic!("NoopTransaction::create_index called");
    }
    fn drop_index(&mut self, _: &str, _: &str, _: &str) -> Result<(), EngineError> {
        panic!("NoopTransaction::drop_index called");
    }
    fn create_function(&mut self, _: &str, _: &str, _: FunctionKind, _: &str, _: u8, _: &[u8]) -> Result<(), EngineError> {
        panic!("NoopTransaction::create_function called");
    }
    fn drop_function(&mut self, _: &str, _: &str, _: FunctionKind, _: &str) -> Result<(), EngineError> {
        panic!("NoopTransaction::drop_function called");
    }
    fn load_functions(&self, _: &str, _: &str, _: FunctionKind) -> Result<Vec<FunctionEntry>, EngineError> {
        Ok(vec![])
    }
}

// ── MockTransaction ─────────────────────────────────────────
//
// Records put and delete calls. Used for Tier 2 mutation tests.
// Since the engine handles encoding/indexing internally, we track
// operations at the doc_id level.

#[derive(Debug)]
struct PutRecord {
    doc: RawDocumentBuf,
}

struct MockTransaction {
    puts: RefCell<Vec<PutRecord>>,
    deletes: RefCell<Vec<String>>,
}

impl MockTransaction {
    fn new() -> Self {
        Self {
            puts: RefCell::new(Vec::new()),
            deletes: RefCell::new(Vec::new()),
        }
    }
}

impl EngineTransaction for MockTransaction {
    type Cf = ();

    fn get(
        &self,
        _handle: &CollectionHandle<Self::Cf>,
        _doc_id: &bson::raw::RawBsonRef<'_>,
    ) -> Result<Option<RawDocumentBuf>, EngineError> {
        Ok(None)
    }

    fn put(
        &self,
        _handle: &CollectionHandle<Self::Cf>,
        doc: &bson::RawDocument,
    ) -> Result<(), EngineError> {
        self.puts.borrow_mut().push(PutRecord {
            doc: doc.to_owned(),
        });
        Ok(())
    }

    fn put_nx(
        &self,
        _handle: &CollectionHandle<Self::Cf>,
        doc: &bson::RawDocument,
    ) -> Result<(), EngineError> {
        self.puts.borrow_mut().push(PutRecord {
            doc: doc.to_owned(),
        });
        Ok(())
    }

    fn delete(
        &self,
        _handle: &CollectionHandle<Self::Cf>,
        doc_id: &bson::raw::RawBsonRef<'_>,
    ) -> Result<(), EngineError> {
        let id_str = format!("{:?}", doc_id);
        self.deletes.borrow_mut().push(id_str);
        Ok(())
    }

    fn scan<'a>(
        &'a self,
        _handle: &CollectionHandle<Self::Cf>,
    ) -> Result<
        Box<dyn Iterator<Item = Result<RawDocumentBuf, EngineError>> + 'a>,
        EngineError,
    > {
        Ok(Box::new(std::iter::empty()))
    }

    fn scan_index<'a>(
        &'a self,
        _handle: &CollectionHandle<Self::Cf>,
        _field: &str,
        _range: IndexRange<'_>,
        _reverse: bool,
    ) -> Result<Box<dyn Iterator<Item = Result<IndexEntry, EngineError>> + 'a>, EngineError>
    {
        Ok(Box::new(std::iter::empty()))
    }

    fn purge(&self, _handle: &CollectionHandle<Self::Cf>) -> Result<u64, EngineError> {
        Ok(0)
    }

    fn purge_before(&self, _handle: &CollectionHandle<Self::Cf>, _as_of_millis: i64) -> Result<u64, EngineError> {
        Ok(0)
    }

    fn commit(self) -> Result<(), EngineError> {
        Ok(())
    }

    fn rollback(self) -> Result<(), EngineError> {
        Ok(())
    }
}

impl Catalog for MockTransaction {
    fn collection(&self, _: &str, _: &str) -> Result<CollectionHandle<()>, EngineError> {
        panic!("MockTransaction::collection called");
    }
    fn list_collections(&self, _: Option<&str>) -> Result<Vec<CollectionHandle<()>>, EngineError> {
        panic!("MockTransaction::list_collections called");
    }
    fn create_collection(&mut self, _: &str, _: &str, _: &CreateCollectionOptions) -> Result<(), EngineError> {
        panic!("MockTransaction::create_collection called");
    }
    fn drop_collection(&mut self, _: &str, _: &str) -> Result<(), EngineError> {
        panic!("MockTransaction::drop_collection called");
    }
    fn create_index(&mut self, _: &str, _: &str, _: &str) -> Result<(), EngineError> {
        panic!("MockTransaction::create_index called");
    }
    fn drop_index(&mut self, _: &str, _: &str, _: &str) -> Result<(), EngineError> {
        panic!("MockTransaction::drop_index called");
    }
    fn create_function(&mut self, _: &str, _: &str, _: FunctionKind, _: &str, _: u8, _: &[u8]) -> Result<(), EngineError> {
        panic!("MockTransaction::create_function called");
    }
    fn drop_function(&mut self, _: &str, _: &str, _: FunctionKind, _: &str) -> Result<(), EngineError> {
        panic!("MockTransaction::drop_function called");
    }
    fn load_functions(&self, _: &str, _: &str, _: FunctionKind) -> Result<Vec<FunctionEntry>, EngineError> {
        Ok(vec![])
    }
}

// ── Helpers ─────────────────────────────────────────────────

/// Collect bare string IDs from an IndexScan/IndexMerge result.
fn collect_ids(iter: RawIter) -> Vec<String> {
    iter.map(|r| {
        let opt_val = r.unwrap();
        match opt_val.unwrap() {
            bson::RawBson::String(s) => s,
            other => panic!("expected String, got {:?}", other),
        }
    })
    .collect()
}

fn collect_docs(iter: RawIter) -> Vec<Option<bson::Document>> {
    iter.map(|r| {
        let opt_val = r.unwrap();
        opt_val.and_then(|v| match v {
            bson::RawBson::Document(raw) => {
                Some(bson::deserialize_from_slice::<bson::Document>(raw.as_bytes()).unwrap())
            }
            _ => None,
        })
    })
    .collect()
}

fn mock_collection(indexes: Vec<String>) -> CollectionHandle<()> {
    CollectionHandle::new(
        "test".to_string(),
        "default_cf".to_string(),
        (),
        indexes,
        "_id".to_string(),
        "ttl".to_string(),
    )
}
