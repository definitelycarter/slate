use super::*;
use crate::encoding;
use bson::raw::RawBsonRef;
use bson::rawdoc;
use slate_query::{Filter, FilterGroup, FilterNode, LogicalOp, Operator, Sort, SortDirection};
use slate_store::{Store, StoreError, Transaction};
use std::borrow::Cow;
use std::cell::RefCell;

// ── NoopTransaction ─────────────────────────────────────────
//
// Panics on all store operations. Used for Tier 1 tests where
// no node touches the transaction (pure read-path over Values).

struct NoopTransaction;

impl Transaction for NoopTransaction {
    type Cf = ();

    fn cf(&mut self, _name: &str) -> Result<Self::Cf, StoreError> {
        panic!("NoopTransaction::cf called");
    }
    fn get<'c>(&self, _cf: &'c Self::Cf, _key: &[u8]) -> Result<Option<Cow<'c, [u8]>>, StoreError> {
        panic!("NoopTransaction::get called");
    }
    fn multi_get<'c>(
        &self,
        _cf: &'c Self::Cf,
        _keys: &[&[u8]],
    ) -> Result<Vec<Option<Cow<'c, [u8]>>>, StoreError> {
        panic!("NoopTransaction::multi_get called");
    }
    fn scan_prefix<'c>(
        &'c self,
        _cf: &'c Self::Cf,
        _prefix: &[u8],
    ) -> Result<
        Box<dyn Iterator<Item = Result<(Cow<'c, [u8]>, Cow<'c, [u8]>), StoreError>> + 'c>,
        StoreError,
    > {
        panic!("NoopTransaction::scan_prefix called");
    }
    fn scan_prefix_rev<'c>(
        &'c self,
        _cf: &'c Self::Cf,
        _prefix: &[u8],
    ) -> Result<
        Box<dyn Iterator<Item = Result<(Cow<'c, [u8]>, Cow<'c, [u8]>), StoreError>> + 'c>,
        StoreError,
    > {
        panic!("NoopTransaction::scan_prefix_rev called");
    }
    fn put(&self, _cf: &Self::Cf, _key: &[u8], _value: &[u8]) -> Result<(), StoreError> {
        panic!("NoopTransaction::put called");
    }
    fn put_batch(&self, _cf: &Self::Cf, _entries: &[(&[u8], &[u8])]) -> Result<(), StoreError> {
        panic!("NoopTransaction::put_batch called");
    }
    fn delete(&self, _cf: &Self::Cf, _key: &[u8]) -> Result<(), StoreError> {
        panic!("NoopTransaction::delete called");
    }
    fn create_cf(&mut self, _name: &str) -> Result<(), StoreError> {
        panic!("NoopTransaction::create_cf called");
    }
    fn drop_cf(&mut self, _name: &str) -> Result<(), StoreError> {
        panic!("NoopTransaction::drop_cf called");
    }
    fn commit(self) -> Result<(), StoreError> {
        Ok(())
    }
    fn rollback(self) -> Result<(), StoreError> {
        Ok(())
    }
}

// ── MockTransaction ─────────────────────────────────────────
//
// Records put and delete calls. Used for Tier 2 mutation tests.

struct MockTransaction {
    puts: RefCell<Vec<(Vec<u8>, Vec<u8>)>>,
    deletes: RefCell<Vec<Vec<u8>>>,
}

impl MockTransaction {
    fn new() -> Self {
        Self {
            puts: RefCell::new(Vec::new()),
            deletes: RefCell::new(Vec::new()),
        }
    }
}

impl Transaction for MockTransaction {
    type Cf = ();

    fn cf(&mut self, _name: &str) -> Result<Self::Cf, StoreError> {
        Ok(())
    }
    fn get<'c>(&self, _cf: &'c Self::Cf, _key: &[u8]) -> Result<Option<Cow<'c, [u8]>>, StoreError> {
        Ok(None)
    }
    fn multi_get<'c>(
        &self,
        _cf: &'c Self::Cf,
        _keys: &[&[u8]],
    ) -> Result<Vec<Option<Cow<'c, [u8]>>>, StoreError> {
        Ok(vec![None; _keys.len()])
    }
    fn scan_prefix<'c>(
        &'c self,
        _cf: &'c Self::Cf,
        _prefix: &[u8],
    ) -> Result<
        Box<dyn Iterator<Item = Result<(Cow<'c, [u8]>, Cow<'c, [u8]>), StoreError>> + 'c>,
        StoreError,
    > {
        Ok(Box::new(std::iter::empty()))
    }
    fn scan_prefix_rev<'c>(
        &'c self,
        _cf: &'c Self::Cf,
        _prefix: &[u8],
    ) -> Result<
        Box<dyn Iterator<Item = Result<(Cow<'c, [u8]>, Cow<'c, [u8]>), StoreError>> + 'c>,
        StoreError,
    > {
        Ok(Box::new(std::iter::empty()))
    }
    fn put(&self, _cf: &Self::Cf, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.puts.borrow_mut().push((key.to_vec(), value.to_vec()));
        Ok(())
    }
    fn put_batch(&self, _cf: &Self::Cf, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError> {
        for (k, v) in entries {
            self.puts.borrow_mut().push((k.to_vec(), v.to_vec()));
        }
        Ok(())
    }
    fn delete(&self, _cf: &Self::Cf, key: &[u8]) -> Result<(), StoreError> {
        self.deletes.borrow_mut().push(key.to_vec());
        Ok(())
    }
    fn create_cf(&mut self, _name: &str) -> Result<(), StoreError> {
        Ok(())
    }
    fn drop_cf(&mut self, _name: &str) -> Result<(), StoreError> {
        Ok(())
    }
    fn commit(self) -> Result<(), StoreError> {
        Ok(())
    }
    fn rollback(self) -> Result<(), StoreError> {
        Ok(())
    }
}

// ── Helpers ─────────────────────────────────────────────────

/// Collect bare string IDs from an IndexScan/IndexMerge result.
fn collect_ids(result: ExecutionResult) -> Vec<String> {
    match result {
        ExecutionResult::Rows(iter) => iter
            .map(|r| {
                let opt_val = r.unwrap();
                match opt_val.unwrap().into_raw_bson().unwrap() {
                    bson::RawBson::String(s) => s,
                    other => panic!("expected String, got {:?}", other),
                }
            })
            .collect(),
        _ => panic!("expected Rows"),
    }
}

fn collect_docs(result: ExecutionResult) -> Vec<Option<bson::Document>> {
    match result {
        ExecutionResult::Rows(iter) => iter
            .map(|r| {
                let opt_val = r.unwrap();
                opt_val.and_then(|v| {
                    v.into_document_buf()
                        .map(|raw| bson::from_slice::<bson::Document>(raw.as_bytes()).unwrap())
                })
            })
            .collect(),
        _ => panic!("expected Rows"),
    }
}

fn noop_executor() -> (NoopTransaction, ()) {
    (NoopTransaction, ())
}

fn mock_executor() -> (MockTransaction, ()) {
    (MockTransaction::new(), ())
}

// ── Tier 1: Pure read-path tests (NoopTransaction) ──────────

#[test]
fn values_yields_all_docs() {
    let (txn, cf) = noop_executor();
    let plan = PlanNode::Values {
        docs: vec![
            rawdoc! { "_id": "1", "name": "Alice", "status": "active" },
            rawdoc! { "_id": "2", "name": "Bob", "status": "inactive" },
            rawdoc! { "_id": "3", "name": "Charlie", "status": "active" },
        ],
    };
    let rows = collect_docs(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].as_ref().unwrap().get_str("_id").unwrap(), "1");
    assert_eq!(rows[1].as_ref().unwrap().get_str("_id").unwrap(), "2");
    assert_eq!(rows[2].as_ref().unwrap().get_str("_id").unwrap(), "3");
    assert_eq!(rows[0].as_ref().unwrap().get_str("name").unwrap(), "Alice");
    assert_eq!(rows[1].as_ref().unwrap().get_str("name").unwrap(), "Bob");
    assert_eq!(
        rows[2].as_ref().unwrap().get_str("name").unwrap(),
        "Charlie"
    );
}

#[test]
fn filter_on_values() {
    let (txn, cf) = noop_executor();
    let plan = PlanNode::Filter {
        predicate: FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "status".into(),
                operator: Operator::Eq,
                value: bson::Bson::String("active".into()),
            })],
        },
        input: Box::new(PlanNode::Values {
            docs: vec![
                rawdoc! { "_id": "1", "name": "Alice", "status": "active" },
                rawdoc! { "_id": "2", "name": "Bob", "status": "inactive" },
                rawdoc! { "_id": "3", "name": "Charlie", "status": "active" },
            ],
        }),
    };
    let rows = collect_docs(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].as_ref().unwrap().get_str("_id").unwrap(), "1");
    assert_eq!(rows[1].as_ref().unwrap().get_str("_id").unwrap(), "3");
}

#[test]
fn filter_empty_result() {
    let (txn, cf) = noop_executor();
    let plan = PlanNode::Filter {
        predicate: FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "status".into(),
                operator: Operator::Eq,
                value: bson::Bson::String("nope".into()),
            })],
        },
        input: Box::new(PlanNode::Values {
            docs: vec![
                rawdoc! { "_id": "1", "name": "Alice", "status": "active" },
                rawdoc! { "_id": "2", "name": "Bob", "status": "inactive" },
            ],
        }),
    };
    let rows = collect_docs(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(rows.len(), 0);
}

#[test]
fn sort_on_values() {
    let (txn, cf) = noop_executor();
    let plan = PlanNode::Sort {
        sorts: vec![Sort {
            field: "score".into(),
            direction: SortDirection::Desc,
        }],
        input: Box::new(PlanNode::Values {
            docs: vec![
                rawdoc! { "_id": "1", "name": "Alice", "score": 70 },
                rawdoc! { "_id": "2", "name": "Bob", "score": 90 },
                rawdoc! { "_id": "3", "name": "Charlie", "score": 80 },
            ],
        }),
    };
    let rows = collect_docs(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].as_ref().unwrap().get_str("_id").unwrap(), "2"); // Bob: 90
    assert_eq!(rows[1].as_ref().unwrap().get_str("_id").unwrap(), "3"); // Charlie: 80
    assert_eq!(rows[2].as_ref().unwrap().get_str("_id").unwrap(), "1"); // Alice: 70
}

#[test]
fn limit_skip_take() {
    let (txn, cf) = noop_executor();
    let plan = PlanNode::Limit {
        skip: 1,
        take: Some(2),
        input: Box::new(PlanNode::Values {
            docs: vec![
                rawdoc! { "_id": "1", "name": "Alice" },
                rawdoc! { "_id": "2", "name": "Bob" },
                rawdoc! { "_id": "3", "name": "Charlie" },
                rawdoc! { "_id": "4", "name": "Diana" },
            ],
        }),
    };
    let rows = collect_docs(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].as_ref().unwrap().get_str("_id").unwrap(), "2");
    assert_eq!(rows[1].as_ref().unwrap().get_str("_id").unwrap(), "3");
}

#[test]
fn limit_take_only() {
    let (txn, cf) = noop_executor();
    let plan = PlanNode::Limit {
        skip: 0,
        take: Some(1),
        input: Box::new(PlanNode::Values {
            docs: vec![
                rawdoc! { "_id": "1", "name": "Alice" },
                rawdoc! { "_id": "2", "name": "Bob" },
            ],
        }),
    };
    let rows = collect_docs(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].as_ref().unwrap().get_str("_id").unwrap(), "1");
}

#[test]
fn projection_on_values() {
    let (txn, cf) = noop_executor();
    let plan = PlanNode::Projection {
        columns: Some(vec!["name".into()]),
        input: Box::new(PlanNode::Values {
            docs: vec![
                rawdoc! { "_id": "1", "name": "Alice", "status": "active", "score": 70 },
                rawdoc! { "_id": "2", "name": "Bob", "status": "inactive", "score": 90 },
            ],
        }),
    };
    let rows = collect_docs(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(rows.len(), 2);
    let doc0 = rows[0].as_ref().unwrap();
    assert_eq!(doc0.get_str("_id").unwrap(), "1");
    assert_eq!(doc0.get_str("name").unwrap(), "Alice");
    assert!(doc0.get("status").is_none());
    assert!(doc0.get("score").is_none());
}

#[test]
fn distinct_on_values() {
    let (txn, cf) = noop_executor();
    let plan = PlanNode::Distinct {
        field: "status".into(),
        input: Box::new(PlanNode::Projection {
            columns: Some(vec!["status".into()]),
            input: Box::new(PlanNode::Values {
                docs: vec![
                    rawdoc! { "_id": "1", "status": "active" },
                    rawdoc! { "_id": "2", "status": "inactive" },
                    rawdoc! { "_id": "3", "status": "active" },
                    rawdoc! { "_id": "4", "status": "inactive" },
                ],
            }),
        }),
    };
    let result = Executor::new(&txn, &cf).execute(&plan).unwrap();
    match result {
        ExecutionResult::Rows(mut iter) => {
            let val = iter.next().unwrap().unwrap();
            let raw_bson = val.unwrap().into_raw_bson().unwrap();
            let arr = match raw_bson {
                RawBson::Array(a) => a,
                _ => panic!("expected array"),
            };
            let values: Vec<String> = arr
                .into_iter()
                .map(|v| match v.unwrap() {
                    RawBsonRef::String(s) => s.to_string(),
                    _ => panic!("expected string"),
                })
                .collect();
            assert_eq!(values.len(), 2);
            assert!(values.contains(&"active".to_string()));
            assert!(values.contains(&"inactive".to_string()));
        }
        _ => panic!("expected Rows"),
    }
}

#[test]
fn composed_filter_sort_limit() {
    let (txn, cf) = noop_executor();
    let plan = PlanNode::Limit {
        skip: 0,
        take: Some(2),
        input: Box::new(PlanNode::Sort {
            sorts: vec![Sort {
                field: "score".into(),
                direction: SortDirection::Desc,
            }],
            input: Box::new(PlanNode::Filter {
                predicate: FilterGroup {
                    logical: LogicalOp::And,
                    children: vec![FilterNode::Condition(Filter {
                        field: "status".into(),
                        operator: Operator::Eq,
                        value: bson::Bson::String("active".into()),
                    })],
                },
                input: Box::new(PlanNode::Values {
                    docs: vec![
                        rawdoc! { "_id": "1", "name": "Alice", "status": "active", "score": 70 },
                        rawdoc! { "_id": "2", "name": "Bob", "status": "inactive", "score": 95 },
                        rawdoc! { "_id": "3", "name": "Charlie", "status": "active", "score": 90 },
                        rawdoc! { "_id": "4", "name": "Diana", "status": "active", "score": 80 },
                    ],
                }),
            }),
        }),
    };
    let rows = collect_docs(Executor::new(&txn, &cf).execute(&plan).unwrap());
    // Filter: removes Bob (inactive) → Alice(70), Charlie(90), Diana(80)
    // Sort desc by score: Charlie(90), Diana(80), Alice(70)
    // Limit take=2: Charlie, Diana
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].as_ref().unwrap().get_str("_id").unwrap(), "3"); // Charlie: 90
    assert_eq!(rows[1].as_ref().unwrap().get_str("_id").unwrap(), "4"); // Diana: 80
}

// ── Tier 2: Mutation tests (MockTransaction) ────────────────

#[test]
fn delete_removes_records() {
    let (txn, cf) = mock_executor();
    let plan = PlanNode::Delete {
        input: Box::new(PlanNode::Values {
            docs: vec![
                rawdoc! { "_id": "1", "name": "Alice" },
                rawdoc! { "_id": "2", "name": "Bob" },
            ],
        }),
    };
    let result = Executor::new(&txn, &cf).execute(&plan).unwrap();
    match result {
        ExecutionResult::Delete { deleted } => assert_eq!(deleted, 2),
        _ => panic!("expected Delete"),
    }
    let deletes = txn.deletes.borrow();
    assert_eq!(deletes.len(), 2);
    assert_eq!(deletes[0], encoding::record_key("1"));
    assert_eq!(deletes[1], encoding::record_key("2"));
}

#[test]
fn delete_index_removes_index_keys() {
    let (txn, cf) = mock_executor();
    let plan = PlanNode::DeleteIndex {
        indexed_fields: vec!["status".into()],
        input: Box::new(PlanNode::Values {
            docs: vec![
                rawdoc! { "_id": "1", "status": "active" },
                rawdoc! { "_id": "2", "status": "inactive" },
            ],
        }),
    };
    let result = Executor::new(&txn, &cf).execute(&plan).unwrap();
    // DeleteIndex passes through as Rows
    let rows = collect_docs(result);
    assert_eq!(rows.len(), 2);

    let deletes = txn.deletes.borrow();
    assert_eq!(deletes.len(), 2);
    assert_eq!(
        deletes[0],
        encoding::raw_index_key("status", RawBsonRef::String("active"), "1")
    );
    assert_eq!(
        deletes[1],
        encoding::raw_index_key("status", RawBsonRef::String("inactive"), "2")
    );
}

#[test]
fn delete_index_with_ttl() {
    let (txn, cf) = mock_executor();
    let dt = bson::DateTime::from_millis(1700000000000);
    let plan = PlanNode::DeleteIndex {
        indexed_fields: vec!["status".into()],
        input: Box::new(PlanNode::Values {
            docs: vec![rawdoc! { "_id": "1", "status": "active", "ttl": dt }],
        }),
    };
    let rows = collect_docs(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(rows.len(), 1);

    let deletes = txn.deletes.borrow();
    // Should delete both the status index key and the ttl index key
    assert_eq!(deletes.len(), 2);
    assert_eq!(
        deletes[0],
        encoding::raw_index_key("status", RawBsonRef::String("active"), "1")
    );
    assert_eq!(
        deletes[1],
        encoding::raw_index_key("ttl", RawBsonRef::DateTime(dt), "1")
    );
}

#[test]
fn update_writes_merged_doc() {
    let (txn, cf) = mock_executor();
    // Wrap Update in InsertIndex so execute() returns Update variant
    let plan = PlanNode::InsertIndex {
        indexed_fields: vec![],
        input: Box::new(PlanNode::Update {
            update: bson::doc! { "score": 100 },
            input: Box::new(PlanNode::Values {
                docs: vec![rawdoc! { "_id": "1", "name": "Alice", "score": 70 }],
            }),
        }),
    };
    let result = Executor::new(&txn, &cf).execute(&plan).unwrap();
    match result {
        ExecutionResult::Update { matched, modified } => {
            assert_eq!(matched, 1);
            assert_eq!(modified, 1);
        }
        _ => panic!("expected Update"),
    }

    let puts = txn.puts.borrow();
    assert_eq!(puts.len(), 1);
    assert_eq!(puts[0].0, encoding::record_key("1"));
    // Verify the written bytes decode to the merged doc
    let written = bson::RawDocument::from_bytes(&puts[0].1).unwrap();
    assert_eq!(written.get_str("name").unwrap(), "Alice");
    assert_eq!(written.get_i32("score").unwrap(), 100);
}

#[test]
fn update_unchanged_skips_write() {
    let (txn, cf) = mock_executor();
    let plan = PlanNode::InsertIndex {
        indexed_fields: vec![],
        input: Box::new(PlanNode::Update {
            update: bson::doc! { "status": "active" },
            input: Box::new(PlanNode::Values {
                docs: vec![rawdoc! { "_id": "1", "name": "Alice", "status": "active" }],
            }),
        }),
    };
    let result = Executor::new(&txn, &cf).execute(&plan).unwrap();
    match result {
        ExecutionResult::Update { matched, modified } => {
            assert_eq!(matched, 1);
            assert_eq!(modified, 0); // unchanged → not modified
        }
        _ => panic!("expected Update"),
    }
    assert!(txn.puts.borrow().is_empty());
}

#[test]
fn replace_writes_replacement() {
    let (txn, cf) = mock_executor();
    let plan = PlanNode::InsertIndex {
        indexed_fields: vec![],
        input: Box::new(PlanNode::Replace {
            replacement: bson::doc! { "replaced": true },
            input: Box::new(PlanNode::Values {
                docs: vec![rawdoc! { "_id": "1", "name": "Alice", "status": "active" }],
            }),
        }),
    };
    let result = Executor::new(&txn, &cf).execute(&plan).unwrap();
    match result {
        ExecutionResult::Update { matched, modified } => {
            assert_eq!(matched, 1);
            assert_eq!(modified, 1);
        }
        _ => panic!("expected Update"),
    }
    let puts = txn.puts.borrow();
    assert_eq!(puts.len(), 1);
    assert_eq!(puts[0].0, encoding::record_key("1"));
    let written: bson::Document = bson::from_slice(&puts[0].1).unwrap();
    assert_eq!(written.get_bool("replaced").unwrap(), true);
}

#[test]
fn insert_index_writes_keys() {
    let (txn, cf) = mock_executor();
    let plan = PlanNode::InsertIndex {
        indexed_fields: vec!["status".into()],
        input: Box::new(PlanNode::Values {
            docs: vec![
                rawdoc! { "_id": "1", "status": "active" },
                rawdoc! { "_id": "2", "status": "inactive" },
            ],
        }),
    };
    let result = Executor::new(&txn, &cf).execute(&plan).unwrap();
    match result {
        ExecutionResult::Update { matched, modified } => {
            assert_eq!(matched, 2);
            assert_eq!(modified, 2);
        }
        _ => panic!("expected Update"),
    }
    let puts = txn.puts.borrow();
    assert_eq!(puts.len(), 2);
    assert_eq!(
        puts[0].0,
        encoding::raw_index_key("status", RawBsonRef::String("active"), "1")
    );
    assert_eq!(
        puts[1].0,
        encoding::raw_index_key("status", RawBsonRef::String("inactive"), "2")
    );
}

#[test]
fn full_delete_pipeline() {
    let (txn, cf) = mock_executor();
    let plan = PlanNode::Delete {
        input: Box::new(PlanNode::DeleteIndex {
            indexed_fields: vec!["status".into()],
            input: Box::new(PlanNode::Values {
                docs: vec![
                    rawdoc! { "_id": "1", "status": "active" },
                    rawdoc! { "_id": "2", "status": "inactive" },
                ],
            }),
        }),
    };
    let result = Executor::new(&txn, &cf).execute(&plan).unwrap();
    match result {
        ExecutionResult::Delete { deleted } => assert_eq!(deleted, 2),
        _ => panic!("expected Delete"),
    }
    let deletes = txn.deletes.borrow();
    // 2 index deletes + 2 record deletes = 4
    assert_eq!(deletes.len(), 4);
    // Streaming: for each doc, DeleteIndex fires then Delete fires
    // Doc 1: index delete, then record delete
    assert_eq!(
        deletes[0],
        encoding::raw_index_key("status", RawBsonRef::String("active"), "1")
    );
    assert_eq!(deletes[1], encoding::record_key("1"));
    // Doc 2: index delete, then record delete
    assert_eq!(
        deletes[2],
        encoding::raw_index_key("status", RawBsonRef::String("inactive"), "2")
    );
    assert_eq!(deletes[3], encoding::record_key("2"));
}

#[test]
fn full_update_pipeline() {
    let (txn, cf) = mock_executor();
    // InsertIndex → Update → DeleteIndex → Values
    let plan = PlanNode::InsertIndex {
        indexed_fields: vec!["status".into()],
        input: Box::new(PlanNode::Update {
            update: bson::doc! { "status": "archived" },
            input: Box::new(PlanNode::DeleteIndex {
                indexed_fields: vec!["status".into()],
                input: Box::new(PlanNode::Values {
                    docs: vec![rawdoc! { "_id": "1", "status": "active" }],
                }),
            }),
        }),
    };
    let result = Executor::new(&txn, &cf).execute(&plan).unwrap();
    match result {
        ExecutionResult::Update { matched, modified } => {
            assert_eq!(matched, 1);
            assert_eq!(modified, 1);
        }
        _ => panic!("expected Update"),
    }

    let deletes = txn.deletes.borrow();
    // Old index key deleted
    assert_eq!(deletes.len(), 1);
    assert_eq!(
        deletes[0],
        encoding::raw_index_key("status", RawBsonRef::String("active"), "1")
    );

    let puts = txn.puts.borrow();
    // Update writes the merged doc + InsertIndex writes the new index key
    assert_eq!(puts.len(), 2);
    // First put: the merged document
    assert_eq!(puts[0].0, encoding::record_key("1"));
    let written = bson::RawDocument::from_bytes(&puts[0].1).unwrap();
    assert_eq!(written.get_str("status").unwrap(), "archived");
    // Second put: new index entry
    assert_eq!(
        puts[1].0,
        encoding::raw_index_key("status", RawBsonRef::String("archived"), "1")
    );
}

// ── Tier 3: Store-backed tests (MemoryStore) ────────────────
//
// These test the 4 arms that require a real store: Scan, IndexScan,
// IndexMerge, and ReadRecord. Uses Database<MemoryStore> to seed data
// through the normal API — no encoding knowledge needed.

use crate::collection::CollectionConfig;
use crate::database::{Database, DatabaseConfig};
use slate_store::MemoryStore;

fn seeded_db() -> Database<MemoryStore> {
    let db = Database::open(MemoryStore::new(), DatabaseConfig::default());
    let mut txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: "test".into(),
        indexes: vec!["status".into(), "score".into()],
    })
    .unwrap();
    txn.insert_many(
        "test",
        vec![
            bson::doc! { "_id": "1", "name": "Alice", "status": "active", "score": 70 },
            bson::doc! { "_id": "2", "name": "Bob", "status": "inactive", "score": 90 },
            bson::doc! { "_id": "3", "name": "Charlie", "status": "active", "score": 80 },
        ],
    )
    .unwrap();
    txn.commit().unwrap();
    db
}

#[test]
fn scan_yields_all_records() {
    let db = seeded_db();
    let mut txn = db.store().begin(true).unwrap();
    let cf = txn.cf("test").unwrap();

    let plan = PlanNode::Scan {
        collection: "test".into(),
    };
    let rows = collect_docs(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(rows.len(), 3);
    // Scan yields in key order (record_key is "d:{id}")
    assert_eq!(rows[0].as_ref().unwrap().get_str("_id").unwrap(), "1");
    assert_eq!(rows[1].as_ref().unwrap().get_str("_id").unwrap(), "2");
    assert_eq!(rows[2].as_ref().unwrap().get_str("_id").unwrap(), "3");
    assert_eq!(rows[0].as_ref().unwrap().get_str("name").unwrap(), "Alice");
}

#[test]
fn index_scan_eq_filter() {
    let db = seeded_db();
    let mut txn = db.store().begin(true).unwrap();
    let cf = txn.cf("test").unwrap();

    let plan = PlanNode::IndexScan {
        collection: "test".into(),
        column: "status".into(),
        value: Some(bson::Bson::String("active".into())),
        direction: SortDirection::Asc,
        limit: None,
        complete_groups: false,
        covered: false,
    };
    let ids = collect_ids(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&"1".to_string())); // Alice
    assert!(ids.contains(&"3".to_string())); // Charlie
}

#[test]
fn index_scan_full_column() {
    let db = seeded_db();
    let mut txn = db.store().begin(true).unwrap();
    let cf = txn.cf("test").unwrap();

    // Scan entire "score" index (value: None) in ascending order
    let plan = PlanNode::IndexScan {
        collection: "test".into(),
        column: "score".into(),
        value: None,
        direction: SortDirection::Asc,
        limit: None,
        complete_groups: false,
        covered: false,
    };
    let ids = collect_ids(Executor::new(&txn, &cf).execute(&plan).unwrap());
    // score order: 70 (id=1), 80 (id=3), 90 (id=2)
    assert_eq!(ids, vec!["1", "3", "2"]);
}

#[test]
fn index_scan_desc() {
    let db = seeded_db();
    let mut txn = db.store().begin(true).unwrap();
    let cf = txn.cf("test").unwrap();

    let plan = PlanNode::IndexScan {
        collection: "test".into(),
        column: "score".into(),
        value: None,
        direction: SortDirection::Desc,
        limit: None,
        complete_groups: false,
        covered: false,
    };
    let ids = collect_ids(Executor::new(&txn, &cf).execute(&plan).unwrap());
    // Descending score: 90 (id=2), 80 (id=3), 70 (id=1)
    assert_eq!(ids, vec!["2", "3", "1"]);
}

#[test]
fn index_scan_with_limit() {
    let db = seeded_db();
    let mut txn = db.store().begin(true).unwrap();
    let cf = txn.cf("test").unwrap();

    let plan = PlanNode::IndexScan {
        collection: "test".into(),
        column: "score".into(),
        value: None,
        direction: SortDirection::Asc,
        limit: Some(2),
        complete_groups: false,
        covered: false,
    };
    let ids = collect_ids(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(ids, vec!["1", "3"]); // score 70, 80
}

#[test]
fn index_merge_or() {
    let db = seeded_db();
    let mut txn = db.store().begin(true).unwrap();
    let cf = txn.cf("test").unwrap();

    // OR: status="active" | status="inactive" → all 3 records
    let plan = PlanNode::IndexMerge {
        logical: LogicalOp::Or,
        lhs: Box::new(PlanNode::IndexScan {
            collection: "test".into(),
            column: "status".into(),
            value: Some(bson::Bson::String("active".into())),
            direction: SortDirection::Asc,
            limit: None,
            complete_groups: false,
            covered: false,
        }),
        rhs: Box::new(PlanNode::IndexScan {
            collection: "test".into(),
            column: "status".into(),
            value: Some(bson::Bson::String("inactive".into())),
            direction: SortDirection::Asc,
            limit: None,
            complete_groups: false,
            covered: false,
        }),
    };
    let ids = collect_ids(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(ids.len(), 3);
    // Union: all three IDs present (deduped)
    assert!(ids.contains(&"1".to_string()));
    assert!(ids.contains(&"2".to_string()));
    assert!(ids.contains(&"3".to_string()));
}

#[test]
fn index_merge_and() {
    let db = seeded_db();
    let mut txn = db.store().begin(true).unwrap();
    let cf = txn.cf("test").unwrap();

    // AND: status="active" & score=80 → only Charlie (id=3)
    let plan = PlanNode::IndexMerge {
        logical: LogicalOp::And,
        lhs: Box::new(PlanNode::IndexScan {
            collection: "test".into(),
            column: "status".into(),
            value: Some(bson::Bson::String("active".into())),
            direction: SortDirection::Asc,
            limit: None,
            complete_groups: false,
            covered: false,
        }),
        rhs: Box::new(PlanNode::IndexScan {
            collection: "test".into(),
            column: "score".into(),
            value: Some(bson::Bson::Int32(80)),
            direction: SortDirection::Asc,
            limit: None,
            complete_groups: false,
            covered: false,
        }),
    };
    let ids = collect_ids(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(ids, vec!["3"]); // Charlie: active AND score=80
}

#[test]
fn read_record_fetches_docs_from_index_scan() {
    let db = seeded_db();
    let mut txn = db.store().begin(true).unwrap();
    let cf = txn.cf("test").unwrap();

    // ReadRecord wrapping an IndexScan — should fetch actual documents
    let plan = PlanNode::ReadRecord {
        input: Box::new(PlanNode::IndexScan {
            collection: "test".into(),
            column: "status".into(),
            value: Some(bson::Bson::String("active".into())),
            direction: SortDirection::Asc,
            limit: None,
            complete_groups: false,
            covered: false,
        }),
    };
    let rows = collect_docs(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(rows.len(), 2);
    // Verify we got full documents back (not just IDs)
    let names: Vec<&str> = rows
        .iter()
        .map(|doc| doc.as_ref().unwrap().get_str("name").unwrap())
        .collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Charlie"));
}

#[test]
fn read_record_skips_dangling_index() {
    let db = seeded_db();

    // Delete record "2" but leave its index entries (dangling index)
    {
        let mut txn = db.store().begin(false).unwrap();
        let cf = txn.cf("test").unwrap();
        txn.delete(&cf, &encoding::record_key("2")).unwrap();
        txn.commit().unwrap();
    }

    let mut txn = db.store().begin(true).unwrap();
    let cf = txn.cf("test").unwrap();

    let plan = PlanNode::ReadRecord {
        input: Box::new(PlanNode::IndexScan {
            collection: "test".into(),
            column: "status".into(),
            value: Some(bson::Bson::String("inactive".into())),
            direction: SortDirection::Asc,
            limit: None,
            complete_groups: false,
            covered: false,
        }),
    };
    let rows = collect_docs(Executor::new(&txn, &cf).execute(&plan).unwrap());
    // Index still has Bob's entry but record is gone → skipped
    assert_eq!(rows.len(), 0);
}

#[test]
fn read_record_over_scan() {
    let db = seeded_db();
    let mut txn = db.store().begin(true).unwrap();
    let cf = txn.cf("test").unwrap();

    // ReadRecord wrapping Scan — passthrough path (Scan already yields docs)
    let plan = PlanNode::ReadRecord {
        input: Box::new(PlanNode::Scan {
            collection: "test".into(),
        }),
    };
    let rows = collect_docs(Executor::new(&txn, &cf).execute(&plan).unwrap());
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].as_ref().unwrap().get_str("name").unwrap(), "Alice");
}

// ── Tier 4: Upsert node tests ───────────────────────────────

use crate::planner::UpsertMode;

#[test]
fn upsert_replace_insert_new() {
    // MockTransaction returns None for get() → insert path
    let (txn, cf) = mock_executor();
    let plan = PlanNode::InsertIndex {
        indexed_fields: vec!["status".into()],
        input: Box::new(PlanNode::Upsert {
            mode: UpsertMode::Replace,
            indexed_fields: vec!["status".into()],
            input: Box::new(PlanNode::Values {
                docs: vec![rawdoc! { "_id": "1", "name": "Alice", "status": "active" }],
            }),
        }),
    };
    let result = Executor::new(&txn, &cf).execute(&plan).unwrap();
    match result {
        ExecutionResult::Upsert { inserted, updated } => {
            assert_eq!(inserted, 1);
            assert_eq!(updated, 0);
        }
        _ => panic!("expected Upsert"),
    }
    let puts = txn.puts.borrow();
    // Record write + index write
    assert_eq!(puts.len(), 2);
    assert_eq!(puts[0].0, encoding::record_key("1"));
    let written = bson::RawDocument::from_bytes(&puts[0].1).unwrap();
    assert_eq!(written.get_str("name").unwrap(), "Alice");
}

#[test]
fn upsert_merge_insert_new() {
    let (txn, cf) = mock_executor();
    let plan = PlanNode::InsertIndex {
        indexed_fields: vec![],
        input: Box::new(PlanNode::Upsert {
            mode: UpsertMode::Merge,
            indexed_fields: vec![],
            input: Box::new(PlanNode::Values {
                docs: vec![rawdoc! { "_id": "1", "name": "Alice" }],
            }),
        }),
    };
    let result = Executor::new(&txn, &cf).execute(&plan).unwrap();
    match result {
        ExecutionResult::Upsert { inserted, updated } => {
            assert_eq!(inserted, 1);
            assert_eq!(updated, 0);
        }
        _ => panic!("expected Upsert"),
    }
    let puts = txn.puts.borrow();
    assert_eq!(puts.len(), 1);
    assert_eq!(puts[0].0, encoding::record_key("1"));
}

#[test]
fn upsert_replace_existing() {
    let db = seeded_db();
    let mut txn = db.begin(false).unwrap();

    let result = txn
        .upsert_many(
            "test",
            vec![bson::doc! { "_id": "1", "name": "Alicia", "status": "archived", "score": 99 }],
        )
        .unwrap();
    assert_eq!(result.inserted, 0);
    assert_eq!(result.updated, 1);

    // Verify the record was replaced
    let doc = txn.find_by_id("test", "1", None).unwrap().unwrap();
    assert_eq!(doc.get_str("name").unwrap(), "Alicia");
    assert_eq!(doc.get_str("status").unwrap(), "archived");
    assert_eq!(doc.get_i32("score").unwrap(), 99);
}

#[test]
fn upsert_merge_existing() {
    let db = seeded_db();
    let mut txn = db.begin(false).unwrap();

    // Merge into Alice (id=1): update score, keep name and status
    let result = txn
        .merge_many("test", vec![bson::doc! { "_id": "1", "score": 99 }])
        .unwrap();
    assert_eq!(result.inserted, 0);
    assert_eq!(result.updated, 1);

    // Verify: name and status preserved, score updated
    let doc = txn.find_by_id("test", "1", None).unwrap().unwrap();
    assert_eq!(doc.get_str("name").unwrap(), "Alice");
    assert_eq!(doc.get_str("status").unwrap(), "active");
    assert_eq!(doc.get_i32("score").unwrap(), 99);
}

#[test]
fn upsert_mixed_insert_and_update() {
    let db = seeded_db();
    let mut txn = db.begin(false).unwrap();

    let result = txn
        .upsert_many(
            "test",
            vec![
                bson::doc! { "_id": "1", "name": "Alicia", "status": "archived", "score": 99 },
                bson::doc! { "_id": "99", "name": "New", "status": "pending", "score": 50 },
            ],
        )
        .unwrap();
    assert_eq!(result.inserted, 1);
    assert_eq!(result.updated, 1);

    let doc1 = txn.find_by_id("test", "1", None).unwrap().unwrap();
    assert_eq!(doc1.get_str("name").unwrap(), "Alicia");

    let doc99 = txn.find_by_id("test", "99", None).unwrap().unwrap();
    assert_eq!(doc99.get_str("name").unwrap(), "New");
}

#[test]
fn upsert_replace_cleans_old_indexes() {
    let db = seeded_db();
    let mut txn = db.begin(false).unwrap();

    // Alice has status="active". Replace with status="archived".
    txn.upsert_many(
        "test",
        vec![bson::doc! { "_id": "1", "name": "Alice", "status": "archived", "score": 70 }],
    )
    .unwrap();

    // Query by old index value should not find Alice
    let query = slate_query::Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "status".into(),
                operator: Operator::Eq,
                value: bson::Bson::String("active".into()),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results: Vec<_> = txn
        .find("test", &query)
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let active_ids: Vec<&str> = results
        .iter()
        .map(|r| {
            let doc = bson::RawDocument::from_bytes(r.as_bytes()).unwrap();
            doc.get_str("_id").unwrap()
        })
        .collect();
    // Only Charlie (id=3) should remain active
    assert_eq!(active_ids, vec!["3"]);

    // Query by new index value should find Alice
    let query2 = slate_query::Query {
        filter: Some(FilterGroup {
            logical: LogicalOp::And,
            children: vec![FilterNode::Condition(Filter {
                field: "status".into(),
                operator: Operator::Eq,
                value: bson::Bson::String("archived".into()),
            })],
        }),
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    };
    let results2: Vec<_> = txn
        .find("test", &query2)
        .unwrap()
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results2.len(), 1);
    let doc = bson::RawDocument::from_bytes(results2[0].as_bytes()).unwrap();
    assert_eq!(doc.get_str("_id").unwrap(), "1");
}
