//! The `KeyLookup` node — point read by primary key.
//!
//! Takes IDs (from an `IndexScan`) or documents (from which the pk is
//! extracted) and fetches the full document for each via `txn.get`. Missing
//! documents (dangling index entries, deleted rows) are dropped.

use bson::RawBson;
use slate_engine::{Catalog, EngineTransaction};
use slate_planner::CollectionRef;

use crate::{ExecError, ValueIter};

/// Fetch the full document for each incoming ID (or document's pk).
pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
    source: ValueIter<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    let handle = txn.collection(&collection.cf, &collection.collection)?;

    Ok(Box::new(source.filter_map(move |result| {
        let val = match result {
            Ok(Some(v)) => v,
            Ok(None) => return None,
            Err(e) => return Some(Err(e)),
        };

        // Accept a bare ID (from IndexScan) or a Document carrying the pk.
        let id_ref = match &val {
            RawBson::Document(d) => match d.get(handle.pk_path()) {
                Ok(Some(id)) => id,
                _ => return None,
            },
            other => other.as_raw_bson_ref(),
        };

        match txn.get(&handle, &id_ref) {
            Ok(Some(doc)) => Some(Ok(Some(RawBson::Document(doc)))),
            Ok(None) => None, // dangling / deleted
            Err(e) => Some(Err(ExecError::Engine(e))),
        }
    })))
}

#[cfg(test)]
mod tests {
    use crate::Executor;
    use crate::nodes::test_support::{people_ref, seeded_people, sv};
    use bson::{Bson, RawBson, rawdoc};
    use slate_engine::Engine;
    use slate_planner::{IndexScanRange, Node, Plan, RowBinding, ScanDirection};

    fn index_eq(age: i64) -> Node {
        Node::IndexScan {
            collection: people_ref(),
            field: "age".into(),
            range: IndexScanRange::Eq(Bson::Int64(age)),
            direction: ScanDirection::Forward,
            limit: None,
            covering: false,
        }
    }

    #[test]
    fn fetches_doc_for_indexscan_id() {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let plan = Plan::Query(Node::KeyLookup {
            collection: people_ref(),
            source: Box::new(index_eq(41)),
        });
        let out = Executor::new(&txn).execute_collect(plan).unwrap();
        assert_eq!(
            out,
            vec![RawBson::Document(
                rawdoc! { "_id": "2", "name": "alan", "age": 41 }
            )]
        );
    }

    #[test]
    fn fetches_doc_for_bare_id_value() {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let plan = Plan::Query(Node::KeyLookup {
            collection: people_ref(),
            source: Box::new(Node::Values(vec![RawBson::String("3".into())])),
        });
        let out = Executor::new(&txn).execute_collect(plan).unwrap();
        assert_eq!(
            out,
            vec![RawBson::Document(
                rawdoc! { "_id": "3", "name": "grace", "age": 44 }
            )]
        );
    }

    #[test]
    fn missing_id_is_dropped() {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let plan = Plan::Query(Node::KeyLookup {
            collection: people_ref(),
            source: Box::new(Node::Values(vec![RawBson::String("999".into())])),
        });
        assert!(
            Executor::new(&txn)
                .execute_collect(plan)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn index_keylookup_project_pipeline() {
        // SELECT VALUE c.name FROM c WHERE c.age = 41  (via the age index)
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let plan = Plan::Query(Node::Project {
            expr: sv("c.name"),
            binding: RowBinding::Env,
            source: Box::new(Node::Bind {
                alias: "c".into(),
                source: Box::new(Node::KeyLookup {
                    collection: people_ref(),
                    source: Box::new(index_eq(41)),
                }),
            }),
        });
        let out = Executor::new(&txn).execute_collect(plan).unwrap();
        assert_eq!(out, vec![RawBson::String("alan".into())]);
    }
}
