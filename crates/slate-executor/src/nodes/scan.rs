//! The `Scan` source — streams every live document in a collection.
//!
//! Resolves the [`CollectionRef`]'s `(cf, name)` pair to a live handle against
//! the transaction's catalog, then wraps the engine's document iterator. This
//! is where the `Cf` handle generic enters — confined to the physical layer.

use bson::RawBson;
use slate_engine::{Catalog, EngineTransaction};
use slate_planner::CollectionRef;

use crate::budget::Ticker;
use crate::{ExecError, ValueIter};

/// Open a full-collection scan, yielding each document as a value.
///
/// `ticker` folds the cooperative deadline check (Resource Limits RFC, A) into
/// the per-row closure — no separate iterator layer (see [`crate::budget`]).
pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
    mut ticker: Ticker,
) -> Result<ValueIter<'a>, ExecError> {
    let handle = txn.collection(&collection.cf, &collection.collection)?;
    crate::trace::trace_event!(
        cf = collection.cf.as_str(),
        collection = collection.collection.as_str(),
        "scan opened"
    );
    let iter = txn.scan(&handle)?;
    Ok(Box::new(iter.map(move |result| {
        ticker.tick()?;
        match result {
            Ok(doc) => Ok(Some(RawBson::Document(doc))),
            Err(e) => Err(ExecError::Engine(e)),
        }
    })))
}

#[cfg(test)]
mod tests {
    use crate::Executor;
    use crate::nodes::test_support::{people_ref, pred, seeded_people, sv};
    use bson::RawBson;
    use slate_engine::Engine;
    use slate_planner::{Node, Plan, RowBinding};

    #[test]
    fn scan_yields_all_documents() {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let out = Executor::new(&txn)
            .execute_collect(Plan::Query(Node::Scan {
                collection: people_ref(),
            }))
            .unwrap();
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn scan_filter_project_pipeline() {
        // SELECT VALUE c.name FROM c WHERE c.age > 40
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let plan = Plan::Query(Node::Project {
            expr: sv("c.name"),
            binding: RowBinding::Env,
            source: Box::new(Node::Filter {
                predicate: pred("c.age > 40"),
                binding: RowBinding::Env,
                source: Box::new(Node::Bind {
                    alias: "c".into(),
                    source: Box::new(Node::Scan {
                        collection: people_ref(),
                    }),
                }),
            }),
        });
        let out = Executor::new(&txn).execute_collect(plan).unwrap();
        assert_eq!(
            out,
            vec![
                RawBson::String("alan".into()),
                RawBson::String("grace".into()),
            ]
        );
    }
}
