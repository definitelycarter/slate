//! The `IndexMerge` node — combine two ID streams by AND (intersection) or OR
//! (union), deduplicating by document identity.
//!
//! A *blocking, binary* transform: `Or` buffers both sides; `And` buffers the
//! right side into an ID set, then filters the left. Identity is the document's
//! primary key (for documents) or the value itself (for bare IDs from an
//! `IndexScan`), hashed — matching v1's hash-based dedup.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use bson::RawBson;
use bson::raw::RawBsonRef;
use slate_engine::{Catalog, EngineTransaction};
use slate_planner::{CollectionRef, LogicalOp};

use crate::budget;
use crate::{ExecError, ValueIter};

pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
    logical: LogicalOp,
    left: ValueIter<'a>,
    right: ValueIter<'a>,
    cap: Option<usize>,
) -> Result<ValueIter<'a>, ExecError> {
    let handle = txn.collection(&collection.cf, &collection.collection)?;
    let pk_path = handle.pk_path().to_string();

    // OOM guard (Resource Limits RFC, B): both legs are buffered eagerly, so cap
    // the rows pulled from each side as they accumulate.
    let merged: Vec<Option<RawBson>> = match logical {
        LogicalOp::Or => {
            let left = collect_capped(left, cap)?;
            let right = collect_capped(right, cap)?;

            let mut seen = HashSet::with_capacity(left.len() + right.len());
            let mut result = Vec::with_capacity(left.len() + right.len());
            for val in left.into_iter().chain(right) {
                if let Some(ref v) = val
                    && let Some(id) = id_hash(v, &pk_path)
                    && !seen.insert(id)
                {
                    continue; // duplicate
                }
                result.push(val);
            }
            result
        }
        LogicalOp::And => {
            let mut right_set = HashSet::new();
            for item in right {
                if let Some(val) = item?
                    && let Some(id) = id_hash(&val, &pk_path)
                {
                    right_set.insert(id);
                }
                budget::check_cap(right_set.len(), cap, "IndexMerge")?;
            }
            collect_capped(left, cap)?
                .into_iter()
                .filter(|val| {
                    val.as_ref()
                        .and_then(|v| id_hash(v, &pk_path))
                        .map(|id| right_set.contains(&id))
                        .unwrap_or(false)
                })
                .collect()
        }
    };

    Ok(Box::new(merged.into_iter().map(Ok)))
}

/// Drain a side into a buffer, tripping the materialization cap as it grows.
fn collect_capped(
    side: ValueIter<'_>,
    cap: Option<usize>,
) -> Result<Vec<Option<RawBson>>, ExecError> {
    let mut out = Vec::new();
    for item in side {
        out.push(item?);
        budget::check_cap(out.len(), cap, "IndexMerge")?;
    }
    Ok(out)
}

/// Hash a row's identity: the pk for documents, the value itself for bare IDs.
fn id_hash(v: &RawBson, pk_path: &str) -> Option<u64> {
    let id_ref = match v {
        RawBson::Document(d) => d.get(pk_path).ok().flatten()?,
        other => other.as_raw_bson_ref(),
    };
    Some(hash_id(id_ref))
}

fn hash_id(r: RawBsonRef) -> u64 {
    let mut h = std::hash::DefaultHasher::new();
    match r {
        RawBsonRef::String(s) => {
            0u8.hash(&mut h);
            s.hash(&mut h);
        }
        RawBsonRef::Int32(i) => {
            1u8.hash(&mut h);
            i.hash(&mut h);
        }
        RawBsonRef::Int64(i) => {
            2u8.hash(&mut h);
            i.hash(&mut h);
        }
        RawBsonRef::ObjectId(oid) => {
            6u8.hash(&mut h);
            oid.bytes().hash(&mut h);
        }
        _ => {
            255u8.hash(&mut h);
        }
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    use crate::Executor;
    use crate::nodes::test_support::{people_ref, seeded_people};
    use bson::{Bson, RawBson};
    use slate_engine::Engine;
    use slate_planner::{IndexScanRange, LogicalOp, Node, Plan, ScanDirection};

    fn eq(age: i64) -> Node {
        Node::IndexScan {
            collection: people_ref(),
            field: "age".into(),
            range: IndexScanRange::Eq(Bson::Int64(age)),
            direction: ScanDirection::Forward,
            limit: None,
            covering: false,
        }
    }

    fn ge(age: i32) -> Node {
        Node::IndexScan {
            collection: people_ref(),
            field: "age".into(),
            range: IndexScanRange::Range {
                lower: Some((Bson::Int32(age), true)),
                upper: None,
            },
            direction: ScanDirection::Forward,
            limit: None,
            covering: false,
        }
    }

    fn merge(logical: LogicalOp, lhs: Node, rhs: Node) -> Vec<RawBson> {
        let engine = seeded_people();
        let txn = engine.begin(true).unwrap();
        let plan = Plan::Query(Node::IndexMerge {
            collection: people_ref(),
            logical,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        });
        Executor::new(&txn).execute_collect(plan).unwrap()
    }

    fn id(s: &str) -> RawBson {
        RawBson::String(s.into())
    }

    #[test]
    fn or_unions_and_dedups() {
        // {2} ∪ {2,3} = {2,3}, id 2 deduped, first-occurrence order
        assert_eq!(merge(LogicalOp::Or, eq(41), ge(41)), vec![id("2"), id("3")]);
    }

    #[test]
    fn and_intersects() {
        // {2,3} ∩ {3} = {3}
        assert_eq!(merge(LogicalOp::And, ge(41), eq(44)), vec![id("3")]);
    }

    #[test]
    fn and_disjoint_is_empty() {
        assert!(merge(LogicalOp::And, eq(36), eq(44)).is_empty());
    }
}
