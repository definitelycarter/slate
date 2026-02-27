use bson::Bson;
use slate_engine::{Catalog, Engine, EngineTransaction, KvEngine};
use slate_query::{Sort, SortDirection};
use slate_store::MemoryStore;

use crate::expression::{Expression, LogicalOp};
use crate::statement::Statement;

use super::plan::{IndexScanRange, Node, Plan, ScanDirection};
use super::planner::Planner;

// ── Helpers ─────────────────────────────────────────────────

fn engine() -> KvEngine<MemoryStore> {
    KvEngine::new(MemoryStore::new())
}

/// Create an engine with "users" collection indexed on "status" and "age".
fn setup() -> KvEngine<MemoryStore> {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    txn.create_index("users", "status").unwrap();
    txn.create_index("users", "age").unwrap();
    txn.commit().unwrap();
    engine
}

fn find_stmt(predicate: Expression) -> Statement<'static> {
    Statement::Find {
        collection: "users",
        predicate,
        sort: vec![],
        skip: None,
        take: None,
        columns: None,
    }
}

/// Unwrap Plan::Find into its node tree.
fn unwrap_find<Cf: Clone>(plan: Plan<Cf>) -> Node<Cf> {
    match plan {
        Plan::Find(node) => node,
        _ => panic!("expected Plan::Find"),
    }
}

/// Check if the innermost source of a node tree is a Scan.
fn is_scan<Cf: Clone>(node: &Node<Cf>) -> bool {
    matches!(node, Node::Scan { .. })
}

/// Check if a node is an IndexScan on the given field.
fn is_index_scan_on<Cf: Clone>(node: &Node<Cf>, expected_field: &str) -> bool {
    matches!(node, Node::IndexScan { field, .. } if field == expected_field)
}

/// Dig through Projection to get the inner node.
fn unwrap_projection<Cf: Clone>(node: Node<Cf>) -> Node<Cf> {
    match node {
        Node::Projection { source, .. } => *source,
        other => other,
    }
}

// ── Find: filter planning ───────────────────────────────────

#[test]
fn find_no_index_falls_back_to_scan() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    // "name" is not indexed
    let plan = planner
        .plan(find_stmt(Expression::Eq("name".into(), Bson::String("alice".into()))))
        .unwrap();
    let node = unwrap_projection(unwrap_find(plan));

    // Should be Filter > Scan
    match node {
        Node::Filter { source, .. } => assert!(is_scan(&source)),
        _ => panic!("expected Filter > Scan, got {:?}", std::mem::discriminant(&node)),
    }
}

#[test]
fn find_indexed_eq_uses_index_scan() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let plan = planner
        .plan(find_stmt(Expression::Eq("status".into(), Bson::String("active".into()))))
        .unwrap();
    let node = unwrap_projection(unwrap_find(plan));

    // Should be KeyLookup > IndexScan(status, Eq)
    match node {
        Node::KeyLookup { source, .. } => {
            assert!(is_index_scan_on(&source, "status"));
            match *source {
                Node::IndexScan { range: IndexScanRange::Eq(v), .. } => {
                    assert_eq!(v, Bson::String("active".into()));
                }
                _ => panic!("expected IndexScan Eq"),
            }
        }
        _ => panic!("expected KeyLookup"),
    }
}

#[test]
fn find_indexed_range_uses_index_scan() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let plan = planner
        .plan(find_stmt(Expression::Gt("age".into(), Bson::Int32(25))))
        .unwrap();
    let node = unwrap_projection(unwrap_find(plan));

    match node {
        Node::KeyLookup { source, .. } => {
            assert!(is_index_scan_on(&source, "age"));
            match *source {
                Node::IndexScan { range: IndexScanRange::Range { lower, upper }, .. } => {
                    assert!(lower.is_some());
                    assert!(upper.is_none());
                }
                _ => panic!("expected IndexScan Range"),
            }
        }
        _ => panic!("expected KeyLookup"),
    }
}

// ── Find: AND planning ──────────────────────────────────────

#[test]
fn and_prefers_eq_over_range() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    // AND(age > 25, status = "active") — should pick status Eq, leave age as residual
    let plan = planner
        .plan(find_stmt(Expression::And(vec![
            Expression::Gt("age".into(), Bson::Int32(25)),
            Expression::Eq("status".into(), Bson::String("active".into())),
        ])))
        .unwrap();
    let node = unwrap_projection(unwrap_find(plan));

    // Filter(age > 25) > KeyLookup > IndexScan(status, Eq)
    match node {
        Node::Filter { predicate, source } => {
            assert!(matches!(predicate, Expression::Gt(ref f, _) if f == "age"));
            match *source {
                Node::KeyLookup { source, .. } => {
                    assert!(is_index_scan_on(&source, "status"));
                }
                _ => panic!("expected KeyLookup"),
            }
        }
        _ => panic!("expected Filter"),
    }
}

#[test]
fn and_dual_range_on_indexed_field() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    // AND(age >= 18, age < 65)
    let plan = planner
        .plan(find_stmt(Expression::And(vec![
            Expression::Gte("age".into(), Bson::Int32(18)),
            Expression::Lt("age".into(), Bson::Int32(65)),
        ])))
        .unwrap();
    let node = unwrap_projection(unwrap_find(plan));

    // KeyLookup > IndexScan(age, Range { lower, upper })
    match node {
        Node::KeyLookup { source, .. } => match *source {
            Node::IndexScan {
                field,
                range: IndexScanRange::Range { lower, upper },
                ..
            } => {
                assert_eq!(field, "age");
                let (lv, li) = lower.unwrap();
                assert_eq!(lv, Bson::Int32(18));
                assert!(li); // inclusive
                let (uv, ui) = upper.unwrap();
                assert_eq!(uv, Bson::Int32(65));
                assert!(!ui); // exclusive
            }
            _ => panic!("expected IndexScan Range"),
        },
        _ => panic!("expected KeyLookup"),
    }
}

#[test]
fn and_no_indexed_field_falls_to_scan() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    // AND(name = "alice", email = "alice@test.com") — neither indexed
    let plan = planner
        .plan(find_stmt(Expression::And(vec![
            Expression::Eq("name".into(), Bson::String("alice".into())),
            Expression::Eq("email".into(), Bson::String("alice@test.com".into())),
        ])))
        .unwrap();
    let node = unwrap_projection(unwrap_find(plan));

    match node {
        Node::Filter { source, .. } => assert!(is_scan(&source)),
        _ => panic!("expected Filter > Scan"),
    }
}

// ── Find: OR planning ───────────────────────────────────────

#[test]
fn or_fully_indexed_builds_index_merge() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    // OR(status = "active", status = "pending") — both on indexed field
    let plan = planner
        .plan(find_stmt(Expression::Or(vec![
            Expression::Eq("status".into(), Bson::String("active".into())),
            Expression::Eq("status".into(), Bson::String("pending".into())),
        ])))
        .unwrap();
    let node = unwrap_projection(unwrap_find(plan));

    // Filter > KeyLookup > IndexMerge(Or, IndexScan, IndexScan)
    // OR always keeps the full predicate as residual for correctness.
    match node {
        Node::Filter { source, .. } => match *source {
            Node::KeyLookup { source, .. } => match *source {
                Node::IndexMerge { logical, lhs, rhs } => {
                    assert_eq!(logical, LogicalOp::Or);
                    assert!(is_index_scan_on(&lhs, "status"));
                    assert!(is_index_scan_on(&rhs, "status"));
                }
                _ => panic!("expected IndexMerge"),
            },
            _ => panic!("expected KeyLookup"),
        },
        _ => panic!("expected Filter"),
    }
}

#[test]
fn or_not_fully_indexed_falls_to_scan() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    // OR(status = "active", name = "alice") — name not indexed
    let plan = planner
        .plan(find_stmt(Expression::Or(vec![
            Expression::Eq("status".into(), Bson::String("active".into())),
            Expression::Eq("name".into(), Bson::String("alice".into())),
        ])))
        .unwrap();
    let node = unwrap_projection(unwrap_find(plan));

    match node {
        Node::Filter { source, .. } => assert!(is_scan(&source)),
        _ => panic!("expected Filter > Scan"),
    }
}

#[test]
fn and_containing_indexed_or_subgroup() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    // AND(OR(status = "active", status = "pending"), name = "alice")
    // The OR sub-group is fully indexed → Priority 2 in plan_and
    let plan = planner
        .plan(find_stmt(Expression::And(vec![
            Expression::Or(vec![
                Expression::Eq("status".into(), Bson::String("active".into())),
                Expression::Eq("status".into(), Bson::String("pending".into())),
            ]),
            Expression::Eq("name".into(), Bson::String("alice".into())),
        ])))
        .unwrap();
    let node = unwrap_projection(unwrap_find(plan));

    // Filter(name = "alice") > KeyLookup > IndexMerge(Or)
    match node {
        Node::Filter { predicate, source } => {
            assert!(matches!(predicate, Expression::Eq(ref f, _) if f == "name"));
            match *source {
                Node::KeyLookup { source, .. } => {
                    assert!(matches!(*source, Node::IndexMerge { logical: LogicalOp::Or, .. }));
                }
                _ => panic!("expected KeyLookup"),
            }
        }
        _ => panic!("expected Filter"),
    }
}

// ── Find: sort optimization ─────────────────────────────────

#[test]
fn sort_on_indexed_field_with_take_uses_index_order() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let plan = planner
        .plan(Statement::Find {
            collection: "users",
            predicate: Expression::Exists("_id".into(), true), // not indexable → Scan
            sort: vec![Sort {
                field: "age".into(),
                direction: SortDirection::Desc,
            }],
            skip: None,
            take: Some(10),
            columns: None,
        })
        .unwrap();
    let node = unwrap_projection(unwrap_find(plan));

    // Limit > Filter > KeyLookup > IndexScan(age, Full, Reverse)
    match node {
        Node::Limit { source, take, .. } => {
            assert_eq!(take, Some(10));
            match *source {
                Node::Filter { source, .. } => match *source {
                    Node::KeyLookup { source, .. } => match *source {
                        Node::IndexScan {
                            field,
                            range: IndexScanRange::Full,
                            direction: ScanDirection::Reverse,
                            ..
                        } => assert_eq!(field, "age"),
                        _ => panic!("expected IndexScan Full Reverse"),
                    },
                    _ => panic!("expected KeyLookup"),
                },
                _ => panic!("expected Filter"),
            }
        }
        _ => panic!("expected Limit"),
    }
}

#[test]
fn sort_without_take_does_not_use_index_order() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let plan = planner
        .plan(Statement::Find {
            collection: "users",
            predicate: Expression::Exists("_id".into(), true),
            sort: vec![Sort {
                field: "age".into(),
                direction: SortDirection::Asc,
            }],
            skip: None,
            take: None, // no take → can't use indexed sort
            columns: None,
        })
        .unwrap();
    let node = unwrap_projection(unwrap_find(plan));

    // Sort > Filter > Scan (no indexed sort)
    match node {
        Node::Sort { source, .. } => match *source {
            Node::Filter { source, .. } => assert!(is_scan(&source)),
            _ => panic!("expected Filter"),
        },
        _ => panic!("expected Sort"),
    }
}

// ── Find: covered index ─────────────────────────────────────

#[test]
fn covered_index_scan_when_projection_matches() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let plan = planner
        .plan(Statement::Find {
            collection: "users",
            predicate: Expression::Eq("status".into(), Bson::String("active".into())),
            sort: vec![],
            skip: None,
            take: None,
            columns: Some(vec!["_id".into(), "status".into()]),
        })
        .unwrap();
    let node = unwrap_find(plan);

    // Should be a covered IndexScan (no KeyLookup, no Projection)
    match node {
        Node::IndexScan { field, covered, .. } => {
            assert_eq!(field, "status");
            assert!(covered);
        }
        _ => panic!("expected covered IndexScan, got {:?}", std::mem::discriminant(&node)),
    }
}

#[test]
fn not_covered_when_projection_needs_extra_fields() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let plan = planner
        .plan(Statement::Find {
            collection: "users",
            predicate: Expression::Eq("status".into(), Bson::String("active".into())),
            sort: vec![],
            skip: None,
            take: None,
            columns: Some(vec!["name".into(), "status".into()]), // "name" not in index
        })
        .unwrap();
    let node = unwrap_find(plan);

    // Should have Projection > KeyLookup > IndexScan (not covered)
    match node {
        Node::Projection { source, .. } => match *source {
            Node::KeyLookup { source, .. } => match *source {
                Node::IndexScan { covered, .. } => assert!(!covered),
                _ => panic!("expected IndexScan"),
            },
            _ => panic!("expected KeyLookup"),
        },
        _ => panic!("expected Projection"),
    }
}

// ── Find: projection + limit ────────────────────────────────

#[test]
fn find_with_skip_and_take() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let plan = planner
        .plan(Statement::Find {
            collection: "users",
            predicate: Expression::Eq("status".into(), Bson::String("active".into())),
            sort: vec![],
            skip: Some(10),
            take: Some(20),
            columns: None,
        })
        .unwrap();
    let node = unwrap_find(plan);

    // Projection > Limit > KeyLookup > IndexScan
    match node {
        Node::Projection { source, .. } => match *source {
            Node::Limit { skip, take, source } => {
                assert_eq!(skip, 10);
                assert_eq!(take, Some(20));
                match *source {
                    Node::KeyLookup { .. } => {}
                    _ => panic!("expected KeyLookup"),
                }
            }
            _ => panic!("expected Limit"),
        },
        _ => panic!("expected Projection"),
    }
}

// ── Distinct ────────────────────────────────────────────────

#[test]
fn distinct_builds_correct_pipeline() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let plan = planner
        .plan(Statement::Distinct {
            collection: "users",
            field: "status".into(),
            predicate: Expression::Eq("age".into(), Bson::Int32(30)),
            sort: Some(SortDirection::Asc),
            skip: None,
            take: Some(5),
        })
        .unwrap();
    let node = unwrap_find(plan);

    // Limit > Sort > Distinct > Projection > KeyLookup > IndexScan(age)
    match node {
        Node::Limit { take, source, .. } => {
            assert_eq!(take, Some(5));
            match *source {
                Node::Sort { source, .. } => match *source {
                    Node::Distinct { field, source } => {
                        assert_eq!(field, "status");
                        match *source {
                            Node::Projection { columns, source } => {
                                assert_eq!(columns, Some(vec!["status".into()]));
                                match *source {
                                    Node::KeyLookup { source, .. } => {
                                        assert!(is_index_scan_on(&source, "age"));
                                    }
                                    _ => panic!("expected KeyLookup"),
                                }
                            }
                            _ => panic!("expected Projection"),
                        }
                    }
                    _ => panic!("expected Distinct"),
                },
                _ => panic!("expected Sort"),
            }
        }
        _ => panic!("expected Limit"),
    }
}

// ── Mutation statements ─────────────────────────────────────

#[test]
fn insert_plan() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let plan = planner
        .plan(Statement::Insert {
            collection: "users",
            docs: vec![bson::rawdoc! { "_id": "1", "name": "alice" }],
        })
        .unwrap();

    match plan {
        Plan::Insert { source, .. } => {
            assert!(matches!(source, Node::Values(docs) if docs.len() == 1));
        }
        _ => panic!("expected Plan::Insert"),
    }
}

#[test]
fn update_plan_with_limit() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let mutation =
        crate::mutation::parse_mutation(&bson::rawdoc! { "$set": { "status": "updated" } }).unwrap();

    let plan = planner
        .plan(Statement::Update {
            collection: "users",
            predicate: Expression::Eq("status".into(), Bson::String("active".into())),
            mutation,
            limit: Some(5),
        })
        .unwrap();

    match plan {
        Plan::Update { source, .. } => {
            // Limit > KeyLookup > IndexScan
            match source {
                Node::Limit { take, source, .. } => {
                    assert_eq!(take, Some(5));
                    match *source {
                        Node::KeyLookup { source, .. } => {
                            assert!(is_index_scan_on(&source, "status"));
                        }
                        _ => panic!("expected KeyLookup"),
                    }
                }
                _ => panic!("expected Limit"),
            }
        }
        _ => panic!("expected Plan::Update"),
    }
}

#[test]
fn delete_plan_no_limit() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let plan = planner
        .plan(Statement::Delete {
            collection: "users",
            predicate: Expression::Eq("status".into(), Bson::String("active".into())),
            limit: None,
        })
        .unwrap();

    match plan {
        Plan::Delete { source, .. } => {
            // KeyLookup > IndexScan (no Limit)
            match source {
                Node::KeyLookup { source, .. } => {
                    assert!(is_index_scan_on(&source, "status"));
                }
                _ => panic!("expected KeyLookup"),
            }
        }
        _ => panic!("expected Plan::Delete"),
    }
}

#[test]
fn replace_plan_has_limit_1() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let plan = planner
        .plan(Statement::Replace {
            collection: "users",
            predicate: Expression::Eq("status".into(), Bson::String("active".into())),
            replacement: bson::rawdoc! { "name": "replaced" },
        })
        .unwrap();

    match plan {
        Plan::Replace { source, .. } => {
            // Limit(take=1) > KeyLookup > IndexScan
            match source {
                Node::Limit { take, .. } => assert_eq!(take, Some(1)),
                _ => panic!("expected Limit"),
            }
        }
        _ => panic!("expected Plan::Replace"),
    }
}

#[test]
fn merge_plan() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let plan = planner
        .plan(Statement::Merge {
            collection: "users",
            docs: vec![bson::rawdoc! { "_id": "1", "email": "a@b.c" }],
        })
        .unwrap();

    assert!(matches!(plan, Plan::Merge { .. }));
}

#[test]
fn upsert_plan() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let plan = planner
        .plan(Statement::Upsert {
            collection: "users",
            docs: vec![bson::rawdoc! { "_id": "1", "name": "alice" }],
        })
        .unwrap();

    assert!(matches!(plan, Plan::Upsert { .. }));
}

// ── Error handling ──────────────────────────────────────────

#[test]
fn collection_not_found() {
    let engine = engine();
    let mut txn = engine.begin(false).unwrap();
    txn.create_collection(None, "users").unwrap();
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let err = planner.plan(find_stmt(Expression::Exists("_id".into(), true)));
    // "users" exists but "nonexistent" does not
    let err = planner
        .plan(Statement::Find {
            collection: "nonexistent",
            predicate: Expression::Exists("_id".into(), true),
            sort: vec![],
            skip: None,
            take: None,
            columns: None,
        });
    assert!(err.is_err());
}

// ── Mutation: non-indexed predicate uses scan ───────────────

#[test]
fn delete_non_indexed_uses_scan() {
    let engine = setup();
    let txn = engine.begin(true).unwrap();
    let planner = Planner::new(|name| Ok(txn.collection(name)?));

    let plan = planner
        .plan(Statement::Delete {
            collection: "users",
            predicate: Expression::Eq("name".into(), Bson::String("alice".into())),
            limit: None,
        })
        .unwrap();

    match plan {
        Plan::Delete { source, .. } => {
            // Filter > Scan (name not indexed)
            match source {
                Node::Filter { source, .. } => assert!(is_scan(&source)),
                _ => panic!("expected Filter > Scan"),
            }
        }
        _ => panic!("expected Plan::Delete"),
    }
}
