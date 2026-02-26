use super::*;

use self::store::seeded_kv_engine;
use slate_engine::Engine;

// seeded_db: Alice(id=1,score=70), Bob(id=2,score=90), Charlie(id=3,score=80)
// score index order ascending: 70(1), 80(3), 90(2)

#[test]
fn index_scan_gt() {
    let engine = seeded_kv_engine();
    let txn = engine.begin(true).unwrap();
    let collection = txn.collection("test").unwrap();

    // score > 70 → Bob(90), Charlie(80)
    let plan = Plan::Find(Node::IndexScan {
        collection,
        field: "score".into(),
        range: IndexScanRange::Range {
            lower: Some((bson::Bson::Int32(70), false)), // exclusive
            upper: None,
        },
        direction: ScanDirection::Forward,
        limit: None,
        covered: false,
    });
    let ids = collect_ids(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(ids, vec!["3", "2"]); // score 80, 90
}

#[test]
fn index_scan_gte() {
    let engine = seeded_kv_engine();
    let txn = engine.begin(true).unwrap();
    let collection = txn.collection("test").unwrap();

    // score >= 80 → Charlie(80), Bob(90)
    let plan = Plan::Find(Node::IndexScan {
        collection,
        field: "score".into(),
        range: IndexScanRange::Range {
            lower: Some((bson::Bson::Int32(80), true)), // inclusive
            upper: None,
        },
        direction: ScanDirection::Forward,
        limit: None,
        covered: false,
    });
    let ids = collect_ids(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(ids, vec!["3", "2"]); // score 80, 90
}

#[test]
fn index_scan_lt() {
    let engine = seeded_kv_engine();
    let txn = engine.begin(true).unwrap();
    let collection = txn.collection("test").unwrap();

    // score < 90 → Alice(70), Charlie(80)
    let plan = Plan::Find(Node::IndexScan {
        collection,
        field: "score".into(),
        range: IndexScanRange::Range {
            lower: None,
            upper: Some((bson::Bson::Int32(90), false)), // exclusive
        },
        direction: ScanDirection::Forward,
        limit: None,
        covered: false,
    });
    let ids = collect_ids(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(ids, vec!["1", "3"]); // score 70, 80
}

#[test]
fn index_scan_lte() {
    let engine = seeded_kv_engine();
    let txn = engine.begin(true).unwrap();
    let collection = txn.collection("test").unwrap();

    // score <= 80 → Alice(70), Charlie(80)
    let plan = Plan::Find(Node::IndexScan {
        collection,
        field: "score".into(),
        range: IndexScanRange::Range {
            lower: None,
            upper: Some((bson::Bson::Int32(80), true)), // inclusive
        },
        direction: ScanDirection::Forward,
        limit: None,
        covered: false,
    });
    let ids = collect_ids(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(ids, vec!["1", "3"]); // score 70, 80
}

#[test]
fn index_scan_range() {
    let engine = seeded_kv_engine();
    let txn = engine.begin(true).unwrap();
    let collection = txn.collection("test").unwrap();

    // score > 70 AND score < 90 → Charlie(80) only
    let plan = Plan::Find(Node::IndexScan {
        collection,
        field: "score".into(),
        range: IndexScanRange::Range {
            lower: Some((bson::Bson::Int32(70), false)), // exclusive
            upper: Some((bson::Bson::Int32(90), false)), // exclusive
        },
        direction: ScanDirection::Forward,
        limit: None,
        covered: false,
    });
    let ids = collect_ids(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(ids, vec!["3"]); // score 80 only
}

#[test]
fn index_scan_range_desc() {
    let engine = seeded_kv_engine();
    let txn = engine.begin(true).unwrap();
    let collection = txn.collection("test").unwrap();

    // score >= 70 AND score <= 90, descending
    let plan = Plan::Find(Node::IndexScan {
        collection,
        field: "score".into(),
        range: IndexScanRange::Range {
            lower: Some((bson::Bson::Int32(70), true)), // inclusive
            upper: Some((bson::Bson::Int32(90), true)), // inclusive
        },
        direction: ScanDirection::Reverse,
        limit: None,
        covered: false,
    });
    let ids = collect_ids(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(ids, vec!["2", "3", "1"]); // descending: 90, 80, 70
}

#[test]
fn index_scan_range_with_limit() {
    let engine = seeded_kv_engine();
    let txn = engine.begin(true).unwrap();
    let collection = txn.collection("test").unwrap();

    // score >= 70, limit 2
    let plan = Plan::Find(Node::IndexScan {
        collection,
        field: "score".into(),
        range: IndexScanRange::Range {
            lower: Some((bson::Bson::Int32(70), true)), // inclusive
            upper: None,
        },
        direction: ScanDirection::Forward,
        limit: Some(2),
        covered: false,
    });
    let ids = collect_ids(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(ids, vec!["1", "3"]); // score 70, 80
}

#[test]
fn index_scan_range_empty_result() {
    let engine = seeded_kv_engine();
    let txn = engine.begin(true).unwrap();
    let collection = txn.collection("test").unwrap();

    // score > 100 → nothing
    let plan = Plan::Find(Node::IndexScan {
        collection,
        field: "score".into(),
        range: IndexScanRange::Range {
            lower: Some((bson::Bson::Int32(100), false)), // exclusive
            upper: None,
        },
        direction: ScanDirection::Forward,
        limit: None,
        covered: false,
    });
    let ids = collect_ids(Executor::new(&txn).execute(plan).unwrap());
    assert!(ids.is_empty());
}

#[test]
fn index_scan_gt_desc() {
    let engine = seeded_kv_engine();
    let txn = engine.begin(true).unwrap();
    let collection = txn.collection("test").unwrap();

    // score > 70, descending → Bob(90), Charlie(80)
    let plan = Plan::Find(Node::IndexScan {
        collection,
        field: "score".into(),
        range: IndexScanRange::Range {
            lower: Some((bson::Bson::Int32(70), false)), // exclusive
            upper: None,
        },
        direction: ScanDirection::Reverse,
        limit: None,
        covered: false,
    });
    let ids = collect_ids(Executor::new(&txn).execute(plan).unwrap());
    assert_eq!(ids, vec!["2", "3"]); // descending: 90, 80
}
