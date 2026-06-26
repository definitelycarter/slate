//! Watch / stream (change-detection) integration tests across the 2×2 surface:
//! {BSON filter (`watch`/`stream`), SQL filter (`watch_query`/`stream_query`)}
//! × {callback push, cursor pull}. Register a filter, write, commit, and assert
//! the delivered batch — exercising the whole stack (front-end → mutation-node
//! capture → commit-time emit → callback or subscription buffer).

mod common;
use common::*;

use std::sync::{Arc, Mutex};

use bson::{Bson, doc};
use slate_db::{ChangeEvent, DEFAULT_CF, Database, WatchStream};
use slate_store::MemoryStore;

const SENSORS: &str = "sensors";

/// A `Send + Sync` sink that records each delivered batch (one per commit).
type Batches = Arc<Mutex<Vec<Vec<ChangeEvent>>>>;

fn collector() -> (Batches, impl Fn(&[ChangeEvent]) + Send + Sync + 'static) {
    let batches: Batches = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&batches);
    let callback = move |events: &[ChangeEvent]| {
        sink.lock().unwrap().push(events.to_vec());
    };
    (batches, callback)
}

/// Number of batches delivered so far.
fn batch_count(b: &Batches) -> usize {
    b.lock().unwrap().len()
}

/// The single event in the single delivered batch (panics otherwise).
fn only_event(b: &Batches) -> ChangeEvent {
    let guard = b.lock().unwrap();
    assert_eq!(guard.len(), 1, "expected exactly one batch");
    assert_eq!(guard[0].len(), 1, "expected exactly one event in the batch");
    guard[0][0].clone()
}

fn insert(db: &Database<MemoryStore>, doc: bson::Document) {
    let txn = db.begin(false).unwrap();
    txn.insert_one(DEFAULT_CF, SENSORS, doc)
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();
}

fn update_temp(db: &Database<MemoryStore>, id: &str, temp: i32) {
    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String(id.into()));
    txn.update_one(DEFAULT_CF, SENSORS, &filter, doc! { "temp": temp })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();
}

// ── Per-op match / no-match ──────────────────────────────────

#[test]
fn insert_matching_fires_insert_event() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let (batches, cb) = collector();
    let _handle = db
        .watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80", cb)
        .unwrap();

    insert(&db, doc! { "_id": "a", "temp": 90 });

    match only_event(&batches) {
        ChangeEvent::Insert { doc } => assert_eq!(doc.get_i32("temp").unwrap(), 90),
        other => panic!("expected Insert, got {other:?}"),
    }
}

#[test]
fn insert_below_threshold_fires_nothing() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let (batches, cb) = collector();
    let _handle = db
        .watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80", cb)
        .unwrap();

    insert(&db, doc! { "_id": "a", "temp": 50 });

    assert_eq!(
        batch_count(&batches),
        0,
        "no match → no batch (no empty batches)"
    );
}

#[test]
fn delete_in_set_fires_delete_event() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    insert(&db, doc! { "_id": "a", "temp": 90 });

    let (batches, cb) = collector();
    let _handle = db
        .watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80", cb)
        .unwrap();

    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("a".into()));
    txn.delete_one(DEFAULT_CF, SENSORS, &filter)
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    match only_event(&batches) {
        ChangeEvent::Delete { doc } => assert_eq!(doc.get_i32("temp").unwrap(), 90),
        other => panic!("expected Delete, got {other:?}"),
    }
}

#[test]
fn match_all_with_no_where_fires_on_every_write() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let (batches, cb) = collector();
    let _handle = db
        .watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s", cb)
        .unwrap();

    insert(&db, doc! { "_id": "a", "temp": 1 });
    insert(&db, doc! { "_id": "b", "temp": 999 });

    assert_eq!(batch_count(&batches), 2);
}

// ── Set-transition recasting (update) ────────────────────────

#[test]
fn update_staying_in_set_carries_old_and_new() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    insert(&db, doc! { "_id": "a", "temp": 85 });

    let (batches, cb) = collector();
    let _handle = db
        .watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80", cb)
        .unwrap();

    update_temp(&db, "a", 95);

    match only_event(&batches) {
        ChangeEvent::Update { old, new } => {
            assert_eq!(old.get_i32("temp").unwrap(), 85);
            assert_eq!(new.get_i32("temp").unwrap(), 95);
        }
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn update_entering_set_recasts_to_insert() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    insert(&db, doc! { "_id": "a", "temp": 50 }); // below threshold, not in set

    let (batches, cb) = collector();
    let _handle = db
        .watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80", cb)
        .unwrap();

    update_temp(&db, "a", 90); // crosses into the set

    match only_event(&batches) {
        ChangeEvent::Insert { doc } => assert_eq!(doc.get_i32("temp").unwrap(), 90),
        other => panic!("expected Insert (entered set), got {other:?}"),
    }
}

#[test]
fn update_leaving_set_recasts_to_delete() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    insert(&db, doc! { "_id": "a", "temp": 90 }); // in set

    let (batches, cb) = collector();
    let _handle = db
        .watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80", cb)
        .unwrap();

    update_temp(&db, "a", 50); // drops out of the set

    match only_event(&batches) {
        ChangeEvent::Delete { doc } => assert_eq!(doc.get_i32("temp").unwrap(), 90),
        other => panic!("expected Delete (left set), got {other:?}"),
    }
}

#[test]
fn update_outside_set_on_both_sides_fires_nothing() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    insert(&db, doc! { "_id": "a", "temp": 10 });

    let (batches, cb) = collector();
    let _handle = db
        .watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80", cb)
        .unwrap();

    update_temp(&db, "a", 20); // still below threshold

    assert_eq!(batch_count(&batches), 0);
}

// ── Coalesce by pk within a commit ───────────────────────────

#[test]
fn two_updates_to_same_doc_in_one_commit_coalesce() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    insert(&db, doc! { "_id": "a", "temp": 85 });

    let (batches, cb) = collector();
    let _handle = db
        .watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80", cb)
        .unwrap();

    // Two mutations to the same document inside ONE transaction.
    let txn = db.begin(false).unwrap();
    let filter = eq_filter("_id", Bson::String("a".into()));
    txn.update_one(DEFAULT_CF, SENSORS, &filter, doc! { "temp": 90 })
        .unwrap()
        .drain()
        .unwrap();
    txn.update_one(DEFAULT_CF, SENSORS, &filter, doc! { "temp": 95 })
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();

    // One net change: old from the first capture (85), new from the last (95).
    match only_event(&batches) {
        ChangeEvent::Update { old, new } => {
            assert_eq!(old.get_i32("temp").unwrap(), 85);
            assert_eq!(new.get_i32("temp").unwrap(), 95);
        }
        other => panic!("expected one coalesced Update, got {other:?}"),
    }
}

// ── Rollback drops events ────────────────────────────────────

#[test]
fn rollback_delivers_no_events() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let (batches, cb) = collector();
    let _handle = db
        .watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80", cb)
        .unwrap();

    let txn = db.begin(false).unwrap();
    txn.insert_one(DEFAULT_CF, SENSORS, doc! { "_id": "a", "temp": 90 })
        .unwrap()
        .drain()
        .unwrap();
    txn.rollback().unwrap();

    assert_eq!(
        batch_count(&batches),
        0,
        "rolled-back writes deliver nothing"
    );
}

// ── unwatch / Drop ───────────────────────────────────────────

#[test]
fn dropping_the_handle_stops_delivery() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let (batches, cb) = collector();
    let handle = db
        .watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80", cb)
        .unwrap();

    insert(&db, doc! { "_id": "a", "temp": 90 });
    assert_eq!(batch_count(&batches), 1);

    handle.unwatch(); // explicit unregister (same as drop)

    insert(&db, doc! { "_id": "b", "temp": 91 });
    assert_eq!(batch_count(&batches), 1, "no delivery after unwatch");
}

#[test]
fn watch_dropped_by_scope_unregisters() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let (batches, cb) = collector();
    {
        let _handle = db
            .watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80", cb)
            .unwrap();
        insert(&db, doc! { "_id": "a", "temp": 90 });
    } // _handle dropped here → unregistered

    insert(&db, doc! { "_id": "b", "temp": 90 });
    assert_eq!(batch_count(&batches), 1);
}

// ── Collection-drop independence ─────────────────────────────

#[test]
fn dropping_another_collection_does_not_disturb_a_watch() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    create_collection(&db, "other");

    let (batches, cb) = collector();
    let _handle = db
        .watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80", cb)
        .unwrap();

    // Dropping an unrelated collection proceeds (a watch holds no lock).
    let txn = db.begin(false).unwrap();
    txn.drop_collection(DEFAULT_CF, "other").unwrap();
    txn.commit().unwrap();

    insert(&db, doc! { "_id": "a", "temp": 90 });
    assert_eq!(batch_count(&batches), 1);
}

#[test]
fn dropping_the_watched_collection_proceeds_and_goes_cold() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let (batches, cb) = collector();
    let _handle = db
        .watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80", cb)
        .unwrap();

    // The watch must not block dropping its own collection.
    let txn = db.begin(false).unwrap();
    txn.drop_collection(DEFAULT_CF, SENSORS).unwrap();
    txn.commit().unwrap();

    assert_eq!(batch_count(&batches), 0);
}

// ── SQL filter parse / rejection ─────────────────────────────

#[test]
fn rejected_clauses_are_errors_at_registration() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let noop = |_: &[ChangeEvent]| {};

    assert!(
        db.watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s ORDER BY s.temp", noop)
            .is_err()
    );
    assert!(
        db.watch_query(DEFAULT_CF, SENSORS, "SELECT * FROM s GROUP BY s.temp", noop)
            .is_err()
    );
    assert!(
        db.watch_query(DEFAULT_CF, SENSORS, "SELECT VALUE c FROM c LIMIT 5", noop)
            .is_err()
    );
    assert!(
        db.watch_query(DEFAULT_CF, SENSORS, "SELECT s.temp FROM s", noop)
            .is_err()
    );
    assert!(
        db.watch_query(
            DEFAULT_CF,
            SENSORS,
            "SELECT * FROM s WHERE s.temp > @t",
            noop
        )
        .is_err()
    );
    // A bare WHERE filter is accepted.
    assert!(
        db.watch_query(
            DEFAULT_CF,
            SENSORS,
            "SELECT * FROM s WHERE s.temp > 80",
            noop
        )
        .is_ok()
    );
}

// ── BSON-filter watch (`db.watch`) ───────────────────────────
//
// Same detection core as `watch_query`, just a `find`-style filter document
// instead of a SQL string. These assert the BSON front-end wires through and
// recasts at the set boundary identically.

#[test]
fn bson_watch_insert_matching_fires_insert() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let (batches, cb) = collector();
    // `{ temp: { $gt: 80 } }` — the same filter `find` would take.
    let _handle = db
        .watch(DEFAULT_CF, SENSORS, doc! { "temp": { "$gt": 80 } }, cb)
        .unwrap();

    insert(&db, doc! { "_id": "a", "temp": 90 });

    match only_event(&batches) {
        ChangeEvent::Insert { doc } => assert_eq!(doc.get_i32("temp").unwrap(), 90),
        other => panic!("expected Insert, got {other:?}"),
    }
}

#[test]
fn bson_watch_below_threshold_fires_nothing() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let (batches, cb) = collector();
    let _handle = db
        .watch(DEFAULT_CF, SENSORS, doc! { "temp": { "$gt": 80 } }, cb)
        .unwrap();

    insert(&db, doc! { "_id": "a", "temp": 50 });

    assert_eq!(batch_count(&batches), 0, "no match → no batch");
}

#[test]
fn bson_watch_update_entering_set_recasts_to_insert() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    insert(&db, doc! { "_id": "a", "temp": 50 }); // not in set

    let (batches, cb) = collector();
    let _handle = db
        .watch(DEFAULT_CF, SENSORS, doc! { "temp": { "$gt": 80 } }, cb)
        .unwrap();

    update_temp(&db, "a", 90); // crosses into the set

    match only_event(&batches) {
        ChangeEvent::Insert { doc } => assert_eq!(doc.get_i32("temp").unwrap(), 90),
        other => panic!("expected Insert (entered set), got {other:?}"),
    }
}

#[test]
fn bson_watch_update_leaving_set_recasts_to_delete() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    insert(&db, doc! { "_id": "a", "temp": 90 }); // in set

    let (batches, cb) = collector();
    let _handle = db
        .watch(DEFAULT_CF, SENSORS, doc! { "temp": { "$gt": 80 } }, cb)
        .unwrap();

    update_temp(&db, "a", 50); // drops out of the set

    match only_event(&batches) {
        ChangeEvent::Delete { doc } => assert_eq!(doc.get_i32("temp").unwrap(), 90),
        other => panic!("expected Delete (left set), got {other:?}"),
    }
}

#[test]
fn bson_watch_empty_filter_is_match_all() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let (batches, cb) = collector();
    // `{}` — match every document, the BSON match-all.
    let _handle = db.watch(DEFAULT_CF, SENSORS, doc! {}, cb).unwrap();

    insert(&db, doc! { "_id": "a", "temp": 1 });
    insert(&db, doc! { "_id": "b", "temp": 999 });

    assert_eq!(batch_count(&batches), 2);
}

// ── Cursor (pull) delivery: `db.stream` / `db.stream_query` ───
//
// A stream is a long-lived subscription cursor the consumer drains; the same
// detection core, only the delivery differs (push to a bounded buffer instead
// of invoking a closure).

/// Drain every batch currently buffered on a stream (non-blocking).
fn drain(stream: &WatchStream) -> Vec<Vec<ChangeEvent>> {
    let mut out = Vec::new();
    while let Some(batch) = stream.try_next() {
        out.push(batch);
    }
    out
}

#[test]
fn stream_bson_drains_batches_across_commits() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let stream = db
        .stream(DEFAULT_CF, SENSORS, doc! { "temp": { "$gt": 80 } })
        .unwrap();

    // Two separate commits → two buffered batches, in order.
    insert(&db, doc! { "_id": "a", "temp": 90 });
    insert(&db, doc! { "_id": "b", "temp": 95 });
    // A non-matching write contributes no batch.
    insert(&db, doc! { "_id": "c", "temp": 10 });

    let batches = drain(&stream);
    assert_eq!(batches.len(), 2, "two matching commits, one empty dropped");
    match (&batches[0][0], &batches[1][0]) {
        (ChangeEvent::Insert { doc: d0 }, ChangeEvent::Insert { doc: d1 }) => {
            assert_eq!(d0.get_str("_id").unwrap(), "a");
            assert_eq!(d1.get_str("_id").unwrap(), "b");
        }
        other => panic!("expected two inserts in commit order, got {other:?}"),
    }
    assert!(!stream.lagged(), "a drained consumer never lags");
}

#[test]
fn stream_query_drains_batches_across_commits() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let stream = db
        .stream_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80")
        .unwrap();

    insert(&db, doc! { "_id": "a", "temp": 90 });
    insert(&db, doc! { "_id": "b", "temp": 95 });

    let batches = drain(&stream);
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].len(), 1);
    assert_eq!(batches[1].len(), 1);
}

#[test]
fn stream_query_rejects_set_operations() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    // Same SQL-clause restrictions as `watch_query`.
    assert!(
        db.stream_query(DEFAULT_CF, SENSORS, "SELECT * FROM s ORDER BY s.temp")
            .is_err()
    );
    assert!(
        db.stream_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80")
            .is_ok()
    );
}

#[test]
fn stream_blocking_consumer_receives_next_batch() {
    use std::sync::mpsc;
    use std::thread;

    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let stream = Arc::new(
        db.stream(DEFAULT_CF, SENSORS, doc! { "temp": { "$gt": 80 } })
            .unwrap(),
    );

    // A consumer thread blocks until the first matching commit lands.
    let consumer_stream = Arc::clone(&stream);
    let (tx, rx) = mpsc::channel();
    let consumer = thread::spawn(move || {
        if let Some(batch) = consumer_stream.next_blocking() {
            tx.send(batch.len()).unwrap();
        }
    });

    insert(&db, doc! { "_id": "a", "temp": 90 });

    // The blocked consumer wakes with the batch.
    let received = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    assert_eq!(received, 1);
    consumer.join().unwrap();
}

#[test]
fn stream_slow_consumer_lags_and_drops_without_blocking_writer() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    // A tiny capacity is the default in this test only via many commits; the
    // public default is large, so saturate it by never draining and writing
    // well past it. Use a custom-capacity stream via the public surface by
    // committing more than the default buffer holds.
    let stream = db
        .stream(DEFAULT_CF, SENSORS, doc! { "temp": { "$gt": 80 } })
        .unwrap();

    // Write more matching commits than the default buffer (1024) can hold,
    // never draining. The writer must NOT block; excess batches are dropped and
    // the stream marked lagged.
    for i in 0..1100 {
        insert(&db, doc! { "_id": format!("k{i}"), "temp": 90 });
    }

    // The writer completed (no deadlock) and the consumer is flagged lagged.
    assert!(
        stream.lagged(),
        "a never-draining consumer past capacity must be marked lagged"
    );
    // Reading the flag clears it; a subsequent read (after the burst) is false.
    assert!(!stream.lagged(), "lagged() clears on read");
    // The buffer still holds up-to-capacity batches the consumer can drain.
    assert!(
        !drain(&stream).is_empty(),
        "buffered batches remain drainable"
    );
}

#[test]
fn dropping_the_stream_stops_capture() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let stream = db
        .stream(DEFAULT_CF, SENSORS, doc! { "temp": { "$gt": 80 } })
        .unwrap();

    insert(&db, doc! { "_id": "a", "temp": 90 });
    assert_eq!(drain(&stream).len(), 1);

    drop(stream); // unregisters the watch

    // A fresh stream sees only writes after it subscribes — the dropped one is
    // gone, so this write is captured by the new stream alone.
    let stream2 = db
        .stream(DEFAULT_CF, SENSORS, doc! { "temp": { "$gt": 80 } })
        .unwrap();
    insert(&db, doc! { "_id": "b", "temp": 90 });
    assert_eq!(drain(&stream2).len(), 1);
}

#[test]
fn try_next_on_empty_stream_is_none() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    let stream = db
        .stream(DEFAULT_CF, SENSORS, doc! { "temp": { "$gt": 80 } })
        .unwrap();

    // Nothing written yet, and a non-matching write contributes no batch.
    assert!(stream.try_next().is_none());
    insert(&db, doc! { "_id": "a", "temp": 10 });
    assert!(stream.try_next().is_none(), "non-match buffers nothing");
}

#[test]
fn two_streams_on_one_collection_each_get_their_own_batches() {
    let (db, _dir) = temp_db();
    create_collection(&db, SENSORS);
    // One BSON stream, one SQL stream, both over the same set — both must see
    // the write (independent subscriptions, shared detection core).
    let s_bson = db
        .stream(DEFAULT_CF, SENSORS, doc! { "temp": { "$gt": 80 } })
        .unwrap();
    let s_sql = db
        .stream_query(DEFAULT_CF, SENSORS, "SELECT * FROM s WHERE s.temp > 80")
        .unwrap();

    insert(&db, doc! { "_id": "a", "temp": 90 });

    assert_eq!(drain(&s_bson).len(), 1);
    assert_eq!(drain(&s_sql).len(), 1);
}
