//! `open_index_cursor` — the seekable equality-index cursor the index-
//! intersection skip-merge zig-zags (Index Intersection RFC, step 2).
//!
//! Pins the cursor contract: it yields exactly what an `Eq` `scan_index` would
//! (TTL-expiry honoured, value-matched), in doc-id order, and `seek` lands at-or-
//! past a raw doc-id target — exercising both galloping paths (cheap `next()` for
//! a small gap, a fresh range seek for a large one) and the reverse direction.

use bson::{Bson, RawBson};
use slate_engine::{Catalog, DEFAULT_CF, Engine, EngineTransaction, IndexCursor, KvEngine};
use slate_store::MemoryStore;

/// An engine with collection `c` (index on `v`) seeded with `{_id, v:"x"}` for
/// each id in `ids`, all sharing the value `"x"` so they form one equality
/// stream. Ids are inserted in the given order; the index returns them sorted.
fn seed(ids: &[&str]) -> KvEngine<MemoryStore> {
    let engine = KvEngine::new(MemoryStore::new());
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "c", &Default::default())
        .unwrap();
    txn.create_index(DEFAULT_CF, "c", "v").unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();
    for id in ids {
        txn.put(&handle, &bson::rawdoc! { "_id": *id, "v": "x" })
            .unwrap();
    }
    txn.commit().unwrap();
    engine
}

/// Zero-padded ids `id-000 .. id-{n-1}` (same length → lexicographic order =
/// numeric order, which is the doc-id byte order for equal-length strings).
fn ids(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("id-{i:03}")).collect()
}

fn id_of(entry: &slate_engine::IndexEntry) -> String {
    match entry.doc_id().unwrap() {
        RawBson::String(s) => s,
        other => panic!("unexpected doc_id type: {other:?}"),
    }
}

/// Drain the cursor via `advance`, collecting the doc-ids it yields.
fn drain(cursor: &mut Box<dyn IndexCursor + '_>) -> Vec<String> {
    let mut out = Vec::new();
    while let Some(entry) = cursor.peek() {
        out.push(id_of(entry));
        cursor.advance().unwrap();
    }
    out
}

/// The raw `doc_id_bytes` of the entry whose id == `target` (owned copy), by
/// walking a fresh forward cursor — exactly the bytes the merge would seek on.
fn bytes_for(engine: &KvEngine<MemoryStore>, target: &str) -> Vec<u8> {
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();
    let mut cursor = txn
        .open_index_cursor(&handle, "v", &Bson::String("x".into()), false)
        .unwrap();
    while let Some(entry) = cursor.peek() {
        if id_of(entry) == target {
            return entry.doc_id_bytes().unwrap().to_vec();
        }
        cursor.advance().unwrap();
    }
    panic!("target {target} not found");
}

#[test]
fn forward_advance_yields_doc_id_order() {
    let names = ids(10);
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    // Insert shuffled; the index must still yield sorted doc-id order.
    let mut shuffled = refs.clone();
    shuffled.reverse();
    let engine = seed(&shuffled);
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();
    let mut cursor = txn
        .open_index_cursor(&handle, "v", &Bson::String("x".into()), false)
        .unwrap();
    assert_eq!(drain(&mut cursor), names);
}

#[test]
fn seek_small_gap_lands_exactly_via_next() {
    // Gap of 5 (< GALLOP_LIMIT = 8): served by cheap `next()` advances.
    let names = ids(20);
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let engine = seed(&refs);
    let target = bytes_for(&engine, "id-005");

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();
    let mut cursor = txn
        .open_index_cursor(&handle, "v", &Bson::String("x".into()), false)
        .unwrap();
    cursor.seek(&target).unwrap();
    assert_eq!(id_of(cursor.peek().expect("landed")), "id-005");
    // And the stream continues correctly from the landing point.
    assert_eq!(drain(&mut cursor)[0], "id-005");
}

#[test]
fn seek_large_gap_lands_exactly_via_reseek() {
    // Gap of 40 (>> GALLOP_LIMIT): the galloping must fall through to one fresh
    // range seek and still land exactly.
    let names = ids(50);
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let engine = seed(&refs);
    let target = bytes_for(&engine, "id-040");

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();
    let mut cursor = txn
        .open_index_cursor(&handle, "v", &Bson::String("x".into()), false)
        .unwrap();
    cursor.seek(&target).unwrap();
    assert_eq!(id_of(cursor.peek().expect("landed")), "id-040");

    // A second, small seek from there exercises the next()-path after a reseek.
    let next = bytes_for(&engine, "id-045");
    cursor.seek(&next).unwrap();
    assert_eq!(id_of(cursor.peek().expect("landed")), "id-045");
}

#[test]
fn seek_absent_target_lands_on_next_greater() {
    // Capture id-025's bytes, then delete it: seeking those bytes must land on
    // the first entry strictly greater (id-026), not skip past it.
    let names = ids(50);
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let engine = seed(&refs);
    let target = bytes_for(&engine, "id-025");

    let wtxn = engine.begin(false).unwrap();
    let handle = wtxn.collection(DEFAULT_CF, "c").unwrap();
    wtxn.delete(&handle, &bson::RawBsonRef::String("id-025"))
        .unwrap();
    wtxn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();
    let mut cursor = txn
        .open_index_cursor(&handle, "v", &Bson::String("x".into()), false)
        .unwrap();
    cursor.seek(&target).unwrap();
    assert_eq!(id_of(cursor.peek().expect("landed")), "id-026");
}

#[test]
fn seek_past_end_exhausts_cursor() {
    let names = ids(10);
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let engine = seed(&refs);
    let last = bytes_for(&engine, "id-009");

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();
    let mut cursor = txn
        .open_index_cursor(&handle, "v", &Bson::String("x".into()), false)
        .unwrap();
    cursor.seek(&last).unwrap();
    assert_eq!(id_of(cursor.peek().expect("at last")), "id-009");
    // Advance once more: exhausted.
    cursor.advance().unwrap();
    assert!(cursor.peek().is_none());
}

#[test]
fn expired_entries_are_skipped() {
    // id-003 carries a long-expired TTL; the cursor must not yield it (advance
    // and seek both skip it), matching `scan_index`.
    let engine = KvEngine::new(MemoryStore::new());
    let txn = engine.begin(false).unwrap();
    txn.create_collection(DEFAULT_CF, "c", &Default::default())
        .unwrap();
    txn.create_index(DEFAULT_CF, "c", "v").unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();
    let expired = bson::DateTime::from_millis(1);
    for i in 0..6 {
        let id = format!("id-{i:03}");
        if i == 3 {
            txn.put(
                &handle,
                &bson::rawdoc! { "_id": id, "v": "x", "ttl": expired },
            )
            .unwrap();
        } else {
            txn.put(&handle, &bson::rawdoc! { "_id": id, "v": "x" })
                .unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();
    let mut cursor = txn
        .open_index_cursor(&handle, "v", &Bson::String("x".into()), false)
        .unwrap();
    let got = drain(&mut cursor);
    assert_eq!(
        got,
        vec!["id-000", "id-001", "id-002", "id-004", "id-005"],
        "id-003 (expired) must be skipped"
    );
}

#[test]
fn reverse_advance_and_seek() {
    let names = ids(30);
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let engine = seed(&refs);

    // Reverse advance yields descending doc-id order.
    let txn = engine.begin(true).unwrap();
    let handle = txn.collection(DEFAULT_CF, "c").unwrap();
    let mut cursor = txn
        .open_index_cursor(&handle, "v", &Bson::String("x".into()), true)
        .unwrap();
    let mut expected_desc = names.clone();
    expected_desc.reverse();
    // Snapshot the first 3 to confirm direction before the seek consumes the rest.
    let first_three: Vec<String> = {
        let mut v = Vec::new();
        for _ in 0..3 {
            v.push(id_of(cursor.peek().unwrap()));
            cursor.advance().unwrap();
        }
        v
    };
    assert_eq!(first_three, vec!["id-029", "id-028", "id-027"]);

    // Reverse seek (≤): from id-026, seek to id-010 lands exactly, then continues
    // descending.
    let target = bytes_for(&engine, "id-010");
    cursor.seek(&target).unwrap();
    assert_eq!(id_of(cursor.peek().expect("landed")), "id-010");
    cursor.advance().unwrap();
    assert_eq!(id_of(cursor.peek().expect("next")), "id-009");
}
