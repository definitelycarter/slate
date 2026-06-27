#![allow(dead_code)]

use bson::raw::RawDocumentBuf;
use bson::rawdoc;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use slate_db::DatabaseBuilder;
use slate_db::bench::Database;
use slate_db::v2::{IndexOptions, UdfFunction};
use slate_db::{UdfError, Value};
use slate_store::MemoryStore;

// ── Constants ───────────────────────────────────────────────

pub const STATUSES: &[&str] = &["active", "rejected"];
pub const REC1: &[&str] = &["ProductA", "ProductB", "ProductC"];
pub const REC2: &[&str] = &["ProductX", "ProductY", "ProductZ"];
pub const REC3: &[&str] = &["Widget1", "Widget2", "Widget3"];
pub const TAGS: &[&str] = &[
    "renewal_due",
    "high_value",
    "churning",
    "new_customer",
    "enterprise",
];

// ── Helpers ─────────────────────────────────────────────────

pub fn generate_docs(n: usize) -> Vec<RawDocumentBuf> {
    (0..n)
        .map(|i| {
            rawdoc! {
                "_id": format!("rec-{i}"),
                "name": format!("User {i}"),
                "status": if i % 2 == 0 { "active" } else { "rejected" },
                "contacts_count": (i % 100) as i32,
                "product_recommendation1": "ProductA",
            }
        })
        .collect()
}

pub fn db_builder() -> DatabaseBuilder {
    DatabaseBuilder::new()
}

/// Create a seeded MemoryStore-backed Engine with `n` documents and indexes
/// on `status` and `contacts_count`.
pub fn seeded_engine(n: usize) -> Database<MemoryStore> {
    let engine = db_builder().open(MemoryStore::new()).unwrap();
    let txn = engine.begin(false).unwrap();
    engine.collections().create("test").execute(&txn).unwrap();
    engine
        .collection("test")
        .indexes()
        .create("status", IndexOptions::default())
        .execute(&txn)
        .unwrap();
    engine
        .collection("test")
        .indexes()
        .create("contacts_count", IndexOptions::default())
        .execute(&txn)
        .unwrap();
    let docs: Vec<bson::Document> = (0..n)
        .map(|i| {
            bson::doc! {
                "_id": format!("rec-{i}"),
                "name": format!("User {i}"),
                "status": if i % 2 == 0 { "active" } else { "rejected" },
                "contacts_count": (i % 100) as i32,
                "product_recommendation1": "ProductA",
            }
        })
        .collect();
    engine
        .collection("test")
        .insert_many(docs)
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();
    engine
}

/// A native UDF `double(x) = x * 2` — the same argument shape as a built-in
/// scalar function, so the bench's `udf` vs `builtin` cases differ only in the
/// per-row call.
fn double_udf(args: &[Value]) -> Result<Value, UdfError> {
    let x = args
        .first()
        .and_then(Value::as_bson)
        .and_then(|b| match b {
            bson::Bson::Int32(i) => Some(i64::from(*i)),
            bson::Bson::Int64(i) => Some(*i),
            _ => None,
        })
        .unwrap_or(0);
    Ok(Value::defined(x * 2))
}

/// An engine with `n` docs (numeric field `x`), the native UDF `double`
/// registered in the bag, and a binding `udf.double -> double` on the
/// collection — for the UDF query scenario (`SELECT VALUE udf.double(c.x)`).
pub fn udf_engine(n: usize) -> Database<MemoryStore> {
    let engine = db_builder()
        .with_udf("double", double_udf)
        .open(MemoryStore::new())
        .unwrap();
    let txn = engine.begin(false).unwrap();
    engine.collections().create("test").execute(&txn).unwrap();
    engine
        .collection("test")
        .functions()
        .create("double", UdfFunction::from_name("double"))
        .execute(&txn)
        .unwrap();
    let docs: Vec<bson::Document> = (0..n)
        .map(|i| bson::doc! { "_id": format!("rec-{i}"), "x": (i % 100) as i32 })
        .collect();
    engine
        .collection("test")
        .insert_many(docs)
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();
    engine
}

pub fn generate_realistic_doc(rng: &mut StdRng, seq: usize) -> bson::Document {
    let mut doc = bson::doc! {
        "_id": format!("rec-{seq}"),
        "name": format!("Company-{seq}"),
        "status": STATUSES[rng.gen_range(0..STATUSES.len())],
        "contacts_count": rng.gen_range(0_i32..100),
        "product_recommendation1": REC1[rng.gen_range(0..REC1.len())],
        "product_recommendation2": REC2[rng.gen_range(0..REC2.len())],
        "product_recommendation3": REC3[rng.gen_range(0..REC3.len())],
    };

    let tag_count = rng.gen_range(2..=4);
    let tags: Vec<&str> = (0..tag_count)
        .map(|_| TAGS[rng.gen_range(0..TAGS.len())])
        .collect();
    doc.insert("tags", tags);

    if rng.gen_ratio(7, 10) {
        let epoch_secs = rng.gen_range(1_700_000_000_i64..1_740_000_000);
        doc.insert(
            "last_contacted_at",
            bson::Bson::DateTime(bson::DateTime::from_millis(epoch_secs * 1000)),
        );
    }

    if rng.gen_bool(0.5) {
        doc.insert("notes", format!("Note for {seq}"));
    }

    doc
}

pub fn generate_realistic_batch(count: usize) -> Vec<bson::Document> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..count)
        .map(|i| generate_realistic_doc(&mut rng, i))
        .collect()
}

/// Seed an array-valued `tags` corpus for the multikey-containment benchmark.
/// Each doc carries 2–4 common tags plus a selective `"rare"` tag on ~1% of
/// documents. With `indexed`, a `tags.[]` multikey index is created so
/// `ARRAY_CONTAINS(c.tags, "rare")` plans as a multikey `IndexScan`; without it
/// the same query full-scans — the before/after the bench contrasts.
pub fn array_tags_engine(n: usize, indexed: bool) -> Database<MemoryStore> {
    let engine = db_builder().open(MemoryStore::new()).unwrap();
    let txn = engine.begin(false).unwrap();
    engine.collections().create("bench").execute(&txn).unwrap();
    if indexed {
        engine
            .collection("bench")
            .indexes()
            .create("tags.[]", IndexOptions::default())
            .execute(&txn)
            .unwrap();
    }
    let mut rng = StdRng::seed_from_u64(7);
    let docs: Vec<bson::Document> = (0..n)
        .map(|i| {
            let tag_count = rng.gen_range(2..=4);
            let mut tags: Vec<&str> = (0..tag_count)
                .map(|_| TAGS[rng.gen_range(0..TAGS.len())])
                .collect();
            if rng.gen_ratio(1, 100) {
                tags.push("rare");
            }
            bson::doc! {
                "_id": format!("rec-{i}"),
                "name": format!("Company-{i}"),
                "tags": tags,
            }
        })
        .collect();
    for chunk in docs.chunks(1000) {
        engine
            .collection("bench")
            .insert_many(chunk.to_vec())
            .execute(&txn)
            .unwrap();
    }
    txn.commit().unwrap();
    engine
}

/// Seed a corpus with a high-cardinality string field `name` (`"Company-{i}"`)
/// for the string-pushdown benchmarks (sargability increments B/C). With
/// `indexed`, a scalar index on `name` is created so `STRINGEQUALS` (Eq seek) and
/// `STARTSWITH` / `LIKE 'pre%'` (prefix range) plan as an `IndexScan`; without it
/// the same query full-scans — the before/after the bench contrasts.
pub fn string_index_engine(n: usize, indexed: bool) -> Database<MemoryStore> {
    let engine = db_builder().open(MemoryStore::new()).unwrap();
    let txn = engine.begin(false).unwrap();
    engine.collections().create("bench").execute(&txn).unwrap();
    if indexed {
        engine
            .collection("bench")
            .indexes()
            .create("name", IndexOptions::default())
            .execute(&txn)
            .unwrap();
    }
    let docs: Vec<bson::Document> = (0..n)
        .map(|i| {
            bson::doc! {
                "_id": format!("rec-{i}"),
                "name": format!("Company-{i}"),
            }
        })
        .collect();
    for chunk in docs.chunks(1000) {
        engine
            .collection("bench")
            .insert_many(chunk.to_vec())
            .execute(&txn)
            .unwrap();
    }
    txn.commit().unwrap();
    engine
}

/// Seed a *realistic* (heavy) corpus with a `meta.note` field nested inside each
/// document, and a single-field index on the dotted path `meta.note`. The body is
/// the same heavy doc as `realistic_seeded_engine` (so the covered dotted
/// projection is directly comparable to the top-level `query_indexed_eq_proj`, and
/// covering's win — skipping the heavy fetch — is exercised, not masked by tiny
/// docs); half the rows are `"active"` (the matched set). Always indexed: the
/// covering before/after is `main` (a dotted field bails → `IndexScan →
/// KeyLookup`) vs the branch (covers, synthesizing `{meta: {note}}`).
pub fn nested_indexed_engine(n: usize) -> Database<MemoryStore> {
    let engine = db_builder().open(MemoryStore::new()).unwrap();
    let txn = engine.begin(false).unwrap();
    engine.collections().create("bench").execute(&txn).unwrap();
    engine
        .collection("bench")
        .indexes()
        .create("meta.note", IndexOptions::default())
        .execute(&txn)
        .unwrap();
    let mut rng = StdRng::seed_from_u64(42);
    let docs: Vec<bson::Document> = (0..n)
        .map(|i| {
            let mut doc = generate_realistic_doc(&mut rng, i);
            doc.insert(
                "meta",
                bson::doc! {
                    "note": if i % 2 == 0 { "active" } else { "rejected" },
                    "tag": format!("t{i}"),
                },
            );
            doc
        })
        .collect();
    for chunk in docs.chunks(1000) {
        engine
            .collection("bench")
            .insert_many(chunk.to_vec())
            .execute(&txn)
            .unwrap();
    }
    txn.commit().unwrap();
    engine
}

/// Seed a corpus with a *compound* index on `["status", "contacts_count"]` and
/// no single-field `status` index, so a leading-equality query
/// (`WHERE c.status = "active"`) plans as a `CompoundIndexScan` on the leftmost
/// prefix. Used to measure Part A of the covering-index RFC: dropping the
/// redundant residual `Filter` the planner used to keep above the `KeyLookup`
/// (the compound scan node already rechecks the equality against the entry).
pub fn compound_indexed_engine(n: usize) -> Database<MemoryStore> {
    let engine = db_builder().open(MemoryStore::new()).unwrap();
    let txn = engine.begin(false).unwrap();
    engine.collections().create("bench").execute(&txn).unwrap();
    engine
        .collection("bench")
        .indexes()
        .create(["status", "contacts_count"], IndexOptions::default())
        .execute(&txn)
        .unwrap();
    let docs = generate_realistic_batch(n);
    for chunk in docs.chunks(1000) {
        engine
            .collection("bench")
            .insert_many(chunk.to_vec())
            .execute(&txn)
            .unwrap();
    }
    txn.commit().unwrap();
    engine
}

pub fn realistic_seeded_engine(n: usize) -> Database<MemoryStore> {
    let engine = db_builder().open(MemoryStore::new()).unwrap();
    let txn = engine.begin(false).unwrap();
    engine.collections().create("bench").execute(&txn).unwrap();
    engine
        .collection("bench")
        .indexes()
        .create("status", IndexOptions::default())
        .execute(&txn)
        .unwrap();
    engine
        .collection("bench")
        .indexes()
        .create("contacts_count", IndexOptions::default())
        .execute(&txn)
        .unwrap();
    let docs = generate_realistic_batch(n);
    for chunk in docs.chunks(1000) {
        engine
            .collection("bench")
            .insert_many(chunk.to_vec())
            .execute(&txn)
            .unwrap();
    }
    txn.commit().unwrap();
    engine
}
