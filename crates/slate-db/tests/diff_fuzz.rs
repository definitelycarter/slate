//! Property/fuzz differential testing: v1 vs v2 through the **real Database
//! API** over randomly generated data + filters + options.
//!
//! Each iteration uses a fixed seed (= the iteration index) so any failure is
//! reproducible and the panic message prints the seed + filter + options. The
//! generator is *type-aware* over a fixed schema (so it can build sensible
//! filters) and deliberately avoids the handful of documented v1/v2
//! divergences (multikey range, `$ne`, distinct-null, nested-projection on a
//! missing parent — see `parity_audit` / the v1-removal blockers).

use bson::{Bson, Document, doc};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder, QueryEngine};
use slate_query::{FindOptions, Sort, SortDirection};
use slate_store::MemoryStore;

const COLL: &str = "fuzz";
const NAMES: &[&str] = &["ada", "alan", "grace", "kurt", "edsger"];
const TAGS: &[&str] = &["x", "y", "z", "w"];
const CITIES: &[&str] = &["nyc", "ldn", "sf"];
const REGEXES: &[&str] = &["^a", "a", "n$", "[xy]"];

// ── Data generation (fixed schema, random values / presence) ─────

/// A document with: pk `_id`; an `age` (mixed Int32/Int64/Double, sometimes
/// absent); a `name` (string), `active` (bool), `city` (string), each sometimes
/// absent; and `tags` (array of strings), sometimes absent/empty.
fn gen_doc(rng: &mut StdRng, i: usize) -> Document {
    // `seq` is always present and unique — a safe field for `sort + take` (v1's
    // index-ordered sort+take drops docs missing the sort field; see
    // `parity_audit::sort_take_on_missing_field_is_a_v1_bug`).
    let mut d = doc! { "_id": format!("d{i:02}"), "seq": i as i32 };
    if rng.gen_bool(0.85) {
        // `age` is indexed and a single numeric type. Mixing Int32/Int64/Double
        // in an *indexed* field triggers a documented engine gap (type-tagged
        // index keys → range bounds under-return other-typed docs); numeric
        // coercion is covered separately in `parity_audit`.
        d.insert("age", Bson::Int32(rng.gen_range(0..5)));
    }
    if rng.gen_bool(0.8) {
        d.insert("name", *NAMES.choose(rng).unwrap());
    }
    if rng.gen_bool(0.6) {
        d.insert("active", rng.gen_bool(0.5));
    }
    if rng.gen_bool(0.6) {
        d.insert("city", *CITIES.choose(rng).unwrap());
    }
    if rng.gen_bool(0.7) {
        let k = rng.gen_range(0..4);
        let tags: Vec<&str> = (0..k).map(|_| *TAGS.choose(rng).unwrap()).collect();
        d.insert("tags", tags);
    }
    d
}

// ── Filter generation (parity-supported subset only) ─────────────

fn gen_value_for(rng: &mut StdRng, field: &str) -> Bson {
    match field {
        "age" => Bson::Int32(rng.gen_range(0..5)),
        "name" => Bson::String((*NAMES.choose(rng).unwrap()).into()),
        "city" => Bson::String((*CITIES.choose(rng).unwrap()).into()),
        "active" => Bson::Boolean(rng.gen_bool(0.5)),
        _ => Bson::String((*TAGS.choose(rng).unwrap()).into()),
    }
}

/// A non-empty predicate (never `{}` — v1 rejects empty sub-filters in
/// `$and`/`$or`). Match-all is added only at the top level by the caller.
fn gen_filter(rng: &mut StdRng, depth: u32) -> Document {
    // At depth 0 only leaf predicates (0..5); deeper, also $and/$or.
    let choice = if depth == 0 {
        rng.gen_range(0..5)
    } else {
        rng.gen_range(0..7)
    };
    match choice {
        0 => {
            // scalar equality
            let f = *["age", "name", "city", "active"].choose(rng).unwrap();
            doc! { f: gen_value_for(rng, f) }
        }
        1 => {
            // numeric range on the scalar `age` (range on arrays diverges)
            let op = *["$gt", "$gte", "$lt", "$lte"].choose(rng).unwrap();
            doc! { "age": { op: rng.gen_range(0..5) } }
        }
        2 => {
            let f = *["age", "name", "tags", "missing"].choose(rng).unwrap();
            doc! { f: { "$exists": rng.gen_bool(0.5) } }
        }
        3 => {
            // array membership — implicit form, which both engines evaluate
            // correctly anywhere (including inside $and/$or). The explicit
            // `.[]` form is only correct in v1 as a *standalone indexed*
            // predicate (it's always-false when evaluated inside logic — a v1
            // bug v2 fixes), so it's exercised by the dedicated
            // `parity_audit::explicit_multikey_index_query` instead.
            doc! { "tags": *TAGS.choose(rng).unwrap() }
        }
        4 => {
            // $regex on a string field
            doc! { "name": { "$regex": *REGEXES.choose(rng).unwrap() } }
        }
        5 => doc! { "$and": [ gen_filter(rng, depth - 1), gen_filter(rng, depth - 1) ] },
        _ => doc! { "$or": [ gen_filter(rng, depth - 1), gen_filter(rng, depth - 1) ] },
    }
}

// ── Options generation ───────────────────────────────────────────

fn dir(rng: &mut StdRng) -> SortDirection {
    if rng.gen_bool(0.5) {
        SortDirection::Asc
    } else {
        SortDirection::Desc
    }
}

fn gen_options(rng: &mut StdRng) -> FindOptions {
    // No skip/take: v1's `sort+skip+take` optimization is buggy in several ways
    // (drops docs missing an indexed sort field; doesn't apply the sort when the
    // filter uses an index), so v1 is not a reliable oracle for limit queries.
    // v2's limit correctness is pinned separately in `parity_audit`. We fuzz
    // filters + full-sort ordering, where both engines agree.
    let mut opts = FindOptions::default();
    if rng.gen_bool(0.5) {
        // 1–2 maybe-missing scalar keys + `_id` tiebreaker (appended by the
        // caller). Both engines full-sort, so missing values and ties match.
        let n = rng.gen_range(1..=2);
        let mut fields = vec!["age", "name", "city"];
        fields.shuffle(rng);
        opts.sort = fields
            .into_iter()
            .take(n)
            .map(|f| Sort {
                field: f.into(),
                direction: dir(rng),
            })
            .collect();
    }
    if rng.gen_bool(0.4) {
        // project a subset of top-level scalar columns (nested projection on a
        // missing parent is a documented divergence — avoid it).
        let mut cols = vec!["name", "age", "active", "city"];
        cols.shuffle(rng);
        let n = rng.gen_range(1..=3);
        opts.columns = Some(cols.into_iter().take(n).map(String::from).collect());
    }
    opts
}

// ── Harness ──────────────────────────────────────────────────────

fn build_db(engine: QueryEngine, docs: &[Document]) -> Database<MemoryStore> {
    let db = DatabaseBuilder::new()
        .query_engine(engine)
        .open(MemoryStore::new())
        .unwrap();
    let txn = db.begin(false).unwrap();
    txn.create_collection(&CollectionConfig {
        name: COLL.into(),
        ..Default::default()
    })
    .unwrap();
    for f in ["age", "name"] {
        txn.create_index(DEFAULT_CF, COLL, f).unwrap();
    }
    txn.create_index(DEFAULT_CF, COLL, "tags.[]").unwrap();
    txn.insert_many(DEFAULT_CF, COLL, docs.to_vec())
        .unwrap()
        .drain()
        .unwrap();
    txn.commit().unwrap();
    db
}

/// Canonical (field-order-insensitive) form of a document.
fn canon(d: &Document) -> std::collections::BTreeMap<String, Bson> {
    d.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn run(
    engine: QueryEngine,
    docs: &[Document],
    filter: Document,
    options: FindOptions,
) -> Vec<std::collections::BTreeMap<String, Bson>> {
    let db = build_db(engine, docs);
    let txn = db.begin(true).unwrap();
    let ordered = !options.sort.is_empty();
    let mut out: Vec<Document> = txn
        .find(DEFAULT_CF, COLL, filter, options)
        .unwrap()
        .iter::<Document>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    if !ordered {
        // No explicit sort → result order is unspecified; compare as a set.
        out.sort_by_key(|d| d.get_str("_id").unwrap_or("").to_string());
    }
    out.iter().map(canon).collect()
}

#[test]
fn fuzz_find_parity() {
    for seed in 0..2000u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let n = rng.gen_range(4..16);
        let docs: Vec<Document> = (0..n).map(|i| gen_doc(&mut rng, i)).collect();
        // Match-all is only valid at the top level.
        let filter = if rng.gen_bool(0.1) {
            doc! {}
        } else {
            gen_filter(&mut rng, 2)
        };
        let mut options = gen_options(&mut rng);
        // Deterministic total order for sort comparisons.
        if !options.sort.is_empty() {
            options.sort.push(Sort {
                field: "_id".into(),
                direction: SortDirection::Asc,
            });
        }

        let v1 = run(QueryEngine::V1, &docs, filter.clone(), options.clone());
        let v2 = run(QueryEngine::V2, &docs, filter.clone(), options.clone());
        assert_eq!(
            v1, v2,
            "MISMATCH seed={seed}\n  filter={filter:?}\n  options={options:?}\n  docs={docs:?}"
        );
    }
}
