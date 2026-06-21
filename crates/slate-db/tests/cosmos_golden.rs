//! Hermetic golden-replay parity suite for slate's *own* curated query matrices.
//!
//! Companion to the Azure-Samples corpus replay (`tools/cosmos-parity/run_samples.py`),
//! and the successor to the removed v1↔v2 differential. The goldens under
//! `tools/cosmos-parity/goldens/` were captured from the **Cosmos emulator** (raw,
//! system fields and all) by `tools/cosmos-parity/capture_goldens.py`. Here we load
//! each golden's dataset into a fresh in-memory slate DB (`pk_path: "id"`, matching
//! Cosmos's system key), run the query through the SQL API, **normalize BOTH sides
//! with the one normalizer below**, and assert equality. No Docker, no network.
//!
//! Normalization is the single source of truth for cosmos-parity SKILL §4:
//!   1. strip Cosmos `_`-prefixed system fields (`_rid`/`_etag`/`_ts`/…),
//!   2. sort result lists unless the query has `ORDER BY` (set order isn't pinned),
//!   3. compare whole-valued floats equal to ints (slate is BSON-typed; Cosmos has
//!      one JSON number type).
//!
//! Two committed sidecars keep the suite honest (see their `description` fields):
//!   - `goldens/_excluded.json`  — emulator-bug queries we refuse to golden (SKILL §5).
//!     They have no golden file, so they are simply not replayed here.
//!   - `goldens/_known_gaps.json` — queries where the emulator is right but slate
//!     diverges (a real slate gap). We DO commit the correct Cosmos golden, but the
//!     replay treats these as expected divergences: it reports them instead of
//!     asserting, so the suite stays green while the gap stays pinned and visible.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use bson::Bson;
use serde_json::{Number, Value};
use slate_db::{CollectionConfig, DEFAULT_CF, Database, DatabaseBuilder};
use slate_store::MemoryStore;

fn parity_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tools/cosmos-parity")
}

// ── normalization (the ONE implementation — cosmos-parity SKILL §4) ──────────

/// Strip Cosmos `_`-prefixed system fields from every object, recursively.
fn strip_system(v: Value) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.into_iter()
                .filter(|(k, _)| !k.starts_with('_'))
                .map(|(k, val)| (k, strip_system(val)))
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.into_iter().map(strip_system).collect()),
        other => other,
    }
}

/// Collapse whole-valued floats to integers. slate's BSON typing makes math
/// functions return `Double` (`SQRT(16)` → `4.0`); Cosmos's unified JSON numbers
/// serialize whole numbers as `4`. Same value, different representation.
fn numify(v: Value) -> Value {
    match v {
        Value::Number(n) => match n.as_f64() {
            Some(f)
                if f.is_finite()
                    && f.fract() == 0.0
                    && (i64::MIN as f64..=i64::MAX as f64).contains(&f) =>
            {
                Value::Number(Number::from(f as i64))
            }
            _ => Value::Number(n),
        },
        Value::Array(a) => Value::Array(a.into_iter().map(numify).collect()),
        Value::Object(m) => Value::Object(m.into_iter().map(|(k, val)| (k, numify(val))).collect()),
        other => other,
    }
}

/// Deterministic, key-sorted serialization — independent of serde_json's Map
/// ordering (the workspace enables `preserve_order`, so insertion order would
/// otherwise leak in). Used both as the sort key and as the equality key.
fn canon(v: &Value) -> String {
    let mut out = String::new();
    canon_into(v, &mut out);
    out
}

fn canon_into(v: &Value, out: &mut String) {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String((*k).clone()).to_string());
                out.push(':');
                canon_into(&m[*k], out);
            }
            out.push('}');
        }
        Value::Array(a) => {
            out.push('[');
            for (i, e) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canon_into(e, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

/// Normalize a result list: strip system fields + numify every item, then sort by
/// canonical form unless the query pins the order with `ORDER BY`.
fn normalize(items: Vec<Value>, ordered: bool) -> Vec<String> {
    let mut canons: Vec<String> = items
        .into_iter()
        .map(|v| canon(&numify(strip_system(v))))
        .collect();
    if !ordered {
        canons.sort();
    }
    canons
}

// ── slate side (mirrors examples/parity_dump.rs, in-process) ─────────────────

/// Run `sql` over a fresh in-memory DB seeded with `docs`. Returns the result
/// items as relaxed-extjson, or an error string if any stage fails (parity with
/// Cosmos's error responses).
fn run_slate(docs: &[Value], sql: &str) -> Result<Vec<Value>, String> {
    let db = DatabaseBuilder::new()
        .open(MemoryStore::new())
        .map_err(|e| e.to_string())?;
    seed(&db, docs)?;

    let txn = db.begin(true).map_err(|e| e.to_string())?;
    let cursor = txn.query(DEFAULT_CF, "c", sql).map_err(|e| e.to_string())?;
    let iter = cursor.iter_raw_values().map_err(|e| e.to_string())?;
    let mut items = Vec::new();
    for it in iter {
        let raw = it.map_err(|e| e.to_string())?;
        let bson = Bson::try_from(raw).map_err(|e| e.to_string())?;
        items.push(bson.into_relaxed_extjson());
    }
    Ok(items)
}

fn seed(db: &Database<MemoryStore>, docs: &[Value]) -> Result<(), String> {
    let txn = db.begin(false).map_err(|e| e.to_string())?;
    txn.create_collection(&CollectionConfig {
        name: "c".into(),
        pk_path: "id".into(), // match Cosmos's system primary key
        ..Default::default()
    })
    .map_err(|e| e.to_string())?;
    txn.insert_many(DEFAULT_CF, "c", docs.to_vec())
        .map_err(|e| e.to_string())?
        .drain()
        .map_err(|e| e.to_string())?;
    txn.commit().map_err(|e| e.to_string())?;
    Ok(())
}

// ── golden loading ───────────────────────────────────────────────────────────

fn read_json(path: &Path) -> Value {
    let text = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

/// Query strings flagged in a `{description, queries: [{query, …}]}` sidecar.
fn sidecar_queries(name: &str) -> BTreeSet<String> {
    let path = parity_dir().join("goldens").join(name);
    if !path.exists() {
        return BTreeSet::new();
    }
    read_json(&path)
        .get("queries")
        .and_then(Value::as_array)
        .map(|qs| {
            qs.iter()
                .filter_map(|q| q.get("query").and_then(Value::as_str))
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

struct Golden {
    label: String,
    file: String,
    query: String,
    dataset: String,
    /// `Some(items)` when Cosmos returned a result; `None` when Cosmos errored.
    result: Option<Vec<Value>>,
}

fn load_goldens() -> Vec<Golden> {
    let dir = parity_dir().join("goldens");
    let mut out = Vec::new();
    let mut labels: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read goldens dir {}: {e}", dir.display()))
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    labels.sort();

    for label_dir in labels {
        let label = label_dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let mut files: Vec<PathBuf> = fs::read_dir(&label_dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", label_dir.display()))
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "json"))
            .collect();
        files.sort();

        for file in files {
            let v = read_json(&file);
            let query = v["query"].as_str().expect("golden.query").to_owned();
            let dataset = v["dataset"].as_str().expect("golden.dataset").to_owned();
            let result = if v.get("error").and_then(Value::as_bool) == Some(true) {
                None
            } else {
                Some(v["result"].as_array().expect("golden.result array").clone())
            };
            out.push(Golden {
                label: label.clone(),
                file: file
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                query,
                dataset,
                result,
            });
        }
    }
    out
}

fn load_dataset(name: &str) -> Vec<Value> {
    let path = parity_dir().join("datasets").join(name);
    read_json(&path)
        .as_array()
        .unwrap_or_else(|| panic!("dataset {name} is not a JSON array"))
        .clone()
}

// ── the replay ───────────────────────────────────────────────────────────────

#[test]
fn cosmos_goldens_replay() {
    let goldens = load_goldens();
    assert!(
        !goldens.is_empty(),
        "no goldens found — run capture_goldens.py"
    );
    let known_gaps = sidecar_queries("_known_gaps.json");

    let mut failures: Vec<String> = Vec::new();
    let mut gaps: Vec<String> = Vec::new();
    let mut passed = 0usize;
    // Coverage telemetry. `empty_parity` is the "succeeds but returns empty" family
    // (undefined/omission, cross-type WHERE, join-on-non-array): slate returns [] AND
    // Cosmos confirms []. A *wrong*-empty (slate [] while Cosmos has rows) is not
    // counted here — it lands in `failures` as a value mismatch. `error_parity` is
    // both-engines-error (negative.sql).
    let mut empty_parity = 0usize;
    let mut error_parity = 0usize;

    for g in &goldens {
        let docs = load_dataset(&g.dataset);
        let ordered = g.query.to_lowercase().contains("order by");
        let slate = run_slate(&docs, &g.query);
        let is_gap = known_gaps.contains(&g.query);

        let outcome: Result<(), String> = match (&g.result, &slate) {
            // Cosmos errored; slate errored too → parity.
            (None, Err(_)) => Ok(()),
            // Cosmos errored; slate produced a result.
            (None, Ok(items)) => Err(format!(
                "cosmos ERROR, slate ok: {}",
                canon(&Value::Array(items.clone()))
            )),
            // Cosmos returned a result; slate did too → compare normalized.
            (Some(expected), Ok(items)) => {
                let s = normalize(items.clone(), ordered);
                let c = normalize(expected.clone(), ordered);
                if s == c {
                    Ok(())
                } else {
                    Err(format!(
                        "value mismatch\n      slate : {}\n      cosmos: {}",
                        s.join(" "),
                        c.join(" ")
                    ))
                }
            }
            // Cosmos returned a result; slate errored.
            (Some(_), Err(e)) => Err(format!("cosmos ok, slate ERROR: {e}")),
        };

        match outcome {
            Ok(()) => {
                passed += 1;
                match &g.result {
                    None => error_parity += 1,
                    Some(r) if r.is_empty() => empty_parity += 1,
                    _ => {}
                }
            }
            Err(detail) if is_gap => {
                gaps.push(format!("[{}/{}] {} — {detail}", g.label, g.file, g.query));
            }
            Err(detail) => {
                failures.push(format!(
                    "[{}/{}] {}\n    {detail}",
                    g.label, g.file, g.query
                ));
            }
        }
    }

    // Known gaps are expected divergences — surfaced, not asserted.
    eprintln!(
        "cosmos golden replay: {passed}/{} matched ({} known gap(s) reported, {} failure(s))",
        goldens.len(),
        gaps.len(),
        failures.len()
    );
    eprintln!(
        "  of which: {error_parity} both-engines-error (negative.sql), \
         {empty_parity} succeeds-but-empty == Cosmos's empty (undefined/omission/cross-type)"
    );
    for gap in &gaps {
        eprintln!("  known slate gap (expected divergence): {gap}");
    }
    // A known gap that now MATCHES the golden is good news — nudge to promote it.
    let matched_gaps = goldens
        .iter()
        .filter(|g| known_gaps.contains(&g.query))
        .count()
        - gaps.len();
    if matched_gaps > 0 {
        eprintln!(
            "  note: {matched_gaps} query in _known_gaps.json now matches its golden — \
             consider removing it from goldens/_known_gaps.json"
        );
    }

    assert!(
        failures.is_empty(),
        "{} golden(s) diverged from Cosmos (not on the known-gap list):\n{}",
        failures.len(),
        failures.join("\n")
    );
}
