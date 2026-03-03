use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

use slate_wasm::SlateDb;

fn log(msg: &str) {
    web_sys::console::log_1(&JsValue::from_str(msg));
}

fn log_val(label: &str, val: &JsValue) {
    web_sys::console::log_2(&JsValue::from_str(label), val);
}

fn obj(pairs: &[(&str, JsValue)]) -> JsValue {
    let obj = js_sys::Object::new();
    for (k, v) in pairs {
        js_sys::Reflect::set(&obj, &JsValue::from_str(k), v).unwrap();
    }
    obj.into()
}

fn empty() -> JsValue {
    js_sys::Object::new().into()
}

fn get_str(obj: &JsValue, key: &str) -> String {
    js_sys::Reflect::get(obj, &JsValue::from_str(key))
        .unwrap()
        .as_string()
        .unwrap()
}

fn get_f64(obj: &JsValue, key: &str) -> f64 {
    js_sys::Reflect::get(obj, &JsValue::from_str(key))
        .unwrap()
        .as_f64()
        .unwrap()
}

// ── Basic CRUD ──────────────────────────────────────────────

#[wasm_bindgen_test]
fn create_collection_and_insert() {
    let db = SlateDb::new().unwrap();
    db.create_collection("users").unwrap();
    log("created collection: users");

    let docs = db
        .insert_one(
            "users",
            obj(&[
                ("_id", JsValue::from_str("u1")),
                ("name", JsValue::from_str("Alice")),
            ]),
        )
        .unwrap();

    log_val("insert_one returned", &docs);
    assert_eq!(docs.length(), 1);
    assert_eq!(get_str(&docs.get(0), "_id"), "u1");
    assert_eq!(get_str(&docs.get(0), "name"), "Alice");
    log("insert_one: pass");
}

#[wasm_bindgen_test]
fn insert_many_and_find() {
    let db = SlateDb::new().unwrap();
    db.create_collection("users").unwrap();

    let arr = js_sys::Array::new();
    arr.push(&obj(&[
        ("_id", JsValue::from_str("u1")),
        ("name", JsValue::from_str("Alice")),
    ]));
    arr.push(&obj(&[
        ("_id", JsValue::from_str("u2")),
        ("name", JsValue::from_str("Bob")),
    ]));
    let inserted = db.insert_many("users", arr).unwrap();
    assert_eq!(inserted.length(), 2);
    log_val("insert_many returned", &inserted);

    let all = db.find("users", empty()).unwrap();
    assert_eq!(all.length(), 2);
    log_val("find all", &all);
    log("insert_many + find: pass");
}

#[wasm_bindgen_test]
fn find_one_returns_doc_or_null() {
    let db = SlateDb::new().unwrap();
    db.create_collection("users").unwrap();
    db.insert_one(
        "users",
        obj(&[
            ("_id", JsValue::from_str("u1")),
            ("name", JsValue::from_str("Alice")),
        ]),
    )
    .unwrap();

    let found = db
        .find_one("users", obj(&[("_id", JsValue::from_str("u1"))]))
        .unwrap();
    log_val("find_one hit", &found);
    assert_eq!(get_str(&found, "name"), "Alice");

    let missing = db
        .find_one("users", obj(&[("_id", JsValue::from_str("nope"))]))
        .unwrap();
    log_val("find_one miss", &missing);
    assert!(missing.is_null());
    log("find_one: pass");
}

#[wasm_bindgen_test]
fn count_documents() {
    let db = SlateDb::new().unwrap();
    db.create_collection("items").unwrap();

    let arr = js_sys::Array::new();
    for i in 0..5 {
        arr.push(&obj(&[("_id", JsValue::from_str(&format!("i{i}")))]));
    }
    db.insert_many("items", arr).unwrap();

    let count = db.count("items", empty()).unwrap();
    log(&format!("count: {count}"));
    assert_eq!(count, 5);
    log("count: pass");
}

// ── Update ──────────────────────────────────────────────────

#[wasm_bindgen_test]
fn update_one_returns_updated_doc() {
    let db = SlateDb::new().unwrap();
    db.create_collection("users").unwrap();
    db.insert_one(
        "users",
        obj(&[
            ("_id", JsValue::from_str("u1")),
            ("name", JsValue::from_str("Alice")),
            ("age", JsValue::from_f64(30.0)),
        ]),
    )
    .unwrap();

    let updated = db
        .update_one(
            "users",
            obj(&[("_id", JsValue::from_str("u1"))]),
            obj(&[("$set", obj(&[("age", JsValue::from_f64(31.0))]).into())]),
        )
        .unwrap();

    log_val("update_one returned", &updated);
    assert_eq!(updated.length(), 1);
    assert_eq!(get_f64(&updated.get(0), "age"), 31.0);
    assert_eq!(get_str(&updated.get(0), "name"), "Alice");
    log("update_one: pass");
}

// ── Delete ──────────────────────────────────────────────────

#[wasm_bindgen_test]
fn delete_one_returns_deleted_doc() {
    let db = SlateDb::new().unwrap();
    db.create_collection("users").unwrap();
    db.insert_one(
        "users",
        obj(&[
            ("_id", JsValue::from_str("u1")),
            ("name", JsValue::from_str("Alice")),
        ]),
    )
    .unwrap();

    let deleted = db
        .delete_one("users", obj(&[("_id", JsValue::from_str("u1"))]))
        .unwrap();
    log_val("delete_one returned", &deleted);
    assert_eq!(deleted.length(), 1);
    assert_eq!(get_str(&deleted.get(0), "name"), "Alice");

    let count = db.count("users", empty()).unwrap();
    assert_eq!(count, 0);
    log("delete_one: pass");
}

// ── Collections & Indexes ───────────────────────────────────

#[wasm_bindgen_test]
fn list_collections() {
    let db = SlateDb::new().unwrap();
    db.create_collection("alpha").unwrap();
    db.create_collection("beta").unwrap();

    let cols = db.list_collections().unwrap();
    log_val("collections", &cols);
    assert_eq!(cols.length(), 2);
    log("list_collections: pass");
}

#[wasm_bindgen_test]
fn create_and_list_indexes() {
    let db = SlateDb::new().unwrap();
    db.create_collection("users").unwrap();
    db.create_index("users", "email").unwrap();

    let indexes = db.list_indexes("users").unwrap();
    log_val("indexes", &indexes);
    let names: Vec<String> = (0..indexes.length())
        .map(|i| indexes.get(i).as_string().unwrap())
        .collect();
    assert!(names.contains(&"email".to_string()));
    log("create_and_list_indexes: pass");
}

#[wasm_bindgen_test]
fn drop_collection() {
    let db = SlateDb::new().unwrap();
    db.create_collection("temp").unwrap();
    db.insert_one(
        "temp",
        obj(&[("_id", JsValue::from_str("t1"))]),
    )
    .unwrap();

    db.drop_collection("temp").unwrap();
    log("dropped collection: temp");

    let cols = db.list_collections().unwrap();
    assert_eq!(cols.length(), 0);
    log("drop_collection: pass");
}

// ── Bulk operations ─────────────────────────────────────────

#[wasm_bindgen_test]
fn upsert_many_inserts_and_replaces() {
    let db = SlateDb::new().unwrap();
    db.create_collection("items").unwrap();
    db.insert_one(
        "items",
        obj(&[
            ("_id", JsValue::from_str("i1")),
            ("val", JsValue::from_f64(1.0)),
        ]),
    )
    .unwrap();

    let arr = js_sys::Array::new();
    arr.push(&obj(&[
        ("_id", JsValue::from_str("i1")),
        ("val", JsValue::from_f64(10.0)),
    ]));
    arr.push(&obj(&[
        ("_id", JsValue::from_str("i2")),
        ("val", JsValue::from_f64(2.0)),
    ]));
    let result = db.upsert_many("items", arr).unwrap();
    log_val("upsert_many returned", &result);
    assert_eq!(result.length(), 2);

    let count = db.count("items", empty()).unwrap();
    assert_eq!(count, 2);

    let doc = db
        .find_one("items", obj(&[("_id", JsValue::from_str("i1"))]))
        .unwrap();
    assert_eq!(get_f64(&doc, "val"), 10.0);
    log("upsert_many: pass");
}
