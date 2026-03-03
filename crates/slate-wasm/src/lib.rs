use bson::{Document, RawDocumentBuf};
use slate_db::{CollectionConfig, Database, DatabaseBuilder, DbError, DEFAULT_CF};
use slate_query::FindOptions;
use slate_store::MemoryStore;
use wasm_bindgen::prelude::*;

type Db = Database<MemoryStore>;
type Txn<'a> = slate_db::DatabaseTransaction<'a, MemoryStore>;

// ── Conversion helpers ──────────────────────────────────────

const SERIALIZER: serde_wasm_bindgen::Serializer =
    serde_wasm_bindgen::Serializer::json_compatible();

fn to_js_err(e: impl std::fmt::Display) -> JsError {
    JsError::new(&e.to_string())
}

fn js_to_doc(val: JsValue) -> Result<Document, JsError> {
    serde_wasm_bindgen::from_value(val).map_err(to_js_err)
}

fn js_to_raw(val: JsValue) -> Result<RawDocumentBuf, JsError> {
    let doc = js_to_doc(val)?;
    RawDocumentBuf::try_from(&doc).map_err(to_js_err)
}

fn raw_to_js(raw: RawDocumentBuf) -> Result<JsValue, JsError> {
    let doc: Document = bson::deserialize_from_slice(raw.as_bytes()).map_err(to_js_err)?;
    serde::Serialize::serialize(&doc, &SERIALIZER).map_err(to_js_err)
}

fn js_array_to_raws(arr: &js_sys::Array) -> Result<Vec<RawDocumentBuf>, JsError> {
    let mut docs = Vec::with_capacity(arr.length() as usize);
    for i in 0..arr.length() {
        docs.push(js_to_raw(arr.get(i))?);
    }
    Ok(docs)
}

fn strings_to_array(strings: Vec<String>) -> js_sys::Array {
    let arr = js_sys::Array::new();
    for s in strings {
        arr.push(&JsValue::from_str(&s));
    }
    arr
}

// ── SlateDb ─────────────────────────────────────────────────

#[wasm_bindgen]
pub struct SlateDb {
    db: Db,
}

impl SlateDb {
    fn read<F, R>(&self, f: F) -> Result<R, JsError>
    where
        F: FnOnce(&mut Txn<'_>) -> Result<R, DbError>,
    {
        let mut txn = self.db.begin(true).map_err(to_js_err)?;
        let result = f(&mut txn).map_err(to_js_err)?;
        Ok(result)
    }

    fn write<F, R>(&self, f: F) -> Result<R, JsError>
    where
        F: FnOnce(&mut Txn<'_>) -> Result<R, DbError>,
    {
        let mut txn = self.db.begin(false).map_err(to_js_err)?;
        let result = f(&mut txn).map_err(to_js_err)?;
        txn.commit().map_err(to_js_err)?;
        Ok(result)
    }

    fn collect_cursor(
        cursor: slate_db::Cursor<'_, '_, MemoryStore>,
    ) -> Result<js_sys::Array, DbError> {
        let arr = js_sys::Array::new();
        for doc in cursor.iter()? {
            let raw = doc?;
            let d: Document = bson::deserialize_from_slice(raw.as_bytes())?;
            let js = serde::Serialize::serialize(&d, &SERIALIZER)
                .map_err(|e| DbError::Serialization(e.to_string()))?;
            arr.push(&js);
        }
        Ok(arr)
    }
}

// ── Constructor ─────────────────────────────────────────────

#[wasm_bindgen]
impl SlateDb {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<SlateDb, JsError> {
        let store = MemoryStore::new();
        let db = DatabaseBuilder::new()
            .with_clock(|| js_sys::Date::now() as i64)
            .open(store)
            .map_err(to_js_err)?;
        Ok(SlateDb { db })
    }
}

// ── CRUD operations ─────────────────────────────────────────

#[wasm_bindgen]
impl SlateDb {
    // ── Insert ──────────────────────────────────────────────

    pub fn insert_one(&self, collection: &str, doc: JsValue) -> Result<js_sys::Array, JsError> {
        let raw = js_to_raw(doc)?;
        self.write(|txn| {
            let cursor = txn.insert_one(DEFAULT_CF, collection, raw)?;
            Self::collect_cursor(cursor)
        })
    }

    pub fn insert_many(
        &self,
        collection: &str,
        docs: js_sys::Array,
    ) -> Result<js_sys::Array, JsError> {
        let raws = js_array_to_raws(&docs)?;
        self.write(|txn| {
            let cursor = txn.insert_many(DEFAULT_CF, collection, raws)?;
            Self::collect_cursor(cursor)
        })
    }

    // ── Query ───────────────────────────────────────────────

    pub fn find(
        &self,
        collection: &str,
        filter: JsValue,
    ) -> Result<js_sys::Array, JsError> {
        let raw = js_to_raw(filter)?;
        self.read(|txn| {
            let cursor = txn.find(DEFAULT_CF, collection, raw, FindOptions::default())?;
            Self::collect_cursor(cursor)
        })
    }

    pub fn find_one(
        &self,
        collection: &str,
        filter: JsValue,
    ) -> Result<JsValue, JsError> {
        let raw = js_to_raw(filter)?;
        self.read(|txn| {
            let doc = txn.find_one(DEFAULT_CF, collection, raw)?;
            Ok(doc)
        })
        .and_then(|opt| match opt {
            Some(raw) => raw_to_js(raw),
            None => Ok(JsValue::NULL),
        })
    }

    pub fn count(
        &self,
        collection: &str,
        filter: JsValue,
    ) -> Result<u32, JsError> {
        let raw = js_to_raw(filter)?;
        self.read(|txn| {
            let count = txn.count(DEFAULT_CF, collection, raw)?;
            Ok(count as u32)
        })
    }

    // ── Update ──────────────────────────────────────────────

    pub fn update_one(
        &self,
        collection: &str,
        filter: JsValue,
        update: JsValue,
    ) -> Result<js_sys::Array, JsError> {
        let filter_raw = js_to_raw(filter)?;
        let update_raw = js_to_raw(update)?;
        self.write(|txn| {
            let cursor = txn.update_one(DEFAULT_CF, collection, filter_raw, update_raw)?;
            Self::collect_cursor(cursor)
        })
    }

    pub fn update_many(
        &self,
        collection: &str,
        filter: JsValue,
        update: JsValue,
    ) -> Result<js_sys::Array, JsError> {
        let filter_raw = js_to_raw(filter)?;
        let update_raw = js_to_raw(update)?;
        self.write(|txn| {
            let cursor = txn.update_many(DEFAULT_CF, collection, filter_raw, update_raw)?;
            Self::collect_cursor(cursor)
        })
    }

    pub fn replace_one(
        &self,
        collection: &str,
        filter: JsValue,
        replacement: JsValue,
    ) -> Result<js_sys::Array, JsError> {
        let filter_raw = js_to_raw(filter)?;
        let replacement_raw = js_to_raw(replacement)?;
        self.write(|txn| {
            let cursor =
                txn.replace_one(DEFAULT_CF, collection, filter_raw, replacement_raw)?;
            Self::collect_cursor(cursor)
        })
    }

    // ── Delete ──────────────────────────────────────────────

    pub fn delete_one(
        &self,
        collection: &str,
        filter: JsValue,
    ) -> Result<js_sys::Array, JsError> {
        let filter_raw = js_to_raw(filter)?;
        self.write(|txn| {
            let cursor = txn.delete_one(DEFAULT_CF, collection, filter_raw)?;
            Self::collect_cursor(cursor)
        })
    }

    pub fn delete_many(
        &self,
        collection: &str,
        filter: JsValue,
    ) -> Result<js_sys::Array, JsError> {
        let filter_raw = js_to_raw(filter)?;
        self.write(|txn| {
            let cursor = txn.delete_many(DEFAULT_CF, collection, filter_raw)?;
            Self::collect_cursor(cursor)
        })
    }

    // ── Bulk ────────────────────────────────────────────────

    pub fn upsert_many(
        &self,
        collection: &str,
        docs: js_sys::Array,
    ) -> Result<js_sys::Array, JsError> {
        let raws = js_array_to_raws(&docs)?;
        self.write(|txn| {
            let cursor = txn.upsert_many(DEFAULT_CF, collection, raws)?;
            Self::collect_cursor(cursor)
        })
    }

    pub fn merge_many(
        &self,
        collection: &str,
        docs: js_sys::Array,
    ) -> Result<js_sys::Array, JsError> {
        let raws = js_array_to_raws(&docs)?;
        self.write(|txn| {
            let cursor = txn.merge_many(DEFAULT_CF, collection, raws)?;
            Self::collect_cursor(cursor)
        })
    }
}

// ── Collection & index operations ───────────────────────────

#[wasm_bindgen]
impl SlateDb {
    pub fn create_collection(&self, name: &str) -> Result<(), JsError> {
        self.write(|txn| {
            let config = CollectionConfig {
                name: name.to_string(),
                ..Default::default()
            };
            txn.create_collection(&config)?;
            Ok(())
        })
    }

    pub fn drop_collection(&self, name: &str) -> Result<(), JsError> {
        self.write(|txn| {
            txn.drop_collection(DEFAULT_CF, name)?;
            Ok(())
        })
    }

    pub fn list_collections(&self) -> Result<js_sys::Array, JsError> {
        self.read(|txn| {
            let collections = txn.list_collections()?;
            Ok(strings_to_array(
                collections.into_iter().map(|(_, name)| name).collect(),
            ))
        })
    }

    pub fn create_index(&self, collection: &str, field: &str) -> Result<(), JsError> {
        self.write(|txn| {
            txn.create_index(DEFAULT_CF, collection, field)?;
            Ok(())
        })
    }

    pub fn drop_index(&self, collection: &str, field: &str) -> Result<(), JsError> {
        self.write(|txn| {
            txn.drop_index(DEFAULT_CF, collection, field)?;
            Ok(())
        })
    }

    pub fn list_indexes(&self, collection: &str) -> Result<js_sys::Array, JsError> {
        self.read(|txn| {
            let indexes = txn.list_indexes(DEFAULT_CF, collection)?;
            Ok(strings_to_array(indexes))
        })
    }
}
