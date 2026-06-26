use bson::{Document, RawDocumentBuf};
use slate_db::v2::IndexOptions;
use slate_db::{Database, DatabaseBuilder, DbError};
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

/// Convert a single SQL result *value* to JS.
///
/// Unlike `find` (which always streams documents), a SQL `SELECT VALUE`
/// projection can yield scalars or arrays — e.g. `SELECT VALUE c.name` yields
/// strings — so query rows are converted at the BSON value level rather than as
/// documents.
fn value_to_js(value: bson::RawBson) -> Result<JsValue, DbError> {
    let bson = bson::Bson::try_from(value.as_raw_bson_ref())?;
    serde::Serialize::serialize(&bson, &SERIALIZER)
        .map_err(|e| DbError::Serialization(e.to_string()))
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

    /// Drain a document-yielding iterator (a `find` read or a write that streams
    /// back its affected rows) into a JS array.
    fn collect_docs(iter: slate_db::CursorIter<'_, Document>) -> Result<js_sys::Array, DbError> {
        let arr = js_sys::Array::new();
        for doc in iter {
            let d = doc?;
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
        // Inject the host's non-deterministic sources from JS: the wasm build has
        // no native clock or PRNG (those live behind slate-db's `runtime`
        // feature, kept out of wasm so `getrandom` never reaches it). `RAND()`
        // takes a callable (a fresh draw per call); `Math.random()` is already an
        // `f64` in `[0, 1)`, matching what slate's evaluator expects.
        let db = DatabaseBuilder::new()
            .with_clock(|| js_sys::Date::now() as i64)
            .with_rand(js_sys::Math::random)
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
            Self::collect_docs(
                self.db
                    .collection(collection)
                    .insert_one(raw)
                    .iter::<Document>(txn)?,
            )
        })
    }

    pub fn insert_many(
        &self,
        collection: &str,
        docs: js_sys::Array,
    ) -> Result<js_sys::Array, JsError> {
        let raws = js_array_to_raws(&docs)?;
        self.write(|txn| {
            Self::collect_docs(
                self.db
                    .collection(collection)
                    .insert_many(raws)
                    .iter::<Document>(txn)?,
            )
        })
    }

    // ── Query ───────────────────────────────────────────────

    pub fn find(&self, collection: &str, filter: JsValue) -> Result<js_sys::Array, JsError> {
        let raw = js_to_raw(filter)?;
        self.read(|txn| {
            Self::collect_docs(
                self.db
                    .collection(collection)
                    .find(raw)
                    .iter::<Document>(txn)?,
            )
        })
    }

    pub fn find_one(&self, collection: &str, filter: JsValue) -> Result<JsValue, JsError> {
        let raw = js_to_raw(filter)?;
        self.read(|txn| {
            let doc = self
                .db
                .collection(collection)
                .find(raw)
                .iter_raw(txn)?
                .next()
                .transpose()?;
            Ok(doc)
        })
        .and_then(|opt| match opt {
            Some(raw) => raw_to_js(raw),
            None => Ok(JsValue::NULL),
        })
    }

    pub fn count(&self, collection: &str, filter: JsValue) -> Result<u32, JsError> {
        let raw = js_to_raw(filter)?;
        self.read(|txn| {
            let count = self
                .db
                .collection(collection)
                .find(raw)
                .iter_raw(txn)?
                .count();
            Ok(count as u32)
        })
    }

    /// Run a CosmosDB-style SQL statement against `collection` and return the
    /// result rows as a JS array.
    ///
    /// SQL is read-only and shares the query stack with `find`. The container is
    /// chosen out-of-band here (the `FROM` clause only binds the row alias,
    /// matching Cosmos), so — like `find` — the collection is an explicit
    /// argument rather than part of the query text. Rows are converted at the
    /// value level (see [`value_to_js`]) so scalar `SELECT VALUE` projections,
    /// not just documents, round-trip cleanly.
    pub fn query(&self, collection: &str, sql: &str) -> Result<js_sys::Array, JsError> {
        self.read(|txn| {
            let arr = js_sys::Array::new();
            for value in self.db.collection(collection).query(sql).iter_raw(txn)? {
                arr.push(&value_to_js(value?)?);
            }
            Ok(arr)
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
            Self::collect_docs(
                self.db
                    .collection(collection)
                    .find(filter_raw)
                    .update(update_raw)
                    .one()
                    .iter::<Document>(txn)?,
            )
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
            Self::collect_docs(
                self.db
                    .collection(collection)
                    .find(filter_raw)
                    .update(update_raw)
                    .iter::<Document>(txn)?,
            )
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
            Self::collect_docs(
                self.db
                    .collection(collection)
                    .find(filter_raw)
                    .replace(replacement_raw)
                    .iter::<Document>(txn)?,
            )
        })
    }

    // ── Delete ──────────────────────────────────────────────

    pub fn delete_one(&self, collection: &str, filter: JsValue) -> Result<js_sys::Array, JsError> {
        let filter_raw = js_to_raw(filter)?;
        self.write(|txn| {
            Self::collect_docs(
                self.db
                    .collection(collection)
                    .find(filter_raw)
                    .delete()
                    .one()
                    .iter::<Document>(txn)?,
            )
        })
    }

    pub fn delete_many(&self, collection: &str, filter: JsValue) -> Result<js_sys::Array, JsError> {
        let filter_raw = js_to_raw(filter)?;
        self.write(|txn| {
            Self::collect_docs(
                self.db
                    .collection(collection)
                    .find(filter_raw)
                    .delete()
                    .iter::<Document>(txn)?,
            )
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
            Self::collect_docs(
                self.db
                    .collection(collection)
                    .upsert_many(raws)
                    .iter::<Document>(txn)?,
            )
        })
    }

    pub fn merge_many(
        &self,
        collection: &str,
        docs: js_sys::Array,
    ) -> Result<js_sys::Array, JsError> {
        let raws = js_array_to_raws(&docs)?;
        self.write(|txn| {
            Self::collect_docs(
                self.db
                    .collection(collection)
                    .merge_many(raws)
                    .iter::<Document>(txn)?,
            )
        })
    }
}

// ── Collection & index operations ───────────────────────────

#[wasm_bindgen]
impl SlateDb {
    pub fn create_collection(&self, name: &str) -> Result<(), JsError> {
        self.write(|txn| {
            self.db.collections().create(name).execute(txn)?;
            Ok(())
        })
    }

    pub fn drop_collection(&self, name: &str) -> Result<(), JsError> {
        self.write(|txn| {
            self.db.collections().remove(name).execute(txn)?;
            Ok(())
        })
    }

    pub fn list_collections(&self) -> Result<js_sys::Array, JsError> {
        self.read(|txn| {
            // v2: no global list (collections().list() is cf-scoped); keep flat
            let collections = txn.list_collections()?;
            Ok(strings_to_array(
                collections.into_iter().map(|(_, name)| name).collect(),
            ))
        })
    }

    pub fn create_index(&self, collection: &str, field: &str) -> Result<(), JsError> {
        self.write(|txn| {
            self.db
                .collection(collection)
                .indexes()
                .create(field, IndexOptions::default())
                .execute(txn)?;
            Ok(())
        })
    }

    pub fn drop_index(&self, collection: &str, field: &str) -> Result<(), JsError> {
        self.write(|txn| {
            self.db
                .collection(collection)
                .indexes()
                .remove(field)
                .execute(txn)?;
            Ok(())
        })
    }

    pub fn list_indexes(&self, collection: &str) -> Result<js_sys::Array, JsError> {
        self.read(|txn| {
            let indexes = self.db.collection(collection).indexes().list(txn)?;
            Ok(strings_to_array(indexes))
        })
    }
}
