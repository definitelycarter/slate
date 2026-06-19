//! Shared helpers for node executor tests: a seeded engine and expression
//! parsing shortcuts.

use bson::rawdoc;
use slate_engine::{Catalog, DEFAULT_CF, Engine, EngineTransaction, KvEngine};
use slate_planner::CollectionRef;
use slate_sql::ast::ScalarExpr;
use slate_store::MemoryStore;

/// A `KvEngine` with a `people` collection (indexed on `age`) holding three
/// documents with `_id`s "1".."3" in ascending age order.
pub(crate) fn seeded_people() -> KvEngine<MemoryStore> {
    let engine = KvEngine::new(MemoryStore::new());
    {
        let txn = engine.begin(false).unwrap();
        txn.create_collection(DEFAULT_CF, "people", &Default::default())
            .unwrap();
        txn.create_index(DEFAULT_CF, "people", "age").unwrap();
        txn.commit().unwrap();
    }
    {
        let txn = engine.begin(false).unwrap();
        let handle = txn.collection(DEFAULT_CF, "people").unwrap();
        for doc in [
            rawdoc! { "_id": "1", "name": "ada", "age": 36 },
            rawdoc! { "_id": "2", "name": "alan", "age": 41 },
            rawdoc! { "_id": "3", "name": "grace", "age": 44 },
        ] {
            txn.put(&handle, &doc).unwrap();
        }
        txn.commit().unwrap();
    }
    engine
}

pub(crate) fn people_ref() -> CollectionRef {
    CollectionRef {
        cf: DEFAULT_CF.into(),
        collection: "people".into(),
    }
}

/// Parse `SELECT VALUE <src> FROM c` and return the projection expression.
pub(crate) fn sv(src: &str) -> ScalarExpr {
    let q = slate_sql::parse(&format!("SELECT VALUE {src} FROM c")).unwrap();
    let slate_sql::ast::SelectClause::Value(e) = q.select;
    e
}

/// Parse the `WHERE` predicate of `SELECT VALUE c FROM c WHERE <src>`.
pub(crate) fn pred(src: &str) -> ScalarExpr {
    let q = slate_sql::parse(&format!("SELECT VALUE c FROM c WHERE {src}")).unwrap();
    q.filter.unwrap()
}

/// A row source binding each of `docs` to the alias `c` (`Bind` over `Values`).
pub(crate) fn bind_c(docs: Vec<bson::RawBson>) -> crate::ValueIter<'static> {
    crate::nodes::bind::execute("c".into(), crate::nodes::values::execute(docs))
}
