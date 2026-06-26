mod common;
use common::*;

use bson::doc;
use slate_db::v2::VectorIndexOptions;
use slate_db::{DbError, VectorDataType, VectorIndexSpec, VectorMetric};

const PHOTOS: &str = "photos";

/// Create the `photos` collection.
fn create_photos(db: &slate_db::Database<slate_store::MemoryStore>) {
    let txn = db.begin(false).unwrap();
    db.collections().create(PHOTOS).execute(&txn).unwrap();
    txn.commit().unwrap();
}

/// Run a kNN query through the db layer and collect the `_id`s in rank order.
fn knn_ids(db: &slate_db::Database<slate_store::MemoryStore>, sql: &str) -> Vec<String> {
    let txn = db.begin(true).unwrap();
    let ids = db
        .collection(PHOTOS)
        .query(sql)
        .iter::<String>(&txn)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    txn.rollback().unwrap();
    ids
}

#[test]
fn create_vector_index_then_knn_query_through_db_layer() {
    // End-to-end through slate-db: create a collection, build a vector index via
    // the new `create_vector_index`, insert docs, then run an
    // `ORDER BY VECTORDISTANCE(...) LIMIT k` query and assert the kNN order.
    let (db, _dir) = temp_db();
    create_photos(&db);

    let txn = db.begin(false).unwrap();
    db.collection(PHOTOS)
        .insert_many(vec![
            // Three unit-ish vectors pointing in distinct directions.
            doc! { "_id": "x", "embedding": [1.0, 0.0, 0.0] },
            doc! { "_id": "y", "embedding": [0.0, 1.0, 0.0] },
            doc! { "_id": "z", "embedding": [0.0, 0.0, 1.0] },
        ])
        .execute(&txn)
        .unwrap();
    // Build the index AFTER data exists — exercises the backfill path too.
    db.collection(PHOTOS)
        .indexes()
        .create(
            "embedding",
            VectorIndexOptions::float32(3, VectorMetric::Cosine),
        )
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    // Query near the x-axis: cosine is "higher is closer", so DESC orders by
    // descending similarity. Top-2 should be `x` (exact), then `y`/`z` tied at 0;
    // take the top-1 for a deterministic assertion, then top-3 for the full set.
    assert_eq!(
        knn_ids(
            &db,
            "SELECT VALUE c._id FROM c \
             ORDER BY VECTORDISTANCE(c.embedding, [1.0, 0.0, 0.0]) DESC LIMIT 1",
        ),
        vec!["x"]
    );

    // Query near the y-axis: `y` must rank first.
    assert_eq!(
        knn_ids(
            &db,
            "SELECT VALUE c._id FROM c \
             ORDER BY VECTORDISTANCE(c.embedding, [0.0, 1.0, 0.0]) DESC LIMIT 1",
        ),
        vec!["y"]
    );

    // A LIMIT larger than the corpus returns all three, `x` first for an
    // x-axis query.
    let all = knn_ids(
        &db,
        "SELECT VALUE c._id FROM c \
         ORDER BY VECTORDISTANCE(c.embedding, [1.0, 0.0, 0.0]) DESC LIMIT 10",
    );
    assert_eq!(all.len(), 3);
    assert_eq!(all[0], "x");
}

#[test]
fn create_vector_index_dims_mismatch_on_existing_data_is_invalid_document() {
    // A pre-existing document whose embedding has the wrong dimensionality must
    // fail the create — surfacing through the db layer as `DbError::InvalidDocument`
    // (an engine `VectorDimsMismatch` maps to a malformed-document error).
    let (db, _dir) = temp_db();
    create_photos(&db);

    let txn = db.begin(false).unwrap();
    db.collection(PHOTOS)
        // 4 dims, but the index below declares 3.
        .insert_many(vec![
            doc! { "_id": "bad", "embedding": [1.0, 2.0, 3.0, 4.0] },
        ])
        .execute(&txn)
        .unwrap();

    let err = db
        .collection(PHOTOS)
        .indexes()
        .create(
            "embedding",
            VectorIndexOptions::float32(3, VectorMetric::Cosine),
        )
        .execute(&txn)
        .unwrap_err();
    assert!(
        matches!(err, DbError::InvalidDocument(_)),
        "expected InvalidDocument, got {err:?}"
    );
    txn.rollback().unwrap();
}

#[test]
fn create_vector_index_spec_carries_explicit_dtype() {
    // The re-exported `VectorDataType` is usable from slate-db when building a
    // spec by struct literal (rather than the `float32` shorthand).
    let (db, _dir) = temp_db();
    create_photos(&db);

    let spec = VectorIndexSpec {
        path: "embedding".to_string(),
        dims: 2,
        metric: VectorMetric::Euclidean,
        dtype: VectorDataType::Float32,
    };

    let txn = db.begin(false).unwrap();
    db.collection(PHOTOS)
        .insert_many(vec![
            doc! { "_id": "a", "embedding": [0.0, 0.0] },
            doc! { "_id": "b", "embedding": [10.0, 10.0] },
        ])
        .execute(&txn)
        .unwrap();
    db.collection(PHOTOS)
        .indexes()
        .create(
            spec.path.as_str(),
            VectorIndexOptions::float32(spec.dims, spec.metric),
        )
        .execute(&txn)
        .unwrap();
    txn.commit().unwrap();

    // Euclidean is "lower is closer" → ASC. Query at the origin: `a` ranks first.
    assert_eq!(
        knn_ids(
            &db,
            "SELECT VALUE c._id FROM c \
             ORDER BY VECTORDISTANCE(c.embedding, [0.0, 0.0], 'euclidean') ASC LIMIT 1",
        ),
        vec!["a"]
    );
}
