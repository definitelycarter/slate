use serde::{Deserialize, Serialize};
use slate_db::{CollectionConfig, DeleteResult, InsertResult, UpdateResult, UpsertResult};
use slate_query::{Sort, SortDirection};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    InsertOne {
        collection: String,
        doc: bson::Document,
    },
    InsertMany {
        collection: String,
        docs: Vec<bson::Document>,
    },
    Find {
        collection: String,
        filter: Option<bson::Document>,
        sort: Vec<Sort>,
        skip: Option<usize>,
        take: Option<usize>,
        columns: Option<Vec<String>>,
    },
    FindOne {
        collection: String,
        filter: Option<bson::Document>,
        sort: Vec<Sort>,
        skip: Option<usize>,
        take: Option<usize>,
        columns: Option<Vec<String>>,
    },
    FindById {
        collection: String,
        id: String,
        columns: Option<Vec<String>>,
    },
    UpdateOne {
        collection: String,
        filter: bson::Document,
        update: bson::Document,
        upsert: bool,
    },
    UpdateMany {
        collection: String,
        filter: bson::Document,
        update: bson::Document,
    },
    ReplaceOne {
        collection: String,
        filter: bson::Document,
        doc: bson::Document,
    },
    DeleteOne {
        collection: String,
        filter: bson::Document,
    },
    DeleteMany {
        collection: String,
        filter: bson::Document,
    },
    Count {
        collection: String,
        filter: Option<bson::Document>,
    },
    CreateIndex {
        collection: String,
        field: String,
    },
    DropIndex {
        collection: String,
        field: String,
    },
    ListIndexes {
        collection: String,
    },
    CreateCollection {
        config: CollectionConfig,
    },
    ListCollections,
    DropCollection {
        collection: String,
    },
    Distinct {
        collection: String,
        field: String,
        filter: Option<bson::Document>,
        sort: Option<SortDirection>,
        skip: Option<usize>,
        take: Option<usize>,
    },
    UpsertMany {
        collection: String,
        docs: Vec<bson::Document>,
    },
    MergeMany {
        collection: String,
        docs: Vec<bson::Document>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Insert(InsertResult),
    Inserts(Vec<InsertResult>),
    Record(Option<bson::Document>),
    Records(Vec<bson::Document>),
    Update(UpdateResult),
    Delete(DeleteResult),
    Count(u64),
    Indexes(Vec<String>),
    Collections(Vec<String>),
    Values(bson::RawBson),
    Upsert(UpsertResult),
    Error(String),
}
