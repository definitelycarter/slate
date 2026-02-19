use serde::{Deserialize, Serialize};
use slate_db::{CollectionConfig, DeleteResult, InsertResult, UpdateResult};
use slate_query::{DistinctQuery, FilterGroup, Query};

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
        query: Query,
    },
    FindOne {
        collection: String,
        query: Query,
    },
    FindById {
        collection: String,
        id: String,
        columns: Option<Vec<String>>,
    },
    UpdateOne {
        collection: String,
        filter: FilterGroup,
        update: bson::Document,
        upsert: bool,
    },
    UpdateMany {
        collection: String,
        filter: FilterGroup,
        update: bson::Document,
    },
    ReplaceOne {
        collection: String,
        filter: FilterGroup,
        doc: bson::Document,
    },
    DeleteOne {
        collection: String,
        filter: FilterGroup,
    },
    DeleteMany {
        collection: String,
        filter: FilterGroup,
    },
    Count {
        collection: String,
        filter: Option<FilterGroup>,
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
        query: DistinctQuery,
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
    Error(String),
}
