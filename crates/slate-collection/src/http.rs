use http::{Method, Request, Response, StatusCode};
use serde::Deserialize;
use slate_client::ClientPool;
use slate_query::{DistinctQuery, FilterGroup, Query};

use crate::error::CollectionHttpError;
use crate::request::{DistinctRequest, DistinctResponse, QueryRequest, QueryResponse};

pub struct CollectionHttp {
    collection: String,
    pool: ClientPool,
}

impl CollectionHttp {
    pub fn new(collection: String, pool: ClientPool) -> Self {
        Self { collection, pool }
    }

    pub fn handle(&self, req: Request<Vec<u8>>) -> Response<Vec<u8>> {
        let path = req.uri().path();
        let method = req.method();

        match (method, path.trim_end_matches('/')) {
            (&Method::POST, "/query") => self.query(&req),
            (&Method::POST, "/query/distinct") => self.query_distinct(&req),
            (&Method::POST, "/data") => self.post_records(&req),
            (&Method::PUT, "/data") => self.put_records(&req),
            (&Method::PATCH, "/data") => self.patch_records(&req),
            (&Method::DELETE, "/data") => self.delete_records(&req),
            _ => json_response(StatusCode::NOT_FOUND, r#"{"error":"not found"}"#),
        }
    }

    fn query(&self, req: &Request<Vec<u8>>) -> Response<Vec<u8>> {
        let request: QueryRequest = if req.body().is_empty() {
            QueryRequest::default()
        } else {
            match serde_json::from_slice(req.body()) {
                Ok(r) => r,
                Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
            }
        };

        match self.execute_query(&request) {
            Ok(response) => match serde_json::to_vec(&response) {
                Ok(body) => json_response(StatusCode::OK, body),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            },
            Err(e) => error_response(e.status_code(), &e.to_string()),
        }
    }

    fn query_distinct(&self, req: &Request<Vec<u8>>) -> Response<Vec<u8>> {
        let request: DistinctRequest = match serde_json::from_slice(req.body()) {
            Ok(r) => r,
            Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
        };

        match self.execute_distinct(&request) {
            Ok(response) => match serde_json::to_vec(&response) {
                Ok(body) => json_response(StatusCode::OK, body),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            },
            Err(e) => error_response(e.status_code(), &e.to_string()),
        }
    }

    fn execute_query(&self, request: &QueryRequest) -> Result<QueryResponse, CollectionHttpError> {
        let total = self
            .pool
            .get()?
            .count(&self.collection, request.filters.as_ref())?;

        let query = Query {
            filter: request.filters.clone(),
            sort: request.sort.clone(),
            skip: request.skip,
            take: request.take,
            columns: request.columns.clone(),
        };
        let records = self.pool.get()?.find(&self.collection, &query)?;

        Ok(QueryResponse { records, total })
    }

    fn execute_distinct(
        &self,
        request: &DistinctRequest,
    ) -> Result<DistinctResponse, CollectionHttpError> {
        let query = DistinctQuery {
            field: request.field.clone(),
            filter: request.filters.clone(),
            sort: request.sort,
            skip: request.skip,
            take: request.take,
        };
        let values = self.pool.get()?.distinct(&self.collection, &query)?;

        Ok(DistinctResponse { values })
    }

    // ── Data write routes ───────────────────────────────────────

    fn post_records(&self, req: &Request<Vec<u8>>) -> Response<Vec<u8>> {
        let docs: Vec<bson::Document> = match serde_json::from_slice(req.body()) {
            Ok(b) => b,
            Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
        };
        match self
            .pool
            .get()
            .and_then(|mut c| Ok(c.insert_many(&self.collection, docs)?))
        {
            Ok(results) => {
                match serde_json::to_vec(&serde_json::json!({ "inserted": results.len() })) {
                    Ok(b) => json_response(StatusCode::OK, b),
                    Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
                }
            }
            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    }

    fn put_records(&self, req: &Request<Vec<u8>>) -> Response<Vec<u8>> {
        let docs: Vec<bson::Document> = match serde_json::from_slice(req.body()) {
            Ok(b) => b,
            Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
        };
        match self
            .pool
            .get()
            .and_then(|mut c| Ok(c.upsert_many(&self.collection, docs)?))
        {
            Ok(result) => match serde_json::to_vec(&result) {
                Ok(b) => json_response(StatusCode::OK, b),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            },
            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    }

    fn patch_records(&self, req: &Request<Vec<u8>>) -> Response<Vec<u8>> {
        let docs: Vec<bson::Document> = match serde_json::from_slice(req.body()) {
            Ok(b) => b,
            Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
        };
        match self
            .pool
            .get()
            .and_then(|mut c| Ok(c.merge_many(&self.collection, docs)?))
        {
            Ok(result) => match serde_json::to_vec(&result) {
                Ok(b) => json_response(StatusCode::OK, b),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            },
            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    }

    fn delete_records(&self, req: &Request<Vec<u8>>) -> Response<Vec<u8>> {
        let body: DeleteBody = match serde_json::from_slice(req.body()) {
            Ok(b) => b,
            Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
        };
        match self
            .pool
            .get()
            .and_then(|mut c| Ok(c.delete_many(&self.collection, &body.filter)?))
        {
            Ok(result) => match serde_json::to_vec(&result) {
                Ok(b) => json_response(StatusCode::OK, b),
                Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            },
            Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    }
}

#[derive(Deserialize)]
struct DeleteBody {
    filter: FilterGroup,
}

fn json_response(status: StatusCode, body: impl Into<Vec<u8>>) -> Response<Vec<u8>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(body.into())
        .unwrap()
}

fn error_response(status: StatusCode, message: &str) -> Response<Vec<u8>> {
    let body = serde_json::json!({ "error": message });
    json_response(status, body.to_string().into_bytes())
}
