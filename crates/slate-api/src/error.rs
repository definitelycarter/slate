use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use slate_client::ClientError;

pub enum ApiError {
    Client(ClientError),
    NotFound(String),
}

impl ApiError {
    pub fn not_found(id: &str) -> Self {
        ApiError::NotFound(format!("not found: {id}"))
    }
}

impl From<ClientError> for ApiError {
    fn from(e: ClientError) -> Self {
        ApiError::Client(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            ApiError::Client(e) => match e {
                ClientError::Io(_) => (StatusCode::BAD_GATEWAY, e.to_string()),
                ClientError::Serialization(_) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
                ClientError::Server(msg) if msg.contains("not found") => {
                    (StatusCode::NOT_FOUND, msg.clone())
                }
                ClientError::Server(_) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            },
        };

        let body = serde_json::json!({ "error": message });
        (status, Json(body)).into_response()
    }
}
