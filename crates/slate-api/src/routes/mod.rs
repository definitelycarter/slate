mod collections;
mod health;

use axum::Router;
use axum::routing::{delete, get};

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/healthz", get(health::healthz))
        .route("/v1/collections", get(collections::list_collections))
        .route(
            "/v1/collections/{name}",
            delete(collections::drop_collection),
        )
}
