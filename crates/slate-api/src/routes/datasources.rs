use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;

use crate::error::ApiError;
use crate::state::AppState;

pub async fn list_collections(
    State(state): State<AppState>,
) -> Result<Json<Vec<String>>, ApiError> {
    tokio::task::spawn_blocking(move || {
        let mut client = state.pool.get()?;
        let collections = client.list_collections()?;
        Ok(Json(collections))
    })
    .await
    .unwrap()
}

pub async fn drop_collection(
    State(state): State<AppState>,
    axum::extract::Path(collection): axum::extract::Path<String>,
) -> Result<StatusCode, ApiError> {
    tokio::task::spawn_blocking(move || {
        let mut client = state.pool.get()?;
        client.drop_collection(&collection)?;
        Ok(StatusCode::NO_CONTENT)
    })
    .await
    .unwrap()
}
