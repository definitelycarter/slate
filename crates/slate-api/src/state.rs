use std::sync::Arc;

use slate_client::ClientPool;

#[derive(Clone)]
pub struct AppState {
    pub pool: Arc<ClientPool>,
}
