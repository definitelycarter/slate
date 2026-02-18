use std::sync::Arc;

use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::reflector::{ObjectRef, Store};
use kube::{Api, Client, ResourceExt};
use slate_client::Client as SlateClient;
use slate_db::CollectionConfig;
use tracing::info;

use crate::Error;
use crate::crd::{Collection, Server, ServerPhase};
use crate::reconcile::server::SLATE_PORT;

pub struct Context {
    pub client: Client,
}

/// When a Server changes, find all Collections in the controller's store that
/// reference it via `spec.server` and trigger their reconciliation.
pub fn map_server_to_collections(
    server: Server,
    collection_store: &Store<Collection>,
) -> Vec<ObjectRef<Collection>> {
    let name = server.name_any();
    let ns = server.namespace();

    collection_store
        .state()
        .into_iter()
        .filter(|col| col.spec.server == name && col.namespace() == ns)
        .map(|col| ObjectRef::from_obj(col.as_ref()))
        .collect()
}

pub async fn reconcile(col: Arc<Collection>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = col.name_any();
    let ns = col.namespace().unwrap_or_else(|| "default".into());
    let server_ref = &col.spec.server;
    info!(name, ns, server = %server_ref, "reconciling collection");

    // Read the Server CR to check its ready_generation status.
    let server_api: Api<Server> = Api::namespaced(ctx.client.clone(), &ns);
    let server = match server_api.get_opt(server_ref).await? {
        Some(s) => s,
        None => {
            info!(name, ns, server = %server_ref, "server not found, requeuing");
            return Ok(Action::requeue(std::time::Duration::from_secs(5)));
        }
    };

    let server_status = server.status.as_ref();
    let server_phase = server_status.and_then(|s| s.phase.as_ref());

    if server_phase != Some(&ServerPhase::Ready) {
        let phase_str = server_phase
            .map(|p| format!("{p:?}"))
            .unwrap_or("None".into());
        info!(name, ns, server = %server_ref, phase = %phase_str, "server not ready, requeuing");
        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
    }

    let server_ready_gen = match server_status.and_then(|s| s.ready_generation) {
        Some(g) => g,
        None => {
            info!(name, ns, server = %server_ref, "server has no ready_generation, requeuing");
            return Ok(Action::requeue(std::time::Duration::from_secs(5)));
        }
    };

    let col_server_gen = col.status.as_ref().and_then(|s| s.server_generation);

    // If the collection was already reconciled against this server generation, skip.
    if col_server_gen == Some(server_ready_gen) {
        info!(name, ns, server = %server_ref, generation = server_ready_gen, "collection up to date");
        return Ok(Action::requeue(std::time::Duration::from_secs(300)));
    }

    info!(name, ns, server = %server_ref, server_gen = server_ready_gen, col_gen = ?col_server_gen, "server generation changed, reconciling");

    // Connect to the Server's k8s Service
    let server_addr = format!("{server_ref}.{ns}.svc.cluster.local:{SLATE_PORT}");

    let mut client = SlateClient::connect(&server_addr)
        .map_err(|e| Error::Reconcile(format!("failed to connect to server {server_addr}: {e}")))?;

    let collections = client
        .list_collections()
        .map_err(|e| Error::Reconcile(format!("failed to list collections: {e}")))?;

    let desired_indexes: std::collections::BTreeSet<&str> =
        col.spec.indexes.iter().map(|s| s.as_str()).collect();

    if collections.iter().any(|c| c == &name) {
        // Collection exists — reconcile indexes
        let actual: std::collections::BTreeSet<String> = client
            .list_indexes(&name)
            .map_err(|e| Error::Reconcile(format!("failed to list indexes: {e}")))?
            .into_iter()
            .collect();

        let to_create: Vec<&str> = desired_indexes
            .iter()
            .filter(|i| !actual.contains(**i))
            .copied()
            .collect();
        let to_drop: Vec<&String> = actual
            .iter()
            .filter(|i| !desired_indexes.contains(i.as_str()))
            .collect();

        for idx in &to_create {
            info!(name, ns, index = idx, "creating index");
            client
                .create_index(&name, idx)
                .map_err(|e| Error::Reconcile(format!("failed to create index {idx}: {e}")))?;
        }
        for idx in &to_drop {
            info!(name, ns, index = idx.as_str(), "dropping index");
            client
                .drop_index(&name, idx)
                .map_err(|e| Error::Reconcile(format!("failed to drop index {idx}: {e}")))?;
        }

        info!(name, ns, server = %server_ref, "collection indexes reconciled");
    } else {
        // Collection missing — create with indexes
        let config = CollectionConfig {
            name: name.clone(),
            indexes: col.spec.indexes.clone(),
        };
        client
            .create_collection(&config)
            .map_err(|e| Error::Reconcile(format!("failed to create collection {name}: {e}")))?;

        info!(name, ns, server = %server_ref, "collection created");
    }

    // Update Collection status with the server generation we just reconciled against.
    let col_api: Api<Collection> = Api::namespaced(ctx.client.clone(), &ns);
    let status = serde_json::json!({
        "apiVersion": "slate.io/v1",
        "kind": "Collection",
        "metadata": { "name": name },
        "status": { "server_generation": server_ready_gen }
    });
    col_api
        .patch_status(
            &name,
            &PatchParams::apply("slate-operator"),
            &Patch::Merge(&status),
        )
        .await?;

    info!(name, ns, server = %server_ref, generation = server_ready_gen, "collection status updated");
    Ok(Action::requeue(std::time::Duration::from_secs(300)))
}

pub fn error_policy(col: Arc<Collection>, error: &Error, _ctx: Arc<Context>) -> Action {
    let name = col.name_any();
    tracing::error!(name, %error, "reconcile failed");
    Action::requeue(std::time::Duration::from_secs(5))
}
