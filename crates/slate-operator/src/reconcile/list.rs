use std::collections::BTreeMap;
use std::sync::Arc;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{ObjectMeta, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::reflector::{ObjectRef, Store};
use kube::{Api, Client, Resource, ResourceExt};
use tracing::info;

use crate::Error;
use crate::crd::{Collection, ListPhase, ListStatus, ListView, ListViewSpec};

pub struct Context {
    pub client: Client,
}

/// When a Collection changes, find all ListViews that reference it.
pub fn map_collection_to_lists(
    collection: Collection,
    list_store: &Store<ListView>,
) -> Vec<ObjectRef<ListView>> {
    let name = collection.name_any();
    let ns = collection.namespace();

    list_store
        .state()
        .into_iter()
        .filter(|list| list.spec.collection_ref == name && list.namespace() == ns)
        .map(|list| ObjectRef::from_obj(list.as_ref()))
        .collect()
}

pub async fn reconcile(list: Arc<ListView>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = list.name_any();
    let ns = list.namespace().unwrap_or_else(|| "default".into());
    let col_ref = &list.spec.collection_ref;
    info!(name, ns, collection = %col_ref, "reconciling list");

    // ── Validate ────────────────────────────────────────────────
    if let Err(msg) = validate_list_spec(&list.spec) {
        info!(name, ns, error = %msg, "list validation failed");
        patch_list_status(&ctx.client, &name, &ns, &ListPhase::Error, None, &msg).await?;
        return Ok(Action::await_change());
    }

    // ── Read Collection CR ──────────────────────────────────────
    let col_api: Api<Collection> = Api::namespaced(ctx.client.clone(), &ns);
    let collection = match col_api.get_opt(col_ref).await? {
        Some(c) => c,
        None => {
            info!(name, ns, collection = %col_ref, "collection not found, pending");
            patch_list_status(
                &ctx.client,
                &name,
                &ns,
                &ListPhase::Pending,
                None,
                "collection not found",
            )
            .await?;
            return Ok(Action::requeue(std::time::Duration::from_secs(5)));
        }
    };

    // ── Check Collection readiness ──────────────────────────────
    let col_status = collection.status.as_ref();
    let col_server_gen = col_status.and_then(|s| s.server_generation);
    let col_server_addr = col_status.and_then(|s| s.server_address.as_deref());

    let (server_gen, server_addr) = match (col_server_gen, col_server_addr) {
        (Some(g), Some(addr)) => (g, addr.to_string()),
        _ => {
            info!(name, ns, collection = %col_ref, "collection not ready, pending");
            patch_list_status(
                &ctx.client,
                &name,
                &ns,
                &ListPhase::Pending,
                None,
                "collection not ready",
            )
            .await?;
            return Ok(Action::requeue(std::time::Duration::from_secs(5)));
        }
    };

    // ── Idempotency check ───────────────────────────────────────
    let current_col_gen = list.status.as_ref().and_then(|s| s.collection_generation);
    let current_phase = list.status.as_ref().and_then(|s| s.phase.as_ref());

    if current_col_gen == Some(server_gen) && current_phase == Some(&ListPhase::Ready) {
        info!(name, ns, collection = %col_ref, generation = server_gen, "list up to date");
        return Ok(Action::requeue(std::time::Duration::from_secs(300)));
    }

    // ── Build ListConfig JSON ───────────────────────────────────
    let columns: Vec<serde_json::Value> = list
        .spec
        .columns
        .iter()
        .map(|c| {
            serde_json::json!({
                "field": c.field,
                "header": c.header,
                "width": c.width,
                "pinned": c.pinned,
            })
        })
        .collect();

    let list_config = serde_json::json!({
        "id": name,
        "title": list.spec.title,
        "collection": col_ref,
        "filters": null,
        "columns": columns,
    });

    let list_config_json =
        serde_json::to_string_pretty(&list_config).expect("failed to serialize list config");

    // ── Apply ConfigMap: {name}-list ───────────────────────────
    let owner_ref = list.controller_owner_ref(&()).unwrap();

    let cm = ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!("{name}-list")),
            namespace: Some(ns.clone()),
            owner_references: Some(vec![owner_ref]),
            ..Default::default()
        },
        data: Some(BTreeMap::from([
            ("list.json".into(), list_config_json),
            ("SLATE_SERVER_ADDR".into(), server_addr),
            ("SLATE_COLLECTION".into(), col_ref.clone()),
        ])),
        ..Default::default()
    };

    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &ns);
    info!(name, ns, "applying configmap");
    cm_api
        .patch(
            &format!("{name}-list"),
            &PatchParams::apply("slate-operator"),
            &Patch::Apply(&cm),
        )
        .await?;

    // ── Patch status ────────────────────────────────────────────
    info!(name, ns, collection = %col_ref, generation = server_gen, "list reconciled");
    patch_list_status(
        &ctx.client,
        &name,
        &ns,
        &ListPhase::Ready,
        Some(server_gen),
        "",
    )
    .await?;

    Ok(Action::requeue(std::time::Duration::from_secs(300)))
}

fn validate_list_spec(spec: &ListViewSpec) -> Result<(), String> {
    if spec.collection_ref.is_empty() {
        return Err("collectionRef must not be empty".into());
    }
    if spec.title.is_empty() {
        return Err("title must not be empty".into());
    }
    if spec.columns.is_empty() {
        return Err("at least one column is required".into());
    }

    let mut seen_fields = std::collections::HashSet::new();
    for (i, col) in spec.columns.iter().enumerate() {
        if col.header.is_empty() {
            return Err(format!("column {i}: header must not be empty"));
        }
        if col.field.is_empty() {
            return Err(format!("column {i}: field must not be empty"));
        }
        if col.width == 0 {
            return Err(format!("column {i}: width must be greater than 0"));
        }
        if !seen_fields.insert(&col.field) {
            return Err(format!("column {i}: duplicate field '{}'", col.field));
        }
    }

    Ok(())
}

async fn patch_list_status(
    client: &Client,
    name: &str,
    ns: &str,
    phase: &ListPhase,
    collection_generation: Option<i64>,
    message: &str,
) -> Result<(), Error> {
    let list_api: Api<ListView> = Api::namespaced(client.clone(), ns);
    let status = serde_json::json!({
        "apiVersion": "slate.io/v1",
        "kind": "ListView",
        "metadata": { "name": name },
        "status": ListStatus {
            phase: Some(phase.clone()),
            collection_generation,
            message: if message.is_empty() { None } else { Some(message.into()) },
        },
    });
    list_api
        .patch_status(
            name,
            &PatchParams::apply("slate-operator"),
            &Patch::Merge(&status),
        )
        .await?;
    Ok(())
}

pub fn error_policy(list: Arc<ListView>, error: &Error, _ctx: Arc<Context>) -> Action {
    let name = list.name_any();
    tracing::error!(name, %error, "reconcile failed");
    Action::requeue(std::time::Duration::from_secs(5))
}
