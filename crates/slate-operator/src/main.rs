mod crd;
mod reconcile;

use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use kube::runtime::Controller;
use kube::runtime::watcher::Config;
use kube::{Api, Client, CustomResourceExt};
use tracing::info;

use crd::{Collection, ListView, Server};
use reconcile::{collection, list, server};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),

    #[error("reconcile error: {0}")]
    Reconcile(String),
}

#[tokio::main]
async fn main() {
    // --print-crds: dump CRD YAML and exit
    if std::env::args().any(|a| a == "--print-crds") {
        let crds = vec![
            serde_yaml::to_string(&Server::crd()).expect("failed to serialize Server CRD"),
            serde_yaml::to_string(&Collection::crd()).expect("failed to serialize Collection CRD"),
            serde_yaml::to_string(&ListView::crd()).expect("failed to serialize ListView CRD"),
        ];
        print!("{}", crds.join("\n---\n"));
        return;
    }

    tracing_subscriber::fmt::init();

    let client = Client::try_default()
        .await
        .expect("failed to create kube client");

    let server_image =
        std::env::var("SLATE_SERVER_IMAGE").unwrap_or_else(|_| "slate-server:latest".into());

    info!(image = %server_image, "starting slate-operator");

    let server_ctx = Arc::new(server::Context {
        client: client.clone(),
        server_image,
    });

    let collection_ctx = Arc::new(collection::Context {
        client: client.clone(),
    });

    let list_ctx = Arc::new(list::Context {
        client: client.clone(),
    });

    let servers = Api::<Server>::all(client.clone());
    let collections = Api::<Collection>::all(client.clone());
    let lists = Api::<ListView>::all(client.clone());

    let server_ctrl = Controller::new(servers, Config::default())
        .owns(Api::<Deployment>::all(client.clone()), Config::default())
        .owns(Api::<Service>::all(client.clone()), Config::default())
        .run(server::reconcile, server::error_policy, server_ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("reconciled server {:?}", o),
                Err(e) => tracing::error!("server reconcile failed: {}", e),
            }
        });

    let col_controller = Controller::new(collections, Config::default());
    let col_store = col_controller.store();
    let collection_ctrl = col_controller
        .watches(
            Api::<Server>::all(client.clone()),
            Config::default(),
            move |server| collection::map_server_to_collections(server, &col_store),
        )
        .run(
            collection::reconcile,
            collection::error_policy,
            collection_ctx,
        )
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("reconciled collection {:?}", o),
                Err(e) => tracing::error!("collection reconcile failed: {}", e),
            }
        });

    let list_controller = Controller::new(lists, Config::default());
    let list_store = list_controller.store();
    let list_ctrl = list_controller
        .watches(
            Api::<Collection>::all(client.clone()),
            Config::default(),
            move |col| list::map_collection_to_lists(col, &list_store),
        )
        .owns(Api::<ConfigMap>::all(client.clone()), Config::default())
        .run(list::reconcile, list::error_policy, list_ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("reconciled list {:?}", o),
                Err(e) => tracing::error!("list reconcile failed: {}", e),
            }
        });

    tokio::join!(server_ctrl, collection_ctrl, list_ctrl);
}
