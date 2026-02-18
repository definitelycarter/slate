use std::collections::BTreeMap;
use std::sync::Arc;

use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, ExecAction, PodSpec, PodTemplateSpec, Probe, Service,
    ServicePort, ServiceSpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::api::{ObjectMeta, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, Client, Resource, ResourceExt};
use tracing::info;

use crate::Error;
use crate::crd::{Server, ServerPhase, ServerStatus, StoreType};

pub const SLATE_PORT: i32 = 9600;

pub struct Context {
    pub client: Client,
    pub server_image: String,
}

pub async fn reconcile(server: Arc<Server>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = server.name_any();
    let ns = server.namespace().unwrap_or_else(|| "default".into());
    info!(name, ns, "reconciling server");

    let labels: BTreeMap<String, String> = BTreeMap::from([
        ("app.kubernetes.io/name".into(), "slate-server".into()),
        ("app.kubernetes.io/instance".into(), name.clone()),
        (
            "app.kubernetes.io/managed-by".into(),
            "slate-operator".into(),
        ),
    ]);

    let owner_ref = server.controller_owner_ref(&()).unwrap();

    let store_env = match server.spec.store {
        StoreType::Memory => "memory",
        StoreType::Rocks => "rocks",
    };

    let env = vec![
        EnvVar {
            name: "SLATE_ADDR".into(),
            value: Some(format!("0.0.0.0:{SLATE_PORT}")),
            ..Default::default()
        },
        EnvVar {
            name: "SLATE_STORE".into(),
            value: Some(store_env.into()),
            ..Default::default()
        },
    ];

    let resources = server.spec.resources.as_ref().map(|r| {
        let mut k8s_resources = k8s_openapi::api::core::v1::ResourceRequirements::default();
        if let Some(requests) = &r.requests {
            k8s_resources.requests = Some(
                requests
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            k8s_openapi::apimachinery::pkg::api::resource::Quantity(v.clone()),
                        )
                    })
                    .collect(),
            );
        }
        if let Some(limits) = &r.limits {
            k8s_resources.limits = Some(
                limits
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            k8s_openapi::apimachinery::pkg::api::resource::Quantity(v.clone()),
                        )
                    })
                    .collect(),
            );
        }
        k8s_resources
    });

    // ── Deployment ──────────────────────────────────────────────
    let deployment = Deployment {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(ns.clone()),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_ref.clone()]),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "slate-server".into(),
                        image: Some(ctx.server_image.clone()),
                        ports: Some(vec![ContainerPort {
                            container_port: SLATE_PORT,
                            ..Default::default()
                        }]),
                        env: Some(env),
                        resources,
                        image_pull_policy: Some("IfNotPresent".into()),
                        readiness_probe: Some(Probe {
                            exec: Some(ExecAction {
                                command: Some(vec![
                                    "test".into(),
                                    "-f".into(),
                                    "/tmp/ready".into(),
                                ]),
                            }),
                            initial_delay_seconds: Some(1),
                            period_seconds: Some(5),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    };

    let deploy_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ns);
    let needs_deploy = match deploy_api.get_opt(&name).await? {
        Some(existing) => !deployment_matches(&existing, &deployment),
        None => true,
    };
    if needs_deploy {
        info!(name, ns, "applying deployment");
        deploy_api
            .patch(
                &name,
                &PatchParams::apply("slate-operator"),
                &Patch::Apply(&deployment),
            )
            .await?;
    }

    // ── Service ─────────────────────────────────────────────────
    let service = Service {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(ns.clone()),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_ref]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(labels),
            ports: Some(vec![ServicePort {
                port: SLATE_PORT,
                target_port: Some(
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(SLATE_PORT),
                ),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };

    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
    let needs_svc = match svc_api.get_opt(&name).await? {
        Some(existing) => !service_matches(&existing, &service),
        None => true,
    };
    if needs_svc {
        info!(name, ns, "applying service");
        svc_api
            .patch(
                &name,
                &PatchParams::apply("slate-operator"),
                &Patch::Apply(&service),
            )
            .await?;
    }

    // ── Status: track lifecycle phase ─────────────────────────────
    let generation = server.metadata.generation.unwrap_or(0);
    let current_ready_gen = server
        .status
        .as_ref()
        .and_then(|s| s.ready_generation)
        .unwrap_or(0);
    let current_phase = server.status.as_ref().and_then(|s| s.phase.as_ref());

    let deploy = deploy_api.get(&name).await?;
    let deploy_gen = deploy.metadata.generation.unwrap_or(0);
    let deploy_status = deploy.status.as_ref();
    let observed_gen = deploy_status
        .and_then(|s| s.observed_generation)
        .unwrap_or(0);
    let replicas = deploy_status.and_then(|s| s.replicas).unwrap_or(0);
    let ready = deploy_status.and_then(|s| s.ready_replicas).unwrap_or(0);
    let updated = deploy_status.and_then(|s| s.updated_replicas).unwrap_or(0);

    let rolled_out =
        observed_gen >= deploy_gen && ready >= replicas && updated >= replicas && replicas > 0;

    if !rolled_out {
        // Deployment is mid-rollout — set phase to Rollout if not already.
        if current_phase != Some(&ServerPhase::Rollout) {
            info!(name, ns, "phase -> Rollout");
            patch_server_status(
                &ctx.client,
                &name,
                &ns,
                &ServerPhase::Rollout,
                current_ready_gen,
                "deployment rolling",
            )
            .await?;
        }
        info!(
            name,
            ns, replicas, ready, updated, "deployment rolling, requeuing"
        );
        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
    }

    // Deployment is rolled out and pods are ready (readiness probe passed).
    // If generation hasn't changed, we're already Ready.
    if current_ready_gen == generation && current_phase == Some(&ServerPhase::Ready) {
        info!(name, ns, "server reconciled");
        return Ok(Action::requeue(std::time::Duration::from_secs(300)));
    }

    // Pods are ready (K8s readiness probe confirms the server is listening).
    info!(name, ns, generation, "deployment ready, phase -> Ready");
    patch_server_status(&ctx.client, &name, &ns, &ServerPhase::Ready, generation, "").await?;
    info!(name, ns, "server reconciled");
    Ok(Action::requeue(std::time::Duration::from_secs(300)))
}

async fn patch_server_status(
    client: &Client,
    name: &str,
    ns: &str,
    phase: &ServerPhase,
    ready_generation: i64,
    message: &str,
) -> Result<(), Error> {
    let server_api: Api<Server> = Api::namespaced(client.clone(), ns);
    let status = serde_json::json!({
        "apiVersion": "slate.io/v1",
        "kind": "Server",
        "metadata": { "name": name },
        "status": ServerStatus {
            phase: Some(phase.clone()),
            ready_generation: Some(ready_generation),
            message: if message.is_empty() { None } else { Some(message.into()) },
        },
    });
    server_api
        .patch_status(
            name,
            &PatchParams::apply("slate-operator"),
            &Patch::Merge(&status),
        )
        .await?;
    Ok(())
}

pub fn error_policy(server: Arc<Server>, error: &Error, _ctx: Arc<Context>) -> Action {
    let name = server.name_any();
    tracing::error!(name, %error, "reconcile failed");
    Action::requeue(std::time::Duration::from_secs(5))
}

/// Compare the fields we manage on the Deployment to avoid no-op patches.
fn deployment_matches(existing: &Deployment, desired: &Deployment) -> bool {
    let Some(existing_spec) = &existing.spec else {
        return false;
    };
    let Some(desired_spec) = &desired.spec else {
        return false;
    };

    if existing_spec.replicas != desired_spec.replicas {
        return false;
    }

    let ex_containers = existing_spec.template.spec.as_ref().map(|s| &s.containers);
    let de_containers = desired_spec.template.spec.as_ref().map(|s| &s.containers);

    let (Some(ex_c), Some(de_c)) = (ex_containers, de_containers) else {
        return false;
    };

    if ex_c.len() != de_c.len() {
        return false;
    }

    for (e, d) in ex_c.iter().zip(de_c.iter()) {
        if e.image != d.image
            || e.env != d.env
            || e.resources != d.resources
            || e.readiness_probe != d.readiness_probe
        {
            return false;
        }
    }

    true
}

/// Compare the fields we manage on the Service to avoid no-op patches.
fn service_matches(existing: &Service, desired: &Service) -> bool {
    let (Some(ex_spec), Some(de_spec)) = (&existing.spec, &desired.spec) else {
        return false;
    };
    ex_spec.selector == de_spec.selector && ex_spec.ports == de_spec.ports
}
