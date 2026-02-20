use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A slate-server instance. The reconciler creates a Deployment + Service.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "slate.io",
    version = "v1",
    kind = "Server",
    namespaced,
    status = "ServerStatus",
    printcolumn = r#"{"name": "Phase", "type": "string", "jsonPath": ".status.phase"}"#,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}"#
)]
pub struct ServerSpec {
    /// Storage backend.
    pub store: StoreType,

    /// Pod resource requests and limits.
    #[serde(default)]
    pub resources: Option<ResourceRequirements>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServerStatus {
    /// Lifecycle phase of the server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<ServerPhase>,

    /// The metadata.generation that was last fully rolled out and probed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_generation: Option<i64>,

    /// Human-readable message about the current state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
pub enum ServerPhase {
    Rollout,
    Ready,
    Error,
}

/// A collection on a Server. The reconciler ensures the collection and indexes
/// exist on the referenced Server via the slate client protocol.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "slate.io",
    version = "v1",
    kind = "Collection",
    namespaced,
    status = "CollectionStatus"
)]
pub struct CollectionSpec {
    /// Reference to a Server CR name in the same namespace.
    pub server: String,

    /// Fields to index.
    #[serde(default)]
    pub indexes: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CollectionStatus {
    /// The Server's ready_generation this collection was last applied against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_generation: Option<i64>,

    /// The in-cluster address of the Server (e.g. `main-db.acme.svc.cluster.local:9600`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_address: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StoreType {
    Memory,
    Rocks,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct ResourceRequirements {
    #[serde(default)]
    pub requests: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub limits: Option<BTreeMap<String, String>>,
}

// ── List CRD ────────────────────────────────────────────────────

/// A list view over a Collection. The reconciler validates the spec,
/// resolves the server address via the Collection, and produces ConfigMaps.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "slate.io",
    version = "v1",
    kind = "ListView",
    namespaced,
    status = "ListStatus",
    printcolumn = r#"{"name": "Phase", "type": "string", "jsonPath": ".status.phase"}"#,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ListViewSpec {
    /// Reference to a Collection CR name in the same namespace.
    pub collection_ref: String,

    /// Display title.
    pub title: String,

    /// Optional description.
    #[serde(default)]
    pub description: Option<String>,

    /// Column definitions for the list view.
    pub columns: Vec<ListColumn>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListColumn {
    /// Display header.
    pub header: String,

    /// Document field path (dot-notation supported).
    pub field: String,

    /// Data type of the column.
    pub data_type: ColumnDataType,

    /// Column width in pixels.
    pub width: u32,

    /// Whether the column is pinned.
    #[serde(default)]
    pub pinned: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ColumnDataType {
    String,
    Int,
    Float,
    Bool,
    Date,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListStatus {
    /// Lifecycle phase of the list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<ListPhase>,

    /// The Collection's server_generation this list was last reconciled against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection_generation: Option<i64>,

    /// Human-readable message about the current state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
pub enum ListPhase {
    Pending,
    Ready,
    Error,
}
