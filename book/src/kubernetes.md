# Kubernetes

Slate runs on Kubernetes via a custom operator that manages the full lifecycle of database servers and their collections. The operator watches two Custom Resource Definitions — `Server` and `Collection` — and reconciles them into Deployments, Services, and database-level state.

## Resource Hierarchy

```
Namespace (tenant boundary)
└── Server (database instance)
    ├── Collection (document store + indexes)
    ├── Collection
    └── ...
```

A **Namespace** represents a tenant. All Slate resources for a tenant live in one namespace — the operator enforces no cross-namespace references.

A **Server** is a single database instance. The operator creates a Deployment (one pod) and a ClusterIP Service for it. The pod runs `slate-server` with the configured storage backend.

A **Collection** belongs to a Server. The operator connects to the server over its internal Service and creates the collection with its declared indexes. When the server rolls out a new pod (e.g. from a spec change), the operator detects the generation change and re-creates all collections on the fresh instance.

## Server

A Server CR declares a database instance.

```yaml
apiVersion: slate.io/v1
kind: Server
metadata:
  name: main-db
  namespace: acme
spec:
  store: memory
  resources:
    requests:
      memory: "1Gi"
      cpu: "500m"
    limits:
      memory: "2Gi"
      cpu: "1"
```

### Spec

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `store` | `memory` \| `rocks` | Yes | Storage backend. `memory` for ephemeral workloads (data lost on restart), `rocks` for persistent storage (requires a volume). |
| `resources` | Object | No | Pod resource requests and limits, passed directly to the container spec. |
| `resources.requests` | Map | No | CPU and memory requests (e.g. `cpu: "500m"`, `memory: "1Gi"`). |
| `resources.limits` | Map | No | CPU and memory limits. |

### Status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | `Rollout` \| `Ready` \| `Error` | Current lifecycle phase. |
| `ready_generation` | Integer | The `metadata.generation` that was last fully rolled out and confirmed ready. |
| `message` | String | Human-readable status message. |

### Lifecycle

1. Server CR is created or updated.
2. Operator applies a Deployment (1 replica) and a ClusterIP Service.
3. Phase is set to `Rollout` while the pod starts.
4. The pod writes `/tmp/ready` after `TcpListener::bind()` succeeds. Kubernetes marks the pod Ready via an exec readiness probe (`test -f /tmp/ready`).
5. Once the Deployment reports all replicas ready, the operator sets phase to `Ready` and updates `ready_generation`.
6. Any spec change (image, resources, store) triggers a new rollout — the operator detects the diff, patches the Deployment, and the cycle repeats.

### What the Operator Creates

For a Server named `main-db` in namespace `acme`:

| Resource | Name | Namespace | Notes |
|----------|------|-----------|-------|
| Deployment | `main-db` | `acme` | 1 replica, owner-referenced to the Server CR |
| Service | `main-db` | `acme` | ClusterIP on port 9600, selector matches the Deployment pods |

The Service gives collections (and other in-cluster clients) a stable address: `main-db.acme.svc.cluster.local:9600`.

## Collection

A Collection CR declares a document store and its indexes on a Server.

```yaml
apiVersion: slate.io/v1
kind: Collection
metadata:
  name: accounts
  namespace: acme
spec:
  server: main-db
  indexes:
    - status
    - score
```

### Spec

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `server` | String | Yes | Name of a Server CR in the same namespace. |
| `indexes` | List of strings | No | Fields to index. The operator ensures these indexes exist on the server, creating missing ones and dropping extra ones. |

### Status

| Field | Type | Description |
|-------|------|-------------|
| `server_generation` | Integer | The Server's `ready_generation` this collection was last reconciled against. |

### Lifecycle

1. Collection CR is created, updated, or the referenced Server transitions to `Ready`.
2. Operator checks the Server's phase — if not `Ready`, requeues.
3. Operator compares `status.server_generation` against the Server's `ready_generation`. If they match, the collection is up to date.
4. If they differ (new server rollout, or first reconcile), the operator connects to the server via its Service and:
   - Creates the collection if it doesn't exist (with all declared indexes).
   - If it exists, diffs the actual indexes against the declared indexes — creates missing ones, drops extra ones.
5. Updates `status.server_generation` to the current `ready_generation`.

This means every Server rollout automatically triggers re-reconciliation of all its collections — the fresh pod gets its collections and indexes set up without manual intervention.

## Example

A tenant `acme` with one server and two collections:

```yaml
# Namespace acts as tenant boundary
apiVersion: v1
kind: Namespace
metadata:
  name: acme
---
# Database server with in-memory storage
apiVersion: slate.io/v1
kind: Server
metadata:
  name: main-db
  namespace: acme
spec:
  store: memory
  resources:
    requests:
      memory: "1Gi"
      cpu: "500m"
    limits:
      memory: "2Gi"
      cpu: "1"
---
# Collection with two indexes
apiVersion: slate.io/v1
kind: Collection
metadata:
  name: accounts
  namespace: acme
spec:
  server: main-db
  indexes:
    - status
    - score
---
# Collection with one index
apiVersion: slate.io/v1
kind: Collection
metadata:
  name: prospects
  namespace: acme
spec:
  server: main-db
  indexes:
    - stage
```

After applying, the end state:

```
$ kubectl get servers -n acme
NAME      PHASE   AGE
main-db   Ready   5m

$ kubectl get collections -n acme
NAME        AGE
accounts    5m
prospects   5m

$ kubectl get pods -n acme
NAME                       READY   STATUS    RESTARTS   AGE
main-db-5766f67848-m8x9q   1/1     Running   0          5m
```
