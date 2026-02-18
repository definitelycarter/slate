# Testing the Operator (Local Kubernetes)

## Build & Deploy

Build both images using a random tag:

```bash
TAG=$(head -c 3 /dev/urandom | xxd -p)
docker build --target server -t slate-server:$TAG .
docker build --target operator -t slate-operator:$TAG .
```

Load images into all cluster nodes:

```bash
for node in desktop-control-plane desktop-worker desktop-worker2 desktop-worker3; do
  docker save slate-server:$TAG | docker exec -i $node ctr -n k8s.io images import -
  docker save slate-operator:$TAG | docker exec -i $node ctr -n k8s.io images import -
done
```

Update the image tags in `deploy.yaml` (`slate-operator:<TAG>` and `SLATE_SERVER_IMAGE: slate-server:<TAG>`), then apply:

```bash
cargo run --bin slate-operator -- --print-crds | kubectl apply -f -
kubectl apply -f crates/slate-operator/deploy.yaml
kubectl apply -f crates/slate-operator/test-resources.yaml
```

## Validating via Logs

After deploying a new image, the first pod created during rollout can have **empty logs** (containerd log capture issue during transition). Delete the pod and let the ReplicaSet recreate it:

```bash
kubectl delete pod <pod-name> -n slate-system
```

The fresh pod will have correct logs.

Check operator logs (container name is `operator`):

```bash
kubectl logs <pod> -n slate-system -c operator
```

Check server logs:

```bash
kubectl logs <pod> -n acme
```

### Operator Logs - What to Look For

- Startup: `starting slate-operator image=<image>`
- Server reconcile: `reconciling server` -> `applying deployment` (if spec changed) -> `server reconciled`
- Rollout lifecycle: `phase -> Rollout` -> requeue loop -> `deployment ready, phase -> Ready`
- Collection reconcile: `reconciling collection` -> `collection up to date ... generation=N`
- Errors: `reconcile failed` lines

### Server Logs - What to Look For

- `slate-server listening on 0.0.0.0:9600`
- `readiness file written: /tmp/ready` (confirms readiness probe will pass)

## End-State Checks

```bash
kubectl get servers -A        # Phase=Ready
kubectl get collections -A    # present with correct age
kubectl get pods -n acme      # 1/1 Ready (readiness probe passed)
```
