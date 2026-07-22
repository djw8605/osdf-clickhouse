# Altinity ClickHouse Operator — install notes

We use the **Altinity ClickHouse Operator** to manage the ClickHouse cluster and
ClickHouse Keeper quorum.

- Upstream: <https://github.com/Altinity/clickhouse-operator>
- **Pinned version: `0.24.5`** (change `OPERATOR_VERSION` below to upgrade).

## Why the operator

It reconciles a single `ClickHouseInstallation` (CHI) custom resource into all
the StatefulSets, Services, ConfigMaps and pod templates for a sharded +
replicated cluster, wires up the `remote_servers` cluster topology and the
Keeper (ZooKeeper-protocol) coordination config, and exports Prometheus metrics.
It also installs the `ClickHouseKeeperInstallation` (CHK) CRD used by
`../keeper.yaml`.

## Install (cluster-scoped, one operator per cluster)

```bash
export OPERATOR_VERSION=0.24.5
# Namespace the operator runs in (kube-system is the upstream default; on NRP
# use a namespace you control).
export OPERATOR_NAMESPACE=clickhouse-operator

kubectl apply -f \
  "https://github.com/Altinity/clickhouse-operator/raw/${OPERATOR_VERSION}/deploy/operator/clickhouse-operator-install-bundle.yaml"
```

The bundle installs, at the pinned version:

- CRDs: `ClickHouseInstallation`, `ClickHouseInstallationTemplate`,
  `ClickHouseKeeperInstallation`, `ClickHouseOperatorConfiguration`.
- The operator Deployment + RBAC + a metrics-exporter sidecar.

> The install bundle is **cluster-scoped** (it creates cluster-wide CRDs and
> ClusterRoles). You typically need cluster-admin once to install it. The CHI /
> CHK custom resources themselves are **namespaced** and live alongside the
> workloads — no elevated privileges needed to create those.

### Pinning by digest (production)

Tags are mutable. After install, record the operator image digest and pin it:

```bash
kubectl -n "${OPERATOR_NAMESPACE}" get deploy clickhouse-operator \
  -o jsonpath='{.spec.template.spec.containers[*].image}'
```

Do the same for the ClickHouse server / Keeper images referenced in
`../clickhouse-installation.yaml` and `../keeper.yaml`.

## Verify

```bash
kubectl -n "${OPERATOR_NAMESPACE}" rollout status deploy/clickhouse-operator
kubectl get crd | grep clickhouse
```

## Order of apply

1. Operator (this file).
2. `../../clickhouse/keeper.yaml`            — 3-node Keeper quorum.
3. `../../clickhouse/secret.example.yaml`    — copy to `secret.yaml`, set a real password, apply.
4. `../../clickhouse/clickhouse-installation.yaml` — the CHI (2 shards × 2 replicas).
5. `../../clickhouse/servicemonitor.yaml`, `../networkpolicy.yaml`.
6. `../../clickhouse/schema/apply-job.yaml`  — creates database, tables, views.

All manifests are idempotent; re-applying is safe.
