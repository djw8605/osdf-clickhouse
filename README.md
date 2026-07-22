# osdf-clickhouse

A production data pipeline that ingests the **fstream** (file-access) records
from the
[xrootd-monitoring-shoveler](https://github.com/opensciencegrid/xrootd-monitoring-shoveler)
**collector** into a sharded + replicated **ClickHouse** cluster on Kubernetes.

Two deliverables in one repo:

1. **ClickHouse on Kubernetes** — Altinity operator, a `ClickHouseInstallation`
   (2 shards × 2 replicas), a 3-node ClickHouse Keeper quorum, hardened pods,
   persistent storage, metrics, network policy, and an idempotent schema.
2. **Go ingester** — consumes the fstream exchange (`shoveled-xrd`) as competing
   consumers, authenticates with a plain username/password, and batch-inserts
   into ClickHouse with at-least-once semantics that the schema's dedup key
   absorbs.

> **Scope:** fstream only. The gstream exchanges (`xrd-cache-events`,
> `xrd-tcp-events`, `xrd-tpc-events`) and the WLCG exchanges (`xrd-wlcg-*`) are
> **not** consumed.

> Full requirements are in [`SPEC.md`](./SPEC.md).

---

## Architecture

```mermaid
flowchart TB
    subgraph edge["XRootD data source"]
        XRD["XRootD servers<br/>UDP monitoring packets"]
        COL["xrootd-monitoring COLLECTOR<br/>correlates packets into JSON records"]
        XRD -->|"UDP :9993"| COL
    end

    subgraph broker["RabbitMQ broker (existing, OSG)"]
        EXF["exchange: shoveled-xrd<br/>fstream / CollectorRecord"]
        EXG["exchanges: xrd-cache/tcp/tpc-events<br/>gstream — NOT consumed"]
        EXW["exchanges: xrd-wlcg-*<br/>WLCG — NOT consumed"]
        Q["durable queue<br/>osdf-clickhouse-ingester.shoveled-xrd"]
        EXF --> Q
    end

    COL -->|"publish: empty routing key, text/plain"| EXF
    COL -.->|"not bound"| EXG
    COL -.->|"not bound"| EXW

    subgraph ing["Ingester — K8s Deployment (competing consumers)"]
        direction LR
        P1["pod 1"]
        P2["pod 2"]
        P3["pod N"]
    end

    Q -->|"consume, prefetch,<br/>username/password auth"| ing
    ing -.->|"ack AFTER commit /<br/>nack-requeue on failure"| Q

    subgraph chs["ClickHouse cluster (Altinity operator)"]
        KEEP["ClickHouse Keeper<br/>3-node quorum"]
        DIST["raw_records_dist<br/>Distributed — shard by cityHash64(event_id)"]
        subgraph shards["shards × replicas"]
            direction LR
            SH1["shard 1<br/>raw_records_local ×2 replicas<br/>ReplicatedReplacingMergeTree<br/>60-day TTL, dedup on event_id"]
            SH2["shard 2<br/>raw_records_local ×2 replicas<br/>ReplicatedReplacingMergeTree<br/>60-day TTL, dedup on event_id"]
        end
        RU["rollup_hourly / rollup_daily<br/>ReplicatedAggregatingMergeTree<br/>2-year TTL"]
        DIST --> SH1
        DIST --> SH2
        SH1 -->|"materialized view on insert"| RU
        SH2 -->|"materialized view on insert"| RU
        SH1 <-.->|"replication coordination"| KEEP
        SH2 <-.->|"replication coordination"| KEEP
    end

    ing -->|"native batch INSERT<br/>10k rows / 1s, clickhouse-go/v2"| DIST

    subgraph obs["Observability & consumers"]
        PROM["Prometheus"]
        GRAF["Grafana / analysts<br/>reader user"]
    end

    PROM -.->|"scrape /metrics :9100"| ing
    PROM -.->|"scrape /metrics :9363"| chs
    GRAF -->|"countMerge / sumMerge / uniqMerge"| RU
    GRAF -->|"ad-hoc FINAL reads"| DIST
```

Coordination (ReplicatedMergeTree) is provided by a 3-node **ClickHouse Keeper**
quorum. Prometheus scrapes both ClickHouse and the ingester.

---

## Step 0 findings (verified against upstream)

Everything below was read directly from the upstream source (pinned clone) and
drives the schema and the consumer. **Re-verify if upstream changes.**

### The record: `CollectorRecord`

`collector/correlator.go` defines `type CollectorRecord struct` with **76**
JSON-tagged fields — far more than the abbreviated README example. All of them
are modeled as typed columns (see [`clickhouse/schema/01_raw.sql`](./clickhouse/schema/01_raw.sql)
and the Go struct in [`ingester/internal/model/record.go`](./ingester/internal/model/record.go)),
including the accounting-relevant `serverID` / `server_hostname` / `server_ip`,
`site`, `user` / `user_dn` / `vo`, `host`, token claims, file path fields, byte
counters, and per-operation size stats.

### The exchanges — and why we consume only fstream

`config.go` defaults (confirmed):

| Exchange           | Payload                          | Consumed here? |
|--------------------|----------------------------------|----------------|
| `shoveled-xrd`     | **fstream** — correlated file-close `CollectorRecord` | **Yes** |
| `xrd-cache-events` | gstream cache event (map)        | No             |
| `xrd-tcp-events`   | gstream TCP event (map)          | No             |
| `xrd-tpc-events`   | gstream TPC event (map)          | No             |
| `xrd-wlcg-*`       | WLCG-converted records           | No             |

Only `shoveled-xrd` carries the structured `CollectorRecord`. The gstream
exchanges carry heterogeneous event maps (`map[string]interface{}` with fields
like `file_path`, `block_size`, `source`, `destination`, …) produced by
`emitGStreamEvent` in `cmd/collector/main.go`; the WLCG exchanges only receive
records when the VO is `cms` or the path starts with `/store` or `/user/dteam`.

**This ingester consumes fstream only** — a deliberate scope choice. All typed
columns are derived from `CollectorRecord`, so a single wide schema fits the one
stream exactly. `INGESTER_EXCHANGES` is still a comma-separated list, so more
exchanges *could* be added later, but the default (and intent) is `shoveled-xrd`
alone. A `source_exchange` column records provenance, and a **`raw_json` column
preserves the exact original body** so nothing is lost if the upstream struct
evolves (recover fields with `JSONExtract*`).

### Publisher behavior

`amqp.go` publishes with `channel.Publish(exchange, "" /*routing key*/, false,
false, {ContentType: "text/plain", Body: json})`. Empty routing key, no
self-declaration of the exchange (exchanges are pre-provisioned on the broker).

### Exchange-type assumption (stated blocker)

The publisher never declares the exchanges (they are pre-provisioned on the OSG
broker), so **the exchange type is not determinable from source alone.** Empty
routing key + the OSG collector fan-out pattern strongly implies **fanout**.

**We proceed assuming `fanout`**, and make it safe and configurable:

- `INGESTER_EXCHANGE_TYPE` (default `fanout`) and `INGESTER_BIND_KEY` (default
  `""`).
- `INGESTER_EXCHANGE_PASSIVE=true` (default) → the ingester declares each
  exchange **passively**, so it never redeclares with a conflicting type (which
  would kill the channel). If passive declare fails, the log says so and you set
  the correct type. For a topic exchange, set `INGESTER_BIND_KEY="#"`.

### Auth

The ingester authenticates with a plain **username/password** embedded in the
AMQP URL (`amqps://user:password@host:port/vhost`). The URL is supplied via the
`INGESTER_AMQP_URL` key of the ingester Secret. See
[Ingester configuration](#ingester-configuration).

---

## Repo layout

```
.
├── README.md                       # this file
├── SPEC.md                         # the build spec
├── Makefile                        # build / test / docker / lint / yaml-validate
├── clickhouse/
│   ├── operator/README.md          # operator install notes + pinned version (0.24.5)
│   ├── clickhouse-installation.yaml # CHI: 2 shards × 2 replicas
│   ├── keeper.yaml                 # 3-node ClickHouse Keeper (CHK)
│   ├── secret.example.yaml         # app-user passwords (copy to secret.yaml)
│   ├── servicemonitor.yaml         # Prometheus scrape of ClickHouse :9363
│   ├── networkpolicy.yaml          # ingress restricted to ingester + Grafana + Prom
│   └── schema/                     # idempotent ON CLUSTER SQL + apply Job
│       ├── 00_database.sql
│       ├── 01_raw.sql              # ReplicatedReplacingMergeTree + Distributed
│       ├── 02_rollup_hourly.sql    # ReplicatedAggregatingMergeTree + MV
│       ├── 03_rollup_daily.sql
│       ├── 04_read_examples.sql    # countMerge / sumMerge / uniqMerge
│       └── apply-job.yaml
├── ingester/
│   ├── go.mod / go.sum
│   ├── cmd/ingester/main.go        # pipeline + graceful shutdown
│   ├── internal/
│   │   ├── config/                 # 12-factor env config
│   │   ├── model/                  # CollectorRecord + dedup-id (+ tests)
│   │   ├── metrics/                # Prometheus collectors
│   │   ├── amqp/                   # resilient consumer, username/password auth
│   │   ├── chwriter/               # ClickHouse batch writer
│   │   └── health/                 # /healthz /readyz /metrics
│   ├── test/                       # testcontainers integration test (build tag)
│   └── Dockerfile                  # multi-stage, distroless nonroot
└── deploy/ingester/                # Deployment, ConfigMap, Secret, Service,
                                    # ServiceMonitor, HPA, NetworkPolicy
```

---

## Pinned versions

| Component                 | Version                                   |
|---------------------------|-------------------------------------------|
| Altinity CH operator      | `0.24.5`                                  |
| ClickHouse server / keeper| `clickhouse/clickhouse-server:24.8` (LTS) |
| Go toolchain              | `1.25`                                    |
| clickhouse-go             | `v2.30.0`                                 |
| rabbitmq/amqp091-go       | `v1.10.0`                                 |
| prometheus/client_golang  | `v1.20.5`                                 |
| ingester base image       | `gcr.io/distroless/static-debian12:nonroot` |

Tags are mutable — pin container images by **digest** in production (see
`clickhouse/operator/README.md`).

---

## Deploy

Set your namespace once:

```bash
export NS=xrootd-monitoring      # PER-CLUSTER: pick your namespace
kubectl create namespace "$NS" || true
```

### Per-cluster values you MUST set

| Value                       | Where                                             |
|-----------------------------|---------------------------------------------------|
| **Storage class** (RWO block, e.g. `rook-ceph-block` on NRP) | `clickhouse/keeper.yaml`, `clickhouse/clickhouse-installation.yaml` (`storageClassName`) |
| **Data PVC size** (~60 days + headroom) | `clickhouse-installation.yaml` `data-volume` |
| **Namespace**               | all `kubectl -n $NS` applies                      |
| **Broker URL / vhost**      | `deploy/ingester/configmap.yaml`                  |
| **Broker URL + credentials**| `deploy/ingester/secret.yaml` (`INGESTER_AMQP_URL`)|
| **ClickHouse passwords**    | `clickhouse/secret.yaml`, `deploy/ingester/secret.yaml` |
| **Resource sizing**         | CHI pod template + ingester Deployment            |
| **Prometheus release label**| `*/servicemonitor.yaml` (if your Prom selects by it)|
| **Grafana/Prom pod labels** | `networkpolicy.yaml` selectors                    |
| **Ingester image**          | `deploy/ingester/deployment.yaml`, `Makefile IMAGE`|

### 1) Operator

```bash
kubectl apply -f \
  https://github.com/Altinity/clickhouse-operator/raw/0.24.5/deploy/operator/clickhouse-operator-install-bundle.yaml
```

See [`clickhouse/operator/README.md`](./clickhouse/operator/README.md).

### 2) Keeper, secrets, ClickHouse

```bash
kubectl -n $NS apply -f clickhouse/keeper.yaml

cp clickhouse/secret.example.yaml clickhouse/secret.yaml   # edit passwords
kubectl -n $NS apply -f clickhouse/secret.yaml

kubectl -n $NS apply -f clickhouse/clickhouse-installation.yaml
kubectl -n $NS apply -f clickhouse/servicemonitor.yaml
kubectl -n $NS apply -f clickhouse/networkpolicy.yaml

kubectl -n $NS wait --for=condition=Ready pod -l clickhouse.altinity.com/chi=xrootd --timeout=600s
```

### 3) Schema

```bash
kubectl -n $NS create configmap clickhouse-schema \
  --from-file=clickhouse/schema/ --dry-run=client -o yaml | kubectl -n $NS apply -f -
kubectl -n $NS apply -f clickhouse/schema/apply-job.yaml
kubectl -n $NS wait --for=condition=complete job/clickhouse-schema-apply --timeout=300s
```

### 4) Ingester

```bash
make docker IMAGE=<your-registry>/osdf-clickhouse-ingester TAG=0.1.0
make docker-push IMAGE=<your-registry>/osdf-clickhouse-ingester TAG=0.1.0
# set the image in deploy/ingester/deployment.yaml

kubectl -n $NS apply -f deploy/ingester/configmap.yaml
cp deploy/ingester/secret.example.yaml deploy/ingester/secret.yaml   # edit AMQP URL + ch-password
kubectl -n $NS apply -f deploy/ingester/secret.yaml
kubectl -n $NS apply -f deploy/ingester/service.yaml
kubectl -n $NS apply -f deploy/ingester/servicemonitor.yaml
kubectl -n $NS apply -f deploy/ingester/networkpolicy.yaml
kubectl -n $NS apply -f deploy/ingester/deployment.yaml
kubectl -n $NS apply -f deploy/ingester/hpa.yaml
```

---

## Ingester configuration

All via environment variables (prefix `INGESTER_`). Defaults in **bold**.

| Variable | Default | Meaning |
|----------|---------|---------|
| `INGESTER_AMQP_URL` | `amqp://guest:guest@localhost:5672/` | Broker URL **with** `user:password`. Supply via the Secret. |
| `INGESTER_EXCHANGES` | **`shoveled-xrd`** | Comma-separated exchanges (fstream only by default). |
| `INGESTER_EXCHANGE_TYPE` | **`fanout`** | Type used for (passive) declare. |
| `INGESTER_EXCHANGE_PASSIVE` | **`true`** | Passive declare — never redeclare with a conflicting type. |
| `INGESTER_BIND_KEY` | **`""`** | Binding key. `""` for fanout/direct, `#` for topic. |
| `INGESTER_QUEUE_PREFIX` | **`osdf-clickhouse-ingester`** | Queue = `<prefix>.<exchange>`, shared by all pods. |
| `INGESTER_PREFETCH` | **`2000`** | QoS prefetch per consumer. |
| `INGESTER_DEAD_LETTER_EXCHANGE` | *(empty)* | If set, failed batches nack **without** requeue (routed to DLX). Else nack-requeue. |
| `INGESTER_CH_ADDRS` | `localhost:9000` | Comma-separated `host:port` (native). Round-robin + failover. |
| `INGESTER_CH_DATABASE` | **`xrootd`** | Database. |
| `INGESTER_CH_TABLE` | **`raw_records_dist`** | Insert target (Distributed table). |
| `INGESTER_CH_USERNAME` | `default` | ClickHouse user (use `ingester`). |
| `INGESTER_CH_PASSWORD` / `_FILE` | *(empty)* | Password inline or from a mounted file. |
| `INGESTER_CH_SECURE` / `_SKIP_VERIFY` | `false` / `false` | TLS to ClickHouse / skip verify (dev). |
| `INGESTER_BATCH_SIZE` | **`10000`** | Flush when the batch reaches this many rows. |
| `INGESTER_FLUSH_INTERVAL` | **`1s`** | Flush at least this often. |
| `INGESTER_RECONNECT_MIN` / `_MAX` | `1s` / `30s` | Backoff bounds (AMQP, CH, insert retry). |
| `INGESTER_METRICS_ADDR` | **`:9100`** | `/metrics`, `/healthz`, `/readyz`. |
| `INGESTER_LOG_LEVEL` / `_FORMAT` | `info` / `json` | Structured logging (slog). |

**Insert path choice** (Distributed vs. direct-to-local): we insert through the
**Distributed** table. Tradeoff — the Distributed table gives cluster-wide
routing and lets the shard key (`cityHash64(event_id)`) place every redelivery
of the same event on the same shard, which is required for `ReplacingMergeTree`
dedup to work (dedup is per-shard). The cost is one extra network hop per row
vs. inserting straight to a local table round-robined by the client. We chose
correctness of dedup over that hop; with 10k-row batches the hop is negligible.

---

## Data model & dedup

### Deterministic `event_id`

RabbitMQ is at-least-once, and `CollectorRecord` has no stable unique id, so the
ingester computes one (`internal/model/record.go`): SHA-256 (first 128 bits,
hex) over the natural key — `serverID`, `server`, `filename`,
`logical_dirname`, `start_time`, `end_time`, `filesize`, and the byte counters.
A redelivery reproduces these exactly ⇒ same id ⇒ collapsed by
`ReplacingMergeTree`. Because the id is derived from struct fields (not raw
bytes), two byte-different JSON encodings of the same record still dedup.

`event_id` is part of `ORDER BY (server_hostname, event_time, event_id)`, and the
`ingest_time` version column makes the latest-inserted copy win.

**Failure modes (accepted):**

- Two genuinely distinct events sharing every hashed field would collapse to one
  row. `serverID+filename+start+end+bytes` is unique per file close in practice,
  so this is vanishingly unlikely.
- *Dedup is eventual:* `ReplacingMergeTree` collapses on background merge. For
  exact counts on small/recent ranges use `FINAL` (expensive) or, better, read
  the **rollups**, which sum idempotent states.

### Type conversions

Upstream byte counters are `int64` and op counts `int32`; the schema uses
`UInt64`/`UInt32` (per spec). The writer clamps negative values (which would
indicate corrupt input) to `0` rather than wrapping. `server_ip` is stored as
`IPv6` (IPv4 maps into `::ffff:0:0/96`); unparseable/empty becomes `::`.

---

## Observability

Ingester metrics (`:9100/metrics`): `ingester_messages_consumed_total{exchange}`,
`ingester_parse_errors_total`, `ingester_rows_inserted_total`,
`ingester_batches_committed_total`, `ingester_insert_failures_total`,
`ingester_dead_letters_total`, `ingester_insert_latency_seconds` (histogram),
`ingester_current_batch_size`, `ingester_amqp_reconnects_total`,
`ingester_clickhouse_reconnects_total`, `ingester_ready`.

Probes: `/healthz` (liveness — process up) and `/readyz` (readiness — flips to
503 while ClickHouse is unreachable, so a stuck pod stops receiving traffic).

---

## Build & test

```bash
make build              # static binary
make test               # unit tests (config + model: parsing & dedup-id)
make test-integration   # testcontainers RabbitMQ + ClickHouse (needs Docker)
make yaml-validate      # parse-check all manifests
make docker             # multi-stage distroless image
```

The integration test (`ingester/test/`, build tag `integration`) stands up real
RabbitMQ and ClickHouse containers, publishes records (including a duplicate),
runs the consumer + writer, and asserts the duplicate collapses via
`ReplacingMergeTree`.

---

## Operator runbook

### Scale shards / replicas

Edit `clickhouse/clickhouse-installation.yaml`:

```yaml
layout:
  shardsCount: 2      # change me
  replicasCount: 2    # change me
```

`kubectl -n $NS apply -f clickhouse/clickhouse-installation.yaml`. The operator
adds StatefulSets and updates `remote_servers`.

- **Adding replicas** is transparent — new replicas fetch existing parts via
  Keeper. No data movement needed.
- **Adding shards** does *not* rebalance existing data (new rows distribute by
  `cityHash64(event_id)` going forward). To rebalance historical data, create a
  temporary Distributed table over the new topology and
  `INSERT INTO new SELECT * FROM old`, or accept that old partitions stay where
  they are (they age out in 60 days anyway).

### Change retention (raw or rollup)

Raw is 60 days, rollups 2 years, via `TTL`. To change, edit the `TTL` clause and
re-apply the (idempotent) DDL — but `CREATE ... IF NOT EXISTS` won't alter an
existing table, so run an explicit `ALTER`:

```sql
ALTER TABLE xrootd.raw_records_local ON CLUSTER '{cluster}'
  MODIFY TTL toDateTime(event_time) + INTERVAL 90 DAY DELETE;
```

Then update `01_raw.sql` so fresh installs match. Partitioning is daily, so TTL
drops whole parts cheaply.

### Add a rollup

1. Copy `03_rollup_daily.sql` to a new file; rename the table/MV and pick the
   grouping (e.g. weekly, or add a `site` dimension).
2. Create the `ReplicatedAggregatingMergeTree` local + `Distributed` tables and
   the materialized view `TO` the local table, all `ON CLUSTER '{cluster}'`.
3. Re-run the schema Job (delete the old Job first). The MV only populates
   **future** inserts; to backfill, `INSERT INTO rollup_x_local SELECT ... FROM
   raw_records_local GROUP BY ...` once.

### Drain & redeploy the ingester without data loss

The ingester is safe to restart at any time because of at-least-once + dedup,
but for a clean drain:

1. `kubectl -n $NS rollout restart deploy/osdf-clickhouse-ingester` (rolling;
   `maxUnavailable: 1`).
2. On SIGTERM each pod: **stops consuming → flushes the in-flight batch →
   acks → exits** (within `terminationGracePeriodSeconds: 60`). Messages already
   pulled but not yet inserted are flushed; messages still in the broker are
   untouched and picked up by the remaining/next pods.
3. Any batch that couldn't be acked before the connection closed is redelivered
   and **deduplicated by `event_id`** — no double counting, no loss.

To pause ingestion entirely: `kubectl -n $NS scale deploy/osdf-clickhouse-ingester
--replicas=0`. The broker queues are durable, so messages accumulate and are
consumed when you scale back up (watch `INGESTER_PREFETCH` × replicas and broker
disk).

### Common checks

```bash
# ingester logs / readiness
kubectl -n $NS logs -l app.kubernetes.io/name=osdf-clickhouse-ingester -f
# row counts by stream over the last hour
kubectl -n $NS exec -it chi-xrootd-xrootd-0-0-0 -- clickhouse-client -q \
  "SELECT source_exchange, count() FROM xrootd.raw_records_dist
   WHERE event_time > now()-INTERVAL 1 HOUR GROUP BY source_exchange"
```
