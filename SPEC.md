# Build spec: ClickHouse cluster on Kubernetes + Go ingester for the xrootd-monitoring-shoveler non-WLCG stream

You are building a production data pipeline. Deliver two things in one repo:

1. Kubernetes manifests for a multi-host (sharded and replicated) ClickHouse service.
2. A Go service that consumes the **non-WLCG** collector stream from an existing RabbitMQ broker and batch-inserts it into ClickHouse.

Work carefully, verify assumptions against the upstream source before writing code, and document every default you pick.

## Step 0: verify the data source first (do not skip)

Before writing any code, read the upstream project so the schema and the consumer match reality:

- Repo: `https://github.com/opensciencegrid/xrootd-monitoring-shoveler`
- Read `collector/correlator.go` and find the record struct the collector emits (the `CollectorRecord`, or whatever the current name is). The record is serialized as one JSON object per message. Derive the ClickHouse schema from the actual struct fields and their JSON tags, not from assumptions.
- Read `amqp.go` and the config (`config.go`, `config/config.yaml`, `config/config-collector.yaml`) to confirm exchange names and publishing behavior.

Facts confirmed from upstream as of this writing (re-verify, they may have changed):

- The **collector** binary (not the plain `shoveler`) emits structured JSON records. The plain shoveler emits base64-wrapped raw UDP packets; you do **not** want that stream.
- WLCG format conversion happens in the collector when the VO is `cms` OR the file path starts with `/store` or `/user/dteam`. Those converted records are published to separate exchanges: `xrd-wlcg-events`, `xrd-wlcg-cache-events`, `xrd-wlcg-tpc-events`.
- The **non-WLCG stream** is therefore everything on the main and type-specific non-WLCG exchanges. Defaults:
  - `shoveled-xrd` (main)
  - `xrd-cache-events`
  - `xrd-tcp-events`
  - `xrd-tpc-events`
- Publisher behavior: messages are published to a named exchange with an **empty routing key** and `ContentType: text/plain`, body is the JSON record. The upstream publisher uses `github.com/streadway/amqp` (deprecated).

Make the set of consumed exchanges a config value (comma-separated list) defaulting to the four non-WLCG exchanges above. Do not bind the `xrd-wlcg-*` exchanges.

If reading the struct reveals fields beyond the abbreviated README example (`@timestamp`, `start_time`, `end_time`, `operation_time`, `read_operations`, `read`, `write`, `filename`, `HasFileCloseMsg`), include them. If server host, client host, user, VO, or site fields exist, they matter for accounting and must be columns. If a field's presence is uncertain, keep the raw JSON in a spare `String` column (see schema) so nothing is lost.

## Part A: ClickHouse on Kubernetes (multi-host)

Use the Altinity ClickHouse Operator (`github.com/Altinity/clickhouse-operator`). Deliver:

- Operator install notes (reference the upstream install manifest; pin a version).
- A `ClickHouseInstallation` CR describing a cluster with configurable shards and replicas. Default to **2 shards x 2 replicas** (4 hosts) and make shard/replica counts easy to change.
- Coordination for replication: deploy **ClickHouse Keeper** (3-node quorum), either via the operator's Keeper support or a dedicated `ClickHouseKeeperInstallation` / StatefulSet. ReplicatedMergeTree requires it.
- Persistent storage via `volumeClaimTemplates`. Leave `storageClassName` as a clearly-marked variable. Note in the README that on NRP/Nautilus this must be set to an available RWO block storage class (Ceph/rook), and size the raw-data PVC for roughly 60 days of retention plus headroom.
- A `Secret` for the ClickHouse application user password. Never hardcode credentials in the CR. Reference the secret.
- Services for client access (native TCP 9000, HTTP 8123) scoped internal to the namespace.
- A Prometheus `ServiceMonitor` scraping the operator-exported ClickHouse metrics (NRP runs Prometheus).
- Resource requests/limits and a non-root `securityContext` (multi-tenant clusters: no privileged pods, drop capabilities, `runAsNonRoot: true`, seccomp `RuntimeDefault`).
- A `NetworkPolicy` restricting ingress to the ClickHouse pods to the ingester and to explicitly allowed query clients (Grafana).

### Schema (SQL, applied via operator `files`/init or a Job)

Design for: high-volume append, keep raw records only 60 days, keep aggregates long-term.

- Local raw table on `ReplicatedReplacingMergeTree`:
  - Columns derived from the verified CollectorRecord struct, with sensible ClickHouse types: `DateTime64(3)` for the event timestamp, `LowCardinality(String)` for repeated categoricals (server host, operation type, VO, site), `IPv6` for client/server IP if present, `UInt64` for byte counters, `UInt32` for durations/op counts, `String` for file path and user DN.
  - A `raw_json String` column holding the original message body, so no field is silently dropped if the struct evolves.
  - `PARTITION BY toYYYYMMDD(event_time)` so TTL expiry drops whole parts cheaply.
  - `ORDER BY` chosen for the common query pattern (host + time, and a dedup key).
  - `TTL event_time + INTERVAL 60 DAY DELETE`.
  - Dedup: RabbitMQ delivery is at-least-once, so pick a dedup strategy. The CollectorRecord may not carry a stable unique id. Prefer a deterministic id computed in the ingester (for example a hash of server id + file id + start_time + end_time + byte counts) written to an `event_id` column and used in the `ReplacingMergeTree` key. Document this choice and its failure modes.
- A `Distributed` table over the cluster in front of the local table for cluster-wide reads and for the ingester to insert through (or have the ingester insert to local tables directly, round-robining across hosts; pick one, document the tradeoff).
- Rollup tables on `ReplicatedAggregatingMergeTree` with long TTL (for example 2 years), plus materialized views that populate them from the raw table on insert:
  - Hourly rollup: `countState`, `sumState(bytes)`, `uniqState(file_path)` grouped by hour, server host, VO/type, operation.
  - Daily rollup with the same aggregate columns.
- Provide the read-side query examples using `countMerge` / `sumMerge` / `uniqMerge`.

Deliver schema as idempotent SQL (`CREATE ... IF NOT EXISTS`, use `ON CLUSTER '{cluster}'`).

## Part B: Go ingester

A single Go module. Idiomatic, tested, container-ready.

- Consume from the existing RabbitMQ broker using `github.com/rabbitmq/amqp091-go` (the maintained fork, not `streadway/amqp`).
- For each configured non-WLCG exchange: declare a **durable** queue owned by this service, and bind it to the exchange. Use passive exchange declaration or declare with a type matching the existing exchange so you never redeclare with a conflicting type (a type mismatch will fail the channel). Determine the existing exchange type from the broker/config during Step 0; empty routing key implies fanout-style binding, so bind with `""` (or `#` if it is a topic exchange).
- Run as **competing consumers**: multiple pods share one durable queue per exchange so throughput scales horizontally. Set a tuned `Qos` prefetch.
- Parse the JSON body into the record struct. Keep the original bytes for the `raw_json` column.
- Batch inserts into ClickHouse with `github.com/ClickHouse/clickhouse-go/v2` using the native protocol and the batch API. Flush on **whichever comes first**: batch size (default ~10k rows) or a time window (default ~1s). ClickHouse strongly prefers large inserts over trickle writes.
- Delivery semantics: manual ack. Ack messages only **after** the batch they belong to is successfully committed to ClickHouse. On insert failure, nack-with-requeue (or route to a dead-letter exchange) and retry with backoff. This gives at-least-once, which the ReplacingMergeTree dedup key absorbs.
- Reconnect logic for both RabbitMQ and ClickHouse with exponential backoff, mirroring the resilience style of the upstream shoveler session handling.
- Graceful shutdown: on SIGTERM, stop consuming, flush the in-flight batch, ack, then exit. Do not lose the partial batch.
- Config via environment variables (12-factor). At minimum: AMQP URL, list of exchanges, queue name prefix, prefetch, ClickHouse DSN/hosts/database/table, batch size, flush interval, log level. Support JWT-in-file auth to match how the shoveler authenticates to the OSG broker (username `shoveler`, password read from a token file, re-read on file mtime change), and also plain user/password in the URL. Document both.
- Observability: expose Prometheus metrics (messages consumed, parse errors, rows inserted, batches committed, insert latency histogram, current batch size, reconnect counts, dead-letters) and a `/healthz` + `/readyz` endpoint.
- Structured logging.
- Tests: unit tests for the record parsing and the dedup-id computation; an integration test path using testcontainers (RabbitMQ + ClickHouse) guarded behind a build tag.

### Ingester Kubernetes manifests

- `Deployment` with a configurable replica count (default 3, competing consumers).
- `ConfigMap` for non-secret config, `Secret` for AMQP credentials / JWT token and ClickHouse password. Mount the token file if using JWT auth.
- `Service` exposing the metrics port; a Prometheus `ServiceMonitor`.
- Liveness/readiness probes hitting `/healthz` and `/readyz`.
- Resource requests/limits; non-root `securityContext` (same hardening as Part A).
- Optional `HorizontalPodAutoscaler` keyed on a custom metric (queue depth or CPU); document how queue-depth scaling would be wired via the RabbitMQ Prometheus exporter and KEDA as an alternative.
- `NetworkPolicy` allowing egress only to RabbitMQ and ClickHouse.
- Multi-stage `Dockerfile` producing a small static binary (distroless or scratch base), non-root user.

## Repo layout and deliverables

See `README.md` for the delivered layout and how it maps to this spec.

## Constraints and working style

- Match the target platform to NRP/Nautilus conventions where relevant, but keep manifests portable. Flag every value that must be set per-cluster (storage class, namespace, broker URL, resource sizing).
- Do not invent CollectorRecord fields. Derive them from the source in Step 0. Where the source is ambiguous, keep the `raw_json` fallback column and note the ambiguity in the README.
- Pin versions (operator, ClickHouse image, Go module deps, base images).
- Idempotent everything: re-applying manifests or SQL must be safe.
- If you hit a genuine blocker (for example the exchange type cannot be determined without broker access), state the assumption you are proceeding with rather than stopping.
- At the end, produce a short "operator runbook" section in the README.
