-- Hourly rollup: long-term aggregates (2-year TTL) populated incrementally by a
-- materialized view as rows land in raw_records_local.
--
-- Engine: ReplicatedAggregatingMergeTree stores partial aggregate STATES
-- (countState/sumState/uniqState). States are mergeable across parts, replicas
-- and shards, so the same rollup answers "events per hour", "bytes per host",
-- "unique files per VO" cheaply and exactly.
--
-- Dimensions: hour, server host, VO, stream type (source_exchange), operation,
-- and the top directory levels (dirname1, dirname2) for directory-level
-- accounting. The full filename is deliberately NOT a dimension -- it appears
-- only inside uniqState(filename), a distinct-count sketch from which no path
-- can be recovered.
-- `operation` is derived: any record that wrote bytes is a 'write', else 'read'.
CREATE TABLE IF NOT EXISTS xrootd.rollup_hourly_local ON CLUSTER '{cluster}'
(
    hour            DateTime,
    server_hostname LowCardinality(String),
    vo              LowCardinality(String),
    source_exchange LowCardinality(String),
    operation       LowCardinality(String),
    dirname1        String,
    dirname2        String,
    events          AggregateFunction(count),
    bytes_read      AggregateFunction(sum, UInt64),
    bytes_written   AggregateFunction(sum, UInt64),
    uniq_files      AggregateFunction(uniq, String)
)
ENGINE = ReplicatedAggregatingMergeTree(
    '/clickhouse/tables/{shard}/xrootd/rollup_hourly_local',
    '{replica}'
)
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, server_hostname, vo, source_exchange, operation, dirname1, dirname2)
TTL hour + INTERVAL 2 YEAR DELETE;

-- Distributed front for cluster-wide reads (re-merges states across shards).
-- Sharding key is rand(): this table is read-only (the MV inserts into the
-- LOCAL rollup, never through here), so the key is irrelevant for correctness.
CREATE TABLE IF NOT EXISTS xrootd.rollup_hourly_dist ON CLUSTER '{cluster}'
AS xrootd.rollup_hourly_local
ENGINE = Distributed('{cluster}', xrootd, rollup_hourly_local, rand());

-- Materialized view: fires on every insert into raw_records_local (including
-- inserts that arrive via the Distributed table) and writes partial states.
CREATE MATERIALIZED VIEW IF NOT EXISTS xrootd.mv_rollup_hourly ON CLUSTER '{cluster}'
TO xrootd.rollup_hourly_local
AS
SELECT
    toStartOfHour(event_time)      AS hour,
    server_hostname,
    vo,
    source_exchange,
    if(write > 0, 'write', 'read') AS operation,
    dirname1,
    dirname2,
    countState()                   AS events,
    sumState(read)                 AS bytes_read,
    sumState(write)                AS bytes_written,
    uniqState(filename)            AS uniq_files
FROM xrootd.raw_records_local
GROUP BY hour, server_hostname, vo, source_exchange, operation, dirname1, dirname2;
