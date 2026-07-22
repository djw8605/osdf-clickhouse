-- Daily rollup: same aggregate columns as the hourly rollup, bucketed by day.
-- Cheaper to scan for long time ranges (dashboards over months/years).
CREATE TABLE IF NOT EXISTS xrootd.rollup_daily_local ON CLUSTER '{cluster}'
(
    day             Date,
    server_hostname LowCardinality(String),
    vo              LowCardinality(String),
    source_exchange LowCardinality(String),
    operation       LowCardinality(String),
    events          AggregateFunction(count),
    bytes_read      AggregateFunction(sum, UInt64),
    bytes_written   AggregateFunction(sum, UInt64),
    uniq_files      AggregateFunction(uniq, String)
)
ENGINE = ReplicatedAggregatingMergeTree(
    '/clickhouse/tables/{shard}/xrootd/rollup_daily_local',
    '{replica}'
)
PARTITION BY toYYYYMM(day)
ORDER BY (day, server_hostname, vo, source_exchange, operation)
TTL day + INTERVAL 2 YEAR DELETE;

CREATE TABLE IF NOT EXISTS xrootd.rollup_daily_dist ON CLUSTER '{cluster}'
AS xrootd.rollup_daily_local
ENGINE = Distributed('{cluster}', xrootd, rollup_daily_local, rand());

CREATE MATERIALIZED VIEW IF NOT EXISTS xrootd.mv_rollup_daily ON CLUSTER '{cluster}'
TO xrootd.rollup_daily_local
AS
SELECT
    toDate(event_time)             AS day,
    server_hostname,
    vo,
    source_exchange,
    if(write > 0, 'write', 'read') AS operation,
    countState()                   AS events,
    sumState(read)                 AS bytes_read,
    sumState(write)                AS bytes_written,
    uniqState(filename)            AS uniq_files
FROM xrootd.raw_records_local
GROUP BY day, server_hostname, vo, source_exchange, operation;
