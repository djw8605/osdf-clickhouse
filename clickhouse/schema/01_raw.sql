-- Raw records table (local + distributed).
--
-- Columns are derived field-for-field from the upstream CollectorRecord struct
-- (github.com/opensciencegrid/xrootd-monitoring-shoveler collector/correlator.go)
-- and MUST stay in lockstep with ingester/internal/chwriter/writer.go `columns`.
--
-- Engine: ReplicatedReplacingMergeTree
--   * Replicated  -> replicas of each shard stay in sync via Keeper.
--   * Replacing   -> rows sharing the ORDER BY key collapse on merge; the row
--                    with the largest `ingest_time` (the version column) wins.
--                    This is how at-least-once redeliveries are deduplicated.
--
-- Dedup key = ORDER BY (server_hostname, event_time, event_id). event_id is a
-- deterministic hash computed by the ingester (see writer/model), so identical
-- redeliveries share the whole tuple and collapse; distinct events differ in
-- event_id and are preserved.
--
-- PARTITION BY toYYYYMMDD(event_time) keeps one part-set per day so the 60-day
-- TTL drops whole parts cheaply.
CREATE TABLE IF NOT EXISTS xrootd.raw_records_local ON CLUSTER '{cluster}'
(
    -- identity / routing
    event_time                DateTime64(3),                 -- primary event timestamp (@timestamp, with fallbacks)
    event_id                  String,                        -- deterministic dedup id (see ingester)
    source_exchange           LowCardinality(String),        -- which non-WLCG exchange produced this row

    -- correlation timing
    start_time                DateTime64(3),                 -- file open  (unix seconds -> DateTime)
    end_time                  DateTime64(3),                 -- file close (unix seconds -> DateTime)
    operation_time            UInt32,                        -- seconds

    -- server / site
    server_id                 LowCardinality(String),
    server_hostname           LowCardinality(String),
    server                    String,
    server_ip                 IPv6,                          -- IPv4 is mapped into ::ffff:0:0/96
    site                      LowCardinality(String),

    -- user / auth
    user                      String,
    user_dn                   String,
    user_domain               LowCardinality(String),
    vo                        LowCardinality(String),
    host                      String,                        -- client host (may be hostname or IP)
    token_subject             String,
    token_username            String,
    token_org                 LowCardinality(String),
    token_role                LowCardinality(String),
    token_groups              String,
    experiment                LowCardinality(String),
    activity                  LowCardinality(String),

    -- file
    filename                  String,
    dirname1                  String,
    dirname2                  String,
    logical_dirname           String,
    protocol                  LowCardinality(String),
    appinfo                   String,
    ipv6                      UInt8,
    filesize                  UInt64,

    -- operation counts
    read_operations           UInt32,
    read_single_operations    UInt32,
    read_vector_operations    UInt32,
    write_operations          UInt32,

    -- byte counters
    read                      UInt64,
    read_single_bytes         UInt64,
    readv                     UInt64,
    write                     UInt64,

    -- per-operation size stats
    read_min                  UInt32,
    read_max                  UInt32,
    read_average              UInt64,
    read_single_min           UInt32,
    read_single_max           UInt32,
    read_single_average       UInt64,
    read_vector_min           UInt32,
    read_vector_max           UInt32,
    read_vector_average       UInt64,
    write_min                 UInt32,
    write_max                 UInt32,
    write_average             UInt64,
    read_vector_count_min     UInt16,
    read_vector_count_max     UInt16,
    read_vector_count_average Float64,
    read_bytes_at_close       UInt64,
    write_bytes_at_close      UInt64,
    has_file_close_msg        UInt8,

    -- lossless fallback: the exact original message body. If the upstream
    -- struct evolves or a gstream event carries fields not modeled above,
    -- nothing is dropped -- it stays queryable here via JSONExtract*.
    raw_json                  String,

    -- server-assigned insert time; also the ReplacingMergeTree version column.
    -- NOT written by the ingester (filled by DEFAULT), so a redelivery inserted
    -- later has a higher version and wins the dedup.
    ingest_time               DateTime64(3) DEFAULT now64(3),

    -- Helpful skip indexes for common filters.
    INDEX idx_vo vo TYPE set(0) GRANULARITY 4,
    INDEX idx_exchange source_exchange TYPE set(0) GRANULARITY 4
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/xrootd/raw_records_local',
    '{replica}',
    ingest_time
)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (server_hostname, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 60 DAY DELETE
SETTINGS index_granularity = 8192;

-- Distributed table: the ingester INSERTs here and reads fan out here.
--
-- Sharding key cityHash64(event_id) is DETERMINISTIC on the dedup id, so every
-- redelivery of the same logical event lands on the SAME shard. This is
-- essential: ReplacingMergeTree dedup is per-shard, so redeliveries must not be
-- scattered across shards (or duplicates would survive).
CREATE TABLE IF NOT EXISTS xrootd.raw_records_dist ON CLUSTER '{cluster}'
AS xrootd.raw_records_local
ENGINE = Distributed('{cluster}', xrootd, raw_records_local, cityHash64(event_id));
