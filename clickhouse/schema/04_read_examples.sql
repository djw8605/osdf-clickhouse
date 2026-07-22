-- Read-side query examples. NOT run by the apply Job (they are SELECTs); keep
-- them here as a reference for Grafana panels and ad-hoc analysis.
--
-- Aggregate rollups store partial STATES, so reads must use the matching
-- *Merge combinators and re-GROUP BY. Always read the *_dist tables so the
-- merge spans all shards.

-- 1) Bytes read per VO over the last 7 days (hourly rollup).
SELECT
    vo,
    sumMerge(bytes_read)  AS bytes_read,
    sumMerge(bytes_written) AS bytes_written,
    countMerge(events)    AS events,
    uniqMerge(uniq_files) AS unique_files
FROM xrootd.rollup_hourly_dist
WHERE hour >= now() - INTERVAL 7 DAY
GROUP BY vo
ORDER BY bytes_read DESC;

-- 2) Hourly read-bytes time series for one server (dashboard panel).
SELECT
    hour,
    sumMerge(bytes_read) AS bytes_read
FROM xrootd.rollup_hourly_dist
WHERE server_hostname = 'xrootd-1.example.org'
  AND hour >= now() - INTERVAL 2 DAY
GROUP BY hour
ORDER BY hour;

-- 3) Top 20 servers by unique files served in the last 30 days (daily rollup).
SELECT
    server_hostname,
    uniqMerge(uniq_files) AS unique_files,
    sumMerge(bytes_read)  AS bytes_read
FROM xrootd.rollup_daily_dist
WHERE day >= today() - 30
GROUP BY server_hostname
ORDER BY unique_files DESC
LIMIT 20;

-- 4) Split by stream type (source_exchange) and operation for one day.
SELECT
    source_exchange,
    operation,
    countMerge(events)   AS events,
    sumMerge(bytes_read) AS bytes_read
FROM xrootd.rollup_daily_dist
WHERE day = today() - 1
GROUP BY source_exchange, operation
ORDER BY events DESC;

-- 4b) Directory-level accounting: top directories by bytes read in the last
--     7 days, using the dirname1/dirname2 dimensions (no filename exposed).
SELECT
    dirname1,
    dirname2,
    sumMerge(bytes_read)  AS bytes_read,
    countMerge(events)    AS events,
    uniqMerge(uniq_files) AS unique_files
FROM xrootd.rollup_daily_dist
WHERE day >= today() - 7
GROUP BY dirname1, dirname2
ORDER BY bytes_read DESC
LIMIT 50;

-- 5) Raw-table spot check (use FINAL to force dedup at read time on small
--    ranges only -- FINAL is expensive; prefer the rollups for aggregates).
SELECT
    event_time, server_hostname, vo, filename, read, write
FROM xrootd.raw_records_dist FINAL
WHERE event_time >= now() - INTERVAL 1 HOUR
  AND vo = 'osg'
ORDER BY event_time DESC
LIMIT 100;

-- 6) Recover a field that isn't modeled as a column (evolved struct / gstream
--    event) straight from the raw_json fallback.
SELECT
    event_time,
    source_exchange,
    JSONExtractString(raw_json, 'file_path') AS cache_file_path
FROM xrootd.raw_records_dist
WHERE source_exchange = 'xrd-cache-events'
  AND event_time >= now() - INTERVAL 1 HOUR
LIMIT 50;
