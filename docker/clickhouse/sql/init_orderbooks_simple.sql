-- Simplified ClickHouse schema for HyperLiquid orderbooks
-- Focuses on minimal processing and logging

-- Create database
CREATE DATABASE IF NOT EXISTS cryptofeed;

-- Create Kafka table engine for raw orderbook data
CREATE OR REPLACE TABLE cryptofeed.orderbooks_raw
(
    exchange      LowCardinality(String)
    , symbol      LowCardinality(String)
    , ts          DateTime64(9)
    , raw_data    String  -- Store the complete raw JSON message
    , coin        LowCardinality(String)  -- Extract coin from data
    , time_ms     UInt64  -- Extract timestamp from data
    , levels_json String   -- Store the levels array as JSON string
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:29092',
    kafka_topic_list = 'orderbooks',
    kafka_group_name = 'clickhouse_simple',
    kafka_format = 'JSONEachRow',
    kafka_flush_interval_ms = 1000,
    kafka_skip_broken_messages = 1;

-- Create materialized view to extract and store parsed data
CREATE MATERIALIZED VIEW IF NOT EXISTS cryptofeed.orderbooks_parsed TO cryptofeed.orderbooks_raw AS
SELECT
    exchange,
    symbol,
    ts,
    raw_data,
    JSONExtractString(raw_data, 'data', 'coin') AS coin,
    JSONExtractUInt(raw_data, 'data', 'time') AS time_ms,
    JSONExtractRaw(raw_data, 'data', 'levels') AS levels_json
FROM cryptofeed.orderbooks_raw;

-- Create a simple table to store the processed orderbook data
CREATE TABLE IF NOT EXISTS cryptofeed.orderbooks_simple
(
    exchange        LowCardinality(String)
    , symbol        LowCardinality(String)
    , ts            DateTime64(9)
    , coin          LowCardinality(String)
    , time_ms       UInt64
    , levels_json   String
    , raw_data      String
) Engine = MergeTree
PARTITION BY toYYYYMM(ts)
TTL toDateTime(ts + INTERVAL 1 HOUR)
ORDER BY (symbol, exchange, ts);

-- Create materialized view to push data to the simple table
CREATE MATERIALIZED VIEW IF NOT EXISTS cryptofeed.orderbooks_simple_mv TO cryptofeed.orderbooks_simple AS
SELECT * FROM cryptofeed.orderbooks_parsed;

-- Create a view for easy querying of recent orderbooks
CREATE VIEW IF NOT EXISTS cryptofeed.orderbooks_recent AS
SELECT 
    exchange,
    symbol,
    ts,
    coin,
    time_ms,
    levels_json,
    raw_data
FROM cryptofeed.orderbooks_simple
WHERE ts >= now() - INTERVAL 1 HOUR
ORDER BY ts DESC;
