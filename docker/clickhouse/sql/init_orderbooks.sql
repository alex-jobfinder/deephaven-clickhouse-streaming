--
-- Orderbook pipeline documentation
--
-- End-to-end flow
--   1) Exchange websocket (e.g., HyperLiquid 'l2Book') is normalized by cryptofeed
--      into a cryptofeed OrderBook with canonical book[BID]/book[ASK] levels.
--   2) cryptofeed -> ClickHouseBookKafka transforms the OrderBook.to_dict() payload
--      into JSON for Kafka topic 'orderbooks' with fields:
--         {
--           exchange: String,
--           symbol: String,
--           ts:      DateTime64(9) (derived from timestamp in ns),
--           bid:     Map(String, Float64),  -- price->size (best bid first when read)
--           ask:     Map(String, Float64)   -- price->size (best ask first when read)
--         }
--      Notes:
--        - Price keys are emitted as JSON object keys; ClickHouse Map(String, Float64)
--          reads them as strings (e.g., "121366.0": 11.41902).
--        - The writer sorts bids descending and asks ascending prior to publish.
--
-- ClickHouse structures in this file
--   - cryptofeed.orderbooks_queue (Kafka engine)
--       Consumes JSONEachRow from topic 'orderbooks' and exposes it as a table for MVs.
--
--   - cryptofeed.orderbooks_1sec_mv (AggregatingMergeTree MV)
--       Maintains the latest snapshot per (exchange, symbol, second) using argMax.
--       Query this for low-granularity views or sparkline-style charts.
--
--   - cryptofeed.orderbooks (MergeTree)
--       Stores ALL snapshots with a one-hour TTL. Adjust TTL to retain more history
--       if needed (be mindful of volume).
--
-- Optional outbound section (commented out)
--   - Demonstrates how to publish aggregated 1s snapshots back to a Kafka topic.
--
-- Troubleshooting
--   - If orderbooks are not appearing, verify producer payload contains 'bid' and 'ask'
--     at the top level (after ClickHouseBookKafka flattens 'book').
--   - Ensure timestamps are numeric seconds in cryptofeed and converted to ns here.
--   - Check ClickHouse logs for JSON parse warnings if Map keys are not strings.
--
-- uncomment these to start from scratch
DROP TABLE IF EXISTS cryptofeed.orderbooks;
DROP TABLE IF EXISTS cryptofeed.orderbooks_queue;
DROP VIEW IF EXISTS cryptofeed.orderbooks_1sec_mv;
DROP VIEW IF EXISTS cryptofeed.orderbooks_all_mv;

DROP TABLE IF EXISTS cryptofeed.orderbooks_out_queue;
DROP VIEW IF EXISTS cryptofeed.orderbooks_out_queue_mv;

-- create database schema
CREATE DATABASE IF NOT EXISTS cryptofeed;

-----------------------------------------------------------------------------------------------------------
-- create Kafka table engine, flush every 1000ms
CREATE OR REPLACE TABLE cryptofeed.orderbooks_queue
(
  exchange      LowCardinality(String)
  , symbol      LowCardinality(String)
  , ts          DateTime64(9)
  , bid         Map(String, Float64)
  , ask         Map(String, Float64)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:29092',
    kafka_topic_list = 'orderbooks',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONEachRow',
    kafka_flush_interval_ms = 1000,
    kafka_skip_broken_messages = 1;

-----------------------------------------------------------------------------------------------------------
-- create AggregatingMergeTree, keeps the LAST orderbook snapshot per second (once merging finalizes)
CREATE MATERIALIZED VIEW IF NOT EXISTS cryptofeed.orderbooks_1sec_mv
(
    exchange        LowCardinality(String)
    , symbol        LowCardinality(String)
    , ts_bin        DateTime64(9)
    , bid           SimpleAggregateFunction(anyLast, Map(String, Float64))
    , ask           SimpleAggregateFunction(anyLast, Map(String, Float64))
)
ENGINE = AggregatingMergeTree
ORDER BY (exchange, symbol, ts_bin)
AS SELECT
  exchange
  , symbol
  , toStartOfSecond(ts) AS ts_bin
  , argMax(bid, ts)     AS bid
  , argMax(ask, ts)     AS ask
FROM cryptofeed.orderbooks_queue
GROUP BY (exchange, symbol, ts_bin);

-----------------------------------------------------------------------------------------------------------
-- create MergeTree, stores ALL orderbook snapshots (only keeps 1 HOUR of history)
CREATE TABLE IF NOT EXISTS cryptofeed.orderbooks
(
    exchange        LowCardinality(String)
    , symbol        LowCardinality(String)
    , ts            DateTime64(9)
    , bid           Map(String, Float64)
    , ask           Map(String, Float64)
) Engine = MergeTree
PARTITION BY toYYYYMM(ts)
TTL toDateTime(ts + INTERVAL 1 HOUR)
ORDER BY (symbol, exchange, ts);

-- create materialized view, pushing ALL orderbooks snapshots from Kafka to cryptofeed.orderbooks
CREATE MATERIALIZED VIEW IF NOT EXISTS cryptofeed.orderbooks_all_mv TO cryptofeed.orderbooks AS
SELECT * FROM cryptofeed.orderbooks_queue;


---------------------------------------------------------------------------------------------------------------------------------------
-- everything above this line pushes ALL orderbook snapshots into 'cryptofeed.orderbooks' and 'cryptofeed.orderbooks_1sec_mv'
-- below this line creates an outbound Kafka topic that pushes from Clickhouse back out to Kafka (experimental)
-- to do this, we follow the steps in https://clickhouse.com/docs/en/integrations/kafka/kafka-table-engine#2-using-materialized-views

-- outbound Kafka topic 'orderbooks_1sec'
--CREATE TABLE IF NOT EXISTS cryptofeed.orderbooks_out_queue
--(
--    exchange        LowCardinality(String)
--    , symbol        LowCardinality(String)
--    , ts_latest     UInt64
--    , ts_1sec       UInt64
--    , bid           Map(String, Float64)
--    , ask           Map(String, Float64)
--)
--ENGINE = Kafka
--SETTINGS
--    kafka_broker_list = 'redpanda:29092',
--    kafka_topic_list = 'orderbooks_1sec',
--    kafka_group_name = 'clickhouse',
--    kafka_format = 'JSONEachRow',
--    kafka_flush_interval_ms = 1000,
--    kafka_thread_per_consumer = 0,
--    kafka_num_consumers = 1;
--
-- aggregates orderbook snapshots to 1sec resolution and pushes them onto the outgoing Kafka queue
--CREATE MATERIALIZED VIEW IF NOT EXISTS cryptofeed.orderbooks_out_queue_mv TO cryptofeed.orderbooks_out_queue AS
--SELECT
--  exchange
--  , symbol
--  , toUnixTimestamp64Nano(max(ts))                  AS ts_latest
--  , toUnixTimestamp64Nano(max(toStartOfSecond(ts))) AS ts_1sec
--  , argMax(bid, ts)                                 AS bid
--  , argMax(ask, ts)                                 AS ask
--FROM cryptofeed.orderbooks
--WHERE
--  ts >= ts - INTERVAL 15 SECOND
--GROUP BY exchange, symbol;