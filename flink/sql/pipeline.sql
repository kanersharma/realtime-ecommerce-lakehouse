-- =====================================================================
-- Real-time e-commerce lakehouse pipeline (Flink SQL)
--   Kafka (clicks, orders)  ->  Iceberg tables in lakehouse.shop
-- Submitted once by the `flink-job` container: sql-client.sh -f pipeline.sql
-- Every statement is idempotent (IF NOT EXISTS), so re-submitting is safe.
-- =====================================================================

SET 'pipeline.name' = 'ecommerce-lakehouse';
-- Iceberg commits data on each successful checkpoint => ~10s end-to-end freshness.
SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
-- A Kafka partition with no traffic must not stall the watermark (and the windows).
SET 'table.exec.source.idle-timeout' = '10 s';

-- ---------------------------------------------------------------------
-- 1. Sources: Kafka topics (temporary tables in Flink's in-memory catalog)
-- ---------------------------------------------------------------------
CREATE TEMPORARY TABLE clicks_src (
    event_id    STRING,
    session_id  STRING,
    user_id     STRING,
    event_type  STRING,           -- page_view | add_to_cart
    product_id  STRING,
    category    STRING,
    event_time  TIMESTAMP(3),     -- UTC, event time (not arrival time)
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-lakehouse',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE orders_src (
    order_id        STRING,
    session_id      STRING,
    user_id         STRING,
    product_id      STRING,
    product_name    STRING,
    category        STRING,
    quantity        INT,
    unit_price      DECIMAL(10, 2),
    total_amount    DECIMAL(12, 2),
    country         STRING,
    payment_method  STRING,
    event_time      TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-lakehouse',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- Stock movements from the shop (the system of record for stock). No windows are computed on it,
-- so it needs no watermark. `seq` is the shop ledger's global order of movements.
CREATE TEMPORARY TABLE inventory_src (
    seq                BIGINT,
    product_id         STRING,
    product_name       STRING,
    category           STRING,
    reason             STRING,       -- initial | snapshot | order | restock | settings | removed
    delta              INT,
    on_hand_after      INT,
    unit_price         DECIMAL(10, 2),
    lead_time_days     INT,          -- demo days a refill takes to arrive
    target_cover_days  INT,          -- demo days of demand a refill should cover
    event_time         TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'inventory',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-lakehouse',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- ---------------------------------------------------------------------
-- 2. Sink catalog: Iceberg REST catalog, data files on RustFS (S3 API)
-- ---------------------------------------------------------------------
CREATE CATALOG lakehouse WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3://warehouse/',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://rustfs:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id' = 'admin',
    's3.secret-access-key' = 'password',
    'client.region' = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS lakehouse.shop;

-- Bronze: raw events, partitioned by day
CREATE TABLE IF NOT EXISTS lakehouse.shop.clicks (
    event_id    STRING,
    session_id  STRING,
    user_id     STRING,
    event_type  STRING,
    product_id  STRING,
    category    STRING,
    event_time  TIMESTAMP(3),
    event_date  DATE
) PARTITIONED BY (event_date) WITH (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS lakehouse.shop.orders (
    order_id        STRING,
    session_id      STRING,
    user_id         STRING,
    product_id      STRING,
    product_name    STRING,
    category        STRING,
    quantity        INT,
    unit_price      DECIMAL(10, 2),
    total_amount    DECIMAL(12, 2),
    country         STRING,
    payment_method  STRING,
    event_time      TIMESTAMP(3),
    event_date      DATE
) PARTITIONED BY (event_date) WITH (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- Bronze: every stock movement. Current stock = the latest movement per product.
CREATE TABLE IF NOT EXISTS lakehouse.shop.inventory_movements (
    seq                BIGINT,
    product_id         STRING,
    product_name       STRING,
    category           STRING,
    reason             STRING,
    delta              INT,
    on_hand_after      INT,
    unit_price         DECIMAL(10, 2),
    lead_time_days     INT,
    target_cover_days  INT,
    event_time         TIMESTAMP(3),
    event_date         DATE
) PARTITIONED BY (event_date) WITH (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- Gold: 1-minute tumbling-window aggregates (append-only, emitted when the
-- watermark passes window_end, i.e. ~1 min + 5 s after the window opens)
CREATE TABLE IF NOT EXISTS lakehouse.shop.revenue_per_minute (
    window_start  TIMESTAMP(3),
    window_end    TIMESTAMP(3),
    category      STRING,
    orders        BIGINT,
    units         BIGINT,
    revenue       DECIMAL(18, 2)
) WITH ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lakehouse.shop.funnel_per_minute (
    window_start  TIMESTAMP(3),
    window_end    TIMESTAMP(3),
    sessions      BIGINT,
    page_views    BIGINT,
    add_to_carts  BIGINT
) WITH ('format-version' = '2');

-- ---------------------------------------------------------------------
-- 3. One Flink job for all five inserts (sources are shared/reused)
-- ---------------------------------------------------------------------
EXECUTE STATEMENT SET
BEGIN

INSERT INTO lakehouse.shop.clicks
SELECT event_id, session_id, user_id, event_type, product_id, category,
       event_time, CAST(event_time AS DATE)
FROM clicks_src;

INSERT INTO lakehouse.shop.orders
SELECT order_id, session_id, user_id, product_id, product_name, category,
       quantity, unit_price, total_amount, country, payment_method,
       event_time, CAST(event_time AS DATE)
FROM orders_src;

INSERT INTO lakehouse.shop.inventory_movements
SELECT seq, product_id, product_name, category, reason, delta, on_hand_after, unit_price,
       lead_time_days, target_cover_days, event_time, CAST(event_time AS DATE)
FROM inventory_src;

INSERT INTO lakehouse.shop.revenue_per_minute
SELECT window_start, window_end, category,
       COUNT(*)                          AS orders,
       CAST(SUM(quantity) AS BIGINT)     AS units,
       CAST(SUM(total_amount) AS DECIMAL(18, 2)) AS revenue
FROM TABLE(TUMBLE(TABLE orders_src, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, category;

INSERT INTO lakehouse.shop.funnel_per_minute
SELECT window_start, window_end,
       COUNT(DISTINCT session_id)                                     AS sessions,
       SUM(CASE WHEN event_type = 'page_view'   THEN 1 ELSE 0 END)    AS page_views,
       SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END)    AS add_to_carts
FROM TABLE(TUMBLE(TABLE clicks_src, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;

END;
