--
CREATE TABLE IF NOT EXISTS emg_sensor_data (
    user_id UInt32,
    prosthesis_type String,
    muscle_group String,
    signal_frequency UInt32,
    signal_duration UInt32,
    signal_amplitude Decimal(5,2),
    signal_time DateTime
) ENGINE = MergeTree()
ORDER BY (user_id, prosthesis_type, signal_time);

--
INSERT INTO emg_sensor_data
SELECT *
FROM file('olap.csv', 'CSV');

-- 
CREATE TABLE IF NOT EXISTS customers_kafka (
    id UInt32,
    name String,
    email String,
    age Decimal(5,2),
    gender String,
    country String,
    address String,
    phone String,
    op String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'dbz.public.customers',
    kafka_group_name = 'clickhouse_customers_group',
    kafka_format = 'JSONEachRow',
    kafka_row_delimiter = '\n';

--
CREATE TABLE IF NOT EXISTS dim_customers (
    id UInt32,
    name String,
    email String,
    age Decimal(5,2),
    gender String,
    country String,
    address String,
    phone String,
    is_deleted UInt8 DEFAULT 0,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

--
CREATE MATERIALIZED VIEW customers_consumer TO dim_customers
AS SELECT 
    id,
    name,
    email,
    age,
    gender,
    country,
    address,
    phone,
    if(op = 'd', 1, 0) as is_deleted,
    now() as updated_at
FROM customers_kafka;

--
CREATE MATERIALIZED VIEW emg_user_summary_mv
ENGINE = AggregatingMergeTree()
ORDER BY (user_id)
POPULATE AS
SELECT
    c.id AS user_id,
    c.name,
    c.age,
    c.gender,
    c.email,
    c.country,
    count() AS total_signals,
    avg(toUInt64(signal_duration)) AS avg_signal_duration_sec,
    avg(signal_amplitude) AS avg_signal_amplitude,
    max(signal_frequency) AS max_frequency,
    max(signal_time) AS last_signal_time,
    length(groupArray(muscle_group)) AS muscle_usage_count
FROM emg_sensor_data e
INNER JOIN dim_customers c ON e.user_id = c.id
GROUP BY c.id, c.name, c.age, c.gender, c.email, c.country;