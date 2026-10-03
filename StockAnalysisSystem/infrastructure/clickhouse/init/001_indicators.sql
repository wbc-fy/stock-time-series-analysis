CREATE DATABASE IF NOT EXISTS stock_analytics;

CREATE TABLE IF NOT EXISTS stock_analytics.daily_indicators
(
    event_id String,
    source_event_id String,
    trace_id String,
    ts_code String,
    trade_date Date,
    source String,
    source_event_time DateTime64(9, 'UTC'),
    close Decimal(38, 6),
    volume Decimal(38, 6),
    pct_chg Nullable(Decimal(38, 6)),
    ma5 Nullable(Decimal(38, 6)),
    ma10 Nullable(Decimal(38, 6)),
    ma20 Nullable(Decimal(38, 6)),
    vol_ma5 Nullable(Decimal(38, 6)),
    vol_ma10 Nullable(Decimal(38, 6)),
    volume_ratio Nullable(Decimal(38, 6)),
    window_size UInt8,
    is_warmup Bool,
    calculation_time DateTime64(9, 'UTC'),
    schema_version UInt32,
    kafka_topic String,
    kafka_partition Int32,
    kafka_offset UInt64,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (ts_code, trade_date);
