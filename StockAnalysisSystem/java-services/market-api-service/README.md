# Market API service (V0.6)

Java 21 / Spring Boot 4.1; HTTP port **8083**. No MySQL schema or Flink state changes.

## Configuration and startup

Export environment variables using `StockAnalysisSystem/.env.example` as a template. Spring does not load dotenv automatically. Both `DB_PASSWORD` and `CLICKHOUSE_PASSWORD` must be nonblank; startup fails otherwise. Use a read-only MySQL account when possible (the JDBC pool is read-only). MySQL connection/socket/query/pool timeouts and ClickHouse connection/request timeouts are bounded to five seconds.

From the infrastructure directory, explicitly supply the environment file if needed and start **only ClickHouse**: `docker compose --env-file ../.env up -d --no-deps clickhouse`. Do not recreate Flink or remove existing volumes. ClickHouse 25.8 LTS listens on host loopback 8123; its dedicated volume persists data. Initialization scripts run during container initialization. If adopting a pre-existing ClickHouse volume, apply the SQL deliberately rather than expecting old initialization scripts to rerun. Do not remove a volume to reinitialize it.

Build from java-services: `mvn -pl market-api-service -am package`. Run the resulting executable JAR with the configured environment. No Docker auto-start integration is included.

Variables: `DB_HOST/PORT/NAME/USER/PASSWORD` match the collector; `CLICKHOUSE_HTTP_URL/DB/USER/PASSWORD` default to loopback / stock_analytics / stock_app (password has no default); `KAFKA_BOOTSTRAP_SERVERS`; `MARKET_KAFKA_GROUP` defaults stock-clickhouse-indicators-v1; `INDICATOR_TOPIC` defaults stock.dws.daily-indicator.v1. `MARKET_LISTENER_ENABLED=false` disables ingestion for API-only/test deployments.

## Guarantees and recovery

Consumer reads committed Kafka transactions from earliest on a new group, processes stock-key-ordered records synchronously, and acknowledges only after ClickHouse returns complete HTTP 200 with `wait_end_of_query=1`. Insert errors, error bodies, malformed/schema-invalid events or mismatched topic/key receive three retries with one-second backoff; exhaustion stops the affected consumer without acknowledging the poison offset. No dead-letter skip or recovered commit. Actuator `/actuator/health` becomes DOWN/503 when a concurrent child stops. Correct the dependency/event issue, then restart deliberately; replay remains idempotent on logical reads.

`daily_indicators` stores all 20 common event fields plus Kafka provenance. Decimal(38,6) values are validated without silent scale truncation; warmup nulls remain null. UTC nanosecond DateTime64 timestamps retain source instants. At-least-once insert may create physical duplicates, while ReplacingMergeTree(version=Kafka offset) plus **FINAL** immediately yields a single logical row per (ts_code,trade_date), choosing the greatest source offset. This is **not distributed exactly-once**.

The output topic identity, stock-key partitioning, and **three-partition count are immutable for this table/group**. Kafka offsets cannot order updates across changed partitions or a recreated/reset topic. Such a migration requires a coordinated new table and new consumer group, with replay/cutover and query routing; do not reuse the existing table with reset offsets. A new consumer group against the unchanged topic is replay-safe. Synthetic nonempty codes accepted by the existing Flink protocol can persist; API codes are strictly six digits plus SH/SZ/BJ and only known MySQL stocks are exposed.

## API

- `GET /api/analysis/stocks?limit=6000`: stock_basic rows in ascending code order; limit 1..6000.
- `GET /api/analysis/kline/000001.SZ?limit=260&start=2026-01-01&end=2026-09-01`: `{ts_code,bars}` using existing stock_daily columns; latest bounded rows, returned ascending date.
- `GET /api/analysis/indicators/000001.SZ?limit=260`: `{ts_code,indicators}` with FINAL, latest bounded rows returned ascending date.

Series limits 1..2000; inclusive ISO start/end dates optional and start <= end. JSON emits numbers, ISO dates, and null optional values. Invalid input 400; missing stock 404; known empty series 200 with empty array; dependency failures 503 with a sanitized error. Stock list and kline need MySQL only; indicators also look up stock existence in MySQL before querying ClickHouse.

Official behavior references: [HTTP parameters, auth, wait_end_of_query and error caveats](https://clickhouse.com/docs/interfaces/http), [ReplacingMergeTree and FINAL](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree), [Docker environment and initialization scripts](https://hub.docker.com/r/clickhouse/clickhouse-server).
