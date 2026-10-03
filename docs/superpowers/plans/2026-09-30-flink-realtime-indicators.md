# V0.5 Flink Realtime Indicators Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Java 21 Apache Flink 2.2.0 job that consumes `StockDailyEvent`, maintains a 20-trading-day keyed window, and publishes exactly-once daily indicator, late-data, and dead-letter Kafka events.

**Architecture:** Add a focused `flink-realtime-job` Maven module beside the existing shared model and Spring consumer. The job parses raw Kafka records, validates the V1 contract, keys valid events by `tsCode`, stores at most 20 daily points in managed state, computes six decimal indicators, and routes main, late, and invalid results to separate transactional Kafka sinks. Python Collector supplies an explicit chronological database replay command for state warmup, while Docker Compose supplies a Flink Session Cluster and durable checkpoint volume.

**Tech Stack:** Java 21, Apache Flink 2.2.0 DataStream API, `flink-connector-kafka` 5.0.0-2.2, Jackson, Maven, JUnit 5, Python 3.12, SQLAlchemy, confluent-kafka, pytest, Kafka 4.1.0, Docker Compose.

## Global Constraints

- Use Java DataStream API; do not introduce Flink SQL or PyFlink.
- Use the verified image `flink:2.2.0-scala_2.12-java21`; never use `latest`.
- Use `flink-connector-kafka:5.0.0-2.2`; Flink 2.3 has no published matching Kafka connector as of this plan's implementation, so do not mix 2.3 core with a 2.2 connector.
- Input Topic stays `stock.ods.daily.v1`; do not change `StockDailyEvent` V1.
- Main output is `stock.dws.daily-indicator.v1`; late output is `stock.late.daily.v1`; invalid output is `stock.flink.dead-letter.v1`.
- Kafka Key is `tsCode` for valid and late events; invalid events preserve the original Key.
- Derived values use `BigDecimal`, scale 6, and `RoundingMode.HALF_UP`; do not calculate through `double`.
- Store at most 20 unique trading dates per `tsCode` in Flink managed keyed state.
- Same latest trade date replaces the last point; an older trade date changes no main state and goes only to the late Topic.
- Emit every valid new/latest-day event; unavailable indicators are `null`; `isWarmup` is `windowSize < 20`.
- All three Kafka sinks use `DeliveryGuarantee.EXACTLY_ONCE` and distinct stable transactional ID prefixes.
- Checkpoints run every 10 seconds, timeout after 60 seconds, have a 5-second minimum pause, and persist to a Docker volume.
- Initial state warmup is performed by Python Collector replaying MySQL rows in `trade_date ASC, ts_code ASC` order.
- Flink does not read or write MySQL, ClickHouse, Redis, `stock_features`, or `analysis_result`.
- Never commit `.env`, `.idea/`, `.superpowers/`, secrets, generated targets, checkpoints, or savepoints.
- Work only on `codex/flink-realtime-pipeline`; push only to the user's personal GitHub repository and use a Pull Request before integrating into a team repository.
- Run Maven commands from `StockAnalysisSystem/java-services`, Collector pytest commands from `StockAnalysisSystem/python-services/python-collector`, and Compose/verification commands from `StockAnalysisSystem` unless a step explicitly says otherwise.

## File and Interface Map

```text
StockAnalysisSystem/java-services/common-model/
  StockDailyIndicatorEvent.java       public main-output protocol
  LateStockDailyEvent.java            public late-output protocol
  FlinkDeadLetterEvent.java           public invalid-output protocol

StockAnalysisSystem/java-services/flink-realtime-job/
  config/FlinkJobConfig.java          validated CLI/environment configuration
  model/RawKafkaRecord.java           raw Kafka metadata and payload
  model/ValidatedStockDailyEvent.java validated event plus source metadata
  model/DailyPoint.java               minimal keyed-state value
  validation/StockDailyEventParser.java
  calculation/IndicatorCalculator.java
  operator/ParseAndValidateProcessFunction.java
  operator/DailyIndicatorProcessFunction.java
  kafka/RawKafkaRecordDeserializationSchema.java
  kafka/JsonKafkaRecordSerializationSchema.java
  DailyIndicatorJob.java              topology and checkpoint wiring

StockAnalysisSystem/python-services/python-collector/
  repositories/stock_repository.py    ordered streaming read
  jobs/daily_event_replay_job.py       EventFactory + StockKafkaProducer reuse
  main.py                              replay-daily-events CLI

StockAnalysisSystem/infrastructure/docker-compose.yml
StockAnalysisSystem/scripts/verify_v05_flink.py
```

---

### Task 1: Add V0.5 Public Message Contracts

**Files:**
- Create: `StockAnalysisSystem/java-services/common-model/src/main/java/com/stock/common/model/StockDailyIndicatorEvent.java`
- Create: `StockAnalysisSystem/java-services/common-model/src/main/java/com/stock/common/model/LateStockDailyEvent.java`
- Create: `StockAnalysisSystem/java-services/common-model/src/main/java/com/stock/common/model/FlinkDeadLetterEvent.java`
- Create: `StockAnalysisSystem/java-services/common-model/src/test/java/com/stock/common/model/StockDailyIndicatorEventJsonTest.java`
- Create: `StockAnalysisSystem/java-services/common-model/src/test/java/com/stock/common/model/FlinkSideOutputEventJsonTest.java`
- Create: `StockAnalysisSystem/docs/examples/stock_daily_indicator_event_v1.json`

**Interfaces:**
- Consumes: existing `StockDailyEvent`.
- Produces: immutable Jackson-compatible records `StockDailyIndicatorEvent`, `LateStockDailyEvent`, and `FlinkDeadLetterEvent` used by Tasks 4–8.

- [ ] **Step 1: Add the canonical main-output fixture**

```json
{
  "eventId": "FLINK_INDICATOR:000001.SZ:2026-08-28:v1",
  "sourceEventId": "TUSHARE:000001.SZ:20260828",
  "traceId": "replay-20260801-20260828",
  "tsCode": "000001.SZ",
  "tradeDate": "2026-08-28",
  "source": "TUSHARE",
  "sourceEventTime": "2026-08-28T15:00:00+08:00",
  "close": 10.550000,
  "volume": 1250345.000000,
  "pctChg": 3.431400,
  "ma5": 10.330000,
  "ma10": 10.210000,
  "ma20": 9.980000,
  "volMa5": 1200000.000000,
  "volMa10": 1150000.000000,
  "volumeRatio": 1.041954,
  "windowSize": 20,
  "isWarmup": false,
  "calculationTime": "2026-08-28T16:30:00+08:00",
  "schemaVersion": 1
}
```

- [ ] **Step 2: Write failing JSON contract tests**

```java
@Test
void shouldDeserializeCanonicalIndicatorFixture() throws Exception {
    ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    StockDailyIndicatorEvent event = mapper.readValue(
            Path.of("../../docs/examples/stock_daily_indicator_event_v1.json").toFile(),
            StockDailyIndicatorEvent.class);
    assertEquals("000001.SZ", event.tsCode());
    assertEquals(new BigDecimal("10.330000"), event.ma5());
    assertFalse(event.isWarmup());
    assertEquals(1, event.schemaVersion());
}

@Test
void shouldPreserveOriginalDeadLetterPayload() {
    FlinkDeadLetterEvent event = new FlinkDeadLetterEvent(
            "stock.ods.daily.v1", 1, 42L, "bad-key", "{bad-json}",
            "JSON_PARSE", "invalid JSON", OffsetDateTime.parse("2026-08-28T16:30:00+08:00"), 1);
    assertEquals("{bad-json}", event.originalPayload());
}
```

- [ ] **Step 3: Run the contract tests and verify they fail**

Run: `mvn -q -pl common-model -Dtest=StockDailyIndicatorEventJsonTest,FlinkSideOutputEventJsonTest test`

Expected: FAIL because the three records do not exist.

- [ ] **Step 4: Implement the three records exactly**

```java
public record StockDailyIndicatorEvent(
        String eventId, String sourceEventId, String traceId, String tsCode,
        LocalDate tradeDate, String source, OffsetDateTime sourceEventTime,
        BigDecimal close, BigDecimal volume, BigDecimal pctChg,
        BigDecimal ma5, BigDecimal ma10, BigDecimal ma20,
        BigDecimal volMa5, BigDecimal volMa10, BigDecimal volumeRatio,
        Integer windowSize, Boolean isWarmup,
        OffsetDateTime calculationTime, Integer schemaVersion) {
}

public record LateStockDailyEvent(
        StockDailyEvent originalEvent, LocalDate latestTradeDate,
        String reason, OffsetDateTime processedAt, Integer schemaVersion) {
}

public record FlinkDeadLetterEvent(
        String originalTopic, Integer originalPartition, Long originalOffset,
        String originalKey, String originalPayload, String errorType,
        String errorMessage, OffsetDateTime failedAt, Integer schemaVersion) {
}
```

- [ ] **Step 5: Run all common-model tests**

Run: `mvn -q -pl common-model test`

Expected: PASS, including existing `StockDailyEvent` and `StockBasicEvent` tests.

- [ ] **Step 6: Commit**

```powershell
git add StockAnalysisSystem/java-services/common-model StockAnalysisSystem/docs/examples/stock_daily_indicator_event_v1.json
git commit -m "feat: add flink indicator message contracts"
```

### Task 2: Scaffold the Flink Module and Validate Configuration

**Files:**
- Create: `.gitignore`
- Modify: `StockAnalysisSystem/java-services/pom.xml`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/pom.xml`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/config/FlinkJobConfig.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/model/RawKafkaRecord.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/model/ValidatedStockDailyEvent.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/validation/InvalidStockDailyEventException.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/validation/StockDailyEventValidator.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/validation/StockDailyEventParser.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/test/java/com/stock/flink/config/FlinkJobConfigTest.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/test/java/com/stock/flink/validation/StockDailyEventParserTest.java`

**Interfaces:**
- Consumes: `StockDailyEvent` V1 from `common-model`.
- Produces: `FlinkJobConfig.fromArgs(String[])`, `StockDailyEventParser.parse(RawKafkaRecord)`, and validated metadata used by Tasks 4–5.

- [ ] **Step 1: Add failing configuration and parser tests**

```java
@Test
void shouldUseDockerDefaults() {
    FlinkJobConfig config = FlinkJobConfig.fromArgs(new String[0]);
    assertEquals("kafka:29092", config.bootstrapServers());
    assertEquals("stock.ods.daily.v1", config.inputTopic());
    assertEquals("stock.dws.daily-indicator.v1", config.outputTopic());
    assertEquals(10_000L, config.checkpointIntervalMs());
}

@Test
void shouldRejectKafkaKeyDifferentFromTsCode() {
    RawKafkaRecord raw = fixture("OTHER", validJson());
    InvalidStockDailyEventException error = assertThrows(
            InvalidStockDailyEventException.class, () -> parser.parse(raw));
    assertEquals("KEY_MISMATCH", error.errorType());
}
```

- [ ] **Step 2: Run tests and verify missing-module failure**

Run: `mvn -q -pl flink-realtime-job -am -Dtest=FlinkJobConfigTest,StockDailyEventParserTest test`

Expected: FAIL because the module and classes do not exist.

- [ ] **Step 3: Create module metadata and repository ignores**

Root `.gitignore`:

```gitignore
.idea/
.superpowers/
.tmp/
**/target/
**/.pytest_cache/
**/__pycache__/
StockAnalysisSystem/.env
```

Add `<module>flink-realtime-job</module>` to the parent. In the new module POM set
`flink.version=2.2.0` and `flink.kafka.connector.version=5.0.0-2.2`, add `common-model`,
`flink-streaming-java` and `flink-clients` as `provided`, add the versioned
`flink-connector-kafka`, Jackson, JUnit, and `flink-test-utils:2.2.0` for tests.
Configure `maven-shade-plugin` to produce
`flink-realtime-job-${project.version}-all.jar` with main class
`com.stock.flink.DailyIndicatorJob` and exclude signature files.

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>com.stock</groupId>
    <artifactId>stock-java-services</artifactId>
    <version>0.3.0-SNAPSHOT</version>
  </parent>
  <artifactId>flink-realtime-job</artifactId>
  <properties>
    <flink.version>2.2.0</flink.version>
    <flink.kafka.connector.version>5.0.0-2.2</flink.kafka.connector.version>
  </properties>
  <dependencies>
    <dependency>
      <groupId>com.stock</groupId><artifactId>common-model</artifactId>
      <version>${project.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId><artifactId>flink-streaming-java</artifactId>
      <version>${flink.version}</version><scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId><artifactId>flink-clients</artifactId>
      <version>${flink.version}</version><scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId><artifactId>flink-connector-kafka</artifactId>
      <version>${flink.kafka.connector.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId><artifactId>flink-test-utils</artifactId>
      <version>${flink.version}</version><scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId><artifactId>flink-streaming-java</artifactId>
      <version>${flink.version}</version><type>test-jar</type><scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId><scope>test</scope>
    </dependency>
  </dependencies>
  <build><plugins><plugin>
    <groupId>org.apache.maven.plugins</groupId><artifactId>maven-shade-plugin</artifactId>
    <version>3.6.1</version>
    <executions><execution><phase>package</phase><goals><goal>shade</goal></goals>
      <configuration>
        <shadedArtifactAttached>true</shadedArtifactAttached>
        <shadedClassifierName>all</shadedClassifierName>
        <createDependencyReducedPom>false</createDependencyReducedPom>
        <filters><filter><artifact>*:*</artifact><excludes>
          <exclude>META-INF/*.SF</exclude><exclude>META-INF/*.DSA</exclude><exclude>META-INF/*.RSA</exclude>
        </excludes></filter></filters>
        <transformers><transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
          <mainClass>com.stock.flink.DailyIndicatorJob</mainClass>
        </transformer></transformers>
      </configuration>
    </execution></executions>
  </plugin></plugins></build>
</project>
```

- [ ] **Step 4: Implement immutable config parsing**

```java
public record FlinkJobConfig(
        String bootstrapServers, String inputTopic, String outputTopic,
        String lateTopic, String deadLetterTopic, String consumerGroup,
        String checkpointUri, long checkpointIntervalMs,
        long checkpointTimeoutMs, long checkpointMinPauseMs,
        long kafkaTransactionTimeoutMs,
        int parallelism, int supportedSchemaVersion) {

    public static FlinkJobConfig fromArgs(String[] args) {
        ParameterTool p = ParameterTool.fromArgs(args);
        return new FlinkJobConfig(
                value(p, "bootstrap-servers", "FLINK_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
                p.get("input-topic", "stock.ods.daily.v1"),
                p.get("output-topic", "stock.dws.daily-indicator.v1"),
                p.get("late-topic", "stock.late.daily.v1"),
                p.get("dead-letter-topic", "stock.flink.dead-letter.v1"),
                p.get("group-id", "stock-flink-daily-indicator-v1"),
                p.get("checkpoint-uri", "file:///opt/flink/checkpoints"),
                positive(p.getLong("checkpoint-interval-ms", 10_000), "checkpoint-interval-ms"),
                positive(p.getLong("checkpoint-timeout-ms", 60_000), "checkpoint-timeout-ms"),
                positive(p.getLong("checkpoint-min-pause-ms", 5_000), "checkpoint-min-pause-ms"),
                positive(p.getLong("kafka-transaction-timeout-ms", 600_000), "kafka-transaction-timeout-ms"),
                positiveInt(p.getInt("parallelism", 3), "parallelism"),
                positiveInt(p.getInt("schema-version", 1), "schema-version"));
    }

    private static String value(ParameterTool p, String arg, String env, String fallback) {
        return p.has(arg) ? p.getRequired(arg) : System.getenv().getOrDefault(env, fallback);
    }
}
```

Apply the same command-line-first/environment-second rule to all string settings. Use
`FLINK_INPUT_TOPIC`, `FLINK_OUTPUT_TOPIC`, `FLINK_LATE_TOPIC`, `FLINK_DLT_TOPIC`,
`FLINK_CONSUMER_GROUP`, and `FLINK_CHECKPOINT_URI`; numeric settings keep validated CLI
defaults and can be supplied by the submit script from environment variables.

- [ ] **Step 5: Implement strict parsing and validation**

`RawKafkaRecord` contains `topic`, `partition`, `offset`, `timestamp`, `key`, and
`payload`. `ValidatedStockDailyEvent` contains the raw record and parsed event.
Configure Jackson with `JavaTimeModule` and `FAIL_ON_UNKNOWN_PROPERTIES=true`.

```java
public ValidatedStockDailyEvent parse(RawKafkaRecord raw) {
    final StockDailyEvent event;
    try {
        event = mapper.readValue(raw.payload(), StockDailyEvent.class);
    } catch (JsonProcessingException e) {
        throw new InvalidStockDailyEventException("JSON_PARSE", "invalid StockDailyEvent JSON", e);
    }
    List<String> violations = validator.violations(raw.key(), event, supportedSchemaVersion);
    if (!violations.isEmpty()) {
        throw new InvalidStockDailyEventException("VALIDATION", String.join("; ", violations));
    }
    return new ValidatedStockDailyEvent(raw, event);
}
```

Validation requires nonblank IDs/source/`tsCode`, nonnull date/OHLC/preClose/volume/
amount/timestamps/version, nonnegative volume/amount, schema version 1, and Kafka Key
equal to `tsCode`.

```java
public List<String> violations(String kafkaKey, StockDailyEvent event,
                               int supportedSchemaVersion) {
    List<String> errors = new ArrayList<>();
    requireText(event.eventId(), "eventId", errors);
    requireText(event.traceId(), "traceId", errors);
    requireText(event.tsCode(), "tsCode", errors);
    requireText(event.source(), "source", errors);
    requireValue(event.tradeDate(), "tradeDate", errors);
    requireValue(event.open(), "open", errors);
    requireValue(event.high(), "high", errors);
    requireValue(event.low(), "low", errors);
    requireValue(event.close(), "close", errors);
    requireValue(event.preClose(), "preClose", errors);
    requireValue(event.volume(), "volume", errors);
    requireValue(event.amount(), "amount", errors);
    requireValue(event.eventTime(), "eventTime", errors);
    requireValue(event.ingestTime(), "ingestTime", errors);
    if (!Objects.equals(kafkaKey, event.tsCode())) errors.add("kafkaKey must equal tsCode");
    if (!Objects.equals(event.schemaVersion(), supportedSchemaVersion)) errors.add("unsupported schemaVersion");
    if (event.volume() != null && event.volume().signum() < 0) errors.add("volume must be non-negative");
    if (event.amount() != null && event.amount().signum() < 0) errors.add("amount must be non-negative");
    return errors;
}
```

- [ ] **Step 6: Run module tests**

Run: `mvn -q -pl flink-realtime-job -am -Dtest=FlinkJobConfigTest,StockDailyEventParserTest test`

Expected: PASS.

- [ ] **Step 7: Commit**

```powershell
git add .gitignore StockAnalysisSystem/java-services/pom.xml StockAnalysisSystem/java-services/flink-realtime-job
git commit -m "build: scaffold flink realtime job"
```

### Task 3: Implement the Pure Rolling Indicator Calculator

**Files:**
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/model/DailyPoint.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/calculation/IndicatorCalculator.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/test/java/com/stock/flink/calculation/IndicatorCalculatorTest.java`

**Interfaces:**
- Consumes: ascending `List<DailyPoint>` with 1–20 unique dates and `ValidatedStockDailyEvent`.
- Produces: `StockDailyIndicatorEvent calculate(List<DailyPoint>, ValidatedStockDailyEvent, OffsetDateTime)`.

- [ ] **Step 1: Write failing boundary and arithmetic tests**

```java
@ParameterizedTest
@CsvSource({"1,true", "5,true", "10,true", "20,false"})
void shouldExposeWarmupBoundary(int size, boolean warmup) {
    StockDailyIndicatorEvent result = calculator.calculate(points(size), input(size), NOW);
    assertEquals(size, result.windowSize());
    assertEquals(warmup, result.isWarmup());
    assertEquals(size >= 5, result.ma5() != null);
    assertEquals(size >= 10, result.ma10() != null);
    assertEquals(size >= 20, result.ma20() != null);
}

@Test
void shouldRoundHalfUpWithoutDoubleConversion() {
    assertEquals(new BigDecimal("1.666667"),
            IndicatorCalculator.average(List.of(
                    new BigDecimal("1"), new BigDecimal("2"), new BigDecimal("2"))));
}

@Test
void shouldReturnNullRatioWhenVolumeAverageIsZero() {
    assertNull(calculator.calculate(zeroVolumePoints(5), input(5), NOW).volumeRatio());
}
```

Keep `average(List<BigDecimal>)` package-private so this rounding behavior is tested without
adding a production-only event accessor. Public `ma5` still always uses five points.

- [ ] **Step 2: Run and verify failure**

Run: `mvn -q -pl flink-realtime-job -am -Dtest=IndicatorCalculatorTest test`

Expected: FAIL because calculator classes do not exist.

- [ ] **Step 3: Implement minimal decimal calculation**

```java
static BigDecimal average(List<BigDecimal> values) {
    if (values.isEmpty()) return null;
    BigDecimal sum = values.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
    return sum.divide(BigDecimal.valueOf(values.size()), 6, RoundingMode.HALF_UP);
}

private static BigDecimal trailingAverage(List<DailyPoint> points, int period,
                                          Function<DailyPoint, BigDecimal> getter) {
    if (points.size() < period) return null;
    return average(points.subList(points.size() - period, points.size())
            .stream().map(getter).toList());
}
```

Build the deterministic ID as
`FLINK_INDICATOR:%s:%s:v1`.formatted(tsCode, tradeDate)` and scale copied numeric
context fields to 6 decimals without changing their meaning.

- [ ] **Step 4: Run calculator tests**

Run: `mvn -q -pl flink-realtime-job -am -Dtest=IndicatorCalculatorTest test`

Expected: PASS for D1/D5/D10/D20, rounding, zero denominator, and deterministic ID.

- [ ] **Step 5: Commit**

```powershell
git add StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/{model,calculation} StockAnalysisSystem/java-services/flink-realtime-job/src/test/java/com/stock/flink/calculation
git commit -m "feat: calculate rolling daily indicators"
```

### Task 4: Add Parsing and Keyed-State Operators

**Files:**
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/operator/FlinkOutputTags.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/operator/ProcessingClock.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/operator/ParseAndValidateProcessFunction.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/operator/DailyIndicatorProcessFunction.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/test/java/com/stock/flink/operator/ParseAndValidateProcessFunctionTest.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/test/java/com/stock/flink/operator/DailyIndicatorProcessFunctionTest.java`

**Interfaces:**
- Consumes: `RawKafkaRecord` and `ValidatedStockDailyEvent` from Task 2; `IndicatorCalculator` from Task 3.
- Produces: main `StockDailyIndicatorEvent`, `FlinkOutputTags.LATE`, and `FlinkOutputTags.DEAD_LETTER` streams.

- [ ] **Step 1: Write failing operator tests**

```java
@Test
void shouldReplaceLatestDateWithoutGrowingWindow() throws Exception {
    harness.processElement(event("2026-08-28", "10.00", "100"), 1L);
    harness.processElement(event("2026-08-28", "11.00", "200"), 2L);
    assertEquals(2, mainOutput().size());
    assertEquals(1, mainOutput().getLast().windowSize());
    assertEquals(new BigDecimal("11.000000"), mainOutput().getLast().close());
}

@Test
void shouldRouteOlderDateWithoutChangingState() throws Exception {
    harness.processElement(event("2026-08-28", "10", "100"), 1L);
    harness.processElement(event("2026-08-27", "9", "90"), 2L);
    assertEquals(1, mainOutput().size());
    assertEquals(1, sideOutput(FlinkOutputTags.LATE).size());
    assertEquals(LocalDate.parse("2026-08-28"),
            sideOutput(FlinkOutputTags.LATE).getFirst().latestTradeDate());
}

@Test
void shouldRouteInvalidJsonToDeadLetter() throws Exception {
    parseHarness.processElement(raw("{bad-json}"), 1L);
    assertTrue(parseHarness.extractOutputValues().isEmpty());
    assertEquals("JSON_PARSE", deadLetters().getFirst().errorType());
}
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn -q -pl flink-realtime-job -am -Dtest=ParseAndValidateProcessFunctionTest,DailyIndicatorProcessFunctionTest test`

Expected: FAIL because operators and tags do not exist.

- [ ] **Step 3: Implement typed side-output tags and parser operator**

```java
public final class FlinkOutputTags {
    public static final OutputTag<LateStockDailyEvent> LATE =
            new OutputTag<>("late-daily", TypeInformation.of(LateStockDailyEvent.class));
    public static final OutputTag<FlinkDeadLetterEvent> DEAD_LETTER =
            new OutputTag<>("flink-dead-letter", TypeInformation.of(FlinkDeadLetterEvent.class));
    private FlinkOutputTags() {}
}
```

`ParseAndValidateProcessFunction` calls the parser and emits a dead-letter envelope on
`InvalidStockDailyEventException`; it never logs the full payload.

```java
@FunctionalInterface
public interface ProcessingClock extends Serializable {
    OffsetDateTime now();

    static ProcessingClock systemShanghai() {
        return () -> OffsetDateTime.now(ZoneId.of("Asia/Shanghai"));
    }
}
```

- [ ] **Step 4: Implement managed 20-day state**

Use `ListState<DailyPoint>` and `ValueState<LocalDate>` initialized in `open()`.

```java
if (latest == null || tradeDate.isAfter(latest)) {
    points.add(DailyPoint.from(event));
    points.sort(Comparator.comparing(DailyPoint::tradeDate));
    if (points.size() > 20) points.remove(0);
    replaceState(points, tradeDate);
    out.collect(calculator.calculate(points, input, clock.now()));
} else if (tradeDate.equals(latest)) {
    points.set(points.size() - 1, DailyPoint.from(event));
    replaceState(points, latest);
    out.collect(calculator.calculate(points, input, clock.now()));
} else {
    ctx.output(FlinkOutputTags.LATE,
            new LateStockDailyEvent(event, latest, "TRADE_DATE_BEFORE_LATEST",
                    clock.now(), 1));
}
```

Inject a serializable clock abstraction so tests use fixed Shanghai timestamps.

- [ ] **Step 5: Add snapshot/restore coverage**

Take a harness snapshot after 10 dates, restore a new harness, process date 11, and assert
`windowSize=11` and `ma10` equals the uninterrupted execution.

- [ ] **Step 6: Run operator tests**

Run: `mvn -q -pl flink-realtime-job -am -Dtest='*ProcessFunctionTest' test`

Expected: PASS for state isolation, replacement, late routing, DLT routing, 20-row cap, and snapshot restore.

- [ ] **Step 7: Commit**

```powershell
git add StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/operator StockAnalysisSystem/java-services/flink-realtime-job/src/test/java/com/stock/flink/operator
git commit -m "feat: process daily events with flink keyed state"
```

### Task 5: Wire Kafka Source, Transactional Sinks, and Job Entrypoint

**Files:**
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/kafka/RawKafkaRecordDeserializationSchema.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/kafka/JsonKafkaRecordSerializationSchema.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/main/java/com/stock/flink/DailyIndicatorJob.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/test/java/com/stock/flink/kafka/KafkaSchemaTest.java`
- Create: `StockAnalysisSystem/java-services/flink-realtime-job/src/test/java/com/stock/flink/DailyIndicatorJobTest.java`

**Interfaces:**
- Consumes: Task 4 operators and four Topic names in `FlinkJobConfig`.
- Produces: executable shaded JAR and topology with stable operator UIDs.

- [ ] **Step 1: Write failing Kafka schema tests**

```java
@Test
void shouldPreserveSourceMetadata() throws Exception {
    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            "stock.ods.daily.v1", 2, 99L,
            "000001.SZ".getBytes(UTF_8), validJson().getBytes(UTF_8));
    RawKafkaRecord raw = deserialize(record);
    assertEquals(2, raw.partition());
    assertEquals(99L, raw.offset());
    assertEquals("000001.SZ", raw.key());
}

@Test
void shouldSerializeIndicatorWithTsCodeKey() {
    ProducerRecord<byte[], byte[]> record = schema.serialize(indicator(), null);
    assertEquals("stock.dws.daily-indicator.v1", record.topic());
    assertEquals("000001.SZ", new String(record.key(), UTF_8));
}
```

- [ ] **Step 2: Run and verify failure**

Run: `mvn -q -pl flink-realtime-job -am -Dtest=KafkaSchemaTest,DailyIndicatorJobTest test`

Expected: FAIL because Kafka schemas and job do not exist.

- [ ] **Step 3: Implement source and three sinks**

```java
KafkaSource<RawKafkaRecord> source = KafkaSource.<RawKafkaRecord>builder()
        .setBootstrapServers(config.bootstrapServers())
        .setTopics(config.inputTopic())
        .setGroupId(config.consumerGroup())
        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
        .setDeserializer(new RawKafkaRecordDeserializationSchema())
        .build();

Properties producerProperties = new Properties();
producerProperties.setProperty(
        ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
        Long.toString(config.kafkaTransactionTimeoutMs()));

KafkaSink<StockDailyIndicatorEvent> mainSink = KafkaSink.<StockDailyIndicatorEvent>builder()
        .setBootstrapServers(config.bootstrapServers())
        .setRecordSerializer(JsonKafkaRecordSerializationSchema.indicators(config.outputTopic()))
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setTransactionalIdPrefix("stock-flink-indicator-v1-main-")
        .setKafkaProducerConfig(producerProperties)
        .build();
```

Build late and DLT sinks with identical delivery guarantee and prefixes
`stock-flink-indicator-v1-late-` and `stock-flink-indicator-v1-dlt-`.

Connect the streams explicitly so an invalid record never enters keyed state:

```java
SingleOutputStreamOperator<ValidatedStockDailyEvent> valid = raw
        .process(new ParseAndValidateProcessFunction(parser, ProcessingClock.systemShanghai()))
        .uid("daily-parse-v1");
DataStream<FlinkDeadLetterEvent> deadLetters = valid.getSideOutput(FlinkOutputTags.DEAD_LETTER);

SingleOutputStreamOperator<StockDailyIndicatorEvent> indicators = valid
        .keyBy(value -> value.event().tsCode())
        .process(new DailyIndicatorProcessFunction(new IndicatorCalculator(),
                ProcessingClock.systemShanghai()))
        .uid("daily-indicator-state-v1");
DataStream<LateStockDailyEvent> late = indicators.getSideOutput(FlinkOutputTags.LATE);

indicators.sinkTo(mainSink).uid("daily-main-sink-v1");
late.sinkTo(lateSink).uid("daily-late-sink-v1");
deadLetters.sinkTo(deadLetterSink).uid("daily-dlt-sink-v1");
```

- [ ] **Step 4: Configure checkpointing and stable UIDs**

```java
env.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setCheckpointTimeout(config.checkpointTimeoutMs());
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.checkpointMinPauseMs());
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
env.getCheckpointConfig().setCheckpointStorage(config.checkpointUri());
env.getCheckpointConfig().setExternalizedCheckpointCleanup(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
env.setParallelism(config.parallelism());
```

Assign UIDs `daily-kafka-source-v1`, `daily-parse-v1`, `daily-indicator-state-v1`,
`daily-main-sink-v1`, `daily-late-sink-v1`, and `daily-dlt-sink-v1`.

- [ ] **Step 5: Run unit tests and package the shaded JAR**

Run: `mvn -q -pl flink-realtime-job -am test package`

Expected: PASS and
`flink-realtime-job/target/flink-realtime-job-0.3.0-SNAPSHOT-all.jar` exists.

- [ ] **Step 6: Inspect the artifact**

Run: `jar tf flink-realtime-job/target/flink-realtime-job-0.3.0-SNAPSHOT-all.jar | Select-String 'DailyIndicatorJob|KafkaSource'`

Expected: both the main class and Kafka connector classes are present; Flink runtime classes are not shaded.

- [ ] **Step 7: Commit**

```powershell
git add StockAnalysisSystem/java-services/flink-realtime-job
git commit -m "feat: wire exactly once flink kafka job"
```

### Task 6: Add Flink and V0.5 Topics to Docker Compose

**Files:**
- Modify: `StockAnalysisSystem/infrastructure/docker-compose.yml`
- Create: `StockAnalysisSystem/scripts/submit_v05_flink_job.ps1`
- Create: `StockAnalysisSystem/python-services/python-collector/tests/test_v05_compose_contract.py`

**Interfaces:**
- Consumes: shaded JAR from Task 5.
- Produces: healthy Session Cluster at `localhost:8082`, durable checkpoints, and all V0.5 Topics.

- [ ] **Step 1: Write a failing Compose contract test**

```python
def test_compose_declares_flink_and_v05_topics():
    text = COMPOSE.read_text(encoding='utf-8')
    for value in [
        'flink:2.2.0-scala_2.12-java21',
        'flink-jobmanager', 'flink-taskmanager',
        '8082:8081', 'flink_checkpoints',
        'stock.dws.daily-indicator.v1',
        'stock.late.daily.v1',
        'stock.flink.dead-letter.v1',
    ]:
        assert value in text
```

- [ ] **Step 2: Run and verify failure**

Run: `D:\Python\python.exe -m pytest tests\test_v05_compose_contract.py -v`

Expected: FAIL because Flink services and Topics are absent.

- [ ] **Step 3: Extend Topic initialization**

Add idempotent creation commands: main output with 3 partitions, late and DLT with 1
partition, all replication factor 1.

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic stock.dws.daily-indicator.v1 --partitions 3 --replication-factor 1 &&
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic stock.late.daily.v1 --partitions 1 --replication-factor 1 &&
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic stock.flink.dead-letter.v1 --partitions 1 --replication-factor 1
```

- [ ] **Step 4: Add Session Cluster services**

Both services use the fixed Flink image, depend on healthy Kafka, mount
`flink_checkpoints:/opt/flink/checkpoints` and the local job target directory read-only at
`/opt/flink/usrlib`. Name the containers `stock-flink-jobmanager` and
`stock-flink-taskmanager`. JobManager maps `8082:8081`; TaskManager exposes 3 slots. Add a
JobManager healthcheck against `http://localhost:8081/overview`; the acceptance script must
also require exactly one registered TaskManager from `http://localhost:8082/taskmanagers`.
Set Kafka `KAFKA_TRANSACTION_MAX_TIMEOUT_MS=900000`, which is greater than the job's
600000 ms producer transaction timeout. Set:

```yaml
flink-jobmanager:
  image: flink:2.2.0-scala_2.12-java21
  container_name: stock-flink-jobmanager
  command: jobmanager
  depends_on:
    kafka:
      condition: service_healthy
  ports: ["8082:8081"]
  volumes:
    - flink_checkpoints:/opt/flink/checkpoints
    - ../java-services/flink-realtime-job/target:/opt/flink/usrlib:ro
  environment:
    FLINK_PROPERTIES: |-
      jobmanager.rpc.address: flink-jobmanager
      taskmanager.numberOfTaskSlots: 3
      state.checkpoints.dir: file:///opt/flink/checkpoints
      execution.checkpointing.interval: 10s
  healthcheck:
    test: ["CMD-SHELL", "bash -c '</dev/tcp/localhost/8081'"]
    interval: 10s
    timeout: 5s
    retries: 12

flink-taskmanager:
  image: flink:2.2.0-scala_2.12-java21
  container_name: stock-flink-taskmanager
  command: taskmanager
  depends_on:
    flink-jobmanager:
      condition: service_healthy
  volumes:
    - flink_checkpoints:/opt/flink/checkpoints
    - ../java-services/flink-realtime-job/target:/opt/flink/usrlib:ro
  environment:
    FLINK_PROPERTIES: |-
      jobmanager.rpc.address: flink-jobmanager
      taskmanager.numberOfTaskSlots: 3
      state.checkpoints.dir: file:///opt/flink/checkpoints

volumes:
  kafka_data:
  flink_checkpoints:
```

- [ ] **Step 5: Implement an idempotent submit script**

```powershell
$jar = '/opt/flink/usrlib/flink-realtime-job-0.3.0-SNAPSHOT-all.jar'
$running = docker compose -f infrastructure\docker-compose.yml exec -T flink-jobmanager `
    flink list -r
if ($running -match 'stock-daily-indicator-v1') {
    throw 'stock-daily-indicator-v1 is already running'
}
docker compose -f infrastructure\docker-compose.yml exec -T flink-jobmanager `
    flink run -d -m flink-jobmanager:8081 `
    -c com.stock.flink.DailyIndicatorJob $jar `
    --bootstrap-servers kafka:29092
```

- [ ] **Step 6: Validate Compose without starting services**

Run: `docker compose -f StockAnalysisSystem\infrastructure\docker-compose.yml config --quiet`

Expected: exit code 0.

- [ ] **Step 7: Run the contract test**

Run: `D:\Python\python.exe -m pytest tests\test_v05_compose_contract.py -v`

Expected: PASS.

- [ ] **Step 8: Commit**

```powershell
git add StockAnalysisSystem/infrastructure/docker-compose.yml StockAnalysisSystem/scripts/submit_v05_flink_job.ps1 StockAnalysisSystem/python-services/python-collector/tests/test_v05_compose_contract.py
git commit -m "infra: add flink session cluster"
```

### Task 7: Add Ordered Collector History Replay

**Files:**
- Modify: `StockAnalysisSystem/python-services/python-collector/app/repositories/stock_repository.py`
- Create: `StockAnalysisSystem/python-services/python-collector/app/jobs/daily_event_replay_job.py`
- Modify: `StockAnalysisSystem/python-services/python-collector/app/main.py`
- Create: `StockAnalysisSystem/python-services/python-collector/tests/test_daily_event_replay_job.py`
- Modify: `StockAnalysisSystem/python-services/python-collector/tests/test_stock_repository.py`

**Interfaces:**
- Consumes: MySQL `stock_daily`, existing `StockDailyRecord`, `EventFactory`, and `StockKafkaProducer`.
- Produces: `StockRepository.iter_daily_records(start, end, batchSize)` and CLI `replay-daily-events --start --end`.

- [ ] **Step 1: Write failing repository-order and replay tests**

```python
def test_iter_daily_records_orders_by_date_then_code():
    repo, session = make_repo_with_rows([ROW_1, ROW_2])
    batches = list(repo.iter_daily_records(date(2026, 8, 1), date(2026, 8, 28), 1))
    sql = str(session.execute.call_args.args[0])
    assert 'ORDER BY trade_date ASC, ts_code ASC' in sql
    assert [len(batch) for batch in batches] == [1, 1]

def test_replay_uses_event_factory_and_flushes_each_batch():
    repo.iter_daily_records.return_value = [[record1, record2], [record3]]
    result = DailyEventReplayJob(repo, producer).execute(START, END)
    assert producer.send_event.call_count == 3
    assert producer.flush.call_count == 2
    assert result == {'success': True, 'published_count': 3, 'batch_count': 2}
```

- [ ] **Step 2: Run and verify failure**

Run: `D:\Python\python.exe -m pytest tests\test_daily_event_replay_job.py tests\test_stock_repository.py -v`

Expected: FAIL because iterator and replay job do not exist.

- [ ] **Step 3: Implement the ordered batch iterator**

```python
def iter_daily_records(self, start_date: date, end_date: date,
                       batch_size: int = _BATCH_SIZE):
    stmt = text(
        'SELECT ts_code, trade_date, open, high, low, close, pre_close, '
        '`change`, pct_chg, vol, amount '
        'FROM stock_daily WHERE trade_date BETWEEN :start AND :end '
        'ORDER BY trade_date ASC, ts_code ASC'
    )
    result = self.session.execute(stmt, {'start': start_date, 'end': end_date}).mappings()
    batch = []
    for row in result:
        batch.append(StockDailyRecord(**dict(row), source='MYSQL_REPLAY'))
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
```

- [ ] **Step 4: Implement replay with existing contracts**

For each record call
`EventFactory.create_daily_event(record, trace_id=f"replay-{start:%Y%m%d}-{end:%Y%m%d}")`,
send it through `StockKafkaProducer.send_event`, flush once per database batch, and stop with
failure on the first enqueue or delivery error. Never write MySQL or collection-task rows.

```python
class DailyEventReplayJob:
    def __init__(self, repository, producer):
        self.repository = repository
        self.producer = producer

    def execute(self, start_date, end_date, batch_size=5000):
        if start_date > end_date:
            raise ValueError('start_date 不能晚于 end_date')
        trace_id = f'replay-{start_date:%Y%m%d}-{end_date:%Y%m%d}'
        published_count = 0
        batch_count = 0
        for batch in self.repository.iter_daily_records(start_date, end_date, batch_size):
            for record in batch:
                event = EventFactory.create_daily_event(record, trace_id=trace_id)
                self.producer.send_event(event)
            self.producer.flush()
            published_count += len(batch)
            batch_count += 1
        return {
            'success': True,
            'published_count': published_count,
            'batch_count': batch_count,
        }
```

- [ ] **Step 5: Add the CLI contract**

```python
p_replay = subparsers.add_parser(
    'replay-daily-events', help='按交易日顺序向 Kafka 重放已有日线行情')
p_replay.add_argument('--start', required=True)
p_replay.add_argument('--end', required=True)
p_replay.add_argument('--batch-size', type=int, default=5000)
```

`cmd_replay_daily_events` must create `StockRepository` and `StockKafkaProducer` directly;
it must ignore `COLLECTOR_OUTPUT_MODE` so replay can never accidentally write MySQL.

```python
def cmd_replay_daily_events(args):
    start_date = parse_date(args.start)
    end_date = parse_date(args.end)
    if args.batch_size <= 0:
        raise ValueError('batch-size 必须大于 0')
    db = _init_database()
    producer = StockKafkaProducer()
    try:
        with db.session_scope() as session:
            result = DailyEventReplayJob(
                StockRepository(session), producer
            ).execute(start_date, end_date, args.batch_size)
    finally:
        producer.close()
    logger.info('Kafka 历史重放完成: %s 条', result['published_count'])
    return 0
```

Add `'replay-daily-events': cmd_replay_daily_events` to the dispatch map. Tests must assert
invalid dates and nonpositive batch sizes fail before any Kafka send.

- [ ] **Step 6: Run focused and full Collector tests**

Run: `D:\Python\python.exe -m pytest tests\test_daily_event_replay_job.py tests\test_stock_repository.py -v`

Expected: PASS.

Run: `D:\Python\python.exe -m pytest tests -q`

Expected: at least 118 existing tests plus new replay/Compose tests pass.

- [ ] **Step 7: Commit**

```powershell
git add StockAnalysisSystem/python-services/python-collector/app StockAnalysisSystem/python-services/python-collector/tests
git commit -m "feat: replay daily events for flink warmup"
```

### Task 8: Add End-to-End V0.5 Verification

**Files:**
- Create: `StockAnalysisSystem/scripts/verify_v05_flink.py`
- Create: `StockAnalysisSystem/python-services/python-collector/tests/test_verify_v05_flink.py`
- Create: `StockAnalysisSystem/docs/V0.5_TEST_CASES.md`

**Interfaces:**
- Consumes: running Compose stack and V0.5 Topics.
- Produces: deterministic `valid`, `late`, `invalid`, and `recovery` acceptance scenarios with nonzero exit on mismatch.

- [ ] **Step 1: Write failing verifier unit tests**

```python
def test_expected_indicators_use_decimal_half_up():
    rows = fixture_rows(20)
    result = expected_indicators(rows)[-1]
    assert result['windowSize'] == 20
    assert result['isWarmup'] is False
    assert result['ma5'] == Decimal('18.000000')

def test_validate_results_rejects_duplicate_event_id():
    with pytest.raises(AssertionError, match='duplicate eventId'):
        validate_results([EVENT, EVENT])
```

- [ ] **Step 2: Run and verify failure**

Run: `D:\Python\python.exe -m pytest tests\test_verify_v05_flink.py -v`

Expected: FAIL because verifier helpers do not exist.

- [ ] **Step 3: Implement deterministic scenarios**

The script uses `Decimal`, publishes keyed V1 JSON, and consumes with:

```python
Consumer({
    'bootstrap.servers': args.bootstrap_servers,
    'group.id': f'v05-verifier-{uuid4()}',
    'auto.offset.reset': 'earliest',
    'isolation.level': 'read_committed',
})
```

Scenarios:

- `valid`: publish D1–D20 for two stocks and compare every available field;
- `late`: after D20 publish D19 again and require exactly one late event and no new main event;
- `invalid`: publish malformed JSON and a Key mismatch and require two DLT events;
- `recovery-before`: wait through the Flink REST API until a completed checkpoint is visible,
  then write its ID and the ten committed event IDs to `.tmp/v05-recovery.json`;
- `recovery-after`: after TaskManager restart publish D11–D20, consume D1–D20 with a fresh
  `read_committed` group, and require exactly one visible event per business key with correct MA20.

- [ ] **Step 4: Write exact manual recovery commands**

Document:

```powershell
python scripts\verify_v05_flink.py --scenario recovery-before
docker restart stock-flink-taskmanager
docker compose -f infrastructure\docker-compose.yml ps
python scripts\verify_v05_flink.py --scenario recovery-after
```

The second verifier fails if it observes a duplicate committed `eventId`, missing date, wrong
window size, or wrong indicator.

- [ ] **Step 5: Run verifier unit tests**

Run: `D:\Python\python.exe -m pytest tests\test_verify_v05_flink.py -v`

Expected: PASS.

- [ ] **Step 6: Run the local integration acceptance**

Run from `StockAnalysisSystem` after building the JAR:

```powershell
docker compose -f infrastructure\docker-compose.yml up -d
powershell -ExecutionPolicy Bypass -File scripts\submit_v05_flink_job.ps1
python scripts\verify_v05_flink.py --scenario valid
python scripts\verify_v05_flink.py --scenario late
python scripts\verify_v05_flink.py --scenario invalid
python scripts\verify_v05_flink.py --scenario recovery-before
docker restart stock-flink-taskmanager
python scripts\verify_v05_flink.py --scenario recovery-after
```

Expected: every command exits 0; Flink job remains RUNNING; all consumers use committed data.

- [ ] **Step 7: Commit**

```powershell
git add StockAnalysisSystem/scripts/verify_v05_flink.py StockAnalysisSystem/python-services/python-collector/tests/test_verify_v05_flink.py StockAnalysisSystem/docs/V0.5_TEST_CASES.md
git commit -m "test: verify flink indicator pipeline"
```

### Task 9: Document, Regress, and Prepare the Pull Request

**Files:**
- Modify: `StockAnalysisSystem/README.md`
- Create: `StockAnalysisSystem/docs/V0.5_RELEASE_NOTES.md`
- Create: `StockAnalysisSystem/docs/V0.5_RECONCILIATION_REPORT.md`
- Modify: `StockAnalysisSystem/.env.example`

**Interfaces:**
- Consumes: verified commands and results from Tasks 1–8.
- Produces: reproducible operator guide, final evidence, and reviewable personal-fork branch.

- [ ] **Step 1: Update environment examples without secrets**

Add names and safe defaults for bootstrap servers, four Topics, group ID, checkpoint URI,
parallelism, and schema version. Do not copy values from local `.env`.

- [ ] **Step 2: Document exact operating flow**

README must show: build JAR, start Compose, submit once, open Flink UI at
`http://localhost:8082`, replay 20 dates, consume `read_committed`, run smoke tests, take a
savepoint before upgrades, cancel the job, and roll back without stopping V0.4.

- [ ] **Step 3: Record reconciliation evidence**

The report records exact commit, Docker image, JAR hash, input/output counts, D1/D5/D10/D20
values, late/DLT counts, checkpoint ID, restart result, lag, and every test command. Never
record `.env` contents, tokens, passwords, or full connection strings.

- [ ] **Step 4: Run all automated tests from the clean branch**

Collector:

```powershell
cd StockAnalysisSystem\python-services\python-collector
D:\Python\python.exe -m pytest tests -q
```

Expected: all existing 118 and all new tests PASS.

Analysis App:

```powershell
cd StockAnalysisSystem\python-services\stock-analysis-app
$env:POLARS_SKIP_CPU_CHECK='1'
D:\Python\python.exe -m pytest -q --basetemp='.pytest-tmp-v05'
```

Expected: 44 passed, 6 skipped; remove only `.pytest-tmp-v05` afterward.

Java:

```powershell
cd StockAnalysisSystem\java-services
mvn -q test
mvn -q -pl flink-realtime-job -am package
```

Expected: all modules PASS and shaded JAR exists.

- [ ] **Step 5: Validate repository and infrastructure**

```powershell
git diff --check
git status --short
docker compose -f StockAnalysisSystem\infrastructure\docker-compose.yml config --quiet
```

Expected: no whitespace errors; only intentional files changed; Compose exits 0.

- [ ] **Step 6: Commit documentation**

```powershell
git add StockAnalysisSystem/README.md StockAnalysisSystem/.env.example StockAnalysisSystem/docs/V0.5_RELEASE_NOTES.md StockAnalysisSystem/docs/V0.5_RECONCILIATION_REPORT.md
git commit -m "docs: release flink realtime indicators"
```

- [ ] **Step 7: Review before publication**

Use `superpowers:requesting-code-review`, fix accepted findings, rerun affected tests, then run
`superpowers:verification-before-completion` before claiming success.

- [ ] **Step 8: Push only to the personal repository and open a PR**

```powershell
git push -u origin codex/flink-realtime-pipeline
gh pr create --base main --head codex/flink-realtime-pipeline `
  --title "feat: add Flink realtime indicator pipeline" `
  --body-file StockAnalysisSystem/docs/V0.5_RELEASE_NOTES.md
```

Expected: branch exists only on the user's personal GitHub repository and the PR targets its
`main`. If a separate team repository is configured later, fork it first and open the team PR
from the personal fork; never push the feature branch directly to a team remote.
