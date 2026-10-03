# V0.5 Flink 实时指标链路设计

## 1. 背景

V0.4 已将 Tushare/AkShare 数据访问统一收口到 `python-collector`，并保留了
`stock-analysis-app` 的离线特征工程与可视化。当前 Kafka 原始日线 Topic
`stock.ods.daily.v1` 已能稳定接收 `StockDailyEvent`，但项目尚无真正的流式状态计算。

V0.5 的第一阶段建立一条可独立验收的实时计算链路：

```text
Python Collector
  -> stock.ods.daily.v1
  -> Apache Flink
  -> stock.dws.daily-indicator.v1
```

本阶段只以 Kafka 作为输入和输出，不让 Flink 直接查询或写入 MySQL、ClickHouse。
ClickHouse 落库和业务页面接入留给后续独立阶段。

## 2. 目标

- 使用 Java DataStream API 实现可提交到 Flink Session Cluster 的作业。
- 按 `tsCode` 维护最多 20 个交易日的 Flink 托管状态。
- 计算 `ma5`、`ma10`、`ma20`、`volMa5`、`volMa10` 和 `volumeRatio`。
- 窗口未满时仍输出事件，暂不可用的指标为 `null`。
- 支持同一交易日幂等覆盖；更早交易日进入迟到数据 Topic。
- 对非法 JSON、字段和协议版本进行隔离，不阻塞其他股票。
- Kafka Source、Flink 状态和 Kafka Sink 形成端到端 exactly-once 链路。
- 通过 Python Collector 按交易日升序重放历史日线，完成初始 20 日预热。
- 在 Docker Compose 中提供 JobManager、TaskManager、checkpoint volume 和 Web UI。

## 3. 非目标

- 不在本阶段接入 ClickHouse、Redis、Iceberg、Spark 或 Flink SQL。
- 不直接更新 `stock_features`、`analysis_result` 或其他 MySQL 表。
- 不计算 EMA、布林带、KDJ、RSI、MACD 或交易信号。
- 不支持任意历史修正后重算全部后续日期。
- 不替换现有 Python 离线特征工程和 Streamlit 页面。
- 不把 PyFlink 或 Python 依赖加入 Flink 容器。

## 4. 技术选型

| 项目 | 选择 |
|---|---|
| Flink | Apache Flink 2.2.0 |
| Java | Java 21 |
| API | Java DataStream API |
| Kafka Connector | `flink-connector-kafka` 5.0.0-2.2 |
| 部署 | Docker Compose Session Cluster |
| 状态 | Flink Managed Keyed State |
| 交付语义 | Exactly-once |
| 序列化 | Jackson camelCase JSON |
| 数值 | `BigDecimal`，6 位小数，`HALF_UP` |

Flink 2.2.0 与 Kafka Connector 5.0.0-2.2 是当前可发布、版本匹配的组合。最初设计的
Flink 2.3.0 在 Maven Central 尚无对应 Kafka Connector，Flink 2.3 官方 Kafka 文档也明确
标注连接器尚未提供，因此不能用于本项目的 Kafka 主链路。Flink 2.x 支持 Java 21；官方文档推荐本地
Docker Compose 使用 Session Cluster。Kafka exactly-once 要求启用 checkpoint、使用事务
KafkaSink，并让消费者使用 `read_committed`。

参考：

- <https://flink.apache.org/downloads/>
- <https://flink.apache.org/2025/03/24/apache-flink-2.0.0-a-new-era-of-real-time-data-processing/>
- <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/resource-providers/standalone/docker/>
- <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/>
- <https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/maven-metadata.xml>

## 5. 模块边界

在 `java-services` Maven 聚合工程中新建 `flink-realtime-job`：

```text
java-services/
├── common-model/
├── kafka-consumer-service/
└── flink-realtime-job/
```

### 5.1 `common-model`

新增跨模块协议类型：

- `StockDailyIndicatorEvent`
- `LateStockDailyEvent`
- `FlinkDeadLetterEvent`

现有 `StockDailyEvent` 协议保持不变。公共模型不依赖 Flink、Spring 或 Kafka 客户端。

### 5.2 `flink-realtime-job`

职责：

- 从 Kafka 读取原始 key/value 和来源元数据；
- 解析并验证 `StockDailyEvent`；
- 按 `tsCode` 分区；
- 管理 20 日滚动状态；
- 计算指标并生成新协议事件；
- 将主结果、迟到事件和非法事件发送到独立 Topic；
- 配置 checkpoint、重启策略、操作器 UID 和 Kafka 事务。

### 5.3 `python-collector`

新增显式历史重放入口。它从已有 MySQL `stock_daily` 读取指定日期范围，严格按
`tradeDate ASC, tsCode ASC` 发布已有 `StockDailyEvent`。该入口只用于补状态或验收，
不实现指标计算，也不绕过现有 EventFactory 和 Kafka Serializer。

建议命令形态：

```powershell
python -m app.main replay-daily-events --start 20260801 --end 20260828 --ts-code 000001.SZ
```

真实对账默认应先通过 `--ts-code` 限定一只股票；省略该参数才重放全市场，以保持原有兼容行为。股票代码会在创建 MySQL/Kafka 资源前完成去空白、大写规范化和格式校验，数据库查询使用绑定参数。重放范围必须至少覆盖该股票 20 个实际交易日；若范围不足，Flink 仍按 warmup 规则输出。

## 6. Kafka Topic

| Topic | 分区 | Key | 用途 |
|---|---:|---|---|
| `stock.ods.daily.v1` | 3 | `tsCode` | 现有原始日线输入 |
| `stock.dws.daily-indicator.v1` | 3 | `tsCode` | 实时指标主输出 |
| `stock.late.daily.v1` | 1 | `tsCode` | 早于当前最新交易日的数据 |
| `stock.flink.dead-letter.v1` | 1 | 原始 Key | Flink 解析或校验失败消息 |

主输出继续以 `tsCode` 为 Key，以保证同一股票在下游的分区内顺序。结果 Topic 不使用
只保留单个股票最新值的 compact-only 策略；下游需要用稳定 `eventId` 或
`(tsCode, tradeDate)` 实现幂等写入。

## 7. 主输出协议

`StockDailyIndicatorEvent` 使用严格 camelCase JSON：

| 字段 | 类型 | 规则 |
|---|---|---|
| `eventId` | String | `FLINK_INDICATOR:{tsCode}:{tradeDate}:v1`，确定且稳定 |
| `sourceEventId` | String | 原 `StockDailyEvent.eventId` |
| `traceId` | String | 原样传递 |
| `tsCode` | String | Kafka Key 必须与其相同 |
| `tradeDate` | LocalDate | 当前指标所属交易日 |
| `source` | String | 原始数据源 |
| `sourceEventTime` | OffsetDateTime | 原始事件时间 |
| `close` | BigDecimal | 原始收盘价 |
| `volume` | BigDecimal | 原始成交量 |
| `pctChg` | BigDecimal | 原始涨跌幅 |
| `ma5` | BigDecimal/null | 至少 5 条时可用 |
| `ma10` | BigDecimal/null | 至少 10 条时可用 |
| `ma20` | BigDecimal/null | 至少 20 条时可用 |
| `volMa5` | BigDecimal/null | 至少 5 条时可用 |
| `volMa10` | BigDecimal/null | 至少 10 条时可用 |
| `volumeRatio` | BigDecimal/null | `volume / volMa5`；分母为 0 时为 `null` |
| `windowSize` | Integer | 当前唯一交易日数量，范围 1–20 |
| `isWarmup` | Boolean | `windowSize < 20` |
| `calculationTime` | OffsetDateTime | Flink 生成结果的处理时间 |
| `schemaVersion` | Integer | 固定为 1 |

所有派生数值最终使用 `setScale(6, RoundingMode.HALF_UP)`。计算过程中不先转换为
`double`，避免引入额外二进制浮点误差。

## 8. 状态与计算规则

数据流在完成协议验证后执行 `keyBy(StockDailyEvent::tsCode)`。每个 Key 保存：

- `latestTradeDate`；
- 按日期升序的最多 20 个 `DailyPoint(tradeDate, close, volume)`。

状态必须使用 Flink Managed Keyed State，不允许使用进程内静态 Map。

### 8.1 新交易日

当 `tradeDate > latestTradeDate`：

1. 追加新点；
2. 超过 20 条时移除最早一天；
3. 更新 `latestTradeDate`；
4. 按当前窗口计算可用指标；
5. 输出一条主结果事件。

### 8.2 同一交易日

当 `tradeDate == latestTradeDate`：

1. 用新值覆盖窗口最后一天；
2. 不增加 `windowSize`；
3. 重新计算并输出；
4. 使用与此前相同的确定性 `eventId`。

这允许数据源对当天数据进行修正，并让后续消费者按业务键覆盖。

### 8.3 更早交易日

当 `tradeDate < latestTradeDate`：

- 不修改主状态；
- 不输出主指标事件；
- 输出 `LateStockDailyEvent` 到 `stock.late.daily.v1`。

`LateStockDailyEvent` 至少包含原事件、当前 `latestTradeDate`、原因、处理时间和
`schemaVersion`。

### 8.4 Warmup

- D1–D4：所有均线和 `volumeRatio` 为 `null`；
- D5–D9：`ma5`、`volMa5`、`volumeRatio` 可用；
- D10–D19：再开放 `ma10`、`volMa10`；
- D20 及以后：`ma20` 可用，`isWarmup=false`。

每条合法的新交易日或同日修正事件都产生主输出，不因 warmup 被丢弃。

## 9. 验证与错误分流

Flink 作业独立验证输入，不能依赖另一个 consumer group 已经校验过消息。

以下情况进入 `stock.flink.dead-letter.v1`：

- JSON 无法反序列化；
- 必填字段为空；
- Kafka Key 与 `tsCode` 不一致；
- `schemaVersion` 不是 1；
- 价格或成交量字段不满足既有日线协议约束。

`FlinkDeadLetterEvent` 至少包含：

- 原 Topic、分区、Offset、Key 和 payload；
- `errorType`、不含秘密信息的 `errorMessage`；
- `failedAt` 和 `schemaVersion`。

单条错误只进入侧输出，不抛出导致整个作业反复重启的未处理异常。Kafka、状态后端、
序列化器初始化等基础设施错误仍应使作业失败并交给重启策略处理。

## 10. Exactly-once 与恢复

- Source group ID：`stock-flink-daily-indicator-v1`；
- 无已提交 Offset 和无 checkpoint 时从 earliest 启动；
- 每 10 秒创建一次 exactly-once checkpoint；
- checkpoint timeout 为 60 秒，最小间隔为 5 秒；
- 同时只允许一个 checkpoint；
- checkpoint 保存到所有 Flink 容器可见的 Docker volume；
- 关键 Source、Process、Sink 设置稳定且显式的 operator UID；
- 主输出、late 和 DLT 三个 KafkaSink 均使用 `DeliveryGuarantee.EXACTLY_ONCE`；
- 三个 Sink 分别使用稳定且互不重复的 transactional ID 前缀
  `stock-flink-indicator-v1-main-`、`stock-flink-indicator-v1-late-` 和
  `stock-flink-indicator-v1-dlt-`，同一 Kafka 集群中的其他作业不得复用；
- 下游验收消费者使用 `isolation.level=read_committed`；
- Kafka transaction timeout 必须大于 checkpoint 最大耗时与预期最长重启时间之和。

采用有限延迟重启策略，例如最多重试 3 次、每次间隔 10 秒。持续配置或协议错误应使作业
最终进入 FAILED，不能无限重启掩盖问题。

## 11. Docker Compose

现有 Compose 增加：

- `flink-jobmanager`；
- `flink-taskmanager`；
- `flink_checkpoints` volume；
- 三个 V0.5 Kafka Topic 的初始化命令。

使用已通过 `docker manifest inspect` 验证的固定官方镜像标签
`flink:2.2.0-scala_2.12-java21`，禁止使用 `latest`。

Flink 容器内 Web UI 仍为 8081，宿主机映射为 `8082:8081`，避免与现有 Kafka UI
`8081:8080` 冲突。Session Cluster 启动后，通过显式命令提交 shaded job JAR，不由容器
启动脚本自动重复提交。

## 12. 配置

作业参数优先从命令行或环境变量读取，至少包括：

- Kafka bootstrap servers；
- input/output/late/DLT Topic；
- consumer group ID；
- supported schema version；
- checkpoint interval、timeout 和目录；
- transactional ID prefix；
- parallelism。

默认值面向本地 Docker 网络，但不得把密码或 Token 写入源码、JAR 或日志。

## 13. 测试策略

### 13.1 公共协议测试

- 标准 JSON fixture 可反序列化；
- 字段为 camelCase；
- 未知字段和不支持版本按协议拒绝；
- BigDecimal、LocalDate、OffsetDateTime 类型正确；
- `eventId` 对相同股票和日期稳定。

### 13.2 纯计算单元测试

- D1、D5、D10、D20 的指标可用性；
- MA 和成交量均线与手工结果一致；
- `volumeRatio` 正确，零分母返回 `null`；
- 所有结果为 6 位小数、`HALF_UP`；
- 状态最多保留 20 条；
- 同日覆盖不增加窗口；
- 更早日期不修改窗口。

### 13.3 Flink 算子测试

- 两只股票状态相互隔离；
- 主输出、late side output、DLT side output 路由正确；
- operator UID 稳定；
- 状态快照与恢复后继续得到相同结果。

### 13.4 Kafka 集成测试

- 读取 `StockDailyEvent` 并产生正确 Key/JSON；
- checkpoint 前未提交事务对 `read_committed` 不可见；
- checkpoint 后结果可见；
- 故障恢复后无丢失和恢复重复；
- 原始非法消息与迟到消息分别进入对应 Topic。

### 13.5 端到端验收

1. Collector 向输入 Topic 按日期重放至少 20 个交易日；
2. 等待 Flink 消费完成；
3. 对选定股票逐日比较 Flink 结果与现有 Python `FeatureEngineer`；
4. 所有可用指标按 6 位小数一致；
5. 验证 D1/D5/D10/D20 的 warmup 边界；
6. 重启 JobManager/TaskManager 后验证状态恢复；
7. 验证三个输入分区无积压；
8. 运行现有 Collector、Analysis App 和 Java 全量回归测试。

当前回归基线是 Collector 118 passed，Analysis App 44 passed/6 skipped，Java Maven 测试
通过。实现后的通过数允许因新增测试增加，但现有测试不得减少或失败。

## 14. 上线与回滚

上线顺序：

1. 创建输出、late 和 Flink DLT Topic；
2. 启动 Flink Session Cluster；
3. 提交作业并确认 RUNNING；
4. 执行至少 20 个交易日的历史重放；
5. 用 `read_committed` 消费结果并对账；
6. 保持 Collector 日常 `mysql` 或 `dual` 输出模式运行。

回滚时取消 Flink 作业并停止 JobManager/TaskManager。原始 Collector、MySQL、现有 Java
Consumer 和 Streamlit 均不依赖 V0.5 输出 Topic，因此停止 Flink 不影响 V0.4 主流程。
保留 checkpoint 和三个新 Topic，排查完成后可从 checkpoint 恢复；不得在未确认不再需要
恢复的情况下删除 checkpoint volume。

## 15. 完成标准

V0.5 第一阶段只有在以下条件全部满足后才算完成：

- Docker Compose 中 Kafka、Kafka UI、JobManager、TaskManager 均健康；
- shaded JAR 可重复构建并提交；
- 主输出协议、late 协议和 DLT 协议测试通过；
- 20 日历史预热和指标对账通过；
- exactly-once 重启测试通过；
- 非法与迟到消息不会终止主作业；
- 现有 Python 和 Java 回归测试通过；
- README、运行命令、Topic、故障恢复和回滚文档更新完成。
