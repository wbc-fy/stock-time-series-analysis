# StockAnalysisSystem V0.7

V0.7 新增单股真实 XGBoost 回归预测：显式离线训练及加载模型推理发布、Python 只读 API（8084）、真实预测标签页和独立 Decimal 对账器。GET 不训练、不自动建表；旧 CLI、缓存、原始行情和 Kafka/Flink 链路保持不变。数据截止日不等于今天，未复权收益率和源记录完整性的边界见 [V0.7 运行指南](docs/V0.7_SINGLE_STOCK_PREDICTION.md)。下文 V0.6 的演示占位不再包含预测标签。

V0.6 新增独立 Market API（8083）与 ClickHouse 持久指标。分析概览使用真实股票池、MySQL Kline 和按日期关联的 ClickHouse MA；监控保留原消费 API，其余页面为演示占位。已有 Kafka/Flink 部署只启动新增 ClickHouse，不重建现有服务。安全运行顺序、查询边界、fail-closed 恢复与只读对账见 [V0.6 业务运行指南](docs/V0.6_CLICKHOUSE_MARKET_API.md)。

V0.5 在 V0.4 统一市场数据入口的基础上增加 Flink 实时指标链路。Java 21/Flink 2.2.0 作业从 Kafka 读取日线事件，按股票维护最多 20 个交易日的 keyed state，输出六位小数的 MA5/10/20、成交量均线和量比。V0.4 的 MySQL 采集、Java Consumer、分析和可视化链路保持独立，可在 Flink 停用时继续运行。

```text
Tushare / AkShare
       |
       v
python-collector
  |          |
  v          v
MySQL      Kafka ---> Java Consumer ---> DLT / Monitoring API
  |          |
  |          +----> Flink realtime job
  |                    |---> daily indicator topic
  |                    |---> late-data topic
  |                    +---> Flink dead-letter topic
  v
stock-analysis-app ---> Streamlit / Features / Models / Results
```

## 服务边界

| 模块 | 职责 | 不负责 |
|---|---|---|
| `python-services/python-collector` | 外部数据采集、标准化、校验、MySQL/Kafka 输出、任务重试 | 特征工程和模型训练 |
| `java-services/kafka-consumer-service` | Kafka 消费、协议校验、DLT、统计 API 和监控页面 | 调用市场数据 SDK |
| `java-services/flink-realtime-job` | 消费日线事件、维护 20 日状态、计算实时指标、隔离迟到/非法事件 | 读写 MySQL、训练模型 |
| `python-services/stock-analysis-app` | MySQL/兼容缓存读取、特征工程、训练、预测和可视化 | 直接调用 Tushare/AkShare |
| `infrastructure` | Kafka、Flink Session Cluster、Topic、Kafka UI 和 MySQL 迁移脚本 | 业务分析逻辑 |

## 目录

```text
StockAnalysisSystem/
├── python-services/
│   ├── python-collector/
│   │   ├── app/collectors/       # Tushare/AkShare 采集适配器
│   │   ├── app/market_data/      # 外部 SDK 客户端唯一入口
│   │   ├── app/jobs/             # 日线、daily-basic、成分股任务
│   │   ├── app/outputs/          # MySQL/Kafka/Dual Output
│   │   └── tests/
│   └── stock-analysis-app/
│       ├── data_loader/          # MySQL 数据访问兼容门面
│       ├── data_processor/       # 面板、传统特征和股票池
│       ├── analysis/             # LightGBM、Transformer、预测
│       ├── visualization/        # Streamlit 和绘图
│       └── tests/
├── java-services/
│   ├── common-model/             # StockDailyEvent / StockBasicEvent
│   ├── kafka-consumer-service/   # Consumer、DLT、监控 API
│   └── flink-realtime-job/       # V0.5 实时指标作业
├── infrastructure/
│   ├── docker-compose.yml
│   └── mysql/migrations/
├── scripts/                      # 冒烟和对账脚本
└── docs/                         # 协议、测试、发布与迁移报告
```

## 数据库表

| 表 | 所有者/用途 |
|---|---|
| `stock_basic` | Collector 写入的股票基础资料 |
| `stock_daily` | Collector 写入的原始日线 OHLCV |
| `stock_daily_basic` | Collector 写入的原始换手率、估值和总市值 |
| `stock_constituent` | Collector 写入的指数/行业成分股历史快照 |
| `collection_task` | Collector 采集任务幂等、状态和失败重试记录 |
| `stock_features` | Analysis App 生成的传统特征 |
| `analysis_result` | Analysis App 保存的分析和预测结果 |

`stock_features` 和 `analysis_result` 没有废弃。V0.4 只是把原始 daily-basic 与成分股数据从分析逻辑中拆出来，交由 Collector 统一采集。

## 环境准备

建议环境：Python 3.12、JDK 21、Maven 3.9+、MySQL 8、Docker Desktop。

```powershell
Copy-Item .env.example .env
```

至少填写：

```env
TUSHARE_TOKEN=
DB_HOST=localhost
DB_PORT=3306
DB_NAME=stock_analysis
DB_USER=root
DB_PASSWORD=
COLLECTOR_SOURCE=tushare
COLLECTOR_OUTPUT_MODE=mysql
```

真实 Token 和密码只能保存在本地 `.env`，不得提交到 Git。

## 基础设施

Docker Compose 启动 Kafka、Topic 初始化任务、Kafka UI 和 Flink Session Cluster：

```powershell
docker compose -f infrastructure\docker-compose.yml up -d
docker compose -f infrastructure\docker-compose.yml ps
```

Flink UI 位于 [http://localhost:8082](http://localhost:8082)，Kafka UI 位于 [http://localhost:8081](http://localhost:8081)。

检查 Topic：

```powershell
docker compose -f infrastructure\docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

MySQL 使用本地实例。首次升级 V0.4 时按顺序执行 `infrastructure/mysql/migrations/` 下的迁移脚本；`V0.4_001_market_reference_tables.sql` 会创建 `stock_daily_basic`、`stock_constituent`，并统一与旧行情表兼容的字符排序规则。

## Python Collector

```powershell
cd python-services\python-collector
python -m app.main --help
```

常用命令：

```powershell
# 同一天分别采集 OHLCV 和 daily-basic，两个任务使用独立事务
python -m app.main daily-market --date 20260817

# 只针对 stock_daily 已存在的交易日逐日补采 daily-basic
python -m app.main daily-basic-history --start 20220104 --end 20260817

# 保存指数成分股快照
python -m app.main constituent --type index --code 000300.SH --date 20260817

# 重试当前数据源的失败任务
python -m app.main retry-failed
```

输出模式：

| `COLLECTOR_OUTPUT_MODE` | 行为 |
|---|---|
| `mysql` | 只写 MySQL，默认值和 Kafka 故障回滚模式 |
| `kafka` | 只发送 Kafka；完整分析数据仍依赖已存在的 MySQL 数据 |
| `dual` | 同时写 MySQL 和发送 Kafka |

`daily` 与 `daily-basic` 使用独立任务状态。一个任务失败不会伪装成另一个任务成功；失败记录保留在 `collection_task` 中。

## Java Consumer

```powershell
cd java-services
$env:JAVA_HOME='C:\path\to\jdk-21'
$env:Path="$env:JAVA_HOME\bin;$env:Path"
mvn -pl kafka-consumer-service -am spring-boot:run
```

Java 服务负责：

- 消费 `stock.ods.daily.v1` 和 `stock.ods.basic.v1`。
- 校验 `schemaVersion`、字段类型和事件协议。
- 将无法处理的事件隔离到 `stock.dead-letter.v1`。
- 暴露健康检查、消费统计、最近错误和监控页面。

## Flink 实时指标

实际版本组合固定为 Flink `2.2.0`、Kafka Connector `5.0.0-2.2`、Docker 镜像 `flink:2.2.0-scala_2.12-java21`。不要将 Flink core 单独升级为 2.3；当前没有与之匹配的已发布 Kafka Connector。

### 1. 构建 JAR

```powershell
cd java-services
mvn -q -pl flink-realtime-job -am package
cd ..
```

产物为 `java-services/flink-realtime-job/target/flink-realtime-job-0.3.0-SNAPSHOT-all.jar`。

### 2. 启动基础设施并提交一次作业

```powershell
docker compose -f infrastructure\docker-compose.yml up -d
docker compose -f infrastructure\docker-compose.yml ps
powershell -ExecutionPolicy Bypass -File scripts\submit_v05_flink_job.ps1
```

提交脚本会拒绝重复提交同名的运行中作业。提交后在 Flink UI 检查 `stock-daily-indicator-v1` 为 `RUNNING`、TaskManager 为 1 个且有 3 个 slot。

### 3. 用数据库历史数据预热并对账 20 个交易日

选择数据库中实际存在、按时间连续的至少 20 个交易日；日期边界为包含关系。重放命令只读 MySQL 并写 Kafka，不受 `COLLECTOR_OUTPUT_MODE` 影响，也不会回写数据库。

真实对账时先限定一只股票，避免误把全市场历史数据写入 Kafka。`--ts-code` 会去除首尾空白、转为大写，并接受 `000001`（自动规范为 `000001.SZ`）或完整的 `000001.SZ`；非法或空值会在打开数据库和 Kafka 连接前失败。

```powershell
cd python-services\python-collector
D:\Python\python.exe -m app.main replay-daily-events --start 20260801 --end 20260828 --ts-code 000001.SZ --batch-size 5000
cd ..\..
```

确认需要全市场预热时才省略 `--ts-code`；省略后的行为与旧版本一致。查询始终使用参数化条件，并严格按 `trade_date ASC, ts_code ASC` 发布。完成或失败日志只记录日期、规范化股票代码和确认计数，不输出数据库或 Kafka 凭据。

### 4. 以 `read_committed` 消费结果并运行验收

生产消费者必须设置 `isolation.level=read_committed`，否则可能看到尚未由 checkpoint 提交的事务消息。可用 Kafka CLI 验证主输出：

```powershell
docker compose -f infrastructure\docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic stock.dws.daily-indicator.v1 --from-beginning --consumer-property isolation.level=read_committed
```

在另一个终端运行确定性 smoke test：

```powershell
D:\Python\python.exe scripts\verify_v05_flink.py --scenario valid
D:\Python\python.exe scripts\verify_v05_flink.py --scenario late
D:\Python\python.exe scripts\verify_v05_flink.py --scenario invalid
```

完整 checkpoint/重启验收步骤见 [V0.5 测试用例](docs/V0.5_TEST_CASES.md)。

### 5. 升级、停止和回滚

升级前先从 Flink UI 或 CLI 取得 Job ID。以下命令必须在同一个 PowerShell 会话中分步执行；只有 savepoint 命令成功且返回了可解析的路径，脚本才会继续取消作业：

```powershell
$jobId = '<job-id>'
$savepointOutput = docker compose -f infrastructure\docker-compose.yml exec -T flink-jobmanager flink savepoint $jobId file:///opt/flink/checkpoints/savepoints
if ($LASTEXITCODE -ne 0) { throw 'Savepoint 创建失败；作业保持运行，禁止 cancel。' }

$savepointText = $savepointOutput -join "`n"
$savepointPath = [regex]::Match($savepointText, 'file:/\S+').Value.TrimEnd('.')
if ([string]::IsNullOrWhiteSpace($savepointPath)) { throw '未解析到 savepoint 路径；作业保持运行，禁止 cancel。' }
Write-Host "Savepoint 已确认：$savepointPath"

docker compose -f infrastructure\docker-compose.yml exec -T flink-jobmanager flink cancel $jobId
if ($LASTEXITCODE -ne 0) { throw 'Savepoint 已创建，但作业取消失败；请先检查 Flink UI。' }
```

确认并持久记录 `$savepointPath`。用兼容 JAR 恢复时，明确将该路径传给 `flink run -s`。下面的 `Get-V05Setting` 与正常提交脚本采用相同的“当前进程环境变量优先、空值回退安全默认值”逻辑，并传递全部作业参数；因此自定义 Topic、group、checkpoint 和并行度等配置不会在恢复时丢失：

```powershell
$savepointPath = 'file:/opt/flink/checkpoints/savepoints/savepoint-xxxxxxxxxxxx'

function Get-V05Setting([string]$name, [string]$fallback) {
    $value = [Environment]::GetEnvironmentVariable($name)
    if ([string]::IsNullOrWhiteSpace($value)) { return $fallback }
    return $value
}

$jobArgs = @(
    '--bootstrap-servers', (Get-V05Setting 'FLINK_KAFKA_BOOTSTRAP_SERVERS' 'kafka:29092'),
    '--input-topic', (Get-V05Setting 'FLINK_INPUT_TOPIC' 'stock.ods.daily.v1'),
    '--output-topic', (Get-V05Setting 'FLINK_OUTPUT_TOPIC' 'stock.dws.daily-indicator.v1'),
    '--late-topic', (Get-V05Setting 'FLINK_LATE_TOPIC' 'stock.late.daily.v1'),
    '--dead-letter-topic', (Get-V05Setting 'FLINK_DLT_TOPIC' 'stock.flink.dead-letter.v1'),
    '--group-id', (Get-V05Setting 'FLINK_CONSUMER_GROUP' 'stock-flink-daily-indicator-v1'),
    '--checkpoint-uri', (Get-V05Setting 'FLINK_CHECKPOINT_URI' 'file:///opt/flink/checkpoints'),
    '--checkpoint-interval-ms', (Get-V05Setting 'FLINK_CHECKPOINT_INTERVAL_MS' '10000'),
    '--checkpoint-timeout-ms', (Get-V05Setting 'FLINK_CHECKPOINT_TIMEOUT_MS' '60000'),
    '--checkpoint-min-pause-ms', (Get-V05Setting 'FLINK_CHECKPOINT_MIN_PAUSE_MS' '5000'),
    '--kafka-transaction-timeout-ms', (Get-V05Setting 'FLINK_KAFKA_TRANSACTION_TIMEOUT_MS' '600000'),
    '--parallelism', (Get-V05Setting 'FLINK_PARALLELISM' '3'),
    '--schema-version', (Get-V05Setting 'FLINK_SCHEMA_VERSION' '1'),
    '--deployment-namespace', (Get-V05Setting 'FLINK_DEPLOYMENT_NAMESPACE' 'stock-flink')
)

$jarInContainer = '/opt/flink/usrlib/flink-realtime-job-0.3.0-SNAPSHOT-all.jar'
docker compose -f infrastructure\docker-compose.yml exec -T flink-jobmanager flink run -d -m flink-jobmanager:8081 -s $savepointPath -c com.stock.flink.DailyIndicatorJob $jarInContainer @jobArgs
if ($LASTEXITCODE -ne 0) { throw '从 savepoint 恢复失败；不要删除原 savepoint。' }
```

`FLINK_DEPLOYMENT_NAMESPACE` namespaces Kafka transactional IDs when multiple deployments share
one Kafka cluster. Its default `stock-flink` preserves the existing
`stock-flink-indicator-v1-{main|late|dlt}-` prefixes; assign a unique, stable value per deployment
to avoid producer fencing.

回滚 V0.5 时先按上述步骤取消 Flink 作业，再只停止 Flink 服务：

```powershell
docker compose -f infrastructure\docker-compose.yml stop flink-taskmanager flink-jobmanager
```

将 Collector 设为 `COLLECTOR_OUTPUT_MODE=mysql`（或保留原有 `dual` 策略）；不要停止 Kafka、V0.4 Java Consumer、MySQL 或 Analysis App，不要删除 checkpoint volume、savepoint 或 Topic，也不要执行 `docker compose down -v`。Session Cluster 没有配置 HA：JobManager 冷启动不会自动找回作业，必须用保留的 checkpoint/savepoint 和原配置显式 `flink run -s` 恢复。

## Stock Analysis App

分析端的数据入口已经改为 MySQL：

- 日线查询 LEFT JOIN `stock_daily_basic`，缺少 daily-basic 时仍保留 OHLCV 行。
- 指数和行业筛选读取 `stock_constituent` 的最新或指定日期快照。
- 旧 `DataCollector` 方法签名继续保留，但内部是数据库门面。
- `raw_panel.parquet` 只在数据库换手率为空时提供兼容值，不覆盖数据库非空字段。
- 传统特征、Transformer 特征、模型和 Streamlit 缓存路径继续兼容。

启动 Streamlit 页面：

```powershell
cd python-services\stock-analysis-app
python -m streamlit run visualization\dashboard.py
```

分析应用的模式、缓存和三条模型管线见 [代码阅读指引](python-services/stock-analysis-app/CODE_READING_GUIDE.md)。

## 页面与接口

| 地址 | 用途 |
|---|---|
| [http://localhost:8080/monitor/](http://localhost:8080/monitor/) | Java 消费监控 |
| [http://localhost:8080/actuator/health](http://localhost:8080/actuator/health) | Java 健康检查 |
| [http://localhost:8080/api/consumer/statistics](http://localhost:8080/api/consumer/statistics) | 消费统计 |
| [http://localhost:8080/api/consumer/errors](http://localhost:8080/api/consumer/errors) | 最近错误 |
| [http://localhost:8081/](http://localhost:8081/) | Kafka UI |
| [http://localhost:8082/](http://localhost:8082/) | Flink JobManager UI |
| [http://localhost:8501/](http://localhost:8501/) | Streamlit 分析页面 |

Java 监控数据仅保存在当前进程内存中，服务重启后重新统计；错误列表不保存完整原始 JSON payload。

## 测试与验收

Collector：

```powershell
cd python-services\python-collector
python -m pytest tests -q
```

Analysis App：

```powershell
cd python-services\stock-analysis-app
python -m pytest -q
```

Java：

```powershell
cd java-services
mvn test
```

Flink 模块打包与 V0.5 smoke test：

```powershell
cd java-services
mvn -q -pl flink-realtime-job -am package
cd ..
D:\Python\python.exe scripts\verify_v05_flink.py --scenario valid
```

Kafka 和 Java 启动后，在 `StockAnalysisSystem` 根目录执行冒烟测试：

```powershell
python scripts\verify_v03_smoke.py --scenario valid --count 1
python scripts\verify_v03_smoke.py --scenario all --count 20
```

## 故障与回滚

- Kafka 不可用：设置 `COLLECTOR_OUTPUT_MODE=mysql`，重启 Collector。
- daily-basic 暂不可用：日线任务可独立成功，分析端 LEFT JOIN 后新增字段为 `NULL`。
- 历史 daily-basic 未补齐：分析端仅对空值使用旧 `raw_panel.parquet` 换手率。
- 外部接口失败：任务进入 `FAILED`，使用 `retry-failed` 恢复，不能写入模拟数据冒充成功。
- 代码回滚：保留 `stock_daily_basic` 与 `stock_constituent`，它们不破坏旧分析表。
- Flink 作业故障：V0.4 不依赖 Flink；取消 Flink 作业并保持 Kafka/MySQL/Java Consumer/Analysis App 运行。

## 文档

- [V0.3 消息协议](docs/V0.3_MESSAGE_SCHEMA.md)
- [V0.3 测试用例](docs/V0.3_TEST_CASES.md)
- [V0.3 对账报告](docs/V0.3_RECONCILIATION_REPORT.md)
- [V0.3 发布说明](docs/V0.3_RELEASE_NOTES.md)
- [V0.4 发布说明](docs/V0.4_RELEASE_NOTES.md)
- [V0.4 市场数据迁移验收报告](docs/V0.4_MARKET_DATA_MIGRATION_REPORT.md)
- [V0.5 发布说明](docs/V0.5_RELEASE_NOTES.md)
- [V0.5 测试用例](docs/V0.5_TEST_CASES.md)
- [V0.5 对账报告](docs/V0.5_RECONCILIATION_REPORT.md)
- [Analysis App 代码阅读指引](python-services/stock-analysis-app/CODE_READING_GUIDE.md)

## 团队提交流程

1. Fork 团队仓库到个人命名空间。
2. 从团队 `main` 创建功能分支。
3. 功能分支只推送到个人 Fork。
4. 从个人 Fork 向团队仓库发起 Merge Request。
5. Merge Request 合并或确认不再需要之前，不删除关联远程分支。

## 风险提示

本项目仅用于学习和研究，不构成投资建议。股市有风险，投资需谨慎。
