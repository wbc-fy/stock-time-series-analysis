# StockAnalysisSystem

基于 Python、Kafka、Java、Flink、MySQL、ClickHouse 的股票数据采集与分析系统。V0.6 在 V0.5 的 20 日状态与事务 Kafka 指标链路后新增 ClickHouse 持久化、Market API 和真实 Vue 分析概览。Python 仍统一负责外部市场数据采集，离线分析应用仍只从 MySQL 与兼容缓存读取数据。

> 本项目用于学习、工程实践和量化研究，不构成投资建议。

## V0.7 单股预测

V0.7 新增显式离线 XGBoost 训练/加载推理、Python 只读 API（8084）及真实预测标签页。真实模式开放概览与预测；API 使用小数收益率、页面转换百分比，GET 不训练。冻结测试与最新无标签预测分开显示并比较零收益基线。数据截止日不是今天；未复权收盘价、历史缺口与估值修订的边界见 [V0.7 业务运行指南](StockAnalysisSystem/docs/V0.7_SINGLE_STOCK_PREDICTION.md)。旧管线、缓存和 Kafka/Flink 数据保持不变。下文 V0.6 架构中的演示占位不再包含预测标签。

## V0.6 当前架构

新增 ClickHouse 持久指标与独立 Market API（8083），Vue 分析概览读取真实 MySQL Kline 和 ClickHouse MA，监控页保留原 Consumer API。其余页面仍为演示占位。已有 Kafka/Flink 集群升级时只添加 ClickHouse，不重建集群或删除 checkpoint；启动、查询边界及只读历史对账见 [V0.6 业务运行指南](StockAnalysisSystem/docs/V0.6_CLICKHOUSE_MARKET_API.md)。

```text
Tushare / AkShare
       |
       v
Python Collector ----> MySQL ----> Stock Analysis App / Streamlit
       |
       v
     Kafka ----> Java Consumer ----> DLT / Monitoring API
       |
       +------> Flink ----> daily-indicator ----> Market API Consumer ----> ClickHouse
                          late / Flink DLT                  |                  |
MySQL ---------------------------------------------------> Market API <-------+
                                                               |
                                                               v
                                                         Vue 分析概览
```

- `python-collector` 是唯一允许调用 Tushare/AkShare SDK 的服务。
- MySQL 保存原始行情、每日估值、成分股、特征及分析结果。
- Kafka 消息链路继承 V0.3 的标准事件、协议校验和失败隔离能力。
- `stock-analysis-app` 负责特征工程、模型训练、预测和可视化，不再直接访问外部行情 API。
- Kafka 不可用时可以切回 `COLLECTOR_OUTPUT_MODE=mysql`，继续使用原有数据库路径。
- Flink 使用 Java 21 / Flink 2.2.0，`read_committed` 下游只读取 checkpoint 提交后的事务结果；不写 MySQL 特征表。

## 核心能力

- Tushare、AkShare 统一采集入口和统一股票代码格式。
- 日线 OHLCV、股票基础资料、每日换手率/估值、指数/行业成分股采集。
- MySQL、Kafka 或双写输出模式，以及采集任务幂等、失败记录和重试。
- Java Kafka Consumer 协议校验、死信隔离、统计 API 和监控页面。
- 传统特征、LightGBM 排名、Transformer 特征与模型训练。
- Streamlit 分析页面及 Parquet、模型文件等旧缓存兼容。

## 项目目录

```text
StockAnalysisSystem/
├── python-services/
│   ├── python-collector/        # 外部数据采集、校验、MySQL/Kafka 输出
│   └── stock-analysis-app/     # 数据分析、特征、训练、预测和可视化
├── java-services/
│   ├── common-model/           # Python/Java 公共消息契约
│   ├── kafka-consumer-service/ # Kafka 消费、校验、DLT、API 和监控页
│   └── flink-realtime-job/     # 实时日线指标、托管状态、exactly-once Kafka 输出
├── infrastructure/             # Kafka、Topic 初始化、Kafka UI、MySQL 迁移
├── scripts/                    # 冒烟测试和对账脚本
└── docs/                       # 消息协议、测试、迁移验收和发布说明
```

详细目录说明见 [StockAnalysisSystem 运行指南](StockAnalysisSystem/README.md)。

## 核心数据表

| 表 | 作用 |
|---|---|
| `stock_basic` | 股票基础资料 |
| `stock_daily` | 原始日线 OHLCV |
| `stock_daily_basic` | 原始换手率、估值和总市值 |
| `stock_constituent` | 指数及行业成分股快照 |
| `stock_features` | 分析端生成的传统特征 |
| `analysis_result` | 分析和预测结果 |
| `collection_task` | 采集幂等键、运行状态和失败重试记录 |

`stock_features` 与 `analysis_result` 仍由分析应用使用；V0.4 新增的是原始数据表 `stock_daily_basic` 和 `stock_constituent`。

## 快速启动

环境要求：Python 3.12、JDK 21、Maven、MySQL 8 和 Docker Desktop。

先进入系统目录并创建本地环境文件：

```powershell
cd StockAnalysisSystem
Copy-Item .env.example .env
```

在 `.env` 中填写自己的 `TUSHARE_TOKEN` 和数据库连接信息。不要提交真实 `.env`。

启动 Kafka、Topic 初始化任务和 Kafka UI：

```powershell
docker compose -f infrastructure\docker-compose.yml up -d
```

运行单日行情与每日指标采集：

```powershell
cd python-services\python-collector
python -m app.main daily-market --date 20260817
```

启动 Java Consumer：

```powershell
cd ..\..\java-services
mvn -pl kafka-consumer-service -am spring-boot:run
```

更多历史补采、成分股、分析应用和数据库迁移步骤见 [完整运行指南](StockAnalysisSystem/README.md)。

### V0.5 实时指标启动

从 `StockAnalysisSystem` 目录启动 Flink 集群，设置 `FLINK_JAVA_HOME` 指向 JDK 21，再运行提交脚本（构建 shaded JAR、等待集群就绪、避免重复提交）：

```powershell
docker compose -f infrastructure\docker-compose.yml up -d flink-jobmanager flink-taskmanager
.\scripts\submit_v05_flink_job.ps1
```

Flink UI：<http://localhost:8082>。从 Collector 目录用 `replay-daily-events --start 20260429 --end 20260529 --ts-code 000001.SZ` 重放 MySQL 中已有数据；先确认该范围实际包含至少 20 个交易日。新部署使用唯一且稳定的 `FLINK_DEPLOYMENT_NAMESPACE`，避免 Kafka 事务 producer fencing。

完整配置、savepoint/retained checkpoint 恢复和停止步骤见 [V0.5 运行指南](StockAnalysisSystem/README.md#flink-实时指标)，验收范围及真实结果见 [对账报告](StockAnalysisSystem/docs/V0.5_RECONCILIATION_REPORT.md)。回滚只取消 Flink 作业并停止 `flink-jobmanager` / `flink-taskmanager`，保留 checkpoint volume、Topic 和 V0.4 服务；不要执行 `down -v`。

## 页面与接口

| 地址 | 用途 |
|---|---|
| [http://localhost:8080/monitor/](http://localhost:8080/monitor/) | Java 消费监控页 |
| [http://localhost:8080/actuator/health](http://localhost:8080/actuator/health) | Java 健康检查 |
| [http://localhost:8080/api/consumer/statistics](http://localhost:8080/api/consumer/statistics) | 消费统计 API |
| [http://localhost:8080/api/consumer/errors](http://localhost:8080/api/consumer/errors) | 最近错误 API |
| [http://localhost:8081/](http://localhost:8081/) | Kafka UI |
| [http://localhost:8501/](http://localhost:8501/) | Streamlit 分析页面（启动后） |

## 测试

```powershell
cd StockAnalysisSystem\python-services\python-collector
python -m pytest tests -q
```

```powershell
cd StockAnalysisSystem\python-services\stock-analysis-app
python -m pytest -q
```

```powershell
cd StockAnalysisSystem\java-services
mvn test
```

## 文档导航

- [V0.8 固定协议历史滚动评估指南](StockAnalysisSystem/docs/V0.8_EVALUATION_GUIDE.md)：2026-10-03 单次真实发布 `eval_e854bcb0ae3348a9b1f244ca57f81be7`，000001.SZ 源1150日（2022-01-04 至2026-09-30）、三窗共300点；独立核对 PASS，旧数据/模型 hash 不变。指南含完整日期、真实浏览器验证范围与单行复现命令。历史已观察、未复权且非前瞻验证；验证范围与限制见指南。

- [V0.3 消息协议](StockAnalysisSystem/docs/V0.3_MESSAGE_SCHEMA.md)
- [V0.3 测试用例](StockAnalysisSystem/docs/V0.3_TEST_CASES.md)
- [V0.3 对账报告](StockAnalysisSystem/docs/V0.3_RECONCILIATION_REPORT.md)
- [V0.3 发布说明](StockAnalysisSystem/docs/V0.3_RELEASE_NOTES.md)
- [V0.4 发布说明](StockAnalysisSystem/docs/V0.4_RELEASE_NOTES.md)
- [V0.4 市场数据迁移验收报告](StockAnalysisSystem/docs/V0.4_MARKET_DATA_MIGRATION_REPORT.md)
- [分析端代码阅读指引](StockAnalysisSystem/python-services/stock-analysis-app/CODE_READING_GUIDE.md)

## V0.8 真实评估指标

本次300行总体指标（RMSE/MAE 为小数收益，R² 无量纲）：

| 方法 | RMSE | MAE | R² |
|---|---:|---:|---:|
| zero_return | 0.010008709928323941 | 0.007574383071790578 | -0.0008824717445446848 |
| ridge | 0.01006797405133397 | 0.007667655823582983 | -0.012770524486203041 |
| xgboost | 0.009995158444063297 | 0.007645006465381049 | 0.001826021341592377 |

XGBoost RMSE 略低于零基线、MAE 更高；Ridge 两者均更高，不证明盈利、前瞻有效性或普遍战胜基线。

## 贡献流程

团队代码必须通过个人 Fork 提交：

1. 将团队仓库 Fork 到个人命名空间。
2. 从团队 `main` 创建功能分支。
3. 代码只推送到个人 Fork。
4. 从个人 Fork 向团队仓库创建 Merge Request。
5. Merge Request 合并或确认不再需要前，不删除关联远程分支。

## 风险提示

外部数据接口可能因权限、频率限制或上游网络临时不可用。采集失败会记录在 `collection_task`，应通过重试任务恢复，不能用模拟数据冒充真实采集结果。
