# StockAnalysisSystem 代码阅读指南

本文按当前代码实际入口整理。系统主要有三条训练/预测管线：单股传统预测、传统 LightGBM 排名、Transformer 排名。

## V0.7 真实单股预测阅读顺序

1. `analysis/forecast/contracts.py`：股票/模型绑定、严格回归 JSON、小数收益率与 horizon=1。
2. `analysis/forecast/continuity.py` 与 `samples.py`：显式可信本地日历、完整源开市日连续性门禁及冻结/当前推理日期证据；历史固定特征、未来标签排除、因果时间切分与最后无标签行。
3. `analysis/forecast/training.py`：有界 CPU XGBoost、冻结测试和零基线、加载模型 latest。
4. `analysis/forecast/artifacts.py` 与 `repository.py`：可信原生 JSON、不可变模型、事务追加发布、有界查询；不触碰旧缓存或自动建表。
5. `scripts/forecast.py`：显式 train/predict 必须传 `--calendar`；`api/forecast.py`：仅只读 GET，不调用训练或抓取日历。旧无连续性证据的发布列表隐藏、直接 503，保留原数据。
6. 系统级 `scripts/verify_v07_prediction.py`：用显式可信本地日历独立核对冻结及当前源连续性、Decimal 标签与完整测试指标，不导入训练/生产连续性/metrics；不能证明历史拟合选择、供应商真实性或 point-in-time 修订。

Windows 单行命令、历史截止日、未复权/公司行动与 daily_basic 修订风险见 [V0.7 指南](../../docs/V0.7_SINGLE_STOCK_PREDICTION.md)。新入口与下文旧 `main.py` demo 分离，旧管线继续保留。

## V0.4 推荐阅读顺序

1. `data_loader/market_data_repository.py`：分析端稳定数据接口。
2. `database/repository.py`：`stock_daily` LEFT JOIN `stock_daily_basic` 和成分股快照 SQL。
3. `data_loader/collector.py`：保留旧调用签名的 MySQL 兼容门面，不再访问 SDK。
4. `data_processor/stock_filter.py`：从 `stock_constituent` 最新快照构建股票池。
5. `data_processor/panel_builder.py`：数据库主数据与 Parquet 兼容缓存的合并规则。

外部 SDK 实现请到相邻服务 `python-services/python-collector/app/market_data` 和 `app/collectors` 阅读。

## 项目结构

```text
StockAnalysisSystem/
├── main.py                        # CLI 入口（argparse 调度器）
├── path_config.py                 # 将项目根目录加入 sys.path
├── requirements.txt               # Python 依赖
│
├── config/
│   ├── settings.py                # 全局配置（数据库、API、模型参数）
│   └── logging_config.py          # 日志配置（控制台 + 文件）
│
├── data_loader/
│   ├── collector.py               # 旧 DataCollector 的 MySQL 兼容门面
│   └── market_data_repository.py  # 分析端市场数据数据库边界
│
├── data_processor/
│   ├── cleaner.py                 # DataCleaner（去重、NaN、异常值）
│   ├── feature_engineer.py        # FeatureEngineer（72 列传统技术指标）
│   ├── stock_filter.py            # resolve_stock_codes（指数/板块 → 股票列表）
│   ├── panel_builder.py           # 面板数据核心：DB加载、增量更新、传统特征缓存
│   └── probe_selection.py         # 探针法特征筛选（LightGBM 三任务竞争）
│
├── database/
│   ├── db_connector.py            # SQLAlchemy engine + session 工厂
│   └── models.py                  # ORM 表定义（4 张表）
│
├── analysis/
│   ├── statistical.py             # 描述统计、趋势、风险指标
│   ├── predictor.py               # StockPredictor（单股 XGBoost / LSTM）
│   ├── backtester.py              # 单股信号和预测回测
│   ├── ranking_predictor.py       # LightGBM 排名模型（传统管线）
│   ├── ranking_backtester.py      # Walk-forward 排名回测
│   ├── transformer_config.py      # TRANSFORMER_CONFIG 配置字典
│   ├── transformer_model.py       # StockTransformer / MultiHeadStockTransformer
│   ├── transformer_utils.py       # Alpha158+39 特征、截面特征、数据集构建
│   └── transformer_trainer.py     # Transformer 训练、预测、特征缓存
│
├── visualization/
│   ├── plotter.py                 # StockPlotter（matplotlib / plotly 图表）
│   └── dashboard.py               # Streamlit 仪表盘
│
├── models/                        # 模型和缓存（不入库的中间产物）
│   ├── traditional_features/      # 传统特征 Parquet 缓存
│   │   └── <N>_<hash>/            #   按股票池数量+hash隔离
│   └── transformer/               # Transformer 输出
│       └── 60_158+39/             #   按序列长度+特征集
│           └── index_000300/      #   按股票池隔离
│
├── output/                        # 预测输出（result.csv）
└── logs/                          # 运行日志
```

## 入口总览

主入口是 [main.py](main.py)。

常用 `--mode`：

| mode | 管线 | 当前用途 |
|---|---|---|
| `demo` | 单股传统预测 | 单股采集、清洗、特征、训练预测、保存、回测 |
| `rank` | 传统排名 | LightGBM 排名训练并输出 Top N |
| `rank_backtest` | 传统排名 | Walk-forward 排名回测 |
| `batch_predict_rank` | 传统批量预测 | 从 `stock_features` 训练传统模型并批量排名 |
| `transform_features` | Transformer | 只计算 Transformer 特征 |
| `transform_train` | Transformer | 训练 Transformer |
| `transform_predict` | Transformer | Transformer 预测 Top N |
| `save_features` | 传统特征 | 计算传统特征并写入 `stock_features` |
| `collect` | 数据采集 | 增量采集行情 |
| `full_update` | 全流程更新 | 采集 + 传统特征 + Transformer 特征 |

注意：`process`、`analyze`、`predict` 在 parser choices 中存在，但当前没有对应 `elif` 分支；单股预测使用 `demo`。

## 数据库表结构

核心表定义在 `database/models.py`：

| 表名 | 主键/索引 | 主要列 | 被哪些管线使用 |
|---|---|---|---|
| `stock_basic` | ts_code (UNIQUE) | symbol, name, area, industry, list_date | 全部（读取股票列表） |
| `stock_daily` | ts_code+trade_date (UNIQUE) | OHLCV, pre_close, pct_chg, vol, amount | 全部（行情数据源） |
| `stock_daily_basic` | ts_code+trade_date (UNIQUE) | turnover_rate, pe, pe_ttm, pb, ps, total_mv | 与行情 LEFT JOIN |
| `stock_constituent` | group+code+date (UNIQUE) | index/industry 快照、weight | 股票池筛选 |
| `stock_features` | ts_code+trade_date (UNIQUE) | 72 个技术指标列 | 管线1(写入) + 管线2(读+写) |
| `analysis_result` | ts_code+analysis_date (UNIQUE) | analysis_type, result, prediction, confidence | 管线1(写入) |

## 管线 1：单股传统预测

入口：

```powershell
python main.py --mode demo --stock 000001 --use_tushare --model xgboost
```

调用链：

```text
main.py demo
  -> DataCollector(use_tushare=...)
      -> MarketDataRepository          # 从 MySQL 读取单股数据
  -> run_data_processing()
      -> DataCleaner                   # 去重、填充、异常值处理
      -> FeatureEngineer.calculate_all_features()   # 计算 72 列传统指标
  -> run_prediction()
      -> StockPredictor.prepare_features()
      -> train_xgboost / train_xgboost_regression / train_xgboost_multiclass / train_lstm
      -> predictor.save_model()
  -> save_data_to_db()                 # 写入 MySQL
      -> stock_daily                   # 原始行情
      -> stock_features                # 72 列传统特征
      -> analysis_result               # 预测结果
  -> run_backtest()                    # 回测
```

关键文件：

| 文件 | 作用 |
|---|---|
| `data_loader/collector.py` | 旧接口兼容门面，内部转 MySQL |
| `data_loader/market_data_repository.py` | OHLCV、daily-basic 和成分股读取接口 |
| `data_processor/cleaner.py` | 单股清洗 |
| `data_processor/feature_engineer.py` | 技术指标与未来标签 |
| `analysis/predictor.py` | 单股 XGBoost/LSTM 模型 |
| `analysis/backtester.py` | 单股信号和预测回测 |

**数据流向**：从 MySQL 读取 Collector 已落库数据 → 处理 → 预测 → 写入特征和分析结果。

## 管线 2：传统 LightGBM 排名

入口：

```powershell
python main.py --mode rank --use_tushare --skip_update --index 000300 --top_n 10
```

调用链：

```text
main.py rank
  -> run_ranking_pipeline()
      -> load_all_stock_data_from_db() 或 incremental_update()
          │  ← panel_builder.py：从 stock_daily 读取面板数据
          │  ← stock_filter.py：将 --index/--sectors 解析为股票代码列表
      -> prepare_panel_for_training()
          │  ← FeatureEngineer.calculate_all_features()  (与管线1共用)
          │  ← add_label()：future_return_5d
          │  ← 传统特征 parquet 缓存（按股票池 hash 隔离）
          │  ← 可选：save_features_batch_to_db() 写入 stock_features 表
      -> train_ranking_model()
          │  ← probe_feature_selection()：探针法筛选（3 任务 LightGBM 竞争）
          │  ← LightGBM regression
      -> predict_top_n()
```

排名回测：

```powershell
python main.py --mode rank_backtest --use_tushare --skip_update --index 000300 --top_n 10 --train_window 365 --rebalance_days 5
```

批量预测排名：

```powershell
python main.py --mode batch_predict_rank --use_tushare --index 000300 --model xgboost --top_n 10
```

`batch_predict_rank` 的特殊之处：直接从 `stock_features` 表读取已计算好的 72 列特征（如果有），省去重新计算。

关键文件：

| 文件 | 作用 |
|---|---|
| `data_processor/panel_builder.py` | 全市场/股票池面板、传统特征缓存、特征入库 |
| `data_processor/stock_filter.py` | 指数/板块股票池解析 |
| `data_processor/probe_selection.py` | 探针法筛选 |
| `analysis/ranking_predictor.py` | LightGBM 排名模型 |
| `analysis/ranking_backtester.py` | Walk-forward 排名回测 |

**数据流向**：从 DB 读取 `stock_daily` → 内存计算特征 → Parquet 缓存 → LightGBM 训练 → 排名输出。

当前重要实现细节：

- `load_all_stock_data_from_db()` 会把 `--index/--sectors` 转成股票池，并在 SQL 中使用 `ts_code IN (...)` 过滤。
- `incremental_update()` 在无新数据或 DB 兜底时会保留股票池过滤，不会退回全市场。
- 传统特征 parquet 缓存按股票池 hash 隔离，避免全市场缓存污染指数缓存。
- `batch_predict_rank` 会对 `stock_features` 也应用股票池过滤。

## 管线 3：Transformer 排名

推荐分两步运行。

### 第一步：计算特征

```powershell
python main.py --mode transform_features --use_tushare --skip_update --index 000300 --transformer_features 158+39
```

调用链：

```text
main.py transform_features
  -> load_all_stock_data_from_db()
      │  ← panel_builder.py（与管线2共用数据加载入口）
  -> build_transformer_config()        # 构建输出目录（按股票池隔离）
  -> compute_and_save_features()
      │  ← transformer_trainer.py
      -> _normalize_input_df()         # 列名统一（English → 中文）
      -> 按股票分组 + 并行计算
          -> engineer_features_158plus39()  # Alpha158 + 39 技术特征
      -> add_cross_sectional_features()     # 8 个截面排名特征
      -> _build_label_and_clean()           # label = (open_t5 - open_t1) / open_t1
      -> StandardScaler.fit_transform()     # 标准化（结果直接存入 parquet）
      -> 保存到 models/transformer/60_158+39/[scope/]
          ├── features_158+39.parquet           # 已标准化的特征
          ├── features_158+39_scaler.pkl        # StandardScaler 对象
          ├── features_158+39_meta.json         # val_start_date + feature_cols
          ├── features_158+39_stockid2idx.json  # 股票代码 → 整数索引
          └── raw_panel.parquet                 # 原始面板缓存（供增量使用）
```

输出目录示例：

```text
models\transformer\60_158+39\index_000300
```

### 第二步：训练

```powershell
python main.py --mode transform_train --feature_path "...\features_158+39.parquet" --transformer_model multi_head --transformer_features 158+39 --transformer_epochs 5
```

调用链：

```text
main.py transform_train
  -> build_transformer_config()
  -> run_transformer_training(feature_path=...)
      -> load_precomputed_features()    # 加载已标准化的 parquet + 元信息
      -> is_val 标记处理               # 从 parquet 列或 val_start_date 重建
      -> probe_feature_selection() 或加载 probe_selection.json
      -> create_ranking_dataset_vectorized()   # 60天滑动窗口 → 序列样本
      -> MultiHeadStockTransformer 训练
          └── MultiTaskRankingLoss（排名 + 回归 + 分类 + 方向 4 个损失头）
      -> 保存 best_model.pth / training_history.json / plots/
```

当前重要实现细节：

- `feature_path` 训练也会运行或复用探针法筛选。
- `probe_selection.json` 同时用于训练和预测，保证特征维度一致。
- Parquet 数据已经过 StandardScaler 标准化，训练时直接使用，不会重复标准化。
- `is_val` 列在 parquet 中可能存储为字符串/整数，加载时会安全转换为 bool。
- 验证集样本构造使用 train+val 提供历史窗口，但只评估验证起始日期之后的样本。
- 输出目录按股票池隔离，例如 `index_000300`、`index_000300_000905`。

### 第三步：预测

```powershell
python main.py --mode transform_predict --feature_path "...\features_158+39.parquet" --transformer_model multi_head --transformer_features 158+39 --top_n 5
```

调用链：

```text
main.py transform_predict
  -> predict_top_stocks_transformer(feature_path=...)
      -> load_precomputed_features()       # 加载全量数据（不在此处过滤日期）
      -> 加载 probe_selection.json         # 特征筛选
      -> 加载 best_model.pth              # 模型
      -> 构建 60 天滑动窗口序列
      -> MultiHeadStockTransformer 推理
          └── 4 个输出头 → 不确定性加权 → 最终排名分数
      -> 输出 Top N → output/result.csv
```

预测输出：

```text
output\result.csv   # stock_id + weight 格式
```

## 三条管线的联系与隔离

### 共享的入口：panel_builder.py

`panel_builder.py` 是管线 2 和管线 3 的 **共享数据网关**：

```text
panel_builder.py
├── load_all_stock_data_from_db()    ← 管线2 和 管线3 都从这里读取 DB 面板数据
├── incremental_update()             ← 管线2 和 管线3 都通过这里增量更新
├── prepare_panel_for_training()     ← 仅管线2 使用（计算传统特征 + 缓存 + 入库）
├── load_stock_features_from_db()    ← 仅管线2 的 batch_predict_rank 使用
└── save_features_batch_to_db()      ← 仅管线2 使用（特征写入 stock_features 表）
```

管线 3 从 `panel_builder` 获取原始面板后，进入 `transformer_trainer.py` 的独立特征计算流程，**不再回到** `prepare_panel_for_training()`。

### 共享的模块

| 模块 | 管线2 | 管线3 | 说明 |
|---|---|---|---|
| `panel_builder.py` | 使用 | 使用 | 数据加载和增量更新 |
| `stock_filter.py` | 使用 | 使用 | 指数/板块成分股解析 |
| `probe_selection.py` | 使用 | 使用 | 探针法特征筛选（各自保存独立的 JSON） |
| `feature_engineer.py` | 使用 | **不使用** | 72 列传统指标，管线3 有独立实现 |
| `transformer_utils.py` | **不使用** | 使用 | Alpha158+39 特征，管线2 不使用 |

### 独立的数据存储

```text
                    ┌───────────────────────────┐
                    │      MySQL 数据库          │
                    │                           │
                    │  stock_basic    ← 全部读取 │
                    │  stock_daily    ← 全部读取 │
                    │  stock_features ← 仅管线2  │
                    │  analysis_result← 仅管线1  │
                    └───────────────────────────┘

管线2 独立缓存:
  models/traditional_features/<N>_<hash>/
    raw_panel.parquet           # 含换手率的原始面板
    panel_features.parquet      # 72 列传统特征

管线3 独立缓存:
  models/transformer/60_158+39/[scope]/
    features_158+39.parquet     # ~205 列 Alpha 特征（已标准化）
    features_158+39_scaler.pkl
    raw_panel.parquet           # 原始面板（中文列名）
    best_model.pth
    probe_selection.json
```

### 特征体系对比

| 特征类别 | 管线2 (FeatureEngineer) | 管线3 (transformer_utils) |
|---|---|---|
| 均线 | MA5, MA10, MA20, MA60 | MA5, MA10, MA20, MA30, MA60 |
| 波动率 | Variance20/60/120 | STD5, STD10, STD20, STD30, STD60 |
| 动量 | BIAS5/10/20/60 | ROC5, MOM5, CNTP5 |
| RSI | RSI(14) | rsi(5), rsi(10), rsi(20), rsi(30), rsi(60) |
| MACD | MACD_dif, MACD_dea, MACD_macd | 不直接计算 |
| 布林带 | BB_upper, BB_middle, BB_lower | 不直接计算 |
| KDJ | K, D, J | 不直接计算 |
| 截面特征 | 无 | cs_return_pct, cs_volume_ratio 等 8 个 |
| Alpha因子 | 无 | KMID, KLEN, KLOW, RSQR5 等 ~100 个 |
| 换手率 | Turnover_rate_5/60/120 | 不直接计算 |
| 标签 | future_return_5d | (open_t5 - open_t1) / open_t1 |

**结论**：两套特征体系虽然概念上有重叠（都计算了均线、波动率、RSI 等），但实现代码、列名、计算窗口和最终数值完全不同，**互不复用**。

## 建议阅读顺序

1. `main.py`：看 CLI mode 如何分发到各管线。
2. `config/settings.py`：了解全局配置（数据库、API、模型参数）。
3. `database/models.py`：了解 4 张表的 ORM 定义。
4. `data_processor/panel_builder.py`：看数据库加载、股票池过滤、传统特征缓存——这是管线 2 和 3 的共享入口。
5. `data_processor/stock_filter.py`：看指数/板块成分股解析。
6. `data_processor/feature_engineer.py`：看 72 列传统指标的计算逻辑（管线 1 和 2 共用）。
7. `data_processor/probe_selection.py`：看探针法筛选（管线 2 和 3 共用算法，各自保存结果）。
8. `analysis/ranking_predictor.py`：看传统 LightGBM 排名模型。
9. `analysis/transformer_utils.py`：看 Alpha158+39 特征和样本构造（管线 3 独有）。
10. `analysis/transformer_trainer.py`：看 Transformer 特征缓存、训练、预测。
11. `analysis/transformer_model.py`：看 MultiHeadStockTransformer 模型结构。
12. `visualization/plotter.py`：看训练历史和结果图表。

## 当前已知注意点

- 单股独立 `predict` mode 未实现，请使用 `demo`。
- Transformer 全市场 `158+39` 训练内存压力大，建议先用指数股票池。
- 如果某个指数目录曾被全市场缓存污染，删除对应目录后重算。
- 训练图为横线通常表示验证集样本为 0 或历史指标全为 0；当前验证构造逻辑已修复，需重新训练生成新图。
- Transformer Parquet 中的数据已经过 StandardScaler 标准化，训练和预测时不会重复标准化。
- `is_val` 列在 Parquet 中可能存储为字符串，加载时已做安全类型转换。
- Windows 上 `multiprocessing` 无法序列化嵌套的局部函数，相关 worker 函数已提取到模块顶层。
- 股票代码支持 6 位数字输入，`to_tushare_code()` 会自动添加市场后缀（`.SZ` / `.SH`）。
