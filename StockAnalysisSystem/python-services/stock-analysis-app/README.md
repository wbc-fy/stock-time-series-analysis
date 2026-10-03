# StockAnalysisSystem

## V0.7 独立单股回归入口

新入口为 `python -m scripts.forecast train` 和 `python -m scripts.forecast predict`（加载既有模型，不重训）；两者均须显式股票代码、历史范围和可信本地 `--calendar` JSON，在特征计算前验证全部源开市日连续性，成功后追加发布结果。冻结源证据不可修改，推理日期必须保留原始前缀；旧无证据发布隐藏、直接查询 503，原数据保留。只读服务为 `python -m uvicorn api.forecast:app --host 127.0.0.1 --port 8084`，GET 不训练、不发布、不抓取日历、不自动 DDL。固定特征、因果切分、同一冻结模型的完整测试与最新无标签预测、零收益基线及安全账号示例见 [V0.7 业务指南](../../docs/V0.7_SINGLE_STOCK_PREDICTION.md)。

API 为小数收益率，不是概率；latest 是数据库历史截止日，不是系统日期。原始未复权价和源记录缺口可能破坏经济收益/真实下一交易日解释，daily_basic 修订不保证 point-in-time。可信原生 JSON 模型存储在忽略的 `models/v07/`。以下旧三条管线及 `main.py` 行为、登记与缓存保持兼容，不能用旧 demo 命令替代 V0.7 验收。

> V0.4 数据边界：本应用不再导入或调用 Tushare/AkShare。所有行情、估值和成分股由 `python-collector` 采集，本应用只读取 MySQL 与既有缓存。`--use_tushare` 参数仅为旧脚本兼容。

A 股数据采集、特征工程、传统机器学习、LightGBM 排名选股、Transformer 排名选股和回测系统。

## 系统架构概览

系统包含 **三条独立管线**，共享底层数据采集和数据库存储，但拥有各自独立的特征工程、模型训练和输出。

```text
                          ┌─────────────────────────────┐
                          │ python-collector + MySQL     │
                          └──────────┬──────────────────┘
                                     │
                                     ▼
                          ┌─────────────────────────────┐
                          │    MySQL 数据库               │
                          │  stock_basic (股票基本信息)    │
                          │  stock_daily (日线行情)        │
                          │  stock_daily_basic (估值/换手) │
                          │  stock_constituent (成分股快照)│
                          │  stock_features (传统特征72列) │
                          │  analysis_result (分析结果)    │
                          └──────┬──────────────┬────────┘
                                 │              │
              ┌──────────────────┘              └──────────────────┐
              ▼                                                    ▼
┌────────────────────────────┐                   ┌─────────────────────────────┐
│  panel_builder.py          │                   │  panel_builder.py            │
│  load_all_stock_data_from_db│                  │  load_all_stock_data_from_db │
│  / incremental_update      │                   │  / incremental_update        │
└──────┬──────────┬──────────┘                   └──────┬──────────────────────┘
       │          │                                     │
       ▼          ▼                                     ▼
┌──────────────┐  ┌──────────────────────┐   ┌──────────────────────────────────┐
│ 管线1:       │  │ 管线2:               │   │ 管线3:                           │
│ 单股传统     │  │ 传统 LightGBM 排名   │   │ Transformer 排名                 │
│ XGBoost/LSTM │  │                      │   │                                  │
│              │  │ FeatureEngineer(72列) │   │ Alpha158+39+截面8列             │
│              │  │ LightGBM regression   │   │ MultiHeadStockTransformer        │
│              │  │ 探针法特征筛选         │   │ 探针法特征筛选                    │
└──────────────┘  └──────────────────────┘   └──────────────────────────────────┘
```

### 三条管线的核心区别

| 对比项 | 管线1: 单股传统 | 管线2: 传统 LightGBM 排名 | 管线3: Transformer 排名 |
|---|---|---|---|
| **预测目标** | 单只股票涨跌/收益率 | 全市场股票排名 | 全市场股票排名 |
| **特征体系** | FeatureEngineer 72 列 | FeatureEngineer 72 列 | Alpha158 + 39技术 + 8截面 = ~205 列 |
| **特征计算** | `feature_engineer.py` | `feature_engineer.py` | `transformer_utils.py` (独立实现) |
| **模型** | XGBoost / LSTM | LightGBM regression | MultiHeadStockTransformer |
| **标签定义** | future_return_1d / future_direction_1d | future_return_5d | (open_t5 - open_t1) / open_t1 |
| **输入格式** | 单股 DataFrame | Panel DataFrame | 60天滑动窗口序列 |
| **特征存储** | MySQL `stock_features` 表 | Parquet 缓存 + 可选入库 | Parquet 缓存 (不入 MySQL) |
| **标准化** | 无 | 无 | StandardScaler |

### 共享数据

| 共享资源 | 管线1 | 管线2 | 管线3 |
|---|---|---|---|
| `stock_basic` (DB) | 读取 | 读取 | 读取 |
| `stock_daily` (DB) | 读+写 | 读+写 | 读取 |
| `stock_features` (DB) | 写入 | 读+写 | **不使用** |
| `analysis_result` (DB) | 写入 | 不使用 | 不使用 |
| `panel_builder.py` | 不使用 | 使用 | 使用 |
| `stock_filter.py` | 不使用 | 使用 | 使用 |
| `probe_selection.py` | 不使用 | 使用 | 使用 |
| 传统特征 Parquet | 不使用 | 使用 | 不使用 |
| Transformer 特征 Parquet | 不使用 | 不使用 | 使用 |

**重要**：传统管线的 `stock_features` 表（72 列）和 Transformer 管线的特征（205 列）是 **完全独立计算、互不复用** 的。虽然两者都计算了 RSI、MACD、布林带等类似指标，但实现代码和列名不同。

## 当前可用管线

### 1. 单股传统预测管线

用途：对单只股票做采集、清洗、技术指标、XGBoost/LSTM 训练预测、保存和回测。

当前实际入口是 `demo` 模式：

```powershell
python main.py --mode demo --stock 000001 --use_tushare --model xgboost
python main.py --mode demo --stock 600519 --use_tushare --model xgboost_regression
python main.py --mode demo --stock 000001 --use_tushare --model xgboost_multiclass
```

说明：

- `--stock` 是 6 位股票代码，系统会自动转换为 Tushare 格式（如 `000001.SZ`、`600519.SH`）。
- `--model` 支持 `xgboost`、`xgboost_regression`、`xgboost_multiclass`、`lstm`。
- `process`、`analyze`、`predict` 虽然在参数枚举里存在，但当前 `main.py` 没有对应分支；单股预测请使用 `demo`。

### 2. 传统全市场/股票池排名管线

用途：用传统技术指标 + 探针法筛选 + LightGBM，对股票池内股票预测未来收益并排名。

沪深300训练并输出 Top N：

```powershell
python main.py --mode rank --use_tushare --skip_update --index 000300 --top_n 10
```

中证500：

```powershell
python main.py --mode rank --use_tushare --skip_update --index 000905 --top_n 10
```

沪深300 + 中证500：

```powershell
python main.py --mode rank --use_tushare --skip_update --index 000300,000905 --top_n 10
```

排名回测：

```powershell
python main.py --mode rank_backtest --use_tushare --skip_update --index 000300 --top_n 10 --train_window 365 --rebalance_days 5
```

传统批量预测排名：

```powershell
python main.py --mode batch_predict_rank --use_tushare --index 000300 --model xgboost --top_n 10
```

说明：

- `--index` 支持指数成分股筛选，例如 `000300` 沪深300、`000905` 中证500。
- `--sectors` 支持行业板块筛选，例如 `--sectors 银行,医药`。
- 指定 `--use_tushare` 时，指数成分股优先使用 Tushare；获取失败不会再静默退回全市场。
- 传统特征 parquet 缓存按股票池隔离，避免全市场缓存污染沪深300/中证500缓存。

### 3. Transformer 排名管线

推荐两阶段运行：先计算特征，再训练模型。

#### 3.1 计算沪深300 Transformer 特征

```powershell
python main.py --mode transform_features --use_tushare --skip_update --index 000300 --transformer_features 158+39
```

输出目录：

```text
models\transformer\60_158+39\index_000300\
```

关键文件：

```text
features_158+39.parquet           # 标准化后的特征（已包含 is_val 标记）
features_158+39_scaler.pkl        # StandardScaler
features_158+39_meta.json         # 元信息（val_start_date, feature_cols）
features_158+39_stockid2idx.json  # 股票ID到索引的映射
raw_panel.parquet                 # 原始面板缓存（供增量计算使用）
```

#### 3.2 训练沪深300 Transformer

试跑 5 个 epoch：

```powershell
python main.py --mode transform_train --feature_path "F:\实习项目\StockAnalysisSystem\models\transformer\60_158+39\index_000300\features_158+39.parquet" --transformer_model multi_head --transformer_features 158+39 --transformer_epochs 5
```

正式训练使用默认 epoch：

```powershell
python main.py --mode transform_train --feature_path "F:\实习项目\StockAnalysisSystem\models\transformer\60_158+39\index_000300\features_158+39.parquet" --transformer_model multi_head --transformer_features 158+39
```

输出文件：

```text
best_model.pth
probe_selection.json
training_history.json
plots\training_history.png
```

说明：

- `--transformer_epochs 5` 表示训练 5 轮；不传时使用配置默认值。
- 使用 `--feature_path` 训练时也会运行或复用探针法筛选结果。
- `probe_selection.json` 会在训练和预测时保持一致，避免特征维度不一致。
- Parquet 中的数据已经过 StandardScaler 标准化，训练时不会重复标准化。
- 验证集构造允许使用训练期末尾历史窗口，但只评估验证起始日期之后的样本。

#### 3.3 Transformer 预测

```powershell
python main.py --mode transform_predict --feature_path "F:\实习项目\StockAnalysisSystem\models\transformer\60_158+39\index_000300\features_158+39.parquet" --transformer_model multi_head --transformer_features 158+39 --top_n 5
```

预测结果会输出并保存到：

```text
output\result.csv
```

## 数据和缓存说明

### 数据源

- 分析端统一读取 MySQL，不再需要 Tushare Token，也不会构造外部 SDK 客户端。
- `--use_tushare` 仍可传入，但只会输出弃用提示。
- 运行分析前，请先使用 `python-collector` 的 `daily-market`/历史补采命令更新数据库。

### 股票池筛选

```powershell
--index 000300
--index 000905
--index 000300,000905
--sectors 银行,医药
```

如果指定股票池但成分股获取失败，系统会停止，不再退回全市场。

### 数据库表结构

| 表名 | 主要列 | 用途 | 管线使用 |
|---|---|---|---|
| `stock_basic` | ts_code, name, industry | 股票基本信息 | 全部 |
| `stock_daily` | ts_code, trade_date, OHLCV | 日线行情 | 全部 |
| `stock_daily_basic` | turnover_rate, pe, pb, ps, total_mv | 原始估值与换手率 | 全部（LEFT JOIN） |
| `stock_constituent` | group_type, group_code, ts_code, as_of_date | 股票池快照 | 排名/Transformer |
| `stock_features` | ts_code, trade_date, 72个指标列 | 传统技术指标 | 管线1(写) + 管线2(读+写) |
| `analysis_result` | ts_code, result, prediction | 预测分析结果 | 管线1(写) |

### 传统特征 Parquet 缓存

```text
models\traditional_features\<股票池数量>_<股票池hash>\
    raw_panel.parquet         # 原始面板（含换手率）
    panel_features.parquet    # 计算后的传统特征
```

### Transformer 输出目录规则

全市场：

```text
models\transformer\60_158+39
```

沪深300：

```text
models\transformer\60_158+39\index_000300
```

沪深300 + 中证500：

```text
models\transformer\60_158+39\index_000300_000905
```

### Transformer 特征文件

```text
features_158+39.parquet           # 标准化后特征（含 is_val 标记）
features_158+39_scaler.pkl        # StandardScaler
features_158+39_meta.json         # val_start_date, feature_cols
features_158+39_stockid2idx.json  # 股票代码 → 整数索引映射
raw_panel.parquet                 # 原始面板缓存（供增量特征计算）
probe_selection.json              # 探针法筛选结果
best_model.pth                    # 训练好的模型
training_history.json             # 训练历史
plots\training_history.png        # 训练曲线图
```

## 初始化和数据准备

初始化数据库：

```powershell
python main.py --mode init_db
```

增量采集：

```powershell
python main.py --mode collect --use_tushare
```

全量更新数据、传统特征和 Transformer 特征：

```powershell
python main.py --mode full_update --use_tushare
```

仅计算并入库传统特征：

```powershell
python main.py --mode save_features --use_tushare
```

## 环境配置

`.env` 示例：

```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=stock_analysis
TUSHARE_TOKEN=your_tushare_token
```

安装依赖：

```powershell
pip install -r requirements.txt
```

初始化：

```powershell
python main.py --mode init_db
```

## 常见问题

### 为什么 Transformer 训练图是一条横线？

通常是验证集样本数为 0，导致 eval 指标全是 0。当前代码已调整验证集构造逻辑，重新训练后应出现有效验证曲线。

### 为什么沪深300特征行数不能和全市场一样？

沪深300应只加载约 300 只股票。如果最终特征行数接近全市场，说明缓存曾被污染。删除对应目录后重算：

```powershell
Remove-Item -Recurse -Force "F:\实习项目\StockAnalysisSystem\models\transformer\60_158+39\index_000300"
```

### 单股预测为什么不用 `--mode predict`？

当前 `predict` 模式没有实现分支。请使用：

```powershell
python main.py --mode demo --stock 000001 --use_tushare --model xgboost
```

### 传统特征和 Transformer 特征有什么区别？

两套特征体系 **完全独立**，互不复用：
- **传统特征**（72列）：由 `FeatureEngineer` 计算，存入 MySQL `stock_features` 表和 Parquet 缓存，供 LightGBM 排名使用。
- **Transformer 特征**（~205列）：由 `transformer_utils` 计算（Alpha158 + 39技术指标 + 8截面特征），仅存 Parquet，供 Transformer 模型使用。
- 虽然两者都计算了 RSI、MACD、布林带等类似指标，但实现代码、列名和计算方式不同。

### Windows 上 Transformer 特征计算报 pickle 错误？

Windows 上 `multiprocessing` 无法序列化嵌套的局部函数。已将所有 worker 函数提取到模块顶层，该问题已修复。

## 风险提示

本项目仅用于学习和研究，不构成投资建议。股市有风险，投资需谨慎。
