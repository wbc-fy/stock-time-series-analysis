# StockAnalysisSystem 可视化前端

在现有演示页面基础上逐步接入后端。布局、深浅主题和 ECharts 图表保留；页面功能不代表对应 API 已实现。

| 路由 | 模块 | 对齐后端 |
|---|---|---|
| `/overview` | 系统总览 | V0.5 架构示意 · 演示服务健康 · 演示 KPI |
| `/collector` | 数据采集 | python-collector：8 个 CLI 命令、collection_task 状态机、4 张数据表 |
| `/monitor` | Kafka 消费监控 | 可接入真实消费统计、最近错误、健康状态；演示模式另含 Topic、趋势和本地校验模拟器 |
| `/analysis` | 分析建模 | stock-analysis-app：K 线/指标/统计/模型/回测/排名/Transformer，9 个子 Tab |

## 启动

```bash
cd StockAnalysisSystem/frontend
npm install
npm run dev        # http://localhost:5173
npm run build      # 产物在 dist/
npm test           # 接口行为回归
```

技术栈：Vite 5 + Vue 3 + Pinia + Vue Router（hash）+ ECharts 5。无 UI 组件库，样式手写；深浅双主题（默认深色），A 股红涨绿跌。

## Mock 数据说明

- 所有数据由 `src/mock/` 生成：K 线为**固定种子的随机游走**（同一股票每次刷新一致），技术指标（MACD/KDJ/RSI/BOLL/CCI/Aroon…）由生成的 K 线**真实公式计算**，回测/权益曲线/交易记录由真实信号驱动。
- 与真实行情无关，仅供演示。顶栏按当前页面显示“演示数据 · 非真实行情”或“监控接口数据”。
- 特征列名对齐 FEATURE_REGISTRY v2.0.0（snake_case，62 时序 + 8 横截面）；`future_*` 为训练标签不作为特征展示。

## 切换到真实后端

顶栏“数据模式”可以随时切换，选择保存在浏览器。默认是演示模式。选择“接入现有 API”后，监控页、分析综合概览及 V0.7 预测读取真实接口；其他未接入分析标签禁用，切换演示模式后可查看原有演示。采集和系统总览仍显示演示数据。

V0.7 预测使用 Python 只读 API（8084），GET 不训练。页面分别显示冻结测试曲线和 latest 未知实际值，收益率仅从 API 小数转百分比一次；RMSE/MAE 页面单位为百分点，R² 无单位，不展示概率或虚构未来价格。没有模型、结果或依赖失败时明确提示，不回退演示。数据截止日不是今天；未复权价、源记录缺口及估值历史修订限制见 [V0.7 指南](../docs/V0.7_SINGLE_STOCK_PREDICTION.md)。

预测新增真实 GET：`/api/analysis/models?ts_code=...`、`/api/analysis/models/{model_id}/importance`、`/api/analysis/prediction/{ts_code}?model=...`、`/api/analysis/results/{ts_code}`。模型列表为元信息数组，预测 DTO 为 `{metadata,test_series,latest}`；同一模型可有多个 `publication_id` 发布。Vite 仅代理这四类具体路径到 8084，行情仍走 8083、监控仍走 8080。生产须保持这些精确同源代理，不覆盖整个分析命名空间，不给浏览器数据库凭据。

也可以使用 `.env.local` 配置首次打开页面的默认值（已有浏览器选择优先）：

```bash
# .env.local
VITE_DATA_SOURCE=hybrid
CONSUMER_API_TARGET=http://127.0.0.1:8080
MARKET_API_TARGET=http://127.0.0.1:8083
PREDICTION_API_TARGET=http://127.0.0.1:8084
```

Vite 开发服务器代理监控请求到 Java 服务，避免开发时的跨域问题；修改环境文件后重启 Vite。`http` 配置值兼容为 `hybrid`。生产部署需要同源反向代理转发 `/api/consumer/*` 和 `/actuator/health`；`vite preview` 本身不提供该代理。也可设置 `VITE_API_BASE` 指向允许该页面来源的 API 地址。

监控页只请求已实现的接口。Topic 积压、历史消费趋势和服务器端校验尚未实现，因此在 API 模式隐藏。真实接口失败或超时会显示错误并自动重试，保留上次成功数据；不会回退成模拟统计。离开监控页面时取消未完成请求。

端点现状：

- **已存在的真实端点**（kafka-consumer-service）：`GET /api/consumer/statistics`、`GET /api/consumer/errors`、`GET /actuator/health`
- **设计约定端点**（未接入）：`/api/collector/*`、其余分析端点、`/api/consumer/{topics,trend,validate,samples}`

| 已实现行情 GET（market-api-service，8083） | 数据来源与边界 |
|---|---|
| `/api/analysis/stocks` | MySQL stock_basic 股票池 |
| `/api/analysis/kline/{tsCode}` | MySQL stock_daily，最新有界区间，按交易日升序 |
| `/api/analysis/indicators/{tsCode}` | ClickHouse 中已落库的 Flink MA5/10/20，最新有界区间 |

生产同源代理也需转发上述三个具体行情路径到 8083；监控仍转发至 8080。真实请求失败显示错误和手动重试，不回退演示行情。切换股票和离开页面取消请求，并拒绝过期响应。指标按交易日对齐，预热 null 不改为零、不从收盘价重算，真实模式不含 MA60。界面显示覆盖数、最新 K 线与指标日期、最新 MA 值和窗口；历史部分覆盖不代表每日更新或完整覆盖。

以上约定端点目前继续使用演示数据，不会在接入模式下发送请求。后续按指标落库和查询 API 的实际字段逐页接入，不能仅切换环境变量就把全部图表变为真实数据。Java Consumer 统计与 Flink 指标作业是不同链路，监控页的计数不包含 Flink 输出。

## 目录结构

```
src/
├── App.vue               # 侧边栏 + 顶栏（MOCK 徽标/时钟/主题切换）
├── router/  stores/      # 4 路由 · theme store（localStorage: sas-theme）
├── api/                  # ★ 接口适配层（mock|http 开关）
├── mock/                 # generator(K线+指标引擎) · collector · consumer · analysis · system
├── components/           # StatCard · StatusTag · ChartBox · DataTable · Icon
├── views/                # Overview · Collector · Monitor · Analysis
│   └── analysis/         # 9 个子 Tab 组件
├── utils/                # format.js · chart.js（双主题 ECharts 工具）
└── styles/               # theme.css（CSS 变量双主题）· base.css
```
