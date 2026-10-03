/**
 * Mock：stock-analysis-app 分析建模数据。
 * - 特征列名以 FEATURE_COL_MAP v2 snake_case 为准（macd_dif/kdj_k/volatility_20d…）
 * - future_* 为训练标签，不作为特征展示
 * - 统计量、相关性、回测曲线均从 generator 生成的 K 线真实计算
 */
import {
  STOCK_POOL, FIXED_TODAY, getBars, getIndicators,
  mulberry32, hashSeed,
} from './generator.js'

/* ── v1(生成器内部名) → v2(数据库列名) 映射 ─────────────── */

const V1_TO_V2 = {
  macd: 'macd_dif', macd_signal: 'macd_dea',
  k: 'kdj_k', d: 'kdj_d', j: 'kdj_j',
  variance20: 'volatility_20d', variance60: 'volatility_60d', variance120: 'volatility_120d',
  skewness20: 'skewness_20d', skewness60: 'skewness_60d', skewness120: 'skewness_120d',
  kurtosis20: 'kurtosis_20d', kurtosis60: 'kurtosis_60d', kurtosis120: 'kurtosis_120d',
}

const LABEL_COLS = ['future_return_1d', 'future_return_5d', 'future_direction_1d', 'future_direction_5d']

function toV2Row(row) {
  const out = { ts_code: row.ts_code, trade_date: row.trade_date }
  for (const [k, v] of Object.entries(row)) {
    if (k === 'ts_code' || k === 'trade_date') continue
    out[V1_TO_V2[k] || k] = v
  }
  return out
}

/* ── 特征目录（对齐 FEATURE_REGISTRY, version 2.0.0） ───── */

export const FEATURE_VERSION = '2.0.0'

export const FEATURE_CATALOG = [
  { group: 'returns', label: '收益率', features: [
    { name: 'return_1d', cn: '1日收益率' }, { name: 'return_5d', cn: '5日收益率' },
    { name: 'return_10d', cn: '10日收益率' }, { name: 'log_return', cn: '对数收益率' },
  ] },
  { group: 'ma', label: '移动均线', features: [
    { name: 'ma5', cn: '5日均线' }, { name: 'ma10', cn: '10日均线' },
    { name: 'ma20', cn: '20日均线' }, { name: 'ma60', cn: '60日均线' },
  ] },
  { group: 'ema', label: '指数均线', features: [
    { name: 'ema5', cn: '5日EMA' }, { name: 'ema10', cn: '10日EMA' }, { name: 'ema20', cn: '20日EMA' },
    { name: 'ema26', cn: '26日EMA' }, { name: 'ema60', cn: '60日EMA' }, { name: 'ema120', cn: '120日EMA' },
  ] },
  { group: 'macd', label: 'MACD', features: [
    { name: 'macd_dif', cn: 'DIF' }, { name: 'macd_dea', cn: 'DEA' }, { name: 'macd_hist', cn: 'MACD柱' },
  ] },
  { group: 'rsi', label: 'RSI', features: [{ name: 'rsi', cn: 'RSI(14)' }] },
  { group: 'bollinger', label: '布林带', features: [
    { name: 'bb_upper', cn: '上轨' }, { name: 'bb_middle', cn: '中轨' },
    { name: 'bb_lower', cn: '下轨' }, { name: 'bb_width', cn: '带宽' },
  ] },
  { group: 'kdj', label: 'KDJ', features: [
    { name: 'kdj_k', cn: 'K值' }, { name: 'kdj_d', cn: 'D值' }, { name: 'kdj_j', cn: 'J值' },
  ] },
  { group: 'volume', label: '成交量', features: [
    { name: 'vol_ma5', cn: '5日量均线' }, { name: 'vol_ma10', cn: '10日量均线' },
    { name: 'volume_ratio', cn: '量比' }, { name: 'mfi14', cn: 'MFI(14)' },
  ] },
  { group: 'turnover', label: '换手率', features: [
    { name: 'turnover_rate_5', cn: '5日换手率' }, { name: 'turnover_rate_60', cn: '60日换手率' },
    { name: 'turnover_rate_120', cn: '120日换手率' },
  ] },
  { group: 'emotion', label: '情绪指标', features: [
    { name: 'br', cn: 'BR' }, { name: 'ar', cn: 'AR' },
  ] },
  { group: 'momentum', label: '动量指标', features: [
    { name: 'arron_up_25', cn: 'Aroon上轨' }, { name: 'arron_down_25', cn: 'Aroon下轨' },
    { name: 'bear_power', cn: '空头力道' }, { name: 'bull_power', cn: '多头力道' },
    { name: 'bias5', cn: '5日乖离率' }, { name: 'bias10', cn: '10日乖离率' },
    { name: 'bias20', cn: '20日乖离率' }, { name: 'bias60', cn: '60日乖离率' },
    { name: 'cci10', cn: 'CCI(10)' }, { name: 'cci15', cn: 'CCI(15)' },
    { name: 'cci20', cn: 'CCI(20)' }, { name: 'cci88', cn: 'CCI(88)' },
    { name: 'cr20', cn: 'CR(20)' }, { name: 'mass', cn: '梅斯线' },
  ] },
  { group: 'risk', label: '风险指标', features: [
    { name: 'volatility_20d', cn: '20日波动率' }, { name: 'volatility_60d', cn: '60日波动率' },
    { name: 'volatility_120d', cn: '120日波动率' }, { name: 'skewness_20d', cn: '20日偏度' },
    { name: 'skewness_60d', cn: '60日偏度' }, { name: 'skewness_120d', cn: '120日偏度' },
    { name: 'kurtosis_20d', cn: '20日峰度' }, { name: 'kurtosis_60d', cn: '60日峰度' },
    { name: 'kurtosis_120d', cn: '120日峰度' },
  ] },
  { group: 'signals', label: '技术信号', features: [
    { name: 'golden_cross', cn: '金叉信号' }, { name: 'death_cross', cn: '死叉信号' },
    { name: 'macd_golden_cross', cn: 'MACD金叉' }, { name: 'rsi_oversold', cn: 'RSI超卖' },
    { name: 'rsi_overbought', cn: 'RSI超买' },
  ] },
  { group: 'cross_sectional', label: '横截面特征', features: [
    { name: 'market_return', cn: '市场收益率' }, { name: 'market_volatility_20d', cn: '市场20日波动' },
    { name: 'industry_return', cn: '行业收益率' }, { name: 'return_vs_market', cn: '相对市场收益' },
    { name: 'return_vs_industry', cn: '相对行业收益' }, { name: 'volatility_vs_market', cn: '相对市场波动' },
    { name: 'return_zscore_industry', cn: '行业内收益Z分' }, { name: 'volatility_zscore_industry', cn: '行业内波动Z分' },
  ] },
]

const TS_FEATURE_COUNT = FEATURE_CATALOG.filter((g) => g.group !== 'cross_sectional')
  .reduce((a, g) => a + g.features.length, 0) // 62，写入 stock_features 的时序特征列
const CS_FEATURE_COUNT = 8

export function getFeatureCatalog() {
  return {
    version: FEATURE_VERSION,
    tsFeatureCount: TS_FEATURE_COUNT,
    crossSectionalCount: CS_FEATURE_COUNT,
    total: TS_FEATURE_COUNT + CS_FEATURE_COUNT,
    labels: LABEL_COLS,
    groups: FEATURE_CATALOG,
  }
}

/** stock_features 表预览行（v2 列名，含横截面特征） */
export function getFeatureRows(tsCode, limit = 30) {
  const rows = getIndicators(tsCode).map(toV2Row)
  const stock = STOCK_POOL.find((s) => s.ts_code === tsCode) || STOCK_POOL[0]
  // 横截面：市场=全池均值，行业=同行业均值（由真实生成数据计算）
  const poolRets = STOCK_POOL.map((s) => getIndicators(s.ts_code).map((r) => r.return_1d || 0))
  const indPeers = STOCK_POOL.filter((s) => s.industry === stock.industry)
    .map((s) => getIndicators(s.ts_code).map((r) => r.return_1d || 0))
  const tail = rows.slice(-limit)
  const offset = rows.length - tail.length
  return tail.map((r, i) => {
    const idx = offset + i
    const mkt = poolRets.reduce((a, arr) => a + (arr[idx] || 0), 0) / poolRets.length
    const ind = indPeers.reduce((a, arr) => a + (arr[idx] || 0), 0) / indPeers.length
    const ret = r.return_1d || 0
    return {
      ...r,
      market_return: +mkt.toFixed(6),
      market_volatility_20d: r.volatility_20d == null ? null : +(r.volatility_20d * 0.82).toFixed(6),
      industry_return: +ind.toFixed(6),
      return_vs_market: +(ret - mkt).toFixed(6),
      return_vs_industry: +(ret - ind).toFixed(6),
      volatility_vs_market: r.volatility_20d == null ? null : +(r.volatility_20d * 0.18).toFixed(6),
      return_zscore_industry: +((ret - ind) / 0.012).toFixed(4),
      volatility_zscore_industry: +(((r.volatility_20d || 0) * 0.18) / 0.05).toFixed(4),
    }
  })
}

/* ── 统计分析 ───────────────────────────────────────────── */

function quantile(sorted, q) {
  const pos = (sorted.length - 1) * q
  const lo = Math.floor(pos), hi = Math.ceil(pos)
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo)
}

function pearson(x, y) {
  const pairs = x.map((v, i) => [v, y[i]]).filter(([a, b]) => a != null && b != null && !isNaN(a) && !isNaN(b))
  const n = pairs.length
  if (n < 3) return 0
  const mx = pairs.reduce((a, p) => a + p[0], 0) / n
  const my = pairs.reduce((a, p) => a + p[1], 0) / n
  let sxy = 0, sxx = 0, syy = 0
  for (const [a, b] of pairs) { sxy += (a - mx) * (b - my); sxx += (a - mx) ** 2; syy += (b - my) ** 2 }
  return sxx && syy ? sxy / Math.sqrt(sxx * syy) : 0
}

const CORR_FEATURES = ['return_1d', 'rsi', 'macd_dif', 'kdj_k', 'bias20', 'cci20', 'volatility_20d', 'volume_ratio', 'mfi14', 'turnover_rate_5']

export function getStatisticalAnalysis(tsCode) {
  const bars = getBars(tsCode)
  const feats = getIndicators(tsCode).map(toV2Row)
  const rets = feats.map((f) => (f.return_1d == null ? null : f.return_1d * 100)).filter((v) => v != null)
  const sorted = [...rets].sort((a, b) => a - b)
  const mean = rets.reduce((a, b) => a + b, 0) / rets.length
  const std = Math.sqrt(rets.reduce((a, b) => a + (b - mean) ** 2, 0) / rets.length)
  const m3 = rets.reduce((a, b) => a + ((b - mean) / std) ** 3, 0) / rets.length
  const m4 = rets.reduce((a, b) => a + ((b - mean) / std) ** 4, 0) / rets.length - 3

  // 直方图（24 桶）
  const min = sorted[0], max = sorted[sorted.length - 1]
  const binW = (max - min) / 24
  const bins = Array.from({ length: 24 }, (_, i) => ({
    range: +(min + i * binW).toFixed(2),
    count: 0,
  }))
  for (const r of rets) {
    const idx = Math.min(23, Math.floor((r - min) / binW))
    bins[idx].count++
  }

  // 回撤曲线
  let peak = -Infinity, maxDd = 0, maxDdDate = null
  const drawdown = bars.map((b) => {
    peak = Math.max(peak, b.close)
    const dd = ((b.close - peak) / peak) * 100
    if (dd < maxDd) { maxDd = dd; maxDdDate = b.trade_date }
    return { date: b.trade_date, value: +dd.toFixed(2) }
  })

  // 滚动波动率（年化 %）
  const roll = (w) => feats.map((f, i) => {
    if (i < w) return null
    const win = rets.slice(i - w, i)
    const m = win.reduce((a, b) => a + b, 0) / w
    return +(Math.sqrt(win.reduce((a, b) => a + (b - m) ** 2, 0) / w) * Math.sqrt(252)).toFixed(2)
  })
  const volatility = {
    dates: bars.map((b) => b.trade_date),
    vol20: roll(20), vol60: roll(60), vol120: roll(120),
  }

  // 相关性热力图
  const corr = CORR_FEATURES.map((f1) =>
    CORR_FEATURES.map((f2) => +pearson(feats.map((r) => r[f1]), feats.map((r) => r[r && f2])).toFixed(2)),
  )

  // 趋势（近 60 日线性回归）
  const win = bars.slice(-60)
  const n = win.length
  const xs = win.map((_, i) => i)
  const ys = win.map((b) => b.close)
  const mx = xs.reduce((a, b) => a + b, 0) / n
  const my = ys.reduce((a, b) => a + b, 0) / n
  let sxy = 0, sxx = 0, syy = 0
  for (let i = 0; i < n; i++) { sxy += (xs[i] - mx) * (ys[i] - my); sxx += (xs[i] - mx) ** 2; syy += (ys[i] - my) ** 2 }
  const slope = sxy / sxx
  const r2 = (sxy * sxy) / (sxx * syy)
  const slopePct = (slope / my) * 100

  return {
    descriptive: {
      mean: +mean.toFixed(4), std: +std.toFixed(4),
      min: +min.toFixed(4), q25: +quantile(sorted, 0.25).toFixed(4),
      q50: +quantile(sorted, 0.5).toFixed(4), q75: +quantile(sorted, 0.75).toFixed(4),
      max: +max.toFixed(4), skewness: +m3.toFixed(4), kurtosis: +m4.toFixed(4),
    },
    histogram: bins,
    drawdown: { series: drawdown, max_drawdown: +maxDd.toFixed(2), max_drawdown_date: maxDdDate },
    volatility,
    correlation: { features: CORR_FEATURES, matrix: corr },
    trend: {
      slope: +slopePct.toFixed(4), r_squared: +r2.toFixed(4),
      direction: slopePct > 0.05 ? 'up' : slopePct < -0.05 ? 'down' : 'flat',
      window: 60,
    },
  }
}

/* ── 模型注册表（对齐 model_registry.py 元数据 + 各评估器指标） ── */

export const MODELS = [
  {
    model_name: 'xgboost_classifier', model_type: 'classification', model_version: '1.0.0',
    created_at: '2026-09-12 21:40:18', train_start_date: '2024-09-30', train_end_date: '2026-06-30',
    feature_version: FEATURE_VERSION, n_features: 62, target: 'future_direction_1d',
    metrics: { Accuracy: 0.5827, Precision: 0.5714, Recall: 0.6192, F1: 0.5944, AUC: 0.6181, Confusion_Matrix: [[1783, 1402], [1188, 1927]] },
    params: { n_estimators: 400, max_depth: 6, learning_rate: 0.05, objective: 'binary:logistic', subsample: 0.85 },
  },
  {
    model_name: 'xgboost_regression', model_type: 'regression', model_version: '1.0.0',
    created_at: '2026-09-14 10:05:44', train_start_date: '2024-09-30', train_end_date: '2026-06-30',
    feature_version: FEATURE_VERSION, n_features: 62, target: 'future_return_5d',
    metrics: { RMSE: 0.0287, MAE: 0.0213, R2: 0.0342, Direction_Accuracy: 0.5611 },
    params: { n_estimators: 500, max_depth: 5, learning_rate: 0.04, objective: 'reg:squarederror' },
  },
  {
    model_name: 'xgboost_multiclass', model_type: 'multiclass', model_version: '1.0.0',
    created_at: '2026-09-15 15:22:07', train_start_date: '2024-09-30', train_end_date: '2026-06-30',
    feature_version: FEATURE_VERSION, n_features: 62, target: 'future_return_5d(5类分箱)',
    labels: ['大幅跌', '小跌', '平', '小涨', '大涨'],
    metrics: {
      Accuracy: 0.3468, F1_Weighted: 0.3312,
      Confusion_Matrix: [
        [402, 355, 190, 88, 25], [311, 486, 372, 143, 38],
        [158, 344, 611, 297, 90], [77, 165, 356, 512, 240],
        [21, 44, 121, 302, 566],
      ],
    },
    params: { n_estimators: 450, max_depth: 6, learning_rate: 0.05, objective: 'multi:softprob', num_class: 5 },
  },
  {
    model_name: 'lstm_classifier', model_type: 'deep', model_version: '1.0.0',
    created_at: '2026-09-18 09:47:31', train_start_date: '2024-09-30', train_end_date: '2026-06-30',
    feature_version: FEATURE_VERSION, n_features: 62, target: 'future_direction_1d',
    metrics: { Accuracy: 0.5643, Precision: 0.5508, Recall: 0.6017, F1: 0.5751, AUC: 0.5984, Confusion_Matrix: [[1840, 1345], [1252, 1863]] },
    params: { hidden_size: 128, num_layers: 2, sequence_length: 20, epochs: 60, learning_rate: 0.001, dropout: 0.3 },
  },
  {
    model_name: 'ranking_lgb', model_type: 'ranking', model_version: '1.0.0',
    created_at: '2026-09-20 20:12:55', train_start_date: '2025-09-20', train_end_date: '2026-08-29',
    feature_version: FEATURE_VERSION, n_features: 41, target: 'future_return_5d(横截面)',
    metrics: { RMSE: 0.0271, Direction_Accuracy: 0.5724 },
    params: { objective: 'regression_l2', train_window: 365, rebalance_days: 5, probe_selection: true, n_estimators: 600, learning_rate: 0.03 },
  },
  {
    model_name: 'transformer_multi_head', model_type: 'deep', model_version: '1.0.0',
    created_at: '2026-09-22 23:58:10', train_start_date: '2024-09-30', train_end_date: '2026-06-30',
    feature_version: FEATURE_VERSION, n_features: 158, target: '多输出头(ranking/regression/classification/direction)',
    metrics: { final_score: 0.5847, ratio_pred: 0.6013, eval_loss: 0.5612, best_epoch: 41 },
    params: { model_type: 'multi_head', d_model: 64, n_heads: 4, epochs: 50, learning_rate: 0.0008, top_k: 8 },
  },
]

export function getModels() {
  return MODELS.map((m) => ({ ...m }))
}

/* ── 特征重要性 ─────────────────────────────────────────── */

const IMPORTANCE_POOL = [
  'volatility_20d', 'bias20', 'return_5d', 'cci20', 'kdj_j', 'macd_hist', 'volume_ratio',
  'turnover_rate_5', 'rsi', 'bias60', 'mfi14', 'return_1d', 'bb_width', 'arron_up_25',
  'volatility_60d', 'cr20', 'kurtosis_20d', 'mass', 'skewness_20d', 'log_return', 'ar',
]

export function getFeatureImportance(modelName, topN = 15) {
  const rnd = mulberry32(hashSeed('imp:' + modelName))
  const rows = IMPORTANCE_POOL.map((f) => ({ feature: f, gain: +(50 + rnd() * 950).toFixed(1) }))
    .sort((a, b) => b.gain - a.gain)
  const total = rows.reduce((a, r) => a + r.gain, 0)
  return rows.slice(0, topN).map((r) => ({ ...r, pct: +((r.gain / total) * 100).toFixed(2) }))
}

/* ── 模型预测（预测 vs 实际） ───────────────────────────── */

export function getPrediction(tsCode, modelName = 'xgboost_regression') {
  const bars = getBars(tsCode)
  const feats = getIndicators(tsCode).map(toV2Row)
  const rnd = mulberry32(hashSeed('pred:' + tsCode + modelName))
  const win = 60
  const out = []
  for (let i = bars.length - win; i < bars.length - 1; i++) {
    const actual = feats[i].future_return_5d != null ? feats[i].future_return_5d * 100 : null
    const noise = (rnd() - 0.5) * 3.4 + (actual == null ? 0 : (0.42 + rnd() * 0.3) * actual * -1 * -1)
    const predicted = actual == null ? null : actual * (0.35 + rnd() * 0.35) + (rnd() - 0.5) * 1.6
    out.push({
      trade_date: bars[i].trade_date,
      close: bars[i].close,
      actual: actual == null ? null : +actual.toFixed(3),
      predicted: predicted == null ? null : +predicted.toFixed(3),
      probability: +(0.5 + (predicted || 0) / 12 + (rnd() - 0.5) * 0.08).toFixed(3),
      noise: +noise.toFixed(3),
    })
  }
  const model = MODELS.find((m) => m.model_name === modelName) || MODELS[0]
  return { model_name: modelName, model_type: model.model_type, series: out }
}

/* ── analysis_result 表预览 ─────────────────────────────── */

export function getAnalysisResults(tsCode) {
  const rnd = mulberry32(hashSeed('ar:' + tsCode))
  const cls = MODELS[0]
  return [
    {
      id: 9101, ts_code: tsCode, analysis_date: FIXED_TODAY, analysis_type: 'xgboost_prediction',
      result: JSON.stringify({ model: 'xgboost_classifier', direction: rnd() > 0.5 ? 'up' : 'down', proba: +(0.5 + rnd() * 0.3).toFixed(3) }),
      prediction: cls.metrics.Accuracy, confidence: cls.metrics.F1,
      created_at: `${FIXED_TODAY} 16:30:12`,
    },
    {
      id: 9102, ts_code: tsCode, analysis_date: FIXED_TODAY, analysis_type: 'regression_prediction',
      result: JSON.stringify({ model: 'xgboost_regression', horizon: '5d', pred_return: +((rnd() - 0.45) * 0.06).toFixed(4) }),
      prediction: MODELS[1].metrics.RMSE, confidence: MODELS[1].metrics.Direction_Accuracy,
      created_at: `${FIXED_TODAY} 16:30:14`,
    },
    {
      id: 9087, ts_code: tsCode, analysis_date: '2026-09-24', analysis_type: 'xgboost_prediction',
      result: JSON.stringify({ model: 'xgboost_classifier', direction: 'up', proba: +(0.5 + rnd() * 0.3).toFixed(3) }),
      prediction: cls.metrics.Accuracy, confidence: cls.metrics.F1,
      created_at: '2026-09-24 16:30:09',
    },
  ]
}

/* ── 策略回测（从真实生成的 K 线信号计算） ───────────────── */

function runBacktest(bars, buySignals, sellSignals, initialCapital, commission) {
  let cash = initialCapital
  let shares = 0
  const equity = []
  const trades = []
  let peak = initialCapital
  const drawdown = []
  for (let i = 0; i < bars.length; i++) {
    const price = bars[i].close
    if (shares === 0 && buySignals[i]) {
      shares = Math.floor(cash / (price * (1 + commission)) / 100) * 100
      if (shares > 0) {
        const cost = shares * price * (1 + commission)
        cash -= cost
        trades.push({ date: bars[i].trade_date, action: 'BUY', price, shares, cost: +cost.toFixed(2), revenue: null })
      }
    } else if (shares > 0 && sellSignals[i]) {
      const revenue = shares * price * (1 - commission)
      cash += revenue
      trades.push({ date: bars[i].trade_date, action: 'SELL', price, shares, cost: null, revenue: +revenue.toFixed(2) })
      shares = 0
    }
    const eq = cash + shares * price
    equity.push({ date: bars[i].trade_date, strategy: +eq.toFixed(2), buyhold: +(initialCapital * (price / bars[0].close)).toFixed(2) })
    peak = Math.max(peak, eq)
    drawdown.push({ date: bars[i].trade_date, value: +(((eq - peak) / peak) * 100).toFixed(2) })
  }
  // 指标
  const totalReturn = equity[equity.length - 1].strategy / initialCapital - 1
  const days = bars.length
  const annualReturn = (1 + totalReturn) ** (252 / days) - 1
  const dailyRets = equity.map((e, i) => (i === 0 ? 0 : e.strategy / equity[i - 1].strategy - 1)).slice(1)
  const m = dailyRets.reduce((a, b) => a + b, 0) / dailyRets.length
  const annualVol = Math.sqrt(dailyRets.reduce((a, b) => a + (b - m) ** 2, 0) / dailyRets.length) * Math.sqrt(252)
  const sharpe = annualVol > 0 ? (annualReturn - 0.02) / annualVol : 0
  const maxDd = Math.min(...drawdown.map((d) => d.value))
  // 配对交易胜率
  let wins = 0, losses = 0, winSum = 0, lossSum = 0
  for (let i = 0; i < trades.length - 1; i += 2) {
    if (trades[i].action === 'BUY' && trades[i + 1]?.action === 'SELL') {
      const pnl = trades[i + 1].revenue - trades[i].cost
      if (pnl >= 0) { wins++; winSum += pnl } else { losses++; lossSum += -pnl }
    }
  }
  return {
    metrics: {
      total_return: +totalReturn.toFixed(4), annual_return: +annualReturn.toFixed(4),
      annual_volatility: +annualVol.toFixed(4), sharpe_ratio: +sharpe.toFixed(3),
      max_drawdown: +maxDd.toFixed(4),
      win_rate: +(wins / Math.max(1, wins + losses)).toFixed(4),
      profit_loss_ratio: losses ? +((winSum / wins) / (lossSum / losses)).toFixed(3) : null,
      trade_count: Math.floor(trades.length / 2),
    },
    equity, drawdown, trades,
  }
}

export function getBacktest(tsCode, type = '技术指标信号', params = {}) {
  const initialCapital = params.initial_capital ?? 1000000
  const commission = params.commission ?? 0.0003
  const bars = getBars(tsCode)
  const feats = getIndicators(tsCode)
  const buy = new Array(bars.length).fill(false)
  const sell = new Array(bars.length).fill(false)
  if (type === '技术指标信号') {
    feats.forEach((f, i) => {
      if (f.golden_cross === 1) buy[i] = true
      if (f.death_cross === 1) sell[i] = true
    })
  } else {
    // 模型预测信号：mock 概率 = 平滑未来收益 + 噪声，>0.55 买入 <0.45 卖出
    const rnd = mulberry32(hashSeed('bt:' + tsCode))
    feats.forEach((f, i) => {
      const base = f.future_return_5d == null ? 0 : f.future_return_5d * 18
      const prob = 0.5 + Math.max(-0.45, Math.min(0.45, base)) + (rnd() - 0.5) * 0.22
      if (prob > 0.55) buy[i] = true
      if (prob < 0.45) sell[i] = true
    })
  }
  const result = runBacktest(bars, buy, sell, initialCapital, commission)
  return {
    ts_code: tsCode, type,
    params: { initial_capital: initialCapital, commission, window: `${bars[0].trade_date} ~ ${bars[bars.length - 1].trade_date}` },
    ...result,
  }
}

/* ── 全市场选股排名（ranking_lgb） ──────────────────────── */

function rankingScores() {
  return STOCK_POOL.map((s) => {
    const rnd = mulberry32(hashSeed('rank:' + s.ts_code))
    const feats = getIndicators(s.ts_code)
    const last = feats[feats.length - 1]
    const base = (last.future_return_5d ?? 0) * 100
    return {
      ts_code: s.ts_code, name: s.name, industry: s.industry,
      pred_return: +(base * 0.55 + (rnd() - 0.42) * 3.2).toFixed(3),
      score: 0,
    }
  }).sort((a, b) => b.pred_return - a.pred_return)
    .map((r, i) => ({ rank: i + 1, ...r, score: +(92 - i * 5.5 - (r.pred_return < 0 ? 8 : 0)).toFixed(1), trade_date: FIXED_TODAY }))
}

export function getRanking() {
  const rows = rankingScores()
  return {
    trade_date: FIXED_TODAY,
    model: 'ranking_lgb',
    top_n: 5,
    rows,
    model_eval: { RMSE: 0.0271, Direction_Accuracy: 0.5724 },
    params: { train_window: 365, rebalance_days: 5 },
  }
}

/** 排名回测：每 rebalance_days 持有 Top3 等权，基准为全池等权 */
export function getRankingBacktest() {
  const ranked = rankingScores()
  const top3 = ranked.slice(0, 3).map((r) => r.ts_code)
  const barsMap = Object.fromEntries(STOCK_POOL.map((s) => [s.ts_code, getBars(s.ts_code)]))
  const dates = barsMap[STOCK_POOL[0].ts_code].map((b) => b.trade_date)
  const n = dates.length
  const equity = []
  let held = top3
  for (let i = 0; i < n; i++) {
    if (i % 5 === 0) {
      // 简化调仓：按当前排名（mock 中排名固定），真实系统按滚动预测
      held = top3
    }
    const port = held.reduce((a, c) => a + barsMap[c][i].close / barsMap[c][0].close, 0) / held.length
    const bench = STOCK_POOL.reduce((a, s) => a + barsMap[s.ts_code][i].close / barsMap[s.ts_code][0].close, 0) / STOCK_POOL.length
    equity.push({ date: dates[i], strategy: +(port * 1000000).toFixed(0), benchmark: +(bench * 1000000).toFixed(0) })
  }
  const last = equity[equity.length - 1]
  return {
    params: { train_window: 365, rebalance_days: 5, hold_top: 3, initial_capital: 1000000 },
    held, equity,
    metrics: {
      total_return: +(last.strategy / 1000000 - 1).toFixed(4),
      benchmark_return: +(last.benchmark / 1000000 - 1).toFixed(4),
      excess_return: +((last.strategy - last.benchmark) / 1000000).toFixed(4),
    },
  }
}

/* ── 探针法特征筛选（probe_selection.py） ───────────────── */

export function getProbeSelection() {
  const all = FEATURE_CATALOG.flatMap((g) => g.features.map((f) => f.name))
  const rnd = mulberry32(hashSeed('probe'))
  const iterations = []
  let remaining = all.length // 62+8=70 参与筛选，口径与 pipeline 一致
  const ths = [0.52, 0.54, 0.56, 0.575, 0.585]
  for (let i = 0; i < 5; i++) {
    const removed = i === 4 ? remaining - 41 : Math.round(3 + rnd() * 7)
    iterations.push({
      iter: i + 1,
      noise_th_cls: ths[i],
      noise_th_reg: +(ths[i] - 0.015).toFixed(3),
      noise_th_dir: +(ths[i] - 0.008).toFixed(3),
      removed_count: removed,
      remaining_count: remaining - removed,
    })
    remaining -= removed
  }
  const retained = ['volatility_20d', 'bias20', 'return_5d', 'cci20', 'kdj_j', 'macd_hist', 'volume_ratio',
    'turnover_rate_5', 'rsi', 'bias60', 'mfi14', 'return_1d', 'bb_width', 'arron_up_25', 'volatility_60d',
    'cr20', 'kurtosis_20d', 'mass', 'skewness_20d', 'log_return', 'ar', 'ma20', 'ema120', 'bias10',
    'bb_upper', 'vol_ma5', 'return_10d', 'golden_cross', 'macd_golden_cross', 'rsi_oversold',
    'market_return', 'return_vs_market', 'industry_return', 'return_zscore_industry',
    'volatility_120d', 'skewness_60d', 'kdj_k', 'bias5', 'cci88', 'bull_power', 'bear_power']
  return {
    original_count: all.length,
    final_count: retained.length,
    removal_rate: +(((all.length - retained.length) / all.length) * 100).toFixed(1),
    iterations,
    retained_features: retained,
    importance: getFeatureImportance('ranking_lgb', 20),
    catalog_total: all.length,
  }
}

/* ── Transformer 训练与预测 ─────────────────────────────── */

export const TRANSFORMER_CONFIGS = {
  model_types: ['single_head', 'multi_head'],
  feature_sets: ['158+39', '39'],
  top_k_range: [3, 20],
  defaults: { transformer_model: 'multi_head', feature_set: '158+39', top_k: 8, epochs: 50, lr: 0.0008 },
}

/** 训练历史：final_score = (pred - random) / (max - random) */
export function getTransformerTraining(config = {}) {
  const epochs = config.epochs || TRANSFORMER_CONFIGS.defaults.epochs
  const multi = (config.transformer_model || 'multi_head') === 'multi_head'
  const rnd = mulberry32(hashSeed('tf:' + multi + epochs))
  const history = []
  let best = { epoch: 1, final_score: -1 }
  for (let e = 1; e <= epochs; e++) {
    const p = e / epochs
    const learn = 1 - Math.exp(-3.1 * p)
    const fs = 0.12 + learn * (multi ? 0.5 : 0.42) + (rnd() - 0.5) * 0.055
    const trainLoss = 0.693 - learn * (multi ? 0.15 : 0.12) + (rnd() - 0.5) * 0.012
    const evalLoss = trainLoss + 0.012 + p * p * 0.05 + (rnd() - 0.5) * 0.014
    const row = {
      epoch: e,
      final_score: +Math.max(0, fs).toFixed(4),
      ratio_pred: +(0.5 + Math.max(0, fs) * 0.32 + (rnd() - 0.5) * 0.03).toFixed(4),
      train_loss: +trainLoss.toFixed(4),
      eval_loss: +evalLoss.toFixed(4),
    }
    history.push(row)
    if (row.final_score > best.final_score) best = { epoch: e, final_score: row.final_score }
  }
  return {
    config: { ...TRANSFORMER_CONFIGS.defaults, ...config, epochs },
    history,
    best_epoch: best.epoch,
    best_final_score: best.final_score,
    heads: multi
      ? ['ranking', 'regression', 'classification', 'direction']
      : ['ranking'],
  }
}

export function getTransformerPredictions(config = {}) {
  const topK = Math.min(config.top_k || TRANSFORMER_CONFIGS.defaults.top_k, STOCK_POOL.length)
  const rnd = mulberry32(hashSeed('tfp:' + topK + (config.transformer_model || '')))
  const rows = STOCK_POOL.map((s) => ({
    ts_code: s.ts_code, name: s.name,
    raw: 0.3 + rnd() * 0.7,
  }))
  rows.sort((a, b) => b.raw - a.raw)
  const top = rows.slice(0, topK)
  const adj = top.map((r) => r.raw * (0.86 + rnd() * 0.12))
  const wSum = adj.reduce((a, b) => a + b, 0)
  return top.map((r, i) => ({
    rank: i + 1,
    ts_code: r.ts_code,
    name: r.name,
    score: +r.raw.toFixed(4),
    adjusted_score: +adj[i].toFixed(4),
    uncertainty: +(0.04 + rnd() * 0.2).toFixed(4),
    weight: +((adj[i] / wSum) * 100).toFixed(2),
  }))
}
