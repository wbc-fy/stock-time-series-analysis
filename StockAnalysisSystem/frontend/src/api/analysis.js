/**
 * 分析接口层：股票池、K 线、Flink 指标已接入 market-api-service。
 * 其他端点仅提供演示数据，真实模式不开放对应标签页。
 */
import { request, dataSource } from './client.js'
import * as mock from '../mock/analysis.js'
import { STOCK_POOL, getBars, getDailyBasic, getIndicators, FIXED_TODAY } from '../mock/generator.js'
import { normalizeStockPool, normalizeKline, normalizeIndicators, STOCK_CODE } from '../utils/market.js'
import { normalizeModels, normalizePrediction, normalizeResults, finiteJson } from '../utils/prediction.js'
import { normalizeEvaluations, normalizeEvaluation } from '../utils/evaluation.js'

const evaluationDemoUnavailable = () => { throw new Error('滚动评估仅提供真实离线发布报告，演示模式不可用') }
const evaluationCode = code => { if (!/^[0-9]{6}\.(?:SZ|SH|BJ)$/.test(code)) throw new Error('股票代码无效') }
export async function getEvaluations(tsCode, options = {}) {
  evaluationCode(tsCode)
  const dto = await request(evaluationDemoUnavailable, `/api/analysis/evaluations?ts_code=${tsCode}`, { ...options, method: 'GET' })
  return normalizeEvaluations(dto, tsCode)
}
export async function getEvaluation(tsCode, reportId, options = {}) {
  evaluationCode(tsCode)
  if (!/^eval_[0-9a-f]{32}$/.test(reportId)) throw new Error('报告标识无效')
  try {
    const dto = await request(evaluationDemoUnavailable, `/api/analysis/evaluations/${tsCode}/${reportId}`, { ...options, method: 'GET' })
    return dto === null ? null : normalizeEvaluation(dto, tsCode, reportId)
  } catch (error) {
    // The shared client exposes HTTP errors as this exact message; only a missing
    // detail is an empty state. Connectivity, contract and 503 errors stay errors.
    if (error.message === '接口返回 HTTP 404') return null
    throw error
  }
}

/** GET /api/analysis/stocks — 股票池（stock_basic 子集） */
export async function getStockPool(options = {}) {
  return normalizeStockPool(await request(() => STOCK_POOL.map(({ ts_code, symbol, name, area, industry, list_date }) => ({ ts_code, symbol, name, area, industry, list_date })), '/api/analysis/stocks', options))
}

/** GET /api/analysis/kline/:tsCode — 日线 OHLCV（stock_daily） */
export async function getKline(tsCode, options = {}) {
  if (!STOCK_CODE.test(tsCode)) throw new Error('股票代码无效')
  const data = await request(() => ({ ts_code: tsCode, bars: getBars(tsCode) }), `/api/analysis/kline/${tsCode}`, options)
  return { ts_code: tsCode, bars: normalizeKline(data, tsCode) }
}

/** GET /api/analysis/daily-basic/:tsCode/:tradeDate — 估值与换手（stock_daily_basic） */
export function getDailyBasicRow(tsCode, tradeDate = FIXED_TODAY) {
  return request(() => getDailyBasic(tsCode, tradeDate), `/api/analysis/daily-basic/${tsCode}/${tradeDate}`)
}

/** GET /api/analysis/features/catalog — 特征目录（FEATURE_REGISTRY） */
export function getFeatureCatalog() {
  return request(() => mock.getFeatureCatalog(), '/api/analysis/features/catalog')
}

/** GET /api/analysis/features/:tsCode — stock_features 行（v2 列名，含横截面） */
export function getFeatureRows(tsCode, limit = 30) {
  return request(() => mock.getFeatureRows(tsCode, limit), `/api/analysis/features/${tsCode}?limit=${limit}`)
}

/** GET /api/analysis/indicators/:tsCode — 全量指标序列（内部列名，供图表） */
export async function getIndicatorSeries(tsCode, options = {}) {
  if (!STOCK_CODE.test(tsCode)) throw new Error('股票代码无效')
  const live = dataSource.value === 'hybrid'
  const data = await request(() => getIndicators(tsCode), `/api/analysis/indicators/${tsCode}`, options)
  return live ? { ts_code: tsCode, indicators: normalizeIndicators(data, tsCode) } : data
}

/** GET /api/analysis/statistics/:tsCode — 描述统计/直方图/回撤/波动率/相关性/趋势 */
export function getStatisticalAnalysis(tsCode) {
  return request(() => mock.getStatisticalAnalysis(tsCode), `/api/analysis/statistics/${tsCode}`)
}

/** GET /api/analysis/models — 模型注册表（model_registry） */
export async function getModels(tsCode, options = {}) {
  const live = dataSource.value === 'hybrid'
  if (live && !STOCK_CODE.test(tsCode)) throw new Error('股票代码无效')
  const rows = await request(() => mock.getModels(), `/api/analysis/models${live ? `?ts_code=${tsCode}` : ''}`, options)
  return live ? normalizeModels(rows, tsCode) : rows
}

/** GET /api/analysis/models/:name/importance — 特征重要性 TopN */
export async function getFeatureImportance(modelName, topN = 15, options = {}) {
  const live = dataSource.value === 'hybrid'
  const data = await request(() => mock.getFeatureImportance(modelName, topN), `/api/analysis/models/${encodeURIComponent(modelName)}/importance?top=${topN}`, options)
  if (live && (data?.model_id !== modelName || !Array.isArray(data.feature_importance))) throw new Error('特征重要性格式无效')
  return live ? finiteJson(data) : data
}

/** GET /api/analysis/prediction/:tsCode — 预测 vs 实际序列 */
export async function getPrediction(tsCode, modelName, options = {}) {
  const live = dataSource.value === 'hybrid'
  const dto = await request(() => mock.getPrediction(tsCode, modelName), `/api/analysis/prediction/${tsCode}?model=${encodeURIComponent(modelName)}${live ? '&limit=500' : ''}`, options)
  return live ? normalizePrediction(dto, tsCode, modelName) : dto
}

/** GET /api/analysis/results/:tsCode — analysis_result 表预览 */
export async function getAnalysisResults(tsCode, options = {}) {
  const live = dataSource.value === 'hybrid'
  const rows = await request(() => mock.getAnalysisResults(tsCode), `/api/analysis/results/${tsCode}`, options)
  return live ? normalizeResults(rows, tsCode) : rows
}

/** POST /api/analysis/backtest — 策略回测 */
export function getBacktest(tsCode, type, params) {
  return request(
    () => mock.getBacktest(tsCode, type, params),
    '/api/analysis/backtest',
    { method: 'POST', body: JSON.stringify({ ts_code: tsCode, type, params }) },
  )
}

/** GET /api/analysis/ranking — 全市场选股排名（ranking_lgb） */
export function getRanking() {
  return request(() => mock.getRanking(), '/api/analysis/ranking')
}

/** GET /api/analysis/ranking/backtest — 排名组合回测 */
export function getRankingBacktest() {
  return request(() => mock.getRankingBacktest(), '/api/analysis/ranking/backtest')
}

/** GET /api/analysis/ranking/probe-selection — 探针法特征筛选过程 */
export function getProbeSelection() {
  return request(() => mock.getProbeSelection(), '/api/analysis/ranking/probe-selection')
}

/** GET /api/analysis/transformer/configs — Transformer 可选项 */
export function getTransformerConfigs() {
  return request(() => mock.TRANSFORMER_CONFIGS, '/api/analysis/transformer/configs')
}

/** POST /api/analysis/transformer/train — 训练历史（mock 直接生成） */
export function getTransformerTraining(config) {
  return request(
    () => mock.getTransformerTraining(config),
    '/api/analysis/transformer/train',
    { method: 'POST', body: JSON.stringify(config) },
  )
}

/** POST /api/analysis/transformer/predict — TopK 预测结果 */
export function getTransformerPredictions(config) {
  return request(
    () => mock.getTransformerPredictions(config),
    '/api/analysis/transformer/predict',
    { method: 'POST', body: JSON.stringify(config) },
  )
}
