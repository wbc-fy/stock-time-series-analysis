export function returnPercent(value) {
  if (value === null) return null
  if (typeof value !== 'number' || !Number.isFinite(value)) throw new Error('预测收益率无效')
  return value * 100
}
export function finiteJson(value) {
  if (typeof value === 'number' && !Number.isFinite(value)) throw new Error('预测数据包含非有限数值')
  if (value && typeof value === 'object') Object.values(value).forEach(finiteJson)
  return value
}
function validateModelBinding(row, code) {
  if (!row || row.ts_code !== code || !/^[A-Za-z0-9_-]+$/.test(row.model_id)) throw new Error('模型与股票不匹配')
  finiteJson(row)
}
function validateRegressionMetadata(row, code) {
  validateModelBinding(row, code)
  if (row.model_type !== 'xgboost' || row.model_name !== 'xgboost_regressor' || row.target !== 'next_trading_day_close_return') throw new Error('模型回归口径无效')
}
export function normalizeModels(rows, code) {
  if (!Array.isArray(rows)) throw new Error('模型列表格式无效')
  rows.forEach(row => validateRegressionMetadata(row, code))
  return rows
}
export function normalizeResults(rows, code) {
  if (!Array.isArray(rows)) throw new Error('发布摘要格式无效')
  const ids = new Set()
  rows.forEach(row => {
    validateModelBinding(row, code)
    if (!Number.isSafeInteger(row.publication_id) || row.publication_id <= 0 || ids.has(row.publication_id)) throw new Error('发布标识无效或重复')
    ids.add(row.publication_id)
  })
  return rows
}
const date = value => typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) && !Number.isNaN(Date.parse(value)) && new Date(value).toISOString().slice(0, 10) === value
export function normalizePrediction(dto, code, id) {
  finiteJson(dto)
  if (dto?.metadata?.ts_code !== code || dto.metadata.model_id !== id || !Array.isArray(dto.test_series)) throw new Error('预测模型与股票不匹配')
  validateRegressionMetadata(dto.metadata, code)
  let previous = ''
  dto.test_series.forEach(row => {
    if (!date(row.signal_date) || !date(row.target_date) || row.target_date <= row.signal_date || row.signal_date <= previous || row.actual_return === null) throw new Error('测试日期或实际值无效')
    if (row.predicted_return === null) throw new Error('预测收益率无效')
    returnPercent(row.actual_return); returnPercent(row.predicted_return); previous = row.signal_date
  })
  if (dto.latest !== null) {
    const row = dto.latest
    if (!row || !date(row.signal_date) || row.horizon !== 1 || row.actual_return !== null || (row.target_date !== null && (!date(row.target_date) || row.target_date <= row.signal_date))) throw new Error('最新预测格式无效')
    if (row.predicted_return === null) throw new Error('预测收益率无效')
    returnPercent(row.predicted_return)
  }
  return dto
}
export async function settleDetailRequest(promise, run, handlers) {
  try {
    const result = await promise
    if (run.current()) handlers.success(result)
  } catch (error) {
    if (run.current() && error.name !== 'AbortError') handlers.error(error)
  } finally {
    if (run.current()) handlers.settled()
  }
}
export function createGenerationGuard() {
  let generation = 0, controller
  const cancel = () => { generation++; controller?.abort() }
  return { cancel, next() { cancel(); controller = new AbortController(); const run = generation; return { signal: controller.signal, current: () => generation === run } } }
}
