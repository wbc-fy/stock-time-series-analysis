export const STOCK_CODE = /^\d{6}\.(SZ|SH|BJ)$/
function number(value, required = false) {
  if (value == null && !required) return null
  if ((typeof value !== 'string' && typeof value !== 'number') || value === '' || !Number.isFinite(Number(value))) throw new Error('行情返回无效数值')
  return Number(value)
}
function rows(data, code, key) {
  if (!STOCK_CODE.test(code) || data?.ts_code !== code || !Array.isArray(data[key])) throw new Error('行情响应格式或股票代码不匹配')
  const seen = new Set()
  return data[key].map(row => {
    if (!row || typeof row.trade_date !== 'string' || !/^(\d{8}|\d{4}-\d{2}-\d{2})$/.test(row.trade_date)) throw new Error('行情日期无效或重复')
    const date = row.trade_date.replaceAll('-', '')
    const iso = `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}`
    if (!Number.isFinite(Date.parse(iso)) || new Date(iso).toISOString().slice(0, 10) !== iso || seen.has(date)) throw new Error('行情日期无效或重复')
    seen.add(date)
    return { ...row, trade_date: date }
  }).sort((a, b) => a.trade_date.localeCompare(b.trade_date))
}
export function normalizeStockPool(data) {
  if (!Array.isArray(data) || data.some(s => !s || !STOCK_CODE.test(s.ts_code) || typeof s.name !== 'string')) throw new Error('股票池响应格式无效')
  return data
}
export function normalizeKline(data, code) {
  return rows(data, code, 'bars').map(row => {
    for (const key of ['open', 'high', 'low', 'close']) row[key] = number(row[key], true)
    for (const key of ['pre_close', 'change', 'pct_chg', 'vol', 'amount']) row[key] = number(row[key])
    return row
  })
}
export function normalizeIndicators(data, code) {
  return rows(data, code, 'indicators').map(row => {
    for (const key of ['ma5', 'ma10', 'ma20', 'close', 'volume', 'pct_chg', 'vol_ma5', 'vol_ma10', 'volume_ratio', 'window_size']) row[key] = number(row[key])
    if (typeof row.is_warmup !== 'boolean') throw new Error('指标预热状态无效')
    return row
  })
}
export function alignIndicators(bars, indicators) {
  const byDate = new Map(indicators.map(row => [row.trade_date, row]))
  return Object.fromEntries(['ma5', 'ma10', 'ma20'].map(key => [key, bars.map(bar => byDate.get(bar.trade_date)?.[key] ?? null)]))
}
