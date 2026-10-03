/**
 * Mock 数据引擎：seeded 随机游走 K 线生成 + 技术指标真实计算。
 *
 * 所有数据由 ts_code 派生的固定种子生成，同一股票每次刷新数据一致，
 * 保证演示时图表、表格、指标之间互相吻合。
 */

/* ── 随机数 ─────────────────────────────────────────────── */

export function mulberry32(a) {
  return function () {
    a |= 0
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

export function hashSeed(str) {
  let h = 2166136261
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i)
    h = Math.imul(h, 16777619)
  }
  return h >>> 0
}

function gaussian(rand) {
  let u = 0
  let v = 0
  while (u === 0) u = rand()
  while (v === 0) v = rand()
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v)
}

/* ── 股票池（mock，代码与名称为公开常识数据） ────────────── */

export const STOCK_POOL = [
  { ts_code: '000001.SZ', symbol: '000001', name: '平安银行', area: '深圳', industry: '银行', list_date: '1991-04-03', basePrice: 11.2, pe: 5.1, pb: 0.55, totalMv: 21800000 },
  { ts_code: '600519.SH', symbol: '600519', name: '贵州茅台', area: '贵州', industry: '白酒', list_date: '2001-08-27', basePrice: 1465, pe: 24.8, pb: 7.9, totalMv: 184000000 },
  { ts_code: '000858.SZ', symbol: '000858', name: '五粮液', area: '四川', industry: '白酒', list_date: '1998-04-27', basePrice: 128.5, pe: 16.2, pb: 3.6, totalMv: 49900000 },
  { ts_code: '601318.SH', symbol: '601318', name: '中国平安', area: '深圳', industry: '保险', list_date: '2007-03-01', basePrice: 48.6, pe: 8.4, pb: 0.98, totalMv: 88700000 },
  { ts_code: '600036.SH', symbol: '600036', name: '招商银行', area: '深圳', industry: '银行', list_date: '2002-04-09', basePrice: 34.8, pe: 6.3, pb: 0.92, totalMv: 87800000 },
  { ts_code: '000333.SZ', symbol: '000333', name: '美的集团', area: '广东', industry: '家电', list_date: '2013-09-18', basePrice: 62.4, pe: 12.1, pb: 3.1, totalMv: 43500000 },
  { ts_code: '002594.SZ', symbol: '002594', name: '比亚迪', area: '深圳', industry: '汽车', list_date: '2011-06-30', basePrice: 241.7, pe: 19.5, pb: 4.4, totalMv: 70400000 },
  { ts_code: '600900.SH', symbol: '600900', name: '长江电力', area: '北京', industry: '电力', list_date: '2003-11-18', basePrice: 28.9, pe: 20.6, pb: 3.3, totalMv: 70600000 },
  { ts_code: '601899.SH', symbol: '601899', name: '紫金矿业', area: '福建', industry: '有色金属', list_date: '2008-04-25', basePrice: 16.8, pe: 14.3, pb: 3.5, totalMv: 44700000 },
  { ts_code: '300750.SZ', symbol: '300750', name: '宁德时代', area: '福建', industry: '电气设备', list_date: '2018-06-11', basePrice: 182.3, pe: 18.7, pb: 4.1, totalMv: 80200000 },
]

export const INDUSTRY_LIST = [...new Set(STOCK_POOL.map((s) => s.industry))]

/* ── 交易日历 ───────────────────────────────────────────── */

const FIXED_TODAY = '2026-09-25' // 演示用固定“最新交易日”，保证数据稳定

function pad(n) {
  return String(n).padStart(2, '0')
}

function fmtDate(d) {
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
}

export function tradingDates(count, endDate = FIXED_TODAY) {
  const dates = []
  const d = new Date(endDate + 'T00:00:00')
  while (dates.length < count) {
    const day = d.getDay()
    if (day !== 0 && day !== 6) dates.unshift(fmtDate(d))
    d.setDate(d.getDate() - 1)
  }
  return dates
}

export { FIXED_TODAY }

/* ── K 线生成（随机游走） ───────────────────────────────── */

const barsCache = new Map()

export function getBars(tsCode, days = 260) {
  const key = `${tsCode}:${days}`
  if (barsCache.has(key)) return barsCache.get(key)

  const stock = STOCK_POOL.find((s) => s.ts_code === tsCode) || STOCK_POOL[0]
  const rand = mulberry32(hashSeed(tsCode))
  const dates = tradingDates(days)

  const drift = (rand() - 0.42) * 0.0008 // 轻微趋势差异
  const vola = 0.012 + rand() * 0.016 // 年化波动 ~19%-44%
  let close = stock.basePrice * (0.82 + rand() * 0.18)

  const bars = []
  for (let i = 0; i < dates.length; i++) {
    const preClose = close
    // 带均值回归的随机游走，避免价格漂移过远
    const pull = (stock.basePrice - close) / stock.basePrice * 0.02
    const ret = drift + pull + gaussian(rand) * vola
    close = Math.max(0.5, preClose * (1 + ret))
    const open = preClose * (1 + gaussian(rand) * vola * 0.4)
    const hi = Math.max(open, close) * (1 + Math.abs(gaussian(rand)) * vola * 0.5)
    const lo = Math.min(open, close) * (1 - Math.abs(gaussian(rand)) * vola * 0.5)
    const vol = Math.round((300000 + rand() * 900000) * (1 + Math.abs(ret) * 30)) // 手
    const change = close - preClose
    bars.push({
      trade_date: dates[i],
      open: round2(open),
      high: round2(hi),
      low: round2(lo),
      close: round2(close),
      pre_close: round2(preClose),
      change: round2(change),
      pct_chg: round4((change / preClose) * 100),
      vol,
      amount: round2((vol * close) / 10), // 千元
    })
  }
  barsCache.set(key, bars)
  return bars
}

function round2(v) {
  return Math.round(v * 100) / 100
}
function round4(v) {
  return Math.round(v * 10000) / 10000
}
function round6(v) {
  return Math.round(v * 1e6) / 1e6
}

/* ── 指标计算工具 ───────────────────────────────────────── */

function sma(arr, n) {
  const out = new Array(arr.length).fill(null)
  let sum = 0
  for (let i = 0; i < arr.length; i++) {
    sum += arr[i]
    if (i >= n) sum -= arr[i - n]
    if (i >= n - 1) out[i] = sum / n
  }
  return out
}

function ema(arr, n) {
  const out = new Array(arr.length).fill(null)
  const k = 2 / (n + 1)
  let prev = arr[0]
  for (let i = 0; i < arr.length; i++) {
    prev = i === 0 ? arr[0] : arr[i] * k + prev * (1 - k)
    out[i] = prev
  }
  return out
}

function stdev(arr, i, n) {
  if (i < n - 1) return null
  let sum = 0
  for (let j = i - n + 1; j <= i; j++) sum += arr[j]
  const mean = sum / n
  let sq = 0
  for (let j = i - n + 1; j <= i; j++) sq += (arr[j] - mean) ** 2
  return Math.sqrt(sq / n)
}

function rollingMoment(rets, i, n, moment) {
  if (i < n - 1) return null
  const win = rets.slice(i - n + 1, i + 1).filter((v) => v != null)
  if (win.length < n) return null
  const mean = win.reduce((a, b) => a + b, 0) / n
  const sd = Math.sqrt(win.reduce((a, b) => a + (b - mean) ** 2, 0) / n)
  if (sd === 0) return null
  // v2 语义修正：volatility_* 为年化标准差（旧 Variance* 命名已废弃）
  if (moment === 2) return round6(sd * Math.sqrt(252))
  if (moment === 3) return round6(win.reduce((a, b) => a + ((b - mean) / sd) ** 3, 0) / n)
  return round6(win.reduce((a, b) => a + ((b - mean) / sd) ** 4, 0) / n - 3)
}

/* ── 全量特征计算（对齐 stock_features 表字段） ─────────── */

const indicatorCache = new Map()

export function getIndicators(tsCode) {
  if (indicatorCache.has(tsCode)) return indicatorCache.get(tsCode)
  const bars = getBars(tsCode)
  const n = bars.length
  const close = bars.map((b) => b.close)
  const high = bars.map((b) => b.high)
  const low = bars.map((b) => b.low)
  const vol = bars.map((b) => b.vol)
  const open = bars.map((b) => b.open)
  const preClose = bars.map((b) => b.pre_close)
  const r2 = (v) => (v == null ? null : round4(v))

  const ma = { 5: sma(close, 5), 10: sma(close, 10), 20: sma(close, 20), 60: sma(close, 60) }
  const emaMap = {
    5: ema(close, 5), 10: ema(close, 10), 20: ema(close, 20),
    26: ema(close, 26), 60: ema(close, 60), 120: ema(close, 120),
  }

  // MACD: DIF = EMA12 - EMA26, DEA = EMA9(DIF), 柱 = 2*(DIF-DEA)
  const e12 = ema(close, 12)
  const e26 = ema(close, 26)
  const dif = e12.map((v, i) => v - e26[i])
  const dea = ema(dif, 9)
  const hist = dif.map((v, i) => round4((v - dea[i]) * 2))

  // RSI(14) Wilder
  const rsi = new Array(n).fill(null)
  let avgGain = 0
  let avgLoss = 0
  for (let i = 1; i < n; i++) {
    const ch = close[i] - close[i - 1]
    const gain = Math.max(ch, 0)
    const loss = Math.max(-ch, 0)
    if (i <= 14) {
      avgGain += gain / 14
      avgLoss += loss / 14
      if (i === 14) rsi[i] = round4(avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss))
    } else {
      avgGain = (avgGain * 13 + gain) / 14
      avgLoss = (avgLoss * 13 + loss) / 14
      rsi[i] = round4(avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss))
    }
  }

  // BOLL(20, 2)
  const bbUpper = new Array(n).fill(null)
  const bbLower = new Array(n).fill(null)
  const bbWidth = new Array(n).fill(null)
  for (let i = 0; i < n; i++) {
    const sd = stdev(close, i, 20)
    if (sd != null && ma[20][i] != null) {
      bbUpper[i] = round4(ma[20][i] + 2 * sd)
      bbLower[i] = round4(ma[20][i] - 2 * sd)
      bbWidth[i] = round4((4 * sd) / ma[20][i])
    }
  }

  // KDJ(9,3,3)
  const kArr = new Array(n).fill(null)
  const dArr = new Array(n).fill(null)
  const jArr = new Array(n).fill(null)
  let k = 50
  let d = 50
  for (let i = 0; i < n; i++) {
    if (i >= 8) {
      let hh = -Infinity
      let ll = Infinity
      for (let j = i - 8; j <= i; j++) {
        hh = Math.max(hh, high[j])
        ll = Math.min(ll, low[j])
      }
      const rsv = hh === ll ? 50 : ((close[i] - ll) / (hh - ll)) * 100
      k = (2 / 3) * k + (1 / 3) * rsv
      d = (2 / 3) * d + (1 / 3) * k
      kArr[i] = round4(k)
      dArr[i] = round4(d)
      jArr[i] = round4(3 * k - 2 * d)
    }
  }

  // 成交量指标
  const volMa5 = sma(vol, 5)
  const volMa10 = sma(vol, 10)
  const volumeRatio = vol.map((v, i) => (volMa5[i] ? round4(v / volMa5[i]) : null))

  // MFI(14)
  const mfi = new Array(n).fill(null)
  const tp = high.map((h, i) => (h + low[i] + close[i]) / 3)
  for (let i = 14; i < n; i++) {
    let pos = 0
    let neg = 0
    for (let j = i - 13; j <= i; j++) {
      const flow = tp[j] * vol[j]
      if (tp[j] > tp[j - 1]) pos += flow
      else neg += flow
    }
    mfi[i] = neg === 0 ? 100 : round4(100 - 100 / (1 + pos / neg))
  }

  // 收益率与标签
  const ret1 = close.map((c, i) => (i === 0 ? null : c / close[i - 1] - 1))
  const ret5 = close.map((c, i) => (i < 5 ? null : c / close[i - 5] - 1))
  const ret10 = close.map((c, i) => (i < 10 ? null : c / close[i - 10] - 1))
  const logRet = close.map((c, i) => (i === 0 ? null : Math.log(c / close[i - 1])))
  const futRet1 = ret1.map((_, i) => (i + 1 < n ? ret1[i + 1] : null))
  const futRet5 = ret5.map((_, i) => (i + 5 < n ? round6(close[i + 5] / close[i] - 1) : null))
  const futDir1 = futRet1.map((v) => (v == null ? null : v > 0 ? 1 : 0))
  const futDir5 = futRet5.map((v) => (v == null ? null : v > 0 ? 1 : 0))

  // 换手率（mock 日换手，5/60/120 日累计）
  const randT = mulberry32(hashSeed(tsCode + ':turnover'))
  const dailyTurnover = bars.map(() => round4(0.4 + randT() * 3.6))
  const cumTurnover = (win) =>
    dailyTurnover.map((_, i) =>
      i < win - 1 ? null : round4(dailyTurnover.slice(i - win + 1, i + 1).reduce((a, b) => a + b, 0)),
    )

  // 情绪指标 AR / BR (26)
  const ar = new Array(n).fill(null)
  const br = new Array(n).fill(null)
  for (let i = 25; i < n; i++) {
    let ho = 0
    let ol = 0
    let hpc = 0
    let pcl = 0
    for (let j = i - 25; j <= i; j++) {
      ho += high[j] - open[j]
      ol += open[j] - low[j]
      hpc += Math.max(high[j] - preClose[j], 0)
      pcl += Math.max(preClose[j] - low[j], 0)
    }
    ar[i] = ol === 0 ? null : round4((ho / ol) * 100)
    br[i] = pcl === 0 ? null : round4((hpc / pcl) * 100)
  }

  // 多空力道（13 日 EMA）
  const e13 = ema(close, 13)
  const bullPower = high.map((h, i) => round4(h - e13[i]))
  const bearPower = low.map((l, i) => round4(l - e13[i]))

  // BIAS
  const bias = (win) =>
    close.map((c, i) => (ma[win] && ma[win][i] ? round4(((c - ma[win][i]) / ma[win][i]) * 100) : win === 10 || win === 60 ? biasN(c, i, win, close) : null))
  function biasN(c, i, win, arr) {
    const m = sma(arr, win)[i]
    return m ? round4(((c - m) / m) * 100) : null
  }
  const ma10Full = sma(close, 10)
  const ma60Full = sma(close, 60)
  const bias5 = close.map((c, i) => (ma[5][i] ? round4(((c - ma[5][i]) / ma[5][i]) * 100) : null))
  const bias10 = close.map((c, i) => (ma10Full[i] ? round4(((c - ma10Full[i]) / ma10Full[i]) * 100) : null))
  const bias20 = close.map((c, i) => (ma[20][i] ? round4(((c - ma[20][i]) / ma[20][i]) * 100) : null))
  const bias60 = close.map((c, i) => (ma60Full[i] ? round4(((c - ma60Full[i]) / ma60Full[i]) * 100) : null))

  // CCI(n)
  function cciCalc(win) {
    const tpMa = sma(tp, win)
    return tp.map((_, i) => {
      if (i < win - 1 || tpMa[i] == null) return null
      let md = 0
      for (let j = i - win + 1; j <= i; j++) md += Math.abs(tp[j] - tpMa[i])
      md /= win
      return md === 0 ? null : round4((tp[i] - tpMa[i]) / (0.015 * md))
    })
  }
  const cci10 = cciCalc(10)
  const cci15 = cciCalc(15)
  const cci20 = cciCalc(20)
  const cci88 = cciCalc(88)

  // 梅斯线 Mass Index (9日EMA/9日EMA的EMA, 25日累计)
  const emaRange = ema(high.map((h, i) => h - low[i]), 9)
  const emaRange2 = ema(emaRange, 9)
  const massRatio = emaRange.map((v, i) => (emaRange2[i] ? v / emaRange2[i] : null)).map((v) => (v == null ? 0 : v))
  const mass = massRatio.map((_, i) => (i < 24 ? null : round4(massRatio.slice(i - 24, i + 1).reduce((a, b) => a + b, 0))))

  // Aroon(25)
  const aroonUp = new Array(n).fill(null)
  const aroonDown = new Array(n).fill(null)
  for (let i = 24; i < n; i++) {
    let hiIdx = i
    let loIdx = i
    for (let j = i - 24; j <= i; j++) {
      if (high[j] >= high[hiIdx]) hiIdx = j
      if (low[j] <= low[loIdx]) loIdx = j
    }
    aroonUp[i] = round4(((25 - (i - hiIdx)) / 25) * 100)
    aroonDown[i] = round4(((25 - (i - loIdx)) / 25) * 100)
  }

  // 风险指标（滚动方差/偏度/峰度，年化）
  const risk = (win, moment) => ret1.map((_, i) => rollingMoment(ret1, i, win, moment))

  // 技术信号
  const goldenCross = ma[5].map((v, i) =>
    i > 0 && v != null && ma[10][i] != null && ma[5][i - 1] <= ma[10][i - 1] && v > ma[10][i] ? 1 : 0,
  )
  const deathCross = ma[5].map((v, i) =>
    i > 0 && v != null && ma[10][i] != null && ma[5][i - 1] >= ma[10][i - 1] && v < ma[10][i] ? 1 : 0,
  )
  const macdGolden = dif.map((v, i) => (i > 0 && dif[i - 1] <= dea[i - 1] && v > dea[i] ? 1 : 0))
  const rsiOversold = rsi.map((v) => (v != null && v < 30 ? 1 : 0))
  const rsiOverbought = rsi.map((v) => (v != null && v > 70 ? 1 : 0))

  const features = bars.map((b, i) => ({
    ts_code: tsCode,
    trade_date: b.trade_date,
    ma5: r2(ma[5][i]), ma10: r2(ma[10][i]), ma20: r2(ma[20][i]), ma60: r2(ma[60][i]),
    ema5: r2(emaMap[5][i]), ema10: r2(emaMap[10][i]), ema20: r2(emaMap[20][i]),
    ema26: r2(emaMap[26][i]), ema60: r2(emaMap[60][i]), ema120: r2(emaMap[120][i]),
    macd_dif: r2(dif[i]), macd_dea: r2(dea[i]), macd_hist: hist[i],
    rsi: rsi[i],
    bb_upper: bbUpper[i], bb_middle: r2(ma[20][i]), bb_lower: bbLower[i], bb_width: bbWidth[i],
    kdj_k: kArr[i], kdj_d: dArr[i], kdj_j: jArr[i],
    vol_ma5: volMa5[i] == null ? null : Math.round(volMa5[i]),
    vol_ma10: volMa10[i] == null ? null : Math.round(volMa10[i]),
    volume_ratio: volumeRatio[i], mfi14: mfi[i],
    return_1d: ret1[i] == null ? null : round6(ret1[i]),
    return_5d: ret5[i] == null ? null : round6(ret5[i]),
    return_10d: ret10[i] == null ? null : round6(ret10[i]),
    log_return: logRet[i] == null ? null : round6(logRet[i]),
    future_return_1d: futRet1[i] == null ? null : round6(futRet1[i]),
    future_return_5d: futRet5[i],
    future_direction_1d: futDir1[i], future_direction_5d: futDir5[i],
    turnover_rate_5: cumTurnover(5)[i],
    turnover_rate_60: cumTurnover(60)[i],
    turnover_rate_120: cumTurnover(120)[i],
    br: br[i], ar: ar[i],
    volatility_20d: risk(20, 2)[i], volatility_60d: risk(60, 2)[i], volatility_120d: risk(120, 2)[i],
    skewness_20d: risk(20, 3)[i], skewness_60d: risk(60, 3)[i], skewness_120d: risk(120, 3)[i],
    kurtosis_20d: risk(20, 4)[i], kurtosis_60d: risk(60, 4)[i], kurtosis_120d: risk(120, 4)[i],
    arron_up_25: aroonUp[i], arron_down_25: aroonDown[i],
    bear_power: bearPower[i], bull_power: bullPower[i],
    bias5, bias10, bias20, bias60,
    cci10: cci10[i], cci15: cci15[i], cci20: cci20[i], cci88: cci88[i],
    cr20: br[i] == null ? null : round4(br[i] * 0.92),
    mass: mass[i],
    golden_cross: goldenCross[i], death_cross: deathCross[i],
    macd_golden_cross: macdGolden[i],
    rsi_oversold: rsiOversold[i], rsi_overbought: rsiOverbought[i],
  }))
  // bias 数组整体挂到每行
  features.forEach((f, i) => {
    f.bias5 = bias5[i]
    f.bias10 = bias10[i]
    f.bias20 = bias20[i]
    f.bias60 = bias60[i]
  })

  indicatorCache.set(tsCode, features)
  return features
}

/* ── stock_daily_basic 行生成 ───────────────────────────── */

export function getDailyBasic(tsCode, tradeDate) {
  const stock = STOCK_POOL.find((s) => s.ts_code === tsCode) || STOCK_POOL[0]
  const rand = mulberry32(hashSeed(tsCode + tradeDate))
  const bars = getBars(tsCode)
  const bar = bars.find((b) => b.trade_date === tradeDate) || bars[bars.length - 1]
  const ratio = bar.close / stock.basePrice
  return {
    ts_code: tsCode,
    trade_date: tradeDate,
    turnover_rate: round4(0.4 + rand() * 3.6),
    pe: round4(stock.pe * ratio * (0.95 + rand() * 0.1)),
    pe_ttm: round4(stock.pe * ratio * (0.96 + rand() * 0.1)),
    pb: round4(stock.pb * ratio * (0.97 + rand() * 0.06)),
    ps: round4(stock.pb * 0.31 * ratio * (0.95 + rand() * 0.1)),
    total_mv: Math.round(stock.totalMv * ratio),
    source: 'TUSHARE',
  }
}

/* ── 横截面特征（对齐 panel_builder.compute_cross_sectional_features） ── */

const crossCache = new Map()

/**
 * 计算某交易日全池横截面特征，返回 { ts_code: {...8 个特征} }。
 * market_* 为全池等权均值；industry_* 为同行业等权均值；zscore 为行业内标准化。
 */
export function getCrossSectional(tradeDate) {
  if (crossCache.has(tradeDate)) return crossCache.get(tradeDate)

  const rows = STOCK_POOL.map((s) => {
    const feats = getIndicators(s.ts_code)
    const row = feats.find((f) => f.trade_date === tradeDate) || feats[feats.length - 1]
    return { ts_code: s.ts_code, industry: s.industry, row }
  })

  const mean = (arr) => (arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null)
  const sd = (arr) => {
    const m = mean(arr)
    return arr.length > 1 ? Math.sqrt(arr.reduce((a, b) => a + (b - m) ** 2, 0) / (arr.length - 1)) : 0
  }

  const marketReturn = mean(rows.map((r) => r.row.return_1d).filter((v) => v != null))
  const marketVol = mean(rows.map((r) => r.row.volatility_20d).filter((v) => v != null))

  const out = {}
  for (const r of rows) {
    const peers = rows.filter((x) => x.industry === r.industry)
    const industryReturn = mean(peers.map((x) => x.row.return_1d).filter((v) => v != null))
    const peerRet = peers.map((x) => x.row.return_1d).filter((v) => v != null)
    const peerVol = peers.map((x) => x.row.volatility_20d).filter((v) => v != null)
    const retSd = sd(peerRet)
    const volSd = sd(peerVol)
    out[r.ts_code] = {
      market_return: marketReturn == null ? null : round6(marketReturn),
      market_volatility_20d: marketVol == null ? null : round6(marketVol),
      industry_return: industryReturn == null ? null : round6(industryReturn),
      return_vs_market: round6((r.row.return_1d ?? 0) - (marketReturn ?? 0)),
      return_vs_industry: round6((r.row.return_1d ?? 0) - (industryReturn ?? 0)),
      volatility_vs_market: round6((r.row.volatility_20d ?? 0) - (marketVol ?? 0)),
      return_zscore_industry: retSd === 0 ? 0 : round4(((r.row.return_1d ?? 0) - (industryReturn ?? 0)) / retSd),
      volatility_zscore_industry: volSd === 0 ? 0 : round4(((r.row.volatility_20d ?? 0) - mean(peerVol)) / volSd),
    }
  }
  crossCache.set(tradeDate, out)
  return out
}
