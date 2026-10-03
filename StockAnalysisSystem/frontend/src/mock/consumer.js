/**
 * Mock：java kafka-consumer-service。
 * 对齐 ConsumerStatisticsSnapshot（10 字段）、ConsumerError（7 字段）、
 * topic 配置（stock.ods.daily.v1 / stock.ods.basic.v1 / stock.dead-letter.v1）
 * 与 StockDailyEventValidator 校验规则。
 */
import { mulberry32, hashSeed, STOCK_POOL, FIXED_TODAY } from './generator.js'

/* ── 消费统计（进程内存计数，每次读取随时间推进，模拟轮询实时增长） ── */

const BASE_TIME = Date.now()
let lastAdvance = BASE_TIME

const stats = {
  totalConsumed: 1284503,
  successCount: 1284390,
  failureCount: 113,
  dailyEventCount: 1283988,
  basicEventCount: 515,
  jsonParseFailureCount: 21,
  validationFailureCount: 74,
  unsupportedSchemaCount: 15,
  deadLetterPublishFailureCount: 3,
  lastConsumedTime: null,
}

function isoNow() {
  return new Date().toISOString().replace('Z', '+00:00')
}
stats.lastConsumedTime = isoNow()

function advance() {
  const now = Date.now()
  const dt = (now - lastAdvance) / 1000
  lastAdvance = now
  if (dt <= 0) return
  const rand = mulberry32(now >>> 0)
  const rate = 12 + rand() * 30 // 条/秒
  const inc = Math.round(rate * Math.min(dt, 60))
  if (inc <= 0) return
  stats.totalConsumed += inc
  const fail = rand() < 0.06 ? Math.ceil(rand() * 2) : 0
  stats.successCount += inc - fail
  stats.failureCount += fail
  if (rand() < 0.9) stats.dailyEventCount += inc - fail
  else stats.basicEventCount += inc - fail
  if (fail > 0) {
    if (rand() < 0.2) stats.jsonParseFailureCount += 1
    else if (rand() < 0.15) stats.unsupportedSchemaCount += 1
    else stats.validationFailureCount += fail
  }
  stats.lastConsumedTime = isoNow()
}

export function getStatistics() {
  advance()
  return { ...stats }
}

/* ── Topic 面板 ─────────────────────────────────────────── */

export function getTopics() {
  advance()
  const rand = mulberry32(hashSeed('topics' + Math.floor(Date.now() / 5000)))
  return [
    {
      name: 'stock.ods.daily.v1', role: '消费', partitions: 3,
      consumerGroup: 'stock-daily-consumer',
      lag: Math.floor(rand() * 120), consumedTotal: stats.dailyEventCount,
      state: 'STABLE',
    },
    {
      name: 'stock.ods.basic.v1', role: '消费', partitions: 1,
      consumerGroup: 'stock-daily-consumer',
      lag: Math.floor(rand() * 15), consumedTotal: stats.basicEventCount,
      state: 'STABLE',
    },
    {
      name: 'stock.dead-letter.v1', role: 'DLT 死信', partitions: 1,
      consumerGroup: '—',
      lag: 0, consumedTotal: stats.failureCount - stats.deadLetterPublishFailureCount,
      state: 'ACTIVE',
    },
  ]
}

/* ── 最近错误（ConsumerError 7 字段；不含原始 payload） ──── */

const ERROR_TEMPLATES = [
  { errorType: 'VALIDATION_FAILURE', message: 'tradeDate must not be null', topic: 'stock.ods.daily.v1' },
  { errorType: 'VALIDATION_FAILURE', message: 'kafkaKey must equal tsCode', topic: 'stock.ods.daily.v1' },
  { errorType: 'VALIDATION_FAILURE', message: 'close must be greater than or equal to zero', topic: 'stock.ods.daily.v1' },
  { errorType: 'VALIDATION_FAILURE', message: 'traceId must not be blank', topic: 'stock.ods.daily.v1' },
  { errorType: 'JSON_PARSE_FAILURE', message: 'Unexpected character (\'o\' (code 111)): was expecting double-quote to start field name', topic: 'stock.ods.daily.v1' },
  { errorType: 'JSON_PARSE_FAILURE', message: 'Unexpected end-of-input: expected close marker for Object', topic: 'stock.ods.basic.v1' },
  { errorType: 'UNSUPPORTED_SCHEMA_VERSION', message: 'Unsupported schemaVersion 2, supported: 1', topic: 'stock.ods.daily.v1' },
  { errorType: 'DEAD_LETTER_PUBLISH_FAILURE', message: 'Failed to publish to stock.dead-letter.v1: Broker not available', topic: 'stock.ods.daily.v1' },
  { errorType: 'VALIDATION_FAILURE', message: 'volume must be greater than or equal to zero', topic: 'stock.ods.daily.v1' },
  { errorType: 'VALIDATION_FAILURE', message: 'eventId must not be blank', topic: 'stock.ods.basic.v1' },
]

let errors = null

export function getErrors() {
  if (!errors) {
    const rand = mulberry32(hashSeed('consumer-errors'))
    errors = ERROR_TEMPLATES.map((t, i) => {
      const stock = STOCK_POOL[Math.floor(rand() * STOCK_POOL.length)]
      const minutesAgo = Math.floor(rand() * 720) + i * 7
      const d = new Date(Date.now() - minutesAgo * 60000)
      return {
        timestamp: d.toISOString().slice(0, 19) + '+08:00',
        errorType: t.errorType,
        message: t.message,
        topic: t.topic,
        key: stock.ts_code,
        partition: Math.floor(rand() * 3),
        offset: 480000 + Math.floor(rand() * 900000),
      }
    }).sort((a, b) => b.timestamp.localeCompare(a.timestamp))
  }
  return errors.map((e) => ({ ...e }))
}

/* ── 消费趋势（最近 24 个 5 分钟桶） ───────────────────── */

export function getConsumeTrend() {
  const rand = mulberry32(hashSeed('trend-' + Math.floor(Date.now() / 30000)))
  const buckets = []
  for (let i = 23; i >= 0; i--) {
    const d = new Date(Date.now() - i * 5 * 60000)
    const label = `${String(d.getHours()).padStart(2, '0')}:${String(Math.floor(d.getMinutes() / 5) * 5).padStart(2, '0')}`
    const base = 3000 + Math.round(rand() * 4500)
    const fail = rand() < 0.3 ? Math.ceil(rand() * 6) : 0
    buckets.push({ time: label, success: base - fail, failure: fail })
  }
  return buckets
}

/* ── 事件校验模拟器（前端复刻 StockDailyEventValidator） ── */

const TEXT_FIELDS = ['eventId', 'traceId', 'tsCode', 'source']
const VALUE_FIELDS = ['tradeDate', 'open', 'high', 'low', 'preClose', 'eventTime', 'ingestTime', 'schemaVersion']
const NON_NEGATIVE = ['close', 'volume', 'amount']
const SUPPORTED_SCHEMA_VERSION = 1

export function validateEvent(payloadText, kafkaKey, eventType = 'daily') {
  let event
  try {
    event = JSON.parse(payloadText)
  } catch (e) {
    return { valid: false, errorType: 'JSON_PARSE_FAILURE', violations: [e.message], unsupportedSchema: false }
  }
  if (event.schemaVersion != null && event.schemaVersion !== SUPPORTED_SCHEMA_VERSION) {
    return {
      valid: false,
      errorType: 'UNSUPPORTED_SCHEMA_VERSION',
      violations: [`Unsupported schemaVersion ${event.schemaVersion}, supported: ${SUPPORTED_SCHEMA_VERSION}`],
      unsupportedSchema: true,
    }
  }
  const violations = []
  for (const f of TEXT_FIELDS) {
    const v = event[f]
    if (v == null || (typeof v === 'string' && v.trim() === '')) violations.push(`${f} must not be blank`)
  }
  for (const f of VALUE_FIELDS) {
    if (event[f] == null) violations.push(`${f} must not be null`)
  }
  for (const f of NON_NEGATIVE) {
    const v = event[f]
    if (v == null) violations.push(`${f} must not be null`)
    else if (typeof v === 'number' && v < 0) violations.push(`${f} must be greater than or equal to zero`)
  }
  if (eventType === 'daily') {
    if (kafkaKey == null || kafkaKey !== event.tsCode) violations.push('kafkaKey must equal tsCode')
  }
  return {
    valid: violations.length === 0,
    errorType: violations.length ? 'VALIDATION_FAILURE' : null,
    violations,
    unsupportedSchema: false,
  }
}

/* ── 示例事件（docs/examples 真实样例 + 异常构造） ───────── */

export function getSampleEvents() {
  const validDaily = {
    eventId: 'TUSHARE:000001.SZ:20260703',
    traceId: 'daily-tushare-20260703-10001',
    tsCode: '000001.SZ',
    tradeDate: '2026-07-03',
    open: 10.25, high: 10.68, low: 10.12, close: 10.55,
    preClose: 10.2, change: 0.35, pctChg: 3.4314,
    volume: 1250345.0, amount: 13054890.25,
    source: 'TUSHARE',
    eventTime: '2026-07-03T15:00:00+08:00',
    ingestTime: '2026-07-03T16:20:30+08:00',
    schemaVersion: 1,
  }
  const validBasic = {
    eventId: 'TUSHARE:000001.SZ:BASIC',
    traceId: 'basic-tushare-20260814-10001',
    tsCode: '000001.SZ',
    symbol: '000001', name: '平安银行', area: '深圳', industry: '银行',
    listDate: '1991-04-03',
    source: 'TUSHARE',
    eventTime: '2026-08-14T09:00:00+08:00',
    ingestTime: '2026-08-14T09:00:02+08:00',
    schemaVersion: 1,
  }
  return [
    { id: 'valid-daily', label: '合法日线事件', eventType: 'daily', kafkaKey: '000001.SZ', payload: validDaily },
    { id: 'valid-basic', label: '合法基础信息事件', eventType: 'basic', kafkaKey: '000001.SZ', payload: validBasic },
    {
      id: 'missing-fields', label: '字段缺失/负值', eventType: 'daily', kafkaKey: '000001.SZ',
      payload: { ...validDaily, traceId: '', tradeDate: null, close: -1.5, eventId: 'TUSHARE:000001.SZ:BAD' },
    },
    {
      id: 'bad-version', label: '不支持的 schemaVersion', eventType: 'daily', kafkaKey: '000001.SZ',
      payload: { ...validDaily, schemaVersion: 2 },
    },
    {
      id: 'key-mismatch', label: 'kafkaKey 与 tsCode 不一致', eventType: 'daily', kafkaKey: '600519.SH',
      payload: validDaily,
    },
  ]
}

export function getHealth() {
  return {
    status: 'UP',
    components: {
      kafka: { status: 'UP', details: { bootstrapServers: 'localhost:9092', consumerGroup: 'stock-daily-consumer' } },
      db: { status: 'UP', details: { database: 'MySQL', version: '8.0.36' } },
      diskSpace: { status: 'UP' },
      ping: { status: 'UP' },
    },
  }
}

export { FIXED_TODAY }
