/**
 * Mock：python-collector 数据采集服务。
 * 对齐 app/main.py 的 8 个 CLI 子命令、collection_task 状态机
 * （PENDING → RUNNING → SUCCESS / PARTIAL_SUCCESS / FAILED）与四张数据表。
 */
import {
  STOCK_POOL, INDUSTRY_LIST, FIXED_TODAY, getBars, getDailyBasic,
  mulberry32, hashSeed, tradingDates,
} from './generator.js'

/* ── CLI 命令定义（对齐 build_parser） ─────────────────── */

export const COMMAND_DEFS = [
  { cmd: 'daily', label: '单日采集', desc: '采集指定交易日 OHLCV 日线', params: [{ name: 'date', type: 'date', required: true }] },
  { cmd: 'daily-basic', label: '每日指标', desc: '采集指定交易日的估值和换手率', params: [{ name: 'date', type: 'date', required: true }] },
  { cmd: 'daily-market', label: '日线+指标', desc: '同日分别提交 OHLCV 和 daily-basic，独立事务互不回滚', params: [{ name: 'date', type: 'date', required: true }] },
  { cmd: 'history', label: '历史回补', desc: '回补指定日期范围的日线数据', params: [{ name: 'start', type: 'date', required: true }, { name: 'end', type: 'date', required: true }] },
  { cmd: 'daily-basic-history', label: '指标补采', desc: '只针对 stock_daily 已存在的交易日逐日补采 daily-basic', params: [{ name: 'start', type: 'date', required: true }, { name: 'end', type: 'date', required: true }] },
  { cmd: 'basic', label: '基础信息', desc: '更新股票基础资料（stock_basic 全量）', params: [] },
  { cmd: 'constituent', label: '成分股快照', desc: '保存指数或行业成分股快照', params: [{ name: 'type', type: 'select', options: ['index', 'industry'], required: true }, { name: 'code', type: 'text', required: true, placeholder: '如 000300.SH / 银行' }, { name: 'date', type: 'date', required: true }] },
  { cmd: 'retry-failed', label: '重试失败任务', desc: '重试当前数据源的所有 FAILED 任务', params: [] },
]

export const OUTPUT_MODES = [
  { mode: 'mysql', desc: '只写 MySQL；默认值和 Kafka 故障回滚模式' },
  { mode: 'kafka', desc: '只发送 Kafka；完整分析数据仍依赖已存在的 MySQL 数据' },
  { mode: 'dual', desc: '同时写 MySQL 和发送 Kafka' },
]

export const CURRENT_OUTPUT_MODE = 'dual'

/* ── collection_task 任务状态机 ─────────────────────────── */

let taskSeq = 100

function ts(dateStr, h, m, s) {
  return `${dateStr}T${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`
}

const TASK_ERRORS = [
  'Tushare API 返回异常: 抱歉，您每分钟最多访问该接口500次',
  'AkShare 请求超时: HTTPSConnectionPool(host=push2his.eastmoney.com): Read timed out (30s)',
  'Kafka 发送失败: Broker not available (dual 模式 MySQL 侧已提交)',
  '数据校验失败: trade_date 不在交易日历内',
]

function makeTask(partial) {
  const dates = tradingDates(30)
  const rand = mulberry32(hashSeed('tasks') + (taskSeq += 1) * 7919)
  const types = ['daily', 'daily_basic', 'constituent', 'history', 'basic', 'retry']
  const type = partial.task_type || types[Math.floor(rand() * types.length)]
  const date = partial.business_date || dates[Math.floor(rand() * dates.length)]
  const businessDate =
    type === 'constituent'
      ? `index:000300.SH:${date.replaceAll('-', '')}`
      : date.replaceAll('-', '')
  const status = partial.status || ['SUCCESS', 'SUCCESS', 'SUCCESS', 'SUCCESS', 'SUCCESS', 'PARTIAL_SUCCESS', 'FAILED', 'PENDING'][Math.floor(rand() * 8)]
  const day = date
  const startH = 9 + Math.floor(rand() * 9)
  const elapsed = Math.floor(rand() * 240) + 5
  return {
    id: partial.id ?? taskSeq,
    task_type: type,
    business_date: businessDate,
    source: partial.source || (rand() > 0.35 ? 'tushare' : 'akshare'),
    status,
    record_count: status === 'SUCCESS' ? Math.floor(rand() * 5200) + 100 : status === 'PARTIAL_SUCCESS' ? Math.floor(rand() * 2600) + 50 : 0,
    retry_count: status === 'FAILED' ? Math.floor(rand() * 3) : 0,
    error_message: status === 'FAILED' ? TASK_ERRORS[Math.floor(rand() * TASK_ERRORS.length)] : status === 'PARTIAL_SUCCESS' ? 'Kafka 发送失败: Broker not available (MySQL 侧已成功)' : null,
    started_at: status === 'PENDING' ? null : ts(day, startH, Math.floor(rand() * 60), 0),
    finished_at: status === 'PENDING' || status === 'RUNNING' ? null : ts(day, startH, Math.floor(rand() * 20) + 20, elapsed % 60),
    created_at: ts(day, startH, 0, 0),
    updated_at: ts(day, startH + 1, 0, 0),
  }
}

let tasks = Array.from({ length: 24 }, () => makeTask({}))
  .sort((a, b) => b.created_at.localeCompare(a.created_at))

const timers = []

/** 提交任务：立即返回 PENDING 任务，后台按状态机流转（演示用 setTimeout 模拟）。 */
export function submitTask(cmd, params) {
  const now = new Date()
  const dateStr = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`
  const taskType = { daily: 'daily', 'daily-basic': 'daily_basic', 'daily-market': 'daily', history: 'history', 'daily-basic-history': 'daily_basic', basic: 'basic', constituent: 'constituent', 'retry-failed': 'retry' }[cmd] || 'daily'
  let businessDate = dateStr.replaceAll('-', '')
  if (cmd === 'constituent' && params.type && params.code) {
    businessDate = `${params.type}:${params.code}:${(params.date || dateStr).replaceAll('-', '')}`
  } else if ((cmd === 'history' || cmd === 'daily-basic-history') && params.start) {
    businessDate = `${params.start.replaceAll('-', '')}~${(params.end || dateStr).replaceAll('-', '')}`
  } else if (params.date) {
    businessDate = params.date.replaceAll('-', '')
  }

  const rand = Math.random
  const task = {
    id: ++taskSeq,
    task_type: taskType,
    business_date: businessDate,
    source: params.source || 'tushare',
    status: 'PENDING',
    record_count: 0,
    retry_count: 0,
    error_message: null,
    started_at: null,
    finished_at: null,
    created_at: now.toISOString().slice(0, 19),
    updated_at: now.toISOString().slice(0, 19),
  }
  tasks = [task, ...tasks]

  const finish = () => {
    const fail = rand() < 0.1
    if (cmd === 'daily-market' && !fail && rand() < 0.15) {
      task.status = 'PARTIAL_SUCCESS'
      task.record_count = Math.floor(rand() * 5000) + 2000
      task.error_message = 'Kafka 发送失败: Broker not available (MySQL 侧已成功)'
    } else if (fail) {
      task.status = 'FAILED'
      task.error_message = TASK_ERRORS[Math.floor(rand() * TASK_ERRORS.length)]
    } else {
      task.status = 'SUCCESS'
      task.record_count = cmd === 'basic' ? STOCK_POOL.length * 523 : Math.floor(rand() * 5200) + 100
    }
    task.finished_at = new Date().toISOString().slice(0, 19)
    task.updated_at = task.finished_at
  }

  timers.push(setTimeout(() => {
    task.status = 'RUNNING'
    task.started_at = new Date().toISOString().slice(0, 19)
    timers.push(setTimeout(finish, 1500 + rand() * 2500))
  }, 700))

  return task
}

/** retry-failed：重试所有 FAILED 任务，恢复为 SUCCESS（retry_count+1）。 */
export function retryFailedTasks(source) {
  const failed = tasks.filter((t) => t.status === 'FAILED' && (!source || t.source === source))
  failed.forEach((t) => {
    t.status = 'RUNNING'
    t.retry_count += 1
    t.started_at = new Date().toISOString().slice(0, 19)
    timers.push(setTimeout(() => {
      t.status = 'SUCCESS'
      t.error_message = null
      t.record_count = Math.floor(Math.random() * 5200) + 100
      t.finished_at = new Date().toISOString().slice(0, 19)
    }, 1800))
  })
  return { retried: failed.length }
}

/** 单任务重试（任务表行内按钮）。 */
export function retryTask(id) {
  const t = tasks.find((x) => x.id === id)
  if (!t || t.status !== 'FAILED') return { ok: false }
  t.status = 'RUNNING'
  t.retry_count += 1
  t.started_at = new Date().toISOString().slice(0, 19)
  timers.push(setTimeout(() => {
    t.status = 'SUCCESS'
    t.error_message = null
    t.record_count = Math.floor(Math.random() * 5200) + 100
    t.finished_at = new Date().toISOString().slice(0, 19)
  }, 1800))
  return { ok: true }
}

export function listTasks() {
  return tasks.map((t) => ({ ...t }))
}

export function taskSummary() {
  const by = (s) => tasks.filter((t) => t.status === s).length
  return {
    total: tasks.length,
    success: by('SUCCESS'),
    partial: by('PARTIAL_SUCCESS'),
    failed: by('FAILED'),
    running: by('RUNNING'),
    pending: by('PENDING'),
    todayRecords: tasks.filter((t) => t.status === 'SUCCESS' || t.status === 'PARTIAL_SUCCESS').reduce((a, t) => a + t.record_count, 0),
  }
}

/* ── 四张数据表预览 ─────────────────────────────────────── */

export function getTableData(table) {
  const date = FIXED_TODAY
  if (table === 'stock_basic') {
    const rand = mulberry32(hashSeed('basic-extra'))
    const extra = Array.from({ length: 30 }, (_, i) => {
      const code = String(600000 + Math.floor(rand() * 900) * 7 + i * 13).slice(0, 6)
      const suffix = rand() > 0.5 ? '.SH' : '.SZ'
      const names = ['华讯科技', '东海航运', '南岭水泥', '北辰地产', '中天能源', '海润光伏', '金丰粮油', '远大重工', '楚天高速', '三江购物']
      return {
        ts_code: code + suffix, symbol: code,
        name: names[i % names.length], area: ['上海', '江苏', '广东', '浙江', '北京'][i % 5],
        industry: INDUSTRY_LIST[i % INDUSTRY_LIST.length],
        list_date: `${1995 + (i % 28)}-0${(i % 9) + 1}-1${i % 9}`,
        source: 'TUSHARE',
      }
    })
    return [...STOCK_POOL.map((s) => ({ ...s, source: 'TUSHARE' })), ...extra]
  }
  if (table === 'stock_daily') {
    return STOCK_POOL.map((s) => {
      const bars = getBars(s.ts_code)
      return { ts_code: s.ts_code, ...bars[bars.length - 1] }
    })
  }
  if (table === 'stock_daily_basic') {
    return STOCK_POOL.map((s) => getDailyBasic(s.ts_code, date))
  }
  if (table === 'stock_constituent') {
    const rand = mulberry32(hashSeed('constituent'))
    const rows = STOCK_POOL.map((s) => ({
      group_type: 'index', group_code: '000300.SH', ts_code: s.ts_code,
      as_of_date: date, weight: Math.round(rand() * 400) / 100, source: 'TUSHARE',
    }))
    STOCK_POOL.filter((s) => s.industry === '银行').forEach((s) => {
      rows.push({ group_type: 'industry', group_code: '银行', ts_code: s.ts_code, as_of_date: date, weight: Math.round(rand() * 2000) / 100, source: 'TUSHARE' })
    })
    return rows
  }
  return []
}
