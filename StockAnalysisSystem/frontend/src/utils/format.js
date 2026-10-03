/** 数字/时间格式化工具 */

export function fmtNum(v, digits = 2) {
  if (v == null || Number.isNaN(v)) return '—'
  return Number(v).toFixed(digits)
}

export function fmtInt(v) {
  if (v == null || Number.isNaN(v)) return '—'
  return Math.round(Number(v)).toLocaleString('zh-CN')
}

/** 比例 → 百分比字符串（0.0583 → 5.83%） */
export function fmtPct(v, digits = 2) {
  if (v == null || Number.isNaN(v)) return '—'
  return (Number(v) * 100).toFixed(digits) + '%'
}

/** 已经是百分数的值（5.83 → 5.83%） */
export function fmtPctRaw(v, digits = 2) {
  if (v == null || Number.isNaN(v)) return '—'
  return Number(v).toFixed(digits) + '%'
}

/** 万元金额 → 亿元显示 */
export function fmtWanToYi(v) {
  if (v == null || Number.isNaN(v)) return '—'
  return (Number(v) / 10000).toFixed(2) + ' 亿'
}

/** 大数缩写：1234567 → 123.5万 */
export function fmtCompact(v) {
  if (v == null || Number.isNaN(v)) return '—'
  const n = Number(v)
  if (Math.abs(n) >= 1e8) return (n / 1e8).toFixed(2) + '亿'
  if (Math.abs(n) >= 1e4) return (n / 1e4).toFixed(1) + '万'
  return n.toLocaleString('zh-CN')
}

export function fmtTime(iso) {
  if (!iso) return '—'
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return '—'
  return new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
  }).format(date)
}

export const signClass = (v) => (v > 0 ? 'up' : v < 0 ? 'down' : '')
