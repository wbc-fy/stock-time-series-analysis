import test from 'node:test'
import assert from 'node:assert/strict'
import { fmtTime } from '../src/utils/format.js'

test('监控时间统一按上海时区显示，UTC 和 +08:00 表示同一时刻', () => {
  assert.equal(fmtTime('2026-10-03T04:52:38Z'), '2026-10-03 12:52:38')
  assert.equal(fmtTime('2026-10-03T12:52:38+08:00'), '2026-10-03 12:52:38')
  assert.equal(fmtTime(null), '—')
  assert.equal(fmtTime('invalid'), '—')
})
