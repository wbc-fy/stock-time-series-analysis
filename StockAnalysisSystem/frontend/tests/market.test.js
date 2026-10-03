import test from 'node:test'
import assert from 'node:assert/strict'
import { normalizeKline, normalizeIndicators, alignIndicators } from '../src/utils/market.js'

test('market validates decimal OHLC, nullable values, ordering and code', () => {
  const bar = { trade_date: '20260529', open: '11', high: '12', low: '10', close: '11.2', pct_chg: null }
  assert.equal(normalizeKline({ ts_code: '000001.SZ', bars: [bar] }, '000001.SZ')[0].close, 11.2)
  assert.throws(() => normalizeKline({ ts_code: '000002.SZ', bars: [bar] }, '000001.SZ'))
  assert.throws(() => normalizeKline({ ts_code: '000001.SZ', bars: [{ ...bar, open: null }] }, '000001.SZ'))
  assert.throws(() => normalizeKline({ ts_code: '000001.SZ', bars: [bar, bar] }, '000001.SZ'))
})

test('sparse indicators align by date without manufacturing MA values', () => {
  const rows = normalizeIndicators({ ts_code: '000001.SZ', indicators: [
    { trade_date: '20260529', ma5: '11.1', ma10: null, ma20: '11.022', window_size: 20, is_warmup: false },
    { trade_date: '20260429', ma5: null, ma10: null, ma20: null, window_size: 1, is_warmup: true },
  ] }, '000001.SZ')
  assert.equal(rows[0].trade_date, '20260429')
  assert.deepEqual(alignIndicators([{ trade_date: '20260429' }, { trade_date: '20260501' }, { trade_date: '20260529' }], rows).ma20, [null, null, 11.022])
  assert.throws(() => normalizeIndicators({ ts_code: '000001.SZ', indicators: [rows[0], rows[0]] }, '000001.SZ'))
})

test('ISO API dates and compact demo dates normalize to a shared chronological key', () => {
  const data = normalizeKline({ ts_code: '000001.SZ', bars: [{ trade_date: '2026-05-29', open: 1, high: 2, low: 1, close: 2 }] }, '000001.SZ')
  assert.equal(data[0].trade_date, '20260529')
})
