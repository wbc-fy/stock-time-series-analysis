import test from 'node:test'
import assert from 'node:assert/strict'
import * as prediction from '../src/utils/prediction.js'

test('fractional returns become percentages exactly once', () => {
  assert.equal(prediction.returnPercent?.(0.01), 1)
  assert.equal(prediction.returnPercent(null), null)
  assert.throws(() => prediction.returnPercent(Infinity))
  assert.throws(() => prediction.returnPercent('0.01'))
})
test('models are stock-bound and empty lists remain empty', () => {
  assert.deepEqual(prediction.normalizeModels?.([], '000001.SZ'), [])
  assert.throws(() => prediction.normalizeModels([{ model_id: 'm', ts_code: '600000.SH' }], '000001.SZ'))
  assert.throws(() => prediction.normalizeModels([{ model_id: 'm', ts_code: '000001.SZ', model_type: 'classifier' }], '000001.SZ'))
})
test('strict DTO rejects invalid numeric JSON and reversed target dates', () => {
  const dto = { metadata: { model_id: 'm', ts_code: '000001.SZ', model_type: 'xgboost', model_name: 'xgboost_regressor', target: 'next_trading_day_close_return' }, test_series: [{ signal_date: '2025-01-01', target_date: '2025-01-02', actual_return: 0.01, predicted_return: 0.02 }], latest: { signal_date: '2025-01-03', target_date: null, actual_return: null, predicted_return: 0.01, horizon: 1 } }
  assert.deepEqual(prediction.normalizePrediction?.(dto, '000001.SZ', 'm'), dto)
  for (const [key, value] of [['model_type', 'classifier'], ['model_name', 'other'], ['target', 'five_day_return']]) assert.throws(() => prediction.normalizePrediction({ ...dto, metadata: { ...dto.metadata, [key]: value } }, '000001.SZ', 'm'))
  assert.throws(() => prediction.normalizePrediction({ ...dto, test_series: [{ ...dto.test_series[0], target_date: '2024-12-31' }] }, '000001.SZ', 'm'))
  assert.throws(() => prediction.normalizePrediction({ ...dto, latest: { ...dto.latest, predicted_return: NaN } }, '000001.SZ', 'm'))
  assert.throws(() => prediction.normalizePrediction({ ...dto, latest: { ...dto.latest, predicted_return: null } }, '000001.SZ', 'm'))
  assert.throws(() => prediction.normalizePrediction({ ...dto, test_series: [{ ...dto.test_series[0], signal_date: '2025-02-30', target_date: '2025-03-03' }] }, '000001.SZ', 'm'))
})
test('result identities distinguish real revisions and reject duplicate or invalid publication IDs', () => {
  const rows = [{ publication_id: 42, model_id: 'm', ts_code: '000001.SZ' }, { publication_id: 41, model_id: 'm', ts_code: '000001.SZ' }]
  assert.deepEqual(prediction.normalizeResults?.(rows, '000001.SZ'), rows)
  for (const id of [0, -1, 1.5, '42', null, undefined]) assert.throws(() => prediction.normalizeResults([{ ...rows[0], publication_id: id }], '000001.SZ'))
  assert.throws(() => prediction.normalizeResults([rows[0], rows[0]], '000001.SZ'))
  assert.deepEqual(prediction.normalizeResults([], '000001.SZ'), [])
})
test('prediction settles while importance remains pending and errors also settle independently', async () => {
  assert.equal(typeof prediction.settleDetailRequest, 'function')
  const run = prediction.createGenerationGuard().next()
  let importanceResolve
  const loading = { prediction: true, importance: true }
  let error = ''
  const importance = prediction.settleDetailRequest(new Promise(resolve => { importanceResolve = resolve }), run, { success() {}, error() {}, settled() { loading.importance = false } })
  await prediction.settleDetailRequest(Promise.resolve('curve'), run, { success(value) { assert.equal(value, 'curve') }, error() {}, settled() { loading.prediction = false } })
  assert.deepEqual(loading, { prediction: false, importance: true })
  await prediction.settleDetailRequest(Promise.reject(new Error('prediction failed')), run, { success() {}, error(e) { error = e.message }, settled() { loading.prediction = false } })
  assert.equal(error, 'prediction failed')
  assert.equal(loading.importance, true)
  importanceResolve('importance')
  await importance
  assert.equal(loading.importance, false)
})
test('late successes and failures cannot overwrite a new selection even when abort is ignored', async () => {
  const guard = prediction.createGenerationGuard()
  let value = 'new'
  let resolveOld
  const first = guard.next()
  const pending = new Promise(resolve => { resolveOld = resolve }).then(result => { if (first.current()) value = result })
  guard.next()
  resolveOld('old')
  await pending
  assert.equal(value, 'new')
  const failure = guard.next()
  guard.cancel()
  await Promise.reject(new Error('old failure')).catch(e => { if (failure.current()) value = e.message })
  assert.equal(value, 'new')
})
test('generation guard invalidates ignored abort responses', () => {
  const guard = prediction.createGenerationGuard?.()
  assert.ok(guard)
  const first = guard.next(); const second = guard.next()
  assert.equal(first.signal.aborted, true)
  assert.equal(first.current(), false)
  assert.equal(second.current(), true)
  guard.cancel()
  assert.equal(second.current(), false)
})
test('API wrappers preserve DTO units, stock-scoped models, full chart limit and external abort', async () => {
  const originalFetch = globalThis.fetch
  const calls = []
  const metadata = { model_id: 'm', ts_code: '000001.SZ', model_type: 'xgboost', model_name: 'xgboost_regressor', target: 'next_trading_day_close_return' }
  const dto = { metadata, test_series: [], latest: { signal_date: '2025-01-03', target_date: null, actual_return: null, predicted_return: 0.01, horizon: 1 } }
  globalThis.fetch = async (url, options) => {
    calls.push({ url, signal: options.signal })
    const body = url.includes('/importance') ? { model_id: 'm', ts_code: '000001.SZ', importance_method: 'gain', feature_importance: [] } : url.includes('/prediction/') ? dto : url.includes('/results/') ? [{ publication_id: 42, model_id: 'm', ts_code: '000001.SZ', latest: dto.latest }] : [metadata]
    return new Response(JSON.stringify(body))
  }
  const { dataSource } = await import('../src/api/client.js')
  const api = await import('../src/api/analysis.js')
  dataSource.value = 'hybrid'
  const controller = new AbortController()
  try {
    assert.deepEqual(await api.getModels('000001.SZ', { signal: controller.signal }), [metadata])
    assert.equal((await api.getPrediction('000001.SZ', 'm', { signal: controller.signal })).latest.predicted_return, 0.01)
    assert.deepEqual((await api.getFeatureImportance('m', 15)).feature_importance, [])
    assert.equal((await api.getAnalysisResults('000001.SZ')).length, 1)
    assert.equal(calls[0].url, '/api/analysis/models?ts_code=000001.SZ')
    assert.equal(calls[1].url, '/api/analysis/prediction/000001.SZ?model=m&limit=500')
    assert.ok(calls[0].signal)
  } finally { dataSource.value = 'mock'; globalThis.fetch = originalFetch }
})
