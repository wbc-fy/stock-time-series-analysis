import test from 'node:test'
import assert from 'node:assert/strict'
import { createRequestClient } from '../src/api/client.js'

test('接入模式只请求已实现的监控 API，其余页面保持明确的演示数据', async () => {
  const calls = []
  const request = createRequestClient({ mode: 'hybrid', fetchImpl: async (url) => {
    calls.push(url)
    return new Response(JSON.stringify({ totalConsumed: 12 }), { status: 200 })
  }, mockDelayMs: 0 })
  assert.deepEqual(await request(() => ({ simulated: true }), '/api/consumer/statistics'), { totalConsumed: 12 })
  assert.deepEqual(await request(() => ({ simulated: true }), '/api/analysis/features/catalog'), { simulated: true })
  assert.deepEqual(calls, ['/api/consumer/statistics'])
})

test('prediction exact allowlist forwards abort options and never falls back', async () => {
  const calls = []
  const controller = new AbortController()
  const client = createRequestClient({ mode: 'hybrid', mockDelayMs: 0, fetchImpl: async (url, options) => { calls.push(url); assert.ok(options.signal); return new Response('[]') } })
  for (const path of ['/api/analysis/models?ts_code=000001.SZ', '/api/analysis/models/m-1/importance?top=15', '/api/analysis/prediction/000001.SZ?model=m-1&limit=500', '/api/analysis/results/000001.SZ']) assert.deepEqual(await client(() => 'mock', path, { signal: controller.signal }), [])
  assert.equal(await client(() => 'mock', '/api/analysis/models/m-1/train'), 'mock')
  assert.equal(calls.length, 4)
  const failing = createRequestClient({ mode: 'hybrid', fetchImpl: async () => new Response('{}', { status: 503 }) })
  await assert.rejects(failing(() => 'mock', '/api/analysis/prediction/000001.SZ?model=m'), /503/)
})

test('真实接口失败不使用演示数据替代', async () => {
  let mockCalled = false
  const request = createRequestClient({ mode: 'hybrid', fetchImpl: async () => new Response('{}', { status: 500 }) })
  await assert.rejects(request(() => { mockCalled = true }, '/api/consumer/statistics'), /500/)
  assert.equal(mockCalled, false)
})

test('健康接口的 503/DOWN 响应仍可用于显示健康状态', async () => {
  const request = createRequestClient({ mode: 'hybrid', fetchImpl: async () => new Response('{"status":"DOWN"}', { status: 503 }) })
  assert.deepEqual(await request(() => {}, '/actuator/health'), { status: 'DOWN' })
})

test('请求超时可以终止轮询，不会一直卡在加载中', async () => {
  const request = createRequestClient({ mode: 'hybrid', timeoutMs: 10, fetchImpl: (_, { signal }) => new Promise((_, reject) => {
    signal.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')), { once: true })
  }) })
  await assert.rejects(request(() => {}, '/api/consumer/statistics'), /超时/)
})

test('演示模式不访问网络，http 配置兼容现有接口接入模式', async () => {
  const request = createRequestClient({ mode: 'mock', mockDelayMs: 0, fetchImpl: () => { throw new Error('unexpected HTTP') } })
  assert.equal(await request(() => 42, '/api/consumer/statistics'), 42)
  const http = createRequestClient({ mode: 'http', base: '/backend/', fetchImpl: async (url) => {
    assert.equal(url, '/backend/api/consumer/errors')
    return new Response('[]', { status: 200 })
  } })
  assert.deepEqual(await http(() => {}, '/api/consumer/errors'), [])
})
test('real market allowlist accepts safe codes and query strings only', async () => {
  const paths = []
  const client = createRequestClient({ mode: 'hybrid', mockDelayMs: 0, fetchImpl: async (path) => { paths.push(path); return { ok: true, json: async () => 'live' } } })
  for (const path of ['/api/analysis/stocks', '/api/analysis/kline/000001.SZ?limit=30', '/api/analysis/indicators/000001.SZ']) assert.equal(await client(() => 'mock', path), 'live')
  for (const path of ['/api/analysis/kline/../bad', '/api/analysis/features/000001.SZ', '/api/analysis/kline/INVALID']) assert.equal(await client(() => 'mock', path), 'mock')
  assert.equal(paths.length, 3)
  const failing = createRequestClient({ mode: 'hybrid', fetchImpl: async () => { throw new TypeError('offline') } })
  await assert.rejects(failing(() => 'mock', '/api/analysis/stocks'), /无法连接/)
})
