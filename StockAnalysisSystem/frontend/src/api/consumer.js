/**
 * Kafka 消费监控接口层（对齐 java kafka-consumer-service）。
 * statistics / errors / health 为已存在的真实端点，字段一一对应；
 * topics / trend / validate 为设计约定。
 */
import { request } from './client'
import * as mock from '../mock/consumer'

/** GET /api/consumer/statistics — ConsumerStatisticsSnapshot（已存在） */
export async function getStatistics(options) {
  const data = await request(() => mock.getStatistics(), '/api/consumer/statistics', options)
  const counters = ['totalConsumed', 'successCount', 'failureCount', 'dailyEventCount', 'basicEventCount',
    'jsonParseFailureCount', 'validationFailureCount', 'unsupportedSchemaCount', 'deadLetterPublishFailureCount']
  if (!data || counters.some((key) => !Number.isFinite(data[key]) || data[key] < 0)) {
    throw new Error('消费统计字段与接口协议不一致')
  }
  return data
}

/** GET /api/consumer/errors — 最近错误列表 ConsumerError[]（已存在） */
export async function getErrors(options) {
  const data = await request(() => mock.getErrors(), '/api/consumer/errors', options)
  if (!Array.isArray(data) || data.some((item) => !item || typeof item.errorType !== 'string' || typeof item.message !== 'string')) {
    throw new Error('最近错误列表与接口协议不一致')
  }
  return data
}

/** GET /actuator/health — 健康检查（已存在） */
export async function getHealth(options) {
  const data = await request(() => mock.getHealth(), '/actuator/health', options)
  if (!data || typeof data.status !== 'string') throw new Error('健康检查字段与接口协议不一致')
  return data
}

/** GET /api/consumer/topics — topic 消费概况（设计约定） */
export function getTopics() {
  return request(() => mock.getTopics(), '/api/consumer/topics')
}

/** GET /api/consumer/trend — 消费趋势桶（设计约定） */
export function getConsumeTrend() {
  return request(() => mock.getConsumeTrend(), '/api/consumer/trend')
}

/** POST /api/consumer/validate — 事件协议校验（设计约定；mock 端前端复刻 StockDailyEventValidator） */
export function validateEvent(payloadText, kafkaKey, eventType = 'daily') {
  return request(
    () => mock.validateEvent(payloadText, kafkaKey, eventType),
    '/api/consumer/validate',
    { method: 'POST', body: JSON.stringify({ payloadText, kafkaKey, eventType }) },
  )
}

/** GET /api/consumer/samples — 示例事件（设计约定；前两个来自 docs/examples） */
export function getSampleEvents() {
  return request(() => mock.getSampleEvents(), '/api/consumer/samples')
}
