/**
 * 系统总览 mock：聚合三大服务状态（演示用）。
 * 真实后端对应：GET /actuator/health（Java）+ 各服务探活。
 */
import { getStatistics } from './consumer.js'
import { taskSummary } from './collector.js'
import { MODELS } from './analysis.js'
import { STOCK_POOL } from './generator.js'

export function getServices() {
  return [
    { name: 'python-collector', desc: '数据采集 · Tushare/AkShare → MySQL/Kafka', status: 'UP', detail: '输出模式 dual · 数据源 tushare' },
    { name: 'kafka', desc: '消息总线 · stock.ods.daily.v1 / basic.v1 / dead-letter.v1', status: 'UP', detail: 'broker localhost:9092 · 3 topics' },
    { name: 'kafka-consumer-service', desc: 'Java 消费 · 协议校验 / DLT / 监控 API', status: 'UP', detail: '消费组 stock-daily-consumer' },
    { name: 'mysql', desc: '存储 · 7 张业务表', status: 'UP', detail: 'stock_analysis · MySQL 8.0.36' },
    { name: 'stock-analysis-app', desc: '特征工程 · 模型训练 · 预测', status: 'UP', detail: 'FEATURE_VERSION 2.0.0 · 6 个已注册模型' },
  ]
}

export function getKpis() {
  const stats = getStatistics()
  const tasks = taskSummary()
  return [
    { key: 'todayRecords', label: '今日采集记录', value: tasks.todayRecords, unit: '条' },
    { key: 'totalConsumed', label: 'Kafka 累计消费', value: stats.totalConsumed, unit: '条' },
    { key: 'failedTasks', label: '失败采集任务', value: tasks.failed, unit: '个', tone: tasks.failed > 0 ? 'warning' : 'good' },
    { key: 'consumerFailures', label: '消费失败事件', value: stats.failureCount, unit: '条', tone: 'good' },
    { key: 'models', label: '已注册模型', value: MODELS.length, unit: '个' },
    { key: 'stocks', label: '覆盖股票', value: STOCK_POOL.length, unit: '只' },
  ]
}
