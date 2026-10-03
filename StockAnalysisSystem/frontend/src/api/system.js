/**
 * 系统总览接口层。
 * 真实端点（部分已存在）：
 * - getHealth → GET /actuator/health（Java consumer，已存在）
 * - 其余为设计约定，后端暂未提供
 */
import { request } from './client'
import { getServices as mockServices, getKpis as mockKpis } from '../mock/system'
import { getHealth as mockHealth } from '../mock/consumer'

/** GET /actuator/health */
export function getHealth() {
  return request(() => mockHealth(), '/actuator/health')
}

/** GET /api/system/services（设计约定） */
export function getServices() {
  return request(() => mockServices(), '/api/system/services')
}

/** GET /api/system/kpis（设计约定） */
export function getKpis() {
  return request(() => mockKpis(), '/api/system/kpis')
}
