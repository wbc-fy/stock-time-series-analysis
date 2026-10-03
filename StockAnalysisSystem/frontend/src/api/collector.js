/**
 * 数据采集接口层（对齐 python-collector）。
 * 后端目前只有 CLI，无 REST API；以下路径为设计约定，
 * 未来可由 collector 增加 FastAPI/Flask 薄封装实现。
 */
import { request } from './client'
import * as mock from '../mock/collector'

/** GET /api/collector/commands — CLI 命令定义 */
export function getCommandDefs() {
  return request(() => mock.COMMAND_DEFS, '/api/collector/commands')
}

/** GET /api/collector/output-modes — COLLECTOR_OUTPUT_MODE 说明 */
export function getOutputModes() {
  return request(
    () => ({ current: mock.CURRENT_OUTPUT_MODE, modes: mock.OUTPUT_MODES }),
    '/api/collector/output-modes',
  )
}

/** GET /api/collector/tasks — collection_task 列表 */
export function listTasks() {
  return request(() => mock.listTasks(), '/api/collector/tasks')
}

/** GET /api/collector/tasks/summary — 任务状态汇总 */
export function getTaskSummary() {
  return request(() => mock.taskSummary(), '/api/collector/tasks/summary')
}

/** POST /api/collector/tasks — 提交采集任务（body: {cmd, params, source}） */
export function submitTask(cmd, params) {
  return request(
    () => mock.submitTask(cmd, params),
    '/api/collector/tasks',
    { method: 'POST', body: JSON.stringify({ cmd, params }) },
  )
}

/** POST /api/collector/tasks/retry-failed — 重试当前数据源全部失败任务 */
export function retryFailedTasks(source) {
  return request(
    () => mock.retryFailedTasks(source),
    '/api/collector/tasks/retry-failed',
    { method: 'POST', body: JSON.stringify({ source }) },
  )
}

/** POST /api/collector/tasks/:id/retry — 单任务重试 */
export function retryTask(id) {
  return request(
    () => mock.retryTask(id),
    `/api/collector/tasks/${id}/retry`,
    { method: 'POST' },
  )
}

/** GET /api/collector/tables/:table — 四张数据表预览 */
export function getTableData(table) {
  return request(() => mock.getTableData(table), `/api/collector/tables/${table}`)
}
