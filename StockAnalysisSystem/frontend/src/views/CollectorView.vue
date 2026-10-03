<template>
  <div>
    <!-- 任务发起 -->
    <section class="section">
      <h2>任务发起 <span class="muted cmd-hint">对齐 python-collector CLI（app/main.py）</span></h2>
      <div class="card">
        <div class="form-row">
          <div class="field cmd-select">
            <label>命令</label>
            <select v-model="form.cmd">
              <option v-for="d in defs" :key="d.cmd" :value="d.cmd">{{ d.label }}（{{ d.cmd }}）</option>
            </select>
          </div>
          <div class="field">
            <label>数据源 --source</label>
            <select v-model="form.source">
              <option value="tushare">tushare</option>
              <option value="akshare">akshare</option>
            </select>
          </div>
          <div v-for="p in activeParams" :key="p.name" class="field">
            <label>--{{ p.name }}{{ p.required ? ' *' : '' }}</label>
            <select v-if="p.type === 'select'" v-model="form.params[p.name]">
              <option v-for="o in p.options" :key="o" :value="o">{{ o }}</option>
            </select>
            <input v-else :type="p.type === 'date' ? 'date' : 'text'"
                   v-model="form.params[p.name]" :placeholder="p.placeholder || ''" />
          </div>
          <div class="field submit-field">
            <label>&nbsp;</label>
            <button class="btn btn-primary" :disabled="submitting" @click="submit">
              <Icon name="play" style="width:13px;height:13px" />
              {{ submitting ? '提交中…' : '提交任务' }}
            </button>
          </div>
        </div>
        <div v-if="activeDef" class="cmd-desc muted">{{ activeDef.desc }}</div>
        <div v-if="feedback" class="feedback" :class="feedback.ok ? 'ok' : 'bad'">{{ feedback.text }}</div>
      </div>
    </section>

    <!-- 输出模式 -->
    <section class="section">
      <h2>输出模式 <span class="muted cmd-hint">COLLECTOR_OUTPUT_MODE</span></h2>
      <div class="grid mode-grid">
        <div v-for="m in outputModes.modes" :key="m.mode"
             class="card mode-card" :class="{ current: m.mode === outputModes.current }">
          <div class="mode-head">
            <span class="mode-name">{{ m.mode }}</span>
            <span v-if="m.mode === outputModes.current" class="mode-badge">当前</span>
          </div>
          <div class="mode-desc muted">{{ m.desc }}</div>
        </div>
      </div>
    </section>

    <!-- 任务汇总 + 列表 -->
    <section class="section">
      <h2>采集任务 <span class="muted cmd-hint">collection_task 表</span></h2>
      <div class="grid summary-grid">
        <StatCard label="任务总数" :value="summary.total" />
        <StatCard label="SUCCESS" :value="summary.success" tone="good" />
        <StatCard label="PARTIAL_SUCCESS" :value="summary.partial" tone="warning" />
        <StatCard label="FAILED" :value="summary.failed" tone="critical" />
        <StatCard label="RUNNING / PENDING" :value="(summary.running || 0) + (summary.pending || 0)" tone="accent" />
        <StatCard label="今日采集记录" :value="summary.todayRecords" unit="条" />
      </div>
      <div class="card" style="margin-top:14px">
        <div class="table-head">
          <span class="card-sub" style="margin:0">提交后任务经历 PENDING → RUNNING → SUCCESS / PARTIAL_SUCCESS / FAILED（mock 约 10% 失败以便演示重试）</span>
          <button class="btn btn-sm" :disabled="!summary.failed" @click="retryAll">
            <Icon name="refresh" style="width:12px;height:12px" />
            重试全部失败（retry-failed）
          </button>
        </div>
        <DataTable :columns="taskColumns" :rows="tasks" :page-size="10" row-key="id">
          <template #cell-_act="{ row }">
            <button v-if="row.status === 'FAILED'" class="btn btn-sm" @click="retryOne(row)">重试</button>
            <span v-else class="muted">—</span>
          </template>
        </DataTable>
      </div>
    </section>

    <!-- 数据表预览 -->
    <section class="section">
      <h2>数据表预览</h2>
      <div class="tabs">
        <button v-for="t in TABLES" :key="t.key"
                class="tab-btn" :class="{ active: activeTable === t.key }"
                @click="switchTable(t.key)">{{ t.label }}</button>
      </div>
      <div class="card">
        <div v-if="loadingTable" class="muted" style="padding:20px;text-align:center">加载中…</div>
        <DataTable v-else :columns="tableColumns" :rows="tableRows" :page-size="8" dense />
      </div>
    </section>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import DataTable from '../components/DataTable.vue'
import StatCard from '../components/StatCard.vue'
import Icon from '../components/Icon.vue'
import { fmtInt, fmtCompact, fmtTime } from '../utils/format'
import * as api from '../api/collector'

/* ── 命令表单 ── */
const defs = ref([])
const form = reactive({ cmd: 'daily', source: 'tushare', params: {} })
const submitting = ref(false)
const feedback = ref(null)

const activeDef = computed(() => defs.value.find((d) => d.cmd === form.cmd))
const activeParams = computed(() => activeDef.value?.params || [])

function initParams() {
  const p = {}
  for (const def of activeParams.value) {
    if (def.type === 'date') p[def.name] = '2026-09-25'
    else if (def.type === 'select') p[def.name] = def.options[0]
    else p[def.name] = ''
  }
  form.params = p
}

async function submit() {
  submitting.value = true
  feedback.value = null
  try {
    const task = await api.submitTask(form.cmd, { source: form.source, ...form.params })
    feedback.value = { ok: true, text: `任务 #${task.id}（${task.task_type} · ${task.business_date}）已提交，状态 PENDING` }
    startPolling()
  } catch (e) {
    feedback.value = { ok: false, text: `提交失败：${e.message}` }
  } finally {
    submitting.value = false
  }
}

async function retryOne(row) {
  await api.retryTask(row.id)
  refreshTasks()
  startPolling()
}

async function retryAll() {
  const r = await api.retryFailedTasks(form.source)
  feedback.value = { ok: true, text: `retry-failed：已重新提交 ${r.retried} 个 FAILED 任务（source=${form.source}）` }
  refreshTasks()
  startPolling()
}

/* ── 任务列表 + 轮询 ── */
const tasks = ref([])
const summary = ref({ total: 0, success: 0, partial: 0, failed: 0, running: 0, pending: 0, todayRecords: 0 })
let pollTimer = null

async function refreshTasks() {
  tasks.value = await api.listTasks()
  summary.value = await api.getTaskSummary()
}

function startPolling() {
  if (pollTimer) return
  pollTimer = setInterval(async () => {
    await refreshTasks()
    const active = tasks.value.some((t) => t.status === 'RUNNING' || t.status === 'PENDING')
    if (!active) { clearInterval(pollTimer); pollTimer = null }
  }, 1200)
}

const taskColumns = [
  { key: 'id', label: 'ID', width: '56px' },
  { key: 'task_type', label: 'task_type' },
  { key: 'business_date', label: 'business_date（幂等键）' },
  { key: 'source', label: 'source' },
  { key: 'status', label: 'status', type: 'status' },
  { key: 'record_count', label: 'record_count', align: 'right', type: 'number', format: (v) => fmtInt(v) },
  { key: 'retry_count', label: 'retry', align: 'right' },
  { key: 'error_message', label: 'error_message', ellipsis: true },
  { key: 'started_at', label: 'started_at', format: (v) => fmtTime(v) },
  { key: 'finished_at', label: 'finished_at', format: (v) => fmtTime(v) },
  { key: '_act', label: '操作', width: '72px', align: 'right' },
]

/* ── 输出模式 ── */
const outputModes = ref({ current: '', modes: [] })

/* ── 数据表预览 ── */
const TABLES = [
  { key: 'stock_basic', label: 'stock_basic' },
  { key: 'stock_daily', label: 'stock_daily' },
  { key: 'stock_daily_basic', label: 'stock_daily_basic' },
  { key: 'stock_constituent', label: 'stock_constituent' },
]
const activeTable = ref('stock_basic')
const tableRows = ref([])
const loadingTable = ref(true)

const TABLE_COLUMNS = {
  stock_basic: [
    { key: 'ts_code', label: 'ts_code' }, { key: 'symbol', label: 'symbol' },
    { key: 'name', label: 'name' }, { key: 'area', label: 'area' },
    { key: 'industry', label: 'industry' }, { key: 'list_date', label: 'list_date' },
    { key: 'source', label: 'source' },
  ],
  stock_daily: [
    { key: 'ts_code', label: 'ts_code' }, { key: 'trade_date', label: 'trade_date' },
    { key: 'open', label: 'open', align: 'right', type: 'number' },
    { key: 'high', label: 'high', align: 'right', type: 'number' },
    { key: 'low', label: 'low', align: 'right', type: 'number' },
    { key: 'close', label: 'close', align: 'right', type: 'number' },
    { key: 'pre_close', label: 'pre_close', align: 'right', type: 'number' },
    { key: 'change', label: 'change', align: 'right', type: 'updown' },
    { key: 'pct_chg', label: 'pct_chg(%)', align: 'right', type: 'updown' },
    { key: 'vol', label: 'vol(手)', align: 'right', format: (v) => fmtCompact(v) },
    { key: 'amount', label: 'amount(千元)', align: 'right', format: (v) => fmtCompact(v) },
  ],
  stock_daily_basic: [
    { key: 'ts_code', label: 'ts_code' }, { key: 'trade_date', label: 'trade_date' },
    { key: 'turnover_rate', label: 'turnover_rate(%)', align: 'right', type: 'number' },
    { key: 'pe', label: 'pe', align: 'right', type: 'number' },
    { key: 'pe_ttm', label: 'pe_ttm', align: 'right', type: 'number' },
    { key: 'pb', label: 'pb', align: 'right', type: 'number' },
    { key: 'ps', label: 'ps', align: 'right', type: 'number' },
    { key: 'total_mv', label: 'total_mv(万元)', align: 'right', format: (v) => fmtCompact(v) },
    { key: 'source', label: 'source' },
  ],
  stock_constituent: [
    { key: 'group_type', label: 'group_type' }, { key: 'group_code', label: 'group_code' },
    { key: 'ts_code', label: 'ts_code' }, { key: 'as_of_date', label: 'as_of_date' },
    { key: 'weight', label: 'weight(%)', align: 'right', type: 'number' },
    { key: 'source', label: 'source' },
  ],
}
const tableColumns = computed(() => TABLE_COLUMNS[activeTable.value] || [])

async function switchTable(key) {
  activeTable.value = key
  loadingTable.value = true
  tableRows.value = await api.getTableData(key)
  loadingTable.value = false
}

onMounted(async () => {
  defs.value = await api.getCommandDefs()
  outputModes.value = await api.getOutputModes()
  form.cmd = defs.value[0]?.cmd || 'daily'
  initParams()
  await refreshTasks()
  startPolling()
  switchTable('stock_basic')
})

// 切换命令时重置参数默认值
watch(() => form.cmd, initParams)

onBeforeUnmount(() => { if (pollTimer) clearInterval(pollTimer) })
</script>

<style scoped>
.cmd-hint {
  font-size: 12px;
  font-weight: 400;
  margin-left: 8px;
}

.form-row {
  display: flex;
  flex-wrap: wrap;
  gap: 14px;
  align-items: flex-end;
}

.form-row .field {
  min-width: 150px;
}

.cmd-select {
  min-width: 230px !important;
}

.submit-field .btn {
  width: 100%;
}

.cmd-desc {
  margin-top: 12px;
  font-size: 12px;
  border-top: 1px dashed var(--border);
  padding-top: 10px;
}

.feedback {
  margin-top: 10px;
  font-size: 12.5px;
  padding: 8px 12px;
  border-radius: 8px;
}

.feedback.ok {
  color: var(--good);
  background: rgba(12, 163, 12, 0.1);
}

.feedback.bad {
  color: var(--critical);
  background: rgba(208, 59, 59, 0.1);
}

.mode-grid {
  grid-template-columns: repeat(auto-fill, minmax(230px, 1fr));
}

.mode-card.current {
  border-color: var(--accent);
}

.mode-head {
  display: flex;
  align-items: center;
  gap: 8px;
}

.mode-name {
  font-family: 'Cascadia Code', Consolas, monospace;
  font-weight: 650;
  font-size: 13.5px;
}

.mode-badge {
  font-size: 10.5px;
  color: var(--accent);
  background: var(--accent-soft);
  padding: 1px 8px;
  border-radius: 999px;
  font-weight: 600;
}

.mode-desc {
  font-size: 12px;
  margin-top: 6px;
  line-height: 1.5;
}

.summary-grid {
  grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
}

.table-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 10px;
  flex-wrap: wrap;
}
</style>
