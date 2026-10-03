<template>
  <div>
    <section class="section">
      <div class="card connection-panel" aria-live="polite">
        <div class="connection-head">
          <b>{{ isDemo ? '消费监控演示' : 'Java 消费服务连接' }}</b>
          <StatusTag :status="health?.status || 'UNKNOWN'" />
        </div>
        <p class="muted">{{ isDemo ? '当前统计和图表均为演示数据，不代表实际运行状态。' : '统计、最近错误和健康状态来自现有 API。Topic 积压、历史趋势和服务器端校验接口尚未接入。' }}</p>
        <p class="muted">{{ lastSync ? `最近一次完整刷新：${fmtTime(lastSync)}` : '等待首次完整刷新' }} · 完成请求后每 3 秒刷新</p>
        <div v-for="warning in warnings" :key="warning" class="connection-error">{{ warning }}。保留上次成功数据；服务恢复后会自动重试。</div>
      </div>
    </section>
    <!-- 统计快照（3s 轮询） -->
    <section class="section">
      <h2>
        消费统计
        <span class="muted cmd-hint">累计计数 · 此服务的统计不包含 Flink 指标作业</span>
      </h2>
      <div class="grid stat-grid">
        <StatCard label="totalConsumed 累计消费" :value="stats.totalConsumed" unit="条" tone="accent" />
        <StatCard label="successCount 成功" :value="stats.successCount" unit="条" tone="good" />
        <StatCard label="failureCount 失败" :value="stats.failureCount" unit="条" tone="critical" />
        <StatCard label="dailyEventCount 日线事件" :value="stats.dailyEventCount" unit="条" />
        <StatCard label="basicEventCount 基础信息事件" :value="stats.basicEventCount" unit="条" />
        <StatCard label="jsonParseFailureCount JSON解析失败" :value="stats.jsonParseFailureCount" unit="条" tone="warning" />
        <StatCard label="validationFailureCount 校验失败" :value="stats.validationFailureCount" unit="条" tone="warning" />
        <StatCard label="unsupportedSchemaCount 版本不支持" :value="stats.unsupportedSchemaCount" unit="条" tone="warning" />
        <StatCard label="deadLetterPublishFailureCount DLT发送失败" :value="stats.deadLetterPublishFailureCount" unit="条" tone="critical" />
        <StatCard label="lastConsumedTime 最近消费" :value="fmtTime(stats.lastConsumedTime)" raw />
      </div>
    </section>

    <!-- Topic 面板 -->
    <section v-if="isDemo" class="section">
      <h2>Topic 概况 <span class="muted cmd-hint">演示数据</span></h2>
      <div class="grid topic-grid">
        <div v-for="t in topics" :key="t.name" class="card topic-card">
          <div class="topic-head">
            <span class="topic-name">{{ t.name }}</span>
            <StatusTag :status="t.state" />
          </div>
          <div class="topic-role muted">{{ t.role }} · {{ t.partitions }} 分区 · 消费组 {{ t.consumerGroup }}</div>
          <div class="topic-nums">
            <div><span class="muted">lag</span><b class="num">{{ fmtInt(t.lag) }}</b></div>
            <div><span class="muted">累计</span><b class="num">{{ fmtCompact(t.consumedTotal) }}</b></div>
          </div>
        </div>
      </div>
    </section>

    <!-- 消费趋势（单轴原则：拆两张图） -->
    <section v-if="isDemo" class="section">
      <h2>消费趋势 <span class="muted cmd-hint">最近 2 小时 · 5 分钟桶</span></h2>
      <div class="grid chart-grid">
        <div class="card">
          <div class="card-title">每桶成功 vs 失败</div>
          <div class="card-sub">堆叠柱 · 失败被 DLT 兜底，不阻塞消费</div>
          <ChartBox :option="trendBarOption" :height="260" />
        </div>
        <div class="card">
          <div class="card-title">桶内累计消费</div>
          <div class="card-sub">窗口内 success + failure 累计</div>
          <ChartBox :option="trendCumOption" :height="260" />
        </div>
      </div>
    </section>

    <!-- 最近错误 -->
    <section class="section">
      <h2>最近错误 <span class="muted cmd-hint">ConsumerError 七字段 · GET /api/consumer/errors</span></h2>
      <div class="card">
        <p v-if="!errors.length" class="muted">{{ warnings.length ? '尚无可确认的错误列表，请查看连接提示。' : '最近错误列表为空。' }}</p>
        <DataTable :columns="errorColumns" :rows="errors" :page-size="8" dense />
      </div>
    </section>

    <!-- 校验模拟器 -->
    <section v-if="isDemo" class="section">
      <h2>事件校验模拟器 <span class="muted cmd-hint">前端复刻 StockDailyEventValidator 规则</span></h2>
      <div class="grid validator-grid">
        <div class="card">
          <div class="card-title">事件 Payload</div>
          <div class="card-sub">粘贴 JSON 或点击下方示例；daily 事件校验 kafkaKey === tsCode</div>
          <div class="sample-row">
            <button v-for="s in samples" :key="s.id" class="btn btn-sm"
                    :class="{ 'btn-primary': activeSample === s.id }"
                    @click="useSample(s)">{{ s.label }}</button>
          </div>
          <div class="form-row">
            <div class="field">
              <label>kafkaKey</label>
              <input type="text" v-model="kafkaKey" placeholder="如 000001.SZ" />
            </div>
            <div class="field">
              <label>eventType</label>
              <select v-model="eventType">
                <option value="daily">daily（stock.ods.daily.v1）</option>
                <option value="basic">basic（stock.ods.basic.v1）</option>
              </select>
            </div>
            <div class="field">
              <label>&nbsp;</label>
              <button class="btn btn-primary" :disabled="validating" @click="runValidate">
                {{ validating ? '校验中…' : '执行校验' }}
              </button>
            </div>
          </div>
          <textarea v-model="payloadText" rows="14" spellcheck="false"
                    placeholder='{"schemaVersion": 1, "eventId": "...", ...}' style="width:100%;margin-top:12px"></textarea>
        </div>
        <div class="card">
          <div class="card-title">校验结果</div>
          <div class="card-sub">规则：schemaVersion=1 · eventId/traceId/tsCode/source 非空 · close/volume/amount ≥ 0 · kafkaKey===tsCode</div>
          <div v-if="!result" class="muted placeholder">尚未校验</div>
          <template v-else>
            <div class="result-banner" :class="result.valid ? 'ok' : 'bad'">
              <Icon :name="result.valid ? 'check' : 'alert'" style="width:16px;height:16px" />
              <b>{{ result.valid ? 'VALID — 事件将正常入库' : `REJECTED — ${result.errorType}` }}</b>
            </div>
            <div v-if="result.violations.length" class="violations">
              <div v-for="(v, i) in result.violations" :key="i" class="violation">
                <span class="v-idx num">{{ i + 1 }}</span>{{ v }}
              </div>
            </div>
            <div v-if="result.unsupportedSchema" class="muted dlt-note">
              该事件将直接写入 stock.dead-letter.v1（不做字段级校验）。
            </div>
            <div v-if="result.valid" class="muted dlt-note">
              消费成功后计入 successCount，并按 eventType 累计 dailyEventCount / basicEventCount。
            </div>
          </template>
        </div>
      </div>
    </section>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import StatCard from '../components/StatCard.vue'
import StatusTag from '../components/StatusTag.vue'
import DataTable from '../components/DataTable.vue'
import ChartBox from '../components/ChartBox.vue'
import Icon from '../components/Icon.vue'
import { fmtInt, fmtCompact, fmtTime } from '../utils/format'
import { useThemeStore } from '../stores/theme'
import { baseOption, xAxis, yAxis, barSeries, lineSeries } from '../utils/chart'
import * as api from '../api/consumer'
import { dataSource } from '../api/client.js'

const theme = useThemeStore()
const isDemo = computed(() => dataSource.value === 'mock')
const health = ref(null)
const warnings = ref([])
const lastSync = ref(null)

/* ── 统计快照 3s 轮询 ── */
const stats = ref({})
const topics = ref([])
let timer = null
let active = false
let pending = null

async function poll() {
  pending = new AbortController()
  const options = { signal: pending.signal }
  const results = await Promise.allSettled([api.getStatistics(options), api.getErrors(options), api.getHealth(options)])
  if (!active) return
  const labels = ['消费统计', '最近错误', '服务健康']
  const targets = [stats, errors, health]
  warnings.value = []
  results.forEach((result, index) => {
    if (result.status === 'fulfilled') targets[index].value = result.value
    else warnings.value.push(`${labels[index]}：${result.reason.message}`)
  })
  if (!warnings.value.length) lastSync.value = new Date().toISOString()
  timer = setTimeout(poll, 3000)
}

/* ── 趋势 ── */
const trend = ref([])

const trendBarOption = computed(() => {
  const dark = theme.isDark
  const times = trend.value.map((b) => b.time)
  return {
    ...baseOption(dark),
    tooltip: { ...baseOption(dark).tooltip, trigger: 'axis', axisPointer: { type: 'shadow' } },
    xAxis: xAxis(dark, { data: times, axisLabel: { interval: 3 } }),
    yAxis: yAxis(dark),
    series: [
      barSeries('success', trend.value.map((b) => b.success), { stack: 'total', itemStyle: { borderRadius: [0, 0, 0, 0] } }),
      barSeries('failure', trend.value.map((b) => b.failure), {
        stack: 'total',
        itemStyle: { color: '#d03b3b', borderRadius: [3, 3, 0, 0] },
      }),
    ],
  }
})

const trendCumOption = computed(() => {
  const dark = theme.isDark
  let acc = 0
  const cum = trend.value.map((b) => (acc += b.success + b.failure))
  return {
    ...baseOption(dark),
    legend: { show: false },
    tooltip: { ...baseOption(dark).tooltip, trigger: 'axis' },
    xAxis: xAxis(dark, { data: trend.value.map((b) => b.time), axisLabel: { interval: 3 } }),
    yAxis: yAxis(dark),
    series: [lineSeries('累计消费', cum, { areaStyle: { opacity: 0.12 } })],
  }
})

/* ── 错误表 ── */
const errors = ref([])
const errorColumns = [
  { key: 'timestamp', label: 'timestamp', format: (v) => fmtTime(v) },
  { key: 'errorType', label: 'errorType', type: 'status' },
  { key: 'message', label: 'message', ellipsis: true },
  { key: 'topic', label: 'topic' },
  { key: 'key', label: 'key' },
  { key: 'partition', label: 'partition', align: 'right' },
  { key: 'offset', label: 'offset', align: 'right', format: (v) => fmtInt(v) },
]

/* ── 校验模拟器 ── */
const samples = ref([])
const activeSample = ref('')
const payloadText = ref('')
const kafkaKey = ref('')
const eventType = ref('daily')
const validating = ref(false)
const result = ref(null)

function useSample(s) {
  activeSample.value = s.id
  payloadText.value = JSON.stringify(s.payload, null, 2)
  kafkaKey.value = s.kafkaKey
  eventType.value = s.eventType
  result.value = null
}

async function runValidate() {
  validating.value = true
  try { result.value = await api.validateEvent(payloadText.value, kafkaKey.value, eventType.value) }
  finally { validating.value = false }
}

onMounted(async () => {
  active = true
  poll()
  if (isDemo.value) {
    const demo = await Promise.all([api.getTopics(), api.getConsumeTrend(), api.getSampleEvents()])
    if (!active) return
    ;[topics.value, trend.value, samples.value] = demo
    if (samples.value.length) useSample(samples.value[0])
  }
})

onBeforeUnmount(() => { active = false; clearTimeout(timer); pending?.abort() })
</script>

<style scoped>
.connection-head { display: flex; justify-content: space-between; align-items: center; gap: 12px; }
.connection-panel p { margin-top: 8px; font-size: 12px; }
.connection-error { margin-top: 8px; color: var(--critical); font-size: 12px; }
.cmd-hint {
  font-size: 12px;
  font-weight: 400;
  margin-left: 8px;
}

.stat-grid {
  grid-template-columns: repeat(auto-fill, minmax(min(190px, 100%), 1fr));
}

.topic-grid {
  grid-template-columns: repeat(auto-fill, minmax(min(280px, 100%), 1fr));
}

.topic-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.topic-name {
  font-family: 'Cascadia Code', Consolas, monospace;
  font-weight: 650;
  font-size: 13px;
}

.topic-role {
  font-size: 11.5px;
  margin-top: 5px;
}

.topic-nums {
  display: flex;
  gap: 26px;
  margin-top: 12px;
}

.topic-nums > div {
  display: flex;
  flex-direction: column;
  gap: 2px;
  font-size: 11.5px;
}

.topic-nums b {
  font-size: 18px;
  font-weight: 680;
  color: var(--text-1);
}

.chart-grid {
  grid-template-columns: repeat(auto-fit, minmax(min(380px, 100%), 1fr));
}

.validator-grid {
  grid-template-columns: repeat(auto-fit, minmax(min(400px, 100%), 1fr));
}

.sample-row {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 12px;
}

.form-row {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
}

.form-row .field {
  flex: 1;
  min-width: 140px;
}

.placeholder {
  padding: 40px 0;
  text-align: center;
}

.result-banner {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 14px;
  border-radius: 8px;
  font-size: 13.5px;
  margin-bottom: 12px;
}

.result-banner.ok {
  color: var(--good);
  background: rgba(12, 163, 12, 0.1);
}

.result-banner.bad {
  color: var(--critical);
  background: rgba(208, 59, 59, 0.1);
}

.violations {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.violation {
  display: flex;
  gap: 9px;
  align-items: baseline;
  font-size: 12.5px;
  font-family: 'Cascadia Code', Consolas, monospace;
  color: var(--text-2);
  background: var(--surface-2);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 6px 10px;
}

.v-idx {
  color: var(--critical);
  font-weight: 700;
  flex-shrink: 0;
}

.dlt-note {
  margin-top: 12px;
  font-size: 12px;
  border-top: 1px dashed var(--border);
  padding-top: 10px;
}
</style>
