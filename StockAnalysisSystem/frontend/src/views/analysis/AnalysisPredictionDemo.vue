<template>
  <div>
    <!-- 模型注册表 -->
    <div class="card section">
      <div class="card-title">模型注册表（model_registry）</div>
      <div class="card-sub">点击模型卡展开训练元数据与超参 · 点击"选中"将其用于下方预测展示</div>
      <div class="model-list">
        <div
          v-for="m in models" :key="m.model_name"
          class="model-card" :class="{ selected: m.model_name === selected }"
        >
          <div class="model-head" @click="toggleExpand(m.model_name)">
            <div class="model-id">
              <code class="model-name">{{ m.model_name }}</code>
              <span class="model-type-tag">{{ m.model_type }}</span>
            </div>
            <div class="model-actions">
              <span class="muted model-date">{{ m.created_at }}</span>
              <button
                class="btn btn-sm" :class="{ 'btn-primary': m.model_name === selected }"
                @click.stop="selectModel(m.model_name)"
              >{{ m.model_name === selected ? '已选中' : '选中' }}</button>
              <span class="expand-caret" :class="{ open: expanded === m.model_name }">▾</span>
            </div>
          </div>
          <div v-if="expanded === m.model_name" class="model-detail">
            <div class="detail-grid">
              <div class="detail-item"><span class="muted">train_start_date</span><b>{{ m.train_start_date }}</b></div>
              <div class="detail-item"><span class="muted">train_end_date</span><b>{{ m.train_end_date }}</b></div>
              <div class="detail-item"><span class="muted">feature_version</span><b>{{ m.feature_version }}</b></div>
              <div class="detail-item"><span class="muted">n_features</span><b class="num">{{ m.n_features }}</b></div>
              <div class="detail-item wide"><span class="muted">target</span><b>{{ m.target }}</b></div>
            </div>
            <div class="detail-sub">
              <span class="muted">metrics：</span>
              <code v-for="(v, k) in metricEntries(m)" :key="k" class="kv">{{ k }}={{ typeof v === 'number' ? v : 'matrix' }}</code>
            </div>
            <div class="detail-sub">
              <span class="muted">params：</span>
              <code v-for="(v, k) in m.params" :key="k" class="kv">{{ k }}={{ v }}</code>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- 选中模型：指标 + 混淆矩阵 + 重要性 -->
    <div v-if="selectedModel" class="grid three-col section">
      <div class="card">
        <div class="card-title">评估指标 · {{ selectedModel.model_name }}</div>
        <div class="metric-list">
          <div v-for="(v, k) in metricEntries(selectedModel)" :key="k" class="metric-row">
            <span class="muted">{{ k }}</span>
            <span v-if="typeof v === 'number'" class="num metric-val">{{ v.toFixed(4) }}</span>
            <span v-else class="muted">{{ v.length }}×{{ v[0].length }} 矩阵 ↓</span>
          </div>
        </div>
      </div>
      <div class="card">
        <div class="card-title">混淆矩阵</div>
        <ChartBox v-if="confData.length" :option="confOption" :height="240" />
        <div v-else class="muted no-data">该模型无混淆矩阵（回归/排序类）</div>
      </div>
      <div class="card">
        <div class="card-title">特征重要性 Top15（Gain）</div>
        <ChartBox :option="importanceOption" :height="240" />
      </div>
    </div>

    <!-- 预测 vs 实际 -->
    <div class="grid two-col section">
      <div class="card">
        <div class="card-title">预测 vs 实际（未来 5 日收益率 %）</div>
        <div class="card-sub">{{ tsCode }} · 最近 60 个信号日 · {{ pred.model_name }}</div>
        <ChartBox :option="predOption" :height="260" />
      </div>
      <div class="card">
        <div class="card-title">上涨概率序列</div>
        <div class="card-sub">probability（0.5 分界参考线）</div>
        <ChartBox :option="probOption" :height="260" />
      </div>
    </div>

    <!-- analysis_result -->
    <div class="card section">
      <div class="card-title">analysis_result 表预览</div>
      <div class="card-sub">analysis_type 如 xgboost_prediction / regression_prediction · prediction / confidence 冗余存模型指标</div>
      <DataTable :columns="resultColumns" :rows="results" row-key="id" />
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import ChartBox from '../../components/ChartBox.vue'
import DataTable from '../../components/DataTable.vue'
import { getModels, getFeatureImportance, getPrediction, getAnalysisResults } from '../../api/analysis.js'
import { useThemeStore } from '../../stores/theme.js'
import { fmtNum } from '../../utils/format.js'
import { baseOption, xAxis, yAxis, barSeries, lineSeries, chrome, seriesColors } from '../../utils/chart.js'

const props = defineProps({ tsCode: { type: String, required: true } })
const theme = useThemeStore()

const models = ref([])
const selected = ref('xgboost_classifier')
const expanded = ref('')
const importance = ref([])
const pred = ref({ series: [], model_name: '' })
const results = ref([])

const selectedModel = computed(() => models.value.find((m) => m.model_name === selected.value))

function metricEntries(m) {
  return m.metrics || {}
}

function toggleExpand(name) {
  expanded.value = expanded.value === name ? '' : name
}

async function selectModel(name) {
  selected.value = name
  importance.value = await getFeatureImportance(name, 15)
  pred.value = await getPrediction(props.tsCode, name)
}

watch(() => props.tsCode, async (code) => {
  results.value = await getAnalysisResults(code)
  pred.value = await getPrediction(code, selected.value)
}, { immediate: true })

watch(() => models.value.length, async (n) => {
  if (n && !importance.value.length) {
    importance.value = await getFeatureImportance(selected.value, 15)
    pred.value = await getPrediction(props.tsCode, selected.value)
  }
})

onMounted(async () => {
  models.value = await getModels()
})

/* ── 混淆矩阵热力图 ── */
const confLabels = computed(() => {
  const m = selectedModel.value
  if (!m) return []
  if (m.labels) return m.labels
  const cm = m.metrics?.Confusion_Matrix
  return cm ? (cm.length === 2 ? ['跌', '涨'] : cm.map((_, i) => `类${i}`)) : []
})
const confData = computed(() => {
  const cm = selectedModel.value?.metrics?.Confusion_Matrix
  if (!cm) return []
  const data = []
  cm.forEach((row, i) => row.forEach((v, j) => data.push([j, i, v])))
  return data
})

const confOption = computed(() => {
  const c = chrome(theme.isDark)
  const max = Math.max(...confData.value.map((d) => d[2]))
  return {
    backgroundColor: 'transparent',
    tooltip: {
      ...baseOption(theme.isDark).tooltip,
      formatter: (p) => `真实 ${confLabels.value[p.value[1]]} → 预测 ${confLabels.value[p.value[0]]}<br/>${p.value[2]} 例`,
    },
    grid: { left: 8, right: 60, top: 10, bottom: 24, containLabel: true },
    xAxis: {
      type: 'category', data: confLabels.value, position: 'bottom',
      axisLine: { show: false }, axisTick: { show: false },
      axisLabel: { color: c.muted, fontSize: 10 },
    },
    yAxis: {
      type: 'category', data: confLabels.value, inverse: true,
      axisLine: { show: false }, axisTick: { show: false },
      axisLabel: { color: c.muted, fontSize: 10 },
    },
    visualMap: {
      min: 0, max, orient: 'vertical', right: 0, top: 'center',
      itemWidth: 9, textStyle: { color: c.muted, fontSize: 9 },
      inRange: { color: [c.surface, seriesColors(theme.isDark)[0]] },
    },
    series: [{
      type: 'heatmap', data: confData.value,
      label: { show: true, fontSize: 10, color: c.text1, formatter: (p) => p.value[2] },
      itemStyle: { borderColor: c.surface, borderWidth: 2 },
    }],
  }
})

/* ── 特征重要性横向柱 ── */
const importanceOption = computed(() => {
  const c = chrome(theme.isDark)
  const rows = [...importance.value].reverse()
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis', axisPointer: { type: 'shadow' } },
    grid: { left: 8, right: 40, top: 8, bottom: 2, containLabel: true },
    xAxis: yAxis(theme.isDark, { axisLabel: { fontSize: 10 } }),
    yAxis: xAxis(theme.isDark, { data: rows.map((r) => r.feature), axisLabel: { color: c.muted, fontSize: 9.5 } }),
    series: [{
      ...barSeries('gain', rows.map((r) => r.gain)),
      label: { show: true, position: 'right', fontSize: 9.5, color: c.muted, formatter: (p) => rows[p.dataIndex].pct + '%' },
      itemStyle: { borderRadius: [0, 3, 3, 0] },
      barMaxWidth: 10,
    }],
  }
})

/* ── 预测 vs 实际 ── */
const predOption = computed(() => {
  const s = pred.value.series || []
  const c = chrome(theme.isDark)
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis' },
    grid: { left: 8, right: 14, top: 28, bottom: 2, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: s.map((r) => r.trade_date.slice(5)), axisLabel: { color: c.muted, fontSize: 10, interval: 8 } }),
    yAxis: yAxis(theme.isDark, { axisLabel: { fontSize: 10, formatter: '{value}%' } }),
    series: [
      lineSeries('actual', s.map((r) => r.actual)),
      lineSeries('predicted', s.map((r) => r.predicted), { lineStyle: { width: 2, type: 'dashed' } }),
    ],
  }
})

const probOption = computed(() => {
  const s = pred.value.series || []
  const c = chrome(theme.isDark)
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis' },
    grid: { left: 8, right: 14, top: 28, bottom: 2, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: s.map((r) => r.trade_date.slice(5)), axisLabel: { color: c.muted, fontSize: 10, interval: 8 } }),
    yAxis: yAxis(theme.isDark, { min: 0.3, max: 0.75, axisLabel: { fontSize: 10 } }),
    series: [
      lineSeries('probability', s.map((r) => r.probability), {
        color: seriesColors(theme.isDark)[4],
        areaStyle: { opacity: 0.1 },
        markLine: {
          symbol: 'none', silent: true,
          lineStyle: { type: 'dashed', color: c.muted },
          label: { color: c.muted, fontSize: 10, formatter: '0.5' },
          data: [{ yAxis: 0.5 }],
        },
      }),
    ],
  }
})

/* ── analysis_result 表 ── */
const resultColumns = [
  { key: 'id', label: 'id', width: '60px', align: 'right', type: 'number', format: (v) => String(v) },
  { key: 'ts_code', label: 'ts_code', width: '96px' },
  { key: 'analysis_date', label: 'analysis_date', width: '104px' },
  { key: 'analysis_type', label: 'analysis_type', width: '160px' },
  { key: 'result', label: 'result(JSON)', ellipsis: true },
  { key: 'prediction', label: 'prediction', width: '96px', align: 'right', type: 'number', format: (v) => fmtNum(v, 4) },
  { key: 'confidence', label: 'confidence', width: '96px', align: 'right', type: 'number', format: (v) => fmtNum(v, 4) },
  { key: 'created_at', label: 'created_at', width: '150px' },
]
</script>

<style scoped>
.model-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.model-card {
  border: 1px solid var(--border);
  border-radius: 10px;
  background: var(--surface-2);
  overflow: hidden;
}

.model-card.selected {
  border-color: var(--accent);
}

.model-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 9px 14px;
  cursor: pointer;
}

.model-head:hover {
  background: var(--surface-3);
}

.model-id {
  display: flex;
  align-items: center;
  gap: 10px;
}

.model-name {
  font-family: 'Cascadia Code', Consolas, monospace;
  font-weight: 650;
  font-size: 13px;
}

.model-type-tag {
  font-size: 10.5px;
  color: var(--accent);
  background: var(--accent-soft);
  border-radius: 999px;
  padding: 1px 9px;
  font-weight: 600;
}

.model-actions {
  display: flex;
  align-items: center;
  gap: 12px;
}

.model-date {
  font-size: 11.5px;
}

.expand-caret {
  color: var(--text-3);
  transition: transform 0.15s;
  font-size: 12px;
}

.expand-caret.open {
  transform: rotate(180deg);
}

.model-detail {
  border-top: 1px dashed var(--border);
  padding: 12px 14px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.detail-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  gap: 8px;
}

.detail-item {
  display: flex;
  flex-direction: column;
  font-size: 12px;
}

.detail-item.wide {
  grid-column: span 2;
}

.detail-sub {
  font-size: 12px;
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  align-items: center;
}

.kv {
  font-family: 'Cascadia Code', Consolas, monospace;
  font-size: 11px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 1px 7px;
  color: var(--text-2);
}

.three-col {
  grid-template-columns: 1fr 1.1fr 1.3fr;
}

.two-col {
  grid-template-columns: 1fr 1fr;
}

@media (max-width: 1200px) {
  .three-col, .two-col { grid-template-columns: 1fr; }
}

.metric-list {
  display: flex;
  flex-direction: column;
  gap: 7px;
}

.metric-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-size: 12.5px;
  border-bottom: 1px dashed var(--border);
  padding-bottom: 6px;
}

.metric-row:last-child { border-bottom: none; }

.metric-val {
  font-size: 15px;
  font-weight: 700;
  color: var(--text-1);
}

.no-data {
  padding: 40px 0;
  text-align: center;
  font-size: 12.5px;
}
</style>
