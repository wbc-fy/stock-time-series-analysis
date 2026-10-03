<template>
  <div>
    <div class="card section">
      <div class="card-title">模型注册表（model_registry）</div>
      <div class="card-sub">{{ tsCode }} · 已发布的下一交易日收盘收益率回归模型 · 不构成投资建议</div>
      <p v-if="modelLoading" role="status">模型加载中…</p>
      <p v-else-if="modelError" role="alert">{{ modelError }} <button class="btn btn-sm" @click="loadStock">重试</button></p>
      <p v-else-if="!models.length" role="status">该股票暂无已发布模型，请先运行离线训练与发布命令。</p>
      <div class="model-list">
        <div v-for="m in models" :key="m.model_id" class="model-card" :class="{ selected: selected === m.model_id }">
          <div class="model-head">
            <div><code class="model-name">{{ m.model_name }}</code> <span class="model-type-tag">{{ m.model_type }}</span><div class="muted">{{ m.model_id }}</div></div>
            <button class="btn btn-sm" :class="{ 'btn-primary': selected === m.model_id }" @click="selectModel(m.model_id)">{{ selected === m.model_id ? '已选中' : '选中' }}</button>
          </div>
        </div>
      </div>
    </div>
    <div v-if="model" class="card section">
      <div class="card-title">冻结模型 · {{ model.model_version }}</div>
      <div class="detail-grid">
        <div v-for="(split, name) in model.splits" :key="name" class="detail-item"><span class="muted">{{ splitLabels[name] || name }} · {{ split.count }} 样本</span><b>{{ split.signal_start }} → {{ split.signal_end }}</b><span class="muted">标签截止 {{ split.label_end }}</span></div>
        <div class="detail-item"><span class="muted">特征版本 / 数量</span><b>{{ model.feature_version }} / {{ model.n_features }}</b></div>
        <div class="detail-item"><span class="muted">冻结训练来源截止</span><b>{{ model.source_cutoff }}</b></div>
        <div class="detail-item"><span class="muted">当前行情截止</span><b>{{ model.data_cutoff }}</b></div>
      </div>
      <p class="muted">{{ cutoffLabel }} · 已见数据截止 {{ model.seen_through }} · 模型创建 {{ model.created_at }}</p>
      <details><summary>超参数与来源摘要</summary><p class="muted">{{ model.target }} · seed={{ model.seed }}</p><code>{{ JSON.stringify(model.params) }}</code><p class="muted hash">{{ model.data_hash }}</p></details>
    </div>
    <div v-if="model" class="grid two-col section">
      <div class="card">
        <div class="card-title">独立测试评估 · {{ model.splits?.test?.count }} 样本</div>
        <div class="card-sub">RMSE / MAE 单位为百分点；R² 无量纲。指标来自完整冻结测试集。</div>
        <div v-for="key in ['rmse', 'mae', 'r2']" :key="key" class="metric-row"><span>{{ key.toUpperCase() }}</span><b class="num">{{ metric(model.metrics?.[key], key) }}</b></div>
        <div class="card-sub">零收益预测基线</div>
        <div v-for="key in ['rmse', 'mae', 'r2']" :key="key" class="metric-row"><span>{{ key.toUpperCase() }}</span><b class="num">{{ metric(model.baseline_metrics?.[key], key) }}</b></div>
      </div>
      <div class="card">
        <div class="card-title">特征重要性 Top15（{{ importance?.importance_method || model.importance_method }}）</div>
        <p v-if="importanceLoading" role="status">重要性加载中…</p><p v-else-if="importanceError" role="alert">{{ importanceError }}</p>
        <ChartBox v-else-if="importance?.feature_importance.length" :option="importanceOption" :height="260" />
        <p v-else class="muted">暂无特征重要性。</p>
      </div>
    </div>
    <div v-if="selected" class="grid two-col section">
      <div class="card">
        <div class="card-title">预测 vs 实际（下一交易日收益率 %）</div>
        <div class="card-sub">冻结独立测试样本 · {{ prediction?.test_series.length || 0 }} 个信号日 · 按信号日与目标日对齐</div>
        <p v-if="predictionLoading" role="status">预测加载中…</p><p v-else-if="predictionError" role="alert">{{ predictionError }} <button class="btn btn-sm" @click="selectModel(selected)">重试</button></p>
        <ChartBox v-else-if="prediction?.test_series.length" :option="predictionOption" :height="260" />
        <p v-else class="muted">暂无独立测试结果。</p>
      </div>
      <div class="card">
        <div class="card-title">最新信号 · 下一交易日</div>
        <template v-if="prediction?.latest && !predictionLoading">
          <p class="muted">信号日 {{ prediction.latest.signal_date }} → {{ prediction.latest.target_date || '下一交易日（日期待交易日历确认）' }}</p>
          <p class="num latest-return">{{ percent(prediction.latest.predicted_return) }}</p>
          <p>预测收盘到收盘收益率</p><p class="muted">真实收益率：未知 · 尚未观测，不参与测试评估。</p>
          <p class="muted">{{ cutoffLabel }}</p>
        </template>
        <p v-else class="muted">{{ predictionLoading ? '最新信号加载中…' : '暂无最新预测。' }}</p>
      </div>
    </div>
    <div class="card section">
      <div class="card-title">analysis_result 发布摘要</div>
      <p v-if="resultLoading" role="status">发布摘要加载中…</p><p v-else-if="resultError" role="alert">{{ resultError }}</p>
      <DataTable v-else-if="results.length" :columns="resultColumns" :rows="results" row-key="publication_id" />
      <p v-else class="muted">暂无发布结果。</p>
    </div>
  </div>
</template>
<script setup>
import { computed, ref, watch, onBeforeUnmount } from 'vue'
import ChartBox from '../../components/ChartBox.vue'
import DataTable from '../../components/DataTable.vue'
import { getModels, getPrediction, getFeatureImportance, getAnalysisResults } from '../../api/analysis.js'
import { dataSource } from '../../api/client.js'
import { returnPercent, createGenerationGuard, settleDetailRequest } from '../../utils/prediction.js'
import { useThemeStore } from '../../stores/theme.js'
import { baseOption, xAxis, yAxis, lineSeries, barSeries } from '../../utils/chart.js'
const props = defineProps({ tsCode: { type: String, required: true } })
const theme = useThemeStore()
const models = ref([]), selected = ref(''), results = ref([]), prediction = ref(null), importance = ref(null)
const modelLoading = ref(false), resultLoading = ref(false), predictionLoading = ref(false), importanceLoading = ref(false)
const modelError = ref(''), resultError = ref(''), predictionError = ref(''), importanceError = ref('')
const stockGuard = createGenerationGuard(), detailGuard = createGenerationGuard()
const model = computed(() => prediction.value?.metadata || models.value.find(m => m.model_id === selected.value))
const splitLabels = { train: '训练', val: '验证', test: '测试' }
const cutoffLabel = computed(() => model.value?.data_cutoff < new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Shanghai' }) ? '历史截止数据：非今日实时预测' : '按已发布行情截止日期预测，非实时行情')
const percent = value => value === null || value === undefined ? '未定义' : `${returnPercent(value).toFixed(4)}%`
const metric = (value, key) => value === null || value === undefined ? '未定义' : key === 'r2' ? value.toFixed(4) : `${returnPercent(value).toFixed(4)} pp`
async function selectModel(id) {
  const run = detailGuard.next(), code = props.tsCode
  selected.value = id; prediction.value = null; importance.value = null; predictionError.value = ''; importanceError.value = ''; predictionLoading.value = true; importanceLoading.value = true
  await Promise.all([
    settleDetailRequest(getPrediction(code, id, { signal: run.signal }), run, {
      success(dto) { prediction.value = dto },
      error(e) { predictionError.value = e.message },
      settled() { predictionLoading.value = false },
    }),
    settleDetailRequest(getFeatureImportance(id, 15, { signal: run.signal }), run, {
      success(dto) { if (dto.ts_code !== code) throw new Error('重要性与股票不匹配'); importance.value = dto },
      error(e) { importanceError.value = e.message },
      settled() { importanceLoading.value = false },
    }),
  ])
}
async function loadStock() {
  const run = stockGuard.next(), code = props.tsCode
  detailGuard.cancel(); models.value = []; selected.value = ''; prediction.value = null; importance.value = null; results.value = []
  modelError.value = ''; resultError.value = ''; predictionError.value = ''; importanceError.value = ''; predictionLoading.value = false; importanceLoading.value = false
  modelLoading.value = true; resultLoading.value = true
  await Promise.all([
    getModels(code, { signal: run.signal }).then(rows => { if (!run.current()) return; models.value = rows; if (rows.length) selectModel(rows[0].model_id) }).catch(e => { if (run.current() && e.name !== 'AbortError') modelError.value = e.message }).finally(() => { if (run.current()) modelLoading.value = false }),
    getAnalysisResults(code, { signal: run.signal }).then(rows => { if (run.current()) results.value = rows }).catch(e => { if (run.current() && e.name !== 'AbortError') resultError.value = e.message }).finally(() => { if (run.current()) resultLoading.value = false }),
  ])
}
watch(() => [props.tsCode, dataSource.value], () => { if (dataSource.value === 'hybrid') loadStock(); else { stockGuard.cancel(); detailGuard.cancel() } }, { immediate: true, flush: 'sync' })
onBeforeUnmount(() => { stockGuard.cancel(); detailGuard.cancel() })
const predictionOption = computed(() => {
  const rows = prediction.value?.test_series || []
  return { ...baseOption(theme.isDark), grid: { left: 8, right: 14, top: 28, bottom: 2, containLabel: true }, xAxis: xAxis(theme.isDark, { data: rows.map(r => `${r.signal_date}\n→${r.target_date}`) }), yAxis: yAxis(theme.isDark, { axisLabel: { formatter: '{value}%' } }), series: [lineSeries('实际收益率', rows.map(r => returnPercent(r.actual_return))), lineSeries('预测收益率', rows.map(r => returnPercent(r.predicted_return)), { lineStyle: { width: 2, type: 'dashed' } })] }
})
const importanceOption = computed(() => {
  const rows = [...(importance.value?.feature_importance || [])].reverse()
  return { ...baseOption(theme.isDark), grid: { left: 8, right: 24, top: 8, bottom: 2, containLabel: true }, xAxis: yAxis(theme.isDark), yAxis: xAxis(theme.isDark, { data: rows.map(r => r.feature), axisLabel: { fontSize: 9.5 } }), series: [barSeries(importance.value?.importance_method || 'importance', rows.map(r => r.importance))] }
})
const resultColumns = [{ key: 'publication_id', label: '发布 ID', type: 'number', format: String }, { key: 'model_id', label: 'model_id', ellipsis: true }, { key: 'ts_code', label: '股票' }, { key: 'data_cutoff', label: '行情截止' }, { key: 'latest', label: '最新信号预测', format: value => value ? percent(value.predicted_return) : '暂无' }]
</script>
<style scoped>
.model-list { display:flex; flex-direction:column; gap:8px; }
.model-card { border:1px solid var(--border); border-radius:10px; background:var(--surface-2); overflow:hidden; }
.model-card.selected { border-color:var(--accent); }
.model-head { display:flex; align-items:center; justify-content:space-between; gap:10px; padding:9px 14px; }
.model-name { font-family:'Cascadia Code',Consolas,monospace; font-weight:650; font-size:13px; }
.model-type-tag { font-size:10.5px; color:var(--accent); background:var(--accent-soft); border-radius:999px; padding:1px 9px; }
.detail-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(180px,1fr)); gap:8px; }
.detail-item { display:flex; flex-direction:column; font-size:12px; }
.metric-row { display:flex; justify-content:space-between; border-bottom:1px dashed var(--border); padding:6px 0; font-size:12.5px; }
.two-col { grid-template-columns:1fr 1fr; }
.latest-return { font-size:28px; font-weight:700; }
.hash, .model-head .muted { overflow-wrap:anywhere; }
details code { overflow-wrap:anywhere; }
@media(max-width:1200px) { .two-col { grid-template-columns:1fr; } }
</style>
