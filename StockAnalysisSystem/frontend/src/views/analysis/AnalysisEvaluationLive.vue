<template>
  <div class="evaluation-panel">
    <div class="card section">
      <div class="card-title">真实滚动评估 · {{ tsCode }}</div>
      <p class="historical-warning">历史数据已被观察：这是回顾性滚动评估，不是前瞻验证、未见过的独立留出集或实时预测；不构成投资建议。</p>
      <p v-if="state.listLoading" role="status">报告列表加载中…</p>
      <p v-else-if="state.listError" role="alert">{{ state.listError }} <button class="btn btn-sm" @click="flow.loadStock(tsCode)">重试</button></p>
      <p v-else-if="!state.rows.length" role="status">该股票暂无已发布滚动评估报告。请在离线环境运行 V0.8 评估与发布命令后重试；此页面不会自动训练。</p>
      <div v-else class="report-picker">
        <label for="evaluation-report">已发布报告</label>
        <select id="evaluation-report" :value="state.selected" @change="flow.selectReport($event.target.value)">
          <option v-for="row in state.rows" :key="row.report_id" :value="row.report_id">{{ row.created_at }} · {{ row.report_id }}</option>
        </select>
      </div>
      <p v-if="state.detailLoading" role="status">报告详情加载中…</p>
      <p v-else-if="state.detailError" role="alert">{{ state.detailError }} <button class="btn btn-sm" @click="flow.selectReport(state.selected)">重试</button></p>
      <p v-else-if="state.selected && !state.report" role="status">该报告不存在或尚未发布。请选择其他报告，或检查离线发布结果。</p>
      <template v-if="state.report">
        <p class="muted">来源截止 {{ state.report.source.signal_end }} · 创建 {{ state.report.created_at }}</p>
        <p class="muted protocol">协议 {{ state.report.protocol_id }} · 特征 {{ state.report.feature_version }} · 下一交易日收盘收益率</p>
      </template>
    </div>
    <template v-if="state.report">
      <div v-for="fold in state.report.folds" :key="fold.fold_id" class="card section fold">
        <div class="card-title">窗口 {{ fold.fold_id }} · 扩展训练 / 100 个评估信号日</div>
        <div class="fold-boundaries">
          <div><span class="muted">训练 · {{ fold.train.count }} 样本</span><p>{{ fold.train.signal_start }} → {{ fold.train.signal_end }}</p><span class="muted">标签截止 {{ fold.train.label_end }} · purge 排除 {{ fold.train.purged_count }} 样本</span></div>
          <div><span class="muted">评估 · {{ fold.evaluation.count }} 样本</span><p>{{ fold.evaluation.signal_start }} → {{ fold.evaluation.signal_end }}</p><span class="muted">目标日 {{ fold.evaluation.target_start }} → {{ fold.evaluation.target_end }}</span></div>
        </div>
        <div class="metric-scroll"><DataTable :columns="metricColumns" :rows="metricRows(fold.metrics)" row-key="method" /></div>
      </div>
      <div class="card section">
        <div class="card-title">总体评估 · 合并 300 个信号日</div>
        <p class="card-sub">直接以合并序列计算，不取三个窗口指标的平均值。RMSE / MAE 单位为百分点；R² 无量纲，未定义时保留未定义。</p>
        <div class="metric-scroll"><DataTable :columns="metricColumns" :rows="metricRows(state.report.overall_metrics)" row-key="method" /></div>
      </div>
      <div class="card section curve">
        <div class="card-title">实际 + 三种方法 · 下一交易日收益率（%）</div>
        <p class="card-sub">同一组 300 个信号日 / 相邻交易目标日。零收益基线始终展示，所有方法使用相同日期。</p>
        <ChartBox :option="curveOption" :height="340" />
      </div>
    </template>
  </div>
</template>
<script setup>
import { computed, reactive, watch, onBeforeUnmount } from 'vue'
import ChartBox from '../../components/ChartBox.vue'
import DataTable from '../../components/DataTable.vue'
import { getEvaluations, getEvaluation } from '../../api/analysis.js'
import { dataSource } from '../../api/client.js'
import { EVALUATION_METHODS, createEvaluationController } from '../../utils/evaluation.js'
import { returnPercent } from '../../utils/prediction.js'
import { useThemeStore } from '../../stores/theme.js'
import { baseOption, xAxis, yAxis, lineSeries } from '../../utils/chart.js'
const props = defineProps({ tsCode: { type: String, required: true } })
const theme = useThemeStore(), state = reactive({})
const flow = createEvaluationController({ state, list: getEvaluations, detail: getEvaluation })
watch(() => [props.tsCode, dataSource.value], () => {
  if (dataSource.value === 'hybrid') void flow.loadStock(props.tsCode)
  else flow.cancel()
}, { immediate: true, flush: 'sync' })
onBeforeUnmount(() => flow.cancel())
const labels = { zero_return: '零收益基线', ridge: 'Ridge', xgboost: 'XGBoost' }
const metricRows = metrics => EVALUATION_METHODS.map(method => ({ method, label: labels[method], ...metrics[method] }))
const metricValue = value => `${returnPercent(value).toFixed(4)} pp`
const metricColumns = [
  { key: 'label', label: '方法' },
  { key: 'rmse', label: 'RMSE（百分点）', type: 'number', format: metricValue },
  { key: 'mae', label: 'MAE（百分点）', type: 'number', format: metricValue },
  { key: 'r2', label: 'R²', type: 'number', format: value => value === null ? '未定义' : value.toFixed(4) },
]
const curveOption = computed(() => {
  const rows = state.report?.series || []
  return {
    ...baseOption(theme.isDark),
    grid: { left: 8, right: 14, top: 50, bottom: 14, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: rows.map(row => `${row.signal_date}\n→${row.target_date}`) }),
    yAxis: yAxis(theme.isDark, { axisLabel: { formatter: '{value}%' } }),
    series: [lineSeries('实际收益率', rows.map(row => returnPercent(row.actual_return))), ...EVALUATION_METHODS.map(method => lineSeries(labels[method], rows.map(row => returnPercent(row.predicted_returns[method]))))],
  }
})
</script>
<style scoped>
.evaluation-panel, .curve { min-width:0; max-width:100%; }
.historical-warning { border-left:3px solid var(--accent); padding:8px 12px; background:var(--accent-soft); line-height:1.7; }
.report-picker { display:flex; flex-wrap:wrap; align-items:center; gap:8px; }
.report-picker select { min-width:0; max-width:100%; flex:1 1 260px; padding:8px; color:var(--text-1); background:var(--surface-2); border:1px solid var(--border); border-radius:6px; }
select:focus-visible, button:focus-visible { outline:2px solid var(--accent); outline-offset:3px; }
.fold-boundaries { display:grid; grid-template-columns:1fr 1fr; gap:16px; margin:12px 0; font-variant-numeric:tabular-nums; }
.fold-boundaries p { margin:4px 0; }
.metric-scroll { overflow-x:auto; max-width:100%; }
.metric-scroll :deep(table) { min-width:530px; }
.protocol { overflow-wrap:anywhere; font-family:'Cascadia Code',Consolas,monospace; font-size:12px; }
@media(max-width:600px) { .fold-boundaries { grid-template-columns:1fr; } }
</style>
