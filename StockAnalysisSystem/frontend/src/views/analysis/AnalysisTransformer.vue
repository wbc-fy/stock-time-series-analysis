<template>
  <div>
    <!-- 控件 -->
    <div class="card section">
      <div class="card-title">训练 / 预测配置</div>
      <div class="card-sub">
        final_score = (pred − random) / (max − random)，衡量超越随机基线的程度（无 IC 口径）·
        multi_head 输出 4 头：{{ HEAD_DESC.join(' / ') }}
      </div>
      <div class="param-row">
        <div class="field">
          <label>transformer_model</label>
          <select v-model="cfg.transformer_model">
            <option v-for="t in configs.model_types || []" :key="t" :value="t">{{ t }}</option>
          </select>
        </div>
        <div class="field">
          <label>feature_set</label>
          <select v-model="cfg.feature_set">
            <option v-for="f in configs.feature_sets || []" :key="f" :value="f">{{ f }}</option>
          </select>
        </div>
        <div class="field">
          <label>top_k（{{ cfg.top_k }}）</label>
          <input v-model.number="cfg.top_k" type="number" :min="kRange[0]" :max="kRange[1]" />
        </div>
        <div class="field">
          <label>epochs</label>
          <input v-model.number="cfg.epochs" type="number" min="5" max="200" step="10" />
        </div>
        <div class="field">
          <label>learning_rate</label>
          <input v-model.number="cfg.lr" type="number" step="0.0002" min="0.0001" max="0.01" />
        </div>
        <div class="field submit-field">
          <label>&nbsp;</label>
          <button class="btn btn-primary" :disabled="running" @click="run">
            {{ running ? '训练中…' : '运行训练 + 预测' }}
          </button>
        </div>
      </div>
    </div>

    <!-- 输出头说明 -->
    <div v-if="train.heads" class="grid heads-grid section">
      <div v-for="h in train.heads" :key="h" class="card head-card">
        <div class="head-name">{{ h }}</div>
        <div class="head-desc muted">{{ headDesc(h) }}</div>
      </div>
    </div>

    <!-- 训练历史 -->
    <div class="grid two-col section">
      <div class="card">
        <div class="card-title">
          训练历史 · final_score / ratio_pred
          <span v-if="train.best_epoch" class="best-badge">Best epoch {{ train.best_epoch }} · {{ fmtNum(train.best_final_score, 4) }}</span>
        </div>
        <ChartBox :option="scoreOption" :height="260" />
      </div>
      <div class="card">
        <div class="card-title">Loss 曲线（train / eval）</div>
        <ChartBox :option="lossOption" :height="260" />
      </div>
    </div>

    <!-- 预测结果 -->
    <div class="card section">
      <div class="card-title">TopK 预测结果（Top{{ cfg.top_k }}）</div>
      <DataTable :columns="predColumns" :rows="preds" row-key="ts_code" />
    </div>

    <div class="grid two-col section">
      <div class="card">
        <div class="card-title">预测分数 vs 调整后分数</div>
        <ChartBox :option="scoreBarOption" :height="260" />
      </div>
      <div class="card">
        <div class="card-title">不确定性散点（adjusted_score × uncertainty）</div>
        <ChartBox :option="uncOption" :height="260" />
      </div>
    </div>

    <div class="grid two-col section">
      <div class="card">
        <div class="card-title">Top5 权重占比</div>
        <ChartBox :option="pieOption" :height="260" />
      </div>
      <div class="card">
        <div class="card-title">权重分布（含均分参考线）</div>
        <ChartBox :option="weightOption" :height="260" />
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, reactive, ref } from 'vue'
import ChartBox from '../../components/ChartBox.vue'
import DataTable from '../../components/DataTable.vue'
import { getTransformerConfigs, getTransformerTraining, getTransformerPredictions } from '../../api/analysis.js'
import { useThemeStore } from '../../stores/theme.js'
import { fmtNum } from '../../utils/format.js'
import { baseOption, xAxis, yAxis, lineSeries, barSeries, chrome, seriesColors } from '../../utils/chart.js'

defineProps({ tsCode: { type: String, required: true } })
const theme = useThemeStore()

const HEAD_DESC = ['ranking 排序头', 'regression 回归头', 'classification 分类头', 'direction 方向头']
function headDesc(h) {
  return {
    ranking: 'ListNet 排序损失，输出横截面相对强弱',
    regression: '预测未来 5 日收益率（连续值）',
    classification: '涨跌二分类概率',
    direction: '方向一致性约束，抑制反向预测',
  }[h] || ''
}

const configs = ref({})
const kRange = computed(() => configs.value.top_k_range || [3, 20])
const cfg = reactive({ transformer_model: 'multi_head', feature_set: '158+39', top_k: 8, epochs: 50, lr: 0.0008 })

const train = ref({})
const preds = ref([])
const running = ref(false)

async function run() {
  running.value = true
  const [t, p] = await Promise.all([
    getTransformerTraining({ ...cfg }),
    getTransformerPredictions({ ...cfg }),
  ])
  train.value = t
  preds.value = p
  running.value = false
}

onMounted(async () => {
  configs.value = await getTransformerConfigs()
  Object.assign(cfg, configs.value.defaults || {})
  await run()
})

const predColumns = [
  { key: 'rank', label: '#', width: '44px', align: 'right', type: 'number', format: (v) => String(v) },
  { key: 'ts_code', label: 'ts_code', width: '96px' },
  { key: 'name', label: '名称', width: '90px' },
  { key: 'score', label: 'score', align: 'right', type: 'number', format: (v) => fmtNum(v, 4) },
  { key: 'adjusted_score', label: 'adjusted_score', align: 'right', type: 'number', format: (v) => fmtNum(v, 4) },
  { key: 'uncertainty', label: 'uncertainty', align: 'right', type: 'number', format: (v) => fmtNum(v, 4) },
  { key: 'weight', label: 'weight(%)', align: 'right', type: 'number', format: (v) => fmtNum(v, 2) },
]

const epochs = computed(() => (train.value.history || []).map((h) => h.epoch))

const scoreOption = computed(() => {
  const h = train.value.history
  if (!h?.length) return baseOption(theme.isDark)
  const colors = seriesColors(theme.isDark)
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis' },
    grid: { left: 8, right: 16, top: 30, bottom: 2, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: epochs.value, name: 'epoch', nameTextStyle: { fontSize: 10 }, axisLabel: { fontSize: 10 } }),
    yAxis: yAxis(theme.isDark, { scale: true, axisLabel: { fontSize: 10 } }),
    series: [
      lineSeries('final_score', h.map((r) => r.final_score), {
        color: colors[0],
        markPoint: {
          symbol: 'pin', symbolSize: 40,
          itemStyle: { color: colors[0] },
          label: { fontSize: 9, color: '#fff', formatter: 'best' },
          data: [{ coord: [String(train.value.best_epoch), train.value.best_final_score] }],
        },
      }),
      lineSeries('ratio_pred', h.map((r) => r.ratio_pred), { color: colors[2] }),
    ],
  }
})

const lossOption = computed(() => {
  const h = train.value.history
  if (!h?.length) return baseOption(theme.isDark)
  const c = chrome(theme.isDark)
  const colors = seriesColors(theme.isDark)
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis' },
    grid: { left: 8, right: 16, top: 30, bottom: 2, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: epochs.value, name: 'epoch', nameTextStyle: { fontSize: 10 }, axisLabel: { color: c.muted, fontSize: 10 } }),
    yAxis: yAxis(theme.isDark, { scale: true, axisLabel: { fontSize: 10 } }),
    series: [
      lineSeries('train_loss', h.map((r) => r.train_loss), { color: colors[1] }),
      lineSeries('eval_loss', h.map((r) => r.eval_loss), { color: colors[4], lineStyle: { width: 2, type: 'dashed' } }),
    ],
  }
})

const scoreBarOption = computed(() => {
  const c = chrome(theme.isDark)
  const names = preds.value.map((p) => p.ts_code)
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis', axisPointer: { type: 'shadow' } },
    grid: { left: 8, right: 16, top: 30, bottom: 2, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: names, axisLabel: { color: c.muted, fontSize: 9.5, rotate: 32 } }),
    yAxis: yAxis(theme.isDark, { axisLabel: { fontSize: 10 } }),
    series: [
      barSeries('score', preds.value.map((p) => p.score)),
      barSeries('adjusted_score', preds.value.map((p) => p.adjusted_score)),
    ],
  }
})

const uncOption = computed(() => {
  const c = chrome(theme.isDark)
  const colors = seriesColors(theme.isDark)
  return {
    ...baseOption(theme.isDark),
    tooltip: {
      ...baseOption(theme.isDark).tooltip,
      formatter: (p) => `${p.data[2]}（${p.data[3]}）<br/>adjusted=${p.data[0]} · uncertainty=${p.data[1]}`,
    },
    grid: { left: 8, right: 16, top: 20, bottom: 2, containLabel: true },
    xAxis: yAxis(theme.isDark, { name: 'adjusted_score', nameTextStyle: { color: c.muted, fontSize: 10 }, scale: true, axisLabel: { fontSize: 10 } }),
    yAxis: yAxis(theme.isDark, { name: 'uncertainty', nameTextStyle: { color: c.muted, fontSize: 10 }, scale: true, axisLabel: { fontSize: 10 } }),
    series: [{
      type: 'scatter',
      symbolSize: 14,
      itemStyle: { color: colors[4], opacity: 0.8 },
      data: preds.value.map((p) => [p.adjusted_score, p.uncertainty, p.ts_code, p.name]),
    }],
  }
})

const pieOption = computed(() => {
  const top5 = preds.value.slice(0, 5)
  const c = chrome(theme.isDark)
  const colors = seriesColors(theme.isDark)
  return {
    backgroundColor: 'transparent',
    color: colors,
    textStyle: { fontFamily: baseOption(theme.isDark).textStyle.fontFamily },
    tooltip: { ...baseOption(theme.isDark).tooltip, formatter: (p) => `${p.name}<br/>权重 ${p.value}%（${p.percent}%）` },
    series: [{
      type: 'pie',
      radius: ['42%', '68%'],
      center: ['50%', '50%'],
      itemStyle: { borderColor: c.surface, borderWidth: 2, borderRadius: 5 },
      label: { color: c.text2, fontSize: 11, formatter: '{b}\n{c}%' },
      data: top5.map((p) => ({ name: `${p.ts_code} ${p.name}`, value: p.weight })),
    }],
  }
})

const weightOption = computed(() => {
  const c = chrome(theme.isDark)
  const n = preds.value.length || 1
  const avg = preds.value.reduce((a, p) => a + p.weight, 0) / n
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis', axisPointer: { type: 'shadow' }, valueFormatter: (v) => v + '%' },
    grid: { left: 8, right: 16, top: 30, bottom: 2, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: preds.value.map((p) => p.ts_code), axisLabel: { color: c.muted, fontSize: 9.5, rotate: 32 } }),
    yAxis: yAxis(theme.isDark, { axisLabel: { fontSize: 10, formatter: '{value}%' } }),
    series: [
      barSeries('weight', preds.value.map((p) => p.weight), {
        markLine: {
          symbol: 'none', silent: true,
          lineStyle: { type: 'dashed', color: c.muted },
          label: { color: c.muted, fontSize: 10, formatter: `均分 ${avg.toFixed(1)}%` },
          data: [{ yAxis: +avg.toFixed(2) }],
        },
      }),
    ],
  }
})
</script>

<style scoped>
.param-row {
  display: flex;
  flex-wrap: wrap;
  gap: 14px;
  align-items: flex-end;
}

.param-row .field {
  min-width: 130px;
}

.submit-field .btn {
  height: 34px;
}

.heads-grid {
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
}

.head-card {
  padding: 12px 14px;
}

.head-name {
  font-family: 'Cascadia Code', Consolas, monospace;
  font-weight: 700;
  font-size: 13.5px;
  color: var(--accent);
}

.head-desc {
  font-size: 12px;
  margin-top: 4px;
  line-height: 1.5;
}

.two-col {
  grid-template-columns: 1fr 1fr;
}

@media (max-width: 1100px) {
  .two-col { grid-template-columns: 1fr; }
}

.best-badge {
  margin-left: 10px;
  font-size: 11px;
  font-weight: 600;
  color: var(--good);
  background: rgba(12, 163, 12, 0.1);
  border: 1px solid rgba(12, 163, 12, 0.3);
  padding: 1px 9px;
  border-radius: 999px;
}
</style>
