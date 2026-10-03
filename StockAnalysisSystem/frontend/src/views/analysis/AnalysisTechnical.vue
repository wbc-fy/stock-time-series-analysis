<template>
  <div>
    <!-- 指标图 -->
    <div class="grid chart-grid section">
      <div class="card">
        <div class="card-title">MACD（DIF / DEA / 柱）</div>
        <ChartBox :option="macdOption" :height="220" />
      </div>
      <div class="card">
        <div class="card-title">KDJ（9,3,3）</div>
        <ChartBox :option="kdjOption" :height="220" />
      </div>
      <div class="card">
        <div class="card-title">RSI(14)</div>
        <ChartBox :option="rsiOption" :height="220" />
      </div>
      <div class="card">
        <div class="card-title">布林带 BOLL(20,2)</div>
        <ChartBox :option="bollOption" :height="220" />
      </div>
      <div class="card">
        <div class="card-title">BIAS 乖离率（5/10/20/60）</div>
        <ChartBox :option="biasOption" :height="220" />
      </div>
      <div class="card">
        <div class="card-title">CCI（10/15/20/88）</div>
        <ChartBox :option="cciOption" :height="220" />
      </div>
      <div class="card">
        <div class="card-title">Aroon(25) 与 AR/BR</div>
        <ChartBox :option="aroonOption" :height="220" />
      </div>
      <div class="card">
        <div class="card-title">成交量与量均线 · 量比</div>
        <ChartBox :option="volOption" :height="220" />
      </div>
    </div>

    <!-- 特征目录 -->
    <div class="card section">
      <div class="card-title">stock_features 特征目录</div>
      <div class="card-sub">
        FEATURE_VERSION {{ catalog.version }} · 时序特征 {{ catalog.tsFeatureCount }} + 横截面 {{ catalog.crossSectionalCount }} = {{ catalog.total }} 列
        （future_* 为训练标签：{{ (catalog.labels || []).join('、') }}，不作为特征展示）
      </div>
      <div v-for="g in catalog.groups || []" :key="g.group" class="feat-group">
        <span class="feat-group-label">{{ g.label }}</span>
        <span v-for="f in g.features" :key="f.name" class="feat-chip" :title="f.cn">
          <code>{{ f.name }}</code>
          <em>{{ f.cn }}</em>
        </span>
      </div>
    </div>

    <!-- 特征行预览 -->
    <div class="card section">
      <div class="card-title">特征值预览（stock_features 最近 30 行 · v2 列名）</div>
      <DataTable :columns="featureColumns" :rows="featureRows" :page-size="10" />
    </div>
  </div>
</template>

<script setup>
import { computed, ref, watch } from 'vue'
import ChartBox from '../../components/ChartBox.vue'
import DataTable from '../../components/DataTable.vue'
import { getIndicatorSeries, getFeatureCatalog, getFeatureRows, getKline } from '../../api/analysis.js'
import { useThemeStore } from '../../stores/theme.js'
import { baseOption, xAxis, yAxis, lineSeries, barSeries, chrome, seriesColors } from '../../utils/chart.js'

const props = defineProps({ tsCode: { type: String, required: true } })
const theme = useThemeStore()

const feats = ref([])
const bars = ref([])
const catalog = ref({})
const featureRows = ref([])
const WINDOW = 120 // 图表展示最近 120 个交易日

watch(() => props.tsCode, async (code) => {
  const [series, cat, rows, kline] = await Promise.all([
    getIndicatorSeries(code), getFeatureCatalog(), getFeatureRows(code, 30), getKline(code),
  ])
  feats.value = series.slice(-WINDOW)
  bars.value = kline.bars.slice(-WINDOW)
  catalog.value = cat
  featureRows.value = [...rows].reverse()
}, { immediate: true })

const dates = computed(() => feats.value.map((f) => f.trade_date.slice(5)))

function pick(key, digits = 2) {
  return feats.value.map((f) => (f[key] == null ? null : +Number(f[key]).toFixed(digits)))
}

function chart(title, series, extra = {}) {
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis' },
    legend: { ...baseOption(theme.isDark).legend },
    grid: { left: 8, right: 14, top: 28, bottom: 2, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: dates.value, axisLabel: { color: chrome(theme.isDark).muted, fontSize: 10, interval: 23 } }),
    yAxis: yAxis(theme.isDark, { scale: true, splitNumber: 3, axisLabel: { fontSize: 10 } }),
    series,
    ...extra,
  }
}

const macdOption = computed(() => {
  const c = chrome(theme.isDark)
  const hist = pick('macd_hist')
  return chart('MACD', [
    {
      name: 'macd_hist', type: 'bar', barMaxWidth: 5,
      data: hist.map((v) => ({ value: v, itemStyle: { color: v >= 0 ? c.up : c.down } })),
    },
    lineSeries('macd_dif', pick('macd_dif')),
    lineSeries('macd_dea', pick('macd_dea')),
  ])
})

const kdjOption = computed(() => chart('KDJ', [
  lineSeries('kdj_k', pick('kdj_k')),
  lineSeries('kdj_d', pick('kdj_d')),
  lineSeries('kdj_j', pick('kdj_j')),
]))

const rsiOption = computed(() => {
  const c = chrome(theme.isDark)
  return chart('RSI', [
    lineSeries('rsi', pick('rsi'), {
      markLine: {
        symbol: 'none', silent: true,
        lineStyle: { type: 'dashed', color: c.muted },
        label: { color: c.muted, fontSize: 10 },
        data: [{ yAxis: 70, name: '超买' }, { yAxis: 30, name: '超卖' }],
      },
    }),
  ], { yAxis: yAxis(theme.isDark, { min: 0, max: 100, splitNumber: 4, axisLabel: { fontSize: 10 } }) })
})

const bollOption = computed(() => chart('BOLL', [
  lineSeries('close', bars.value.map((b) => b.close), { lineStyle: { width: 1.6 } }),
  lineSeries('bb_upper', pick('bb_upper'), { lineStyle: { width: 1.2, type: 'dashed' } }),
  lineSeries('bb_middle', pick('bb_middle'), { lineStyle: { width: 1.2 } }),
  lineSeries('bb_lower', pick('bb_lower'), { lineStyle: { width: 1.2, type: 'dashed' } }),
]))

const biasOption = computed(() => chart('BIAS', [
  lineSeries('bias5', pick('bias5')),
  lineSeries('bias10', pick('bias10')),
  lineSeries('bias20', pick('bias20')),
  lineSeries('bias60', pick('bias60')),
]))

const cciOption = computed(() => chart('CCI', [
  lineSeries('cci10', pick('cci10', 1)),
  lineSeries('cci15', pick('cci15', 1)),
  lineSeries('cci20', pick('cci20', 1)),
  lineSeries('cci88', pick('cci88', 1)),
]))

const aroonOption = computed(() => chart('Aroon', [
  lineSeries('arron_up_25', pick('arron_up_25', 1)),
  lineSeries('arron_down_25', pick('arron_down_25', 1)),
  lineSeries('ar', pick('ar', 1), { lineStyle: { width: 1.2, type: 'dotted' } }),
  lineSeries('br', pick('br', 1), { lineStyle: { width: 1.2, type: 'dotted' } }),
]))

const volOption = computed(() => {
  const c = chrome(theme.isDark)
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis' },
    legend: { ...baseOption(theme.isDark).legend },
    grid: [
      { left: 8, right: 14, top: 28, height: '52%', containLabel: true },
      { left: 8, right: 14, top: '72%', height: '22%', containLabel: true },
    ],
    xAxis: [
      xAxis(theme.isDark, { data: dates.value, axisLabel: { show: false } }),
      xAxis(theme.isDark, { data: dates.value, gridIndex: 1, axisLabel: { color: c.muted, fontSize: 10, interval: 23 } }),
    ],
    yAxis: [
      yAxis(theme.isDark, { name: '手', nameTextStyle: { color: c.muted, fontSize: 10 }, splitNumber: 3, axisLabel: { fontSize: 10, formatter: (v) => (v >= 10000 ? (v / 10000).toFixed(0) + '万' : v) } }),
      yAxis(theme.isDark, { gridIndex: 1, name: '量比', nameTextStyle: { color: c.muted, fontSize: 10 }, splitNumber: 2, axisLabel: { fontSize: 10 } }),
    ],
    series: [
      {
        name: 'vol', type: 'bar', barMaxWidth: 5,
        data: bars.value.map((b) => ({
          value: b.vol,
          itemStyle: { color: b.close >= b.open ? c.up : c.down, opacity: 0.7 },
        })),
      },
      lineSeries('vol_ma5', pick('vol_ma5', 0)),
      lineSeries('vol_ma10', pick('vol_ma10', 0)),
      lineSeries('volume_ratio', pick('volume_ratio'), { xAxisIndex: 1, yAxisIndex: 1, lineStyle: { width: 1.4 } }),
    ],
  }
})

/* 特征表列（挑选代表性 v2 列，其余横向滚动） */
const FEATURE_TABLE_COLS = [
  'trade_date', 'return_1d', 'return_5d', 'ma5', 'ma20', 'macd_dif', 'macd_hist',
  'rsi', 'kdj_k', 'kdj_j', 'bb_width', 'bias20', 'cci20', 'volume_ratio', 'mfi14',
  'volatility_20d', 'skewness_20d', 'kurtosis_20d',
  'market_return', 'industry_return', 'return_vs_market', 'return_zscore_industry',
]
const featureColumns = FEATURE_TABLE_COLS.map((k) => ({
  key: k, label: k, align: k === 'trade_date' ? 'left' : 'right',
  type: k === 'trade_date' ? undefined : 'number',
  format: k === 'trade_date' ? undefined : (v) => (v == null ? '—' : Number(v).toFixed(4)),
}))
</script>

<style scoped>
.chart-grid {
  grid-template-columns: repeat(2, 1fr);
}

@media (max-width: 1200px) {
  .chart-grid { grid-template-columns: 1fr; }
}

.feat-group {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
  padding: 7px 0;
  border-bottom: 1px dashed var(--border);
}

.feat-group:last-child {
  border-bottom: none;
}

.feat-group-label {
  font-size: 12px;
  font-weight: 650;
  color: var(--text-2);
  min-width: 84px;
}

.feat-chip {
  display: inline-flex;
  align-items: baseline;
  gap: 5px;
  padding: 2px 9px;
  border: 1px solid var(--border);
  border-radius: 999px;
  background: var(--surface-2);
}

.feat-chip code {
  font-family: 'Cascadia Code', Consolas, monospace;
  font-size: 11px;
  color: var(--text-1);
}

.feat-chip em {
  font-style: normal;
  font-size: 10.5px;
  color: var(--text-3);
}
</style>
