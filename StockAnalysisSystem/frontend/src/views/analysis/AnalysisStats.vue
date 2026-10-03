<template>
  <div>
    <!-- 描述统计 + 趋势 -->
    <div class="grid stat-top section">
      <div class="card">
        <div class="card-title">日收益率描述统计（%）</div>
        <div class="card-sub">{{ tsCode }} · 最近 260 个交易日</div>
        <div class="desc-grid">
          <div v-for="d in descItems" :key="d.k" class="desc-item">
            <span class="muted">{{ d.k }}</span>
            <span class="num desc-val" :class="d.cls">{{ d.v }}</span>
          </div>
        </div>
      </div>
      <div class="card">
        <div class="card-title">趋势分析（近 {{ stat.trend?.window || 60 }} 日线性回归）</div>
        <div class="card-sub">close ~ t 最小二乘拟合</div>
        <div class="trend-body">
          <div class="trend-dir" :class="stat.trend?.direction">
            {{ { up: '↑ 上升趋势', down: '↓ 下降趋势', flat: '→ 横盘震荡' }[stat.trend?.direction] || '…' }}
          </div>
          <div class="desc-grid">
            <div class="desc-item">
              <span class="muted">slope（日均 %）</span>
              <span class="num desc-val" :class="signClass(stat.trend?.slope)">{{ fmtNum(stat.trend?.slope, 4) }}</span>
            </div>
            <div class="desc-item">
              <span class="muted">r_squared</span>
              <span class="num desc-val">{{ fmtNum(stat.trend?.r_squared, 4) }}</span>
            </div>
          </div>
          <p class="muted trend-note">r² 越接近 1，趋势越"干净"；低 r² 表示价格以震荡为主，线性方向参考意义有限。</p>
        </div>
      </div>
    </div>

    <!-- 直方图 + 回撤 -->
    <div class="grid two-col section">
      <div class="card">
        <div class="card-title">收益率分布直方图（24 桶）</div>
        <ChartBox :option="histOption" :height="260" />
      </div>
      <div class="card">
        <div class="card-title">
          回撤曲线
          <span v-if="stat.drawdown" class="dd-badge">最大回撤 {{ fmtPctRaw(stat.drawdown.max_drawdown) }} @ {{ stat.drawdown.max_drawdown_date }}</span>
        </div>
        <ChartBox :option="drawdownOption" :height="260" />
      </div>
    </div>

    <!-- 波动率 + 相关性 -->
    <div class="grid two-col section">
      <div class="card">
        <div class="card-title">滚动波动率（年化 %）</div>
        <ChartBox :option="volOption" :height="280" />
      </div>
      <div class="card">
        <div class="card-title">特征相关性热力图（Pearson）</div>
        <ChartBox :option="corrOption" :height="280" />
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, ref, watch } from 'vue'
import ChartBox from '../../components/ChartBox.vue'
import { getStatisticalAnalysis } from '../../api/analysis.js'
import { useThemeStore } from '../../stores/theme.js'
import { fmtNum, fmtPctRaw, signClass } from '../../utils/format.js'
import { baseOption, xAxis, yAxis, barSeries, lineSeries, chrome, seriesColors } from '../../utils/chart.js'

const props = defineProps({ tsCode: { type: String, required: true } })
const theme = useThemeStore()
const stat = ref({})

watch(() => props.tsCode, async (code) => {
  stat.value = await getStatisticalAnalysis(code)
}, { immediate: true })

const descItems = computed(() => {
  const d = stat.value.descriptive
  if (!d) return []
  return [
    { k: 'mean', v: fmtNum(d.mean, 4) },
    { k: 'std', v: fmtNum(d.std, 4) },
    { k: 'min', v: fmtNum(d.min, 4), cls: 'down' },
    { k: '25%', v: fmtNum(d.q25, 4) },
    { k: '50%', v: fmtNum(d.q50, 4) },
    { k: '75%', v: fmtNum(d.q75, 4) },
    { k: 'max', v: fmtNum(d.max, 4), cls: 'up' },
    { k: 'skewness', v: fmtNum(d.skewness, 4) },
    { k: 'kurtosis', v: fmtNum(d.kurtosis, 4) },
  ]
})

const histOption = computed(() => {
  const bins = stat.value.histogram || []
  const c = chrome(theme.isDark)
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis', axisPointer: { type: 'shadow' } },
    grid: { left: 8, right: 14, top: 24, bottom: 2, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: bins.map((b) => b.range), axisLabel: { color: c.muted, fontSize: 10, interval: 3 } }),
    yAxis: yAxis(theme.isDark, { name: '天数', nameTextStyle: { color: c.muted, fontSize: 10 } }),
    series: [
      barSeries('count', bins.map((b) => ({
        value: b.count,
        itemStyle: { color: b.range >= 0 ? c.up : c.down, opacity: 0.8, borderRadius: [3, 3, 0, 0] },
      })), { barMaxWidth: 18 }),
    ],
  }
})

const drawdownOption = computed(() => {
  const dd = stat.value.drawdown
  if (!dd) return baseOption(theme.isDark)
  const c = chrome(theme.isDark)
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis' },
    grid: { left: 8, right: 14, top: 24, bottom: 2, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: dd.series.map((p) => p.date.slice(5)), axisLabel: { color: c.muted, fontSize: 10, interval: 25 } }),
    yAxis: yAxis(theme.isDark, { max: 0, axisLabel: { fontSize: 10, formatter: '{value}%' } }),
    series: [
      {
        ...lineSeries('回撤', dd.series.map((p) => p.value)),
        color: c.critical,
        areaStyle: { color: c.critical, opacity: 0.14 },
        markPoint: {
          symbol: 'pin', symbolSize: 42,
          itemStyle: { color: c.critical },
          label: { fontSize: 9, color: '#fff', formatter: () => fmtPctRaw(dd.max_drawdown) },
          data: [{ coord: [dd.max_drawdown_date.slice(5), dd.max_drawdown] }],
        },
      },
    ],
  }
})

const volOption = computed(() => {
  const v = stat.value.volatility
  if (!v) return baseOption(theme.isDark)
  const c = chrome(theme.isDark)
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis' },
    grid: { left: 8, right: 14, top: 28, bottom: 2, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: v.dates.map((d) => d.slice(5)), axisLabel: { color: c.muted, fontSize: 10, interval: 25 } }),
    yAxis: yAxis(theme.isDark, { axisLabel: { fontSize: 10, formatter: '{value}%' } }),
    series: [
      lineSeries('vol20', v.vol20),
      lineSeries('vol60', v.vol60),
      lineSeries('vol120', v.vol120),
    ],
  }
})

const corrOption = computed(() => {
  const corr = stat.value.correlation
  if (!corr) return baseOption(theme.isDark)
  const c = chrome(theme.isDark)
  const data = []
  corr.matrix.forEach((row, i) => row.forEach((v, j) => data.push([j, i, v])))
  return {
    backgroundColor: 'transparent',
    textStyle: { fontFamily: baseOption(theme.isDark).textStyle.fontFamily },
    tooltip: {
      ...baseOption(theme.isDark).tooltip,
      formatter: (p) => `${corr.features[p.value[1]]} × ${corr.features[p.value[0]]}<br/>ρ = ${p.value[2]}`,
    },
    grid: { left: 8, right: 8, top: 8, bottom: 60, containLabel: true },
    xAxis: {
      type: 'category', data: corr.features, position: 'bottom',
      axisLine: { show: false }, axisTick: { show: false },
      axisLabel: { color: c.muted, fontSize: 9, rotate: 42 },
      splitArea: { show: false },
    },
    yAxis: {
      type: 'category', data: corr.features, inverse: true,
      axisLine: { show: false }, axisTick: { show: false },
      axisLabel: { color: c.muted, fontSize: 9 },
      splitArea: { show: false },
    },
    visualMap: {
      min: -1, max: 1, calculable: false, orient: 'horizontal',
      left: 'center', bottom: 0, itemWidth: 10, itemHeight: 90,
      textStyle: { color: c.muted, fontSize: 10 },
      inRange: { color: [c.down, c.surface, c.up] },
    },
    series: [{
      type: 'heatmap', data,
      label: { show: true, fontSize: 8.5, color: c.text1, formatter: (p) => p.value[2].toFixed(1) },
      itemStyle: { borderColor: c.surface, borderWidth: 1 },
      emphasis: { itemStyle: { shadowBlur: 6, shadowColor: 'rgba(0,0,0,0.3)' } },
    }],
  }
})
</script>

<style scoped>
.stat-top {
  grid-template-columns: 1.4fr 1fr;
}

.two-col {
  grid-template-columns: 1fr 1fr;
}

@media (max-width: 1100px) {
  .stat-top, .two-col { grid-template-columns: 1fr; }
}

.desc-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 8px 18px;
}

.desc-item {
  display: flex;
  flex-direction: column;
  gap: 1px;
  font-size: 11.5px;
}

.desc-val {
  font-size: 16px;
  font-weight: 650;
  color: var(--text-1);
}

.trend-body {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.trend-dir {
  font-size: 26px;
  font-weight: 750;
}

.trend-dir.up { color: var(--up); }
.trend-dir.down { color: var(--down); }
.trend-dir.flat { color: var(--text-2); }

.trend-note {
  font-size: 11.5px;
  line-height: 1.5;
}

.dd-badge {
  margin-left: 10px;
  font-size: 11px;
  font-weight: 600;
  color: var(--critical);
  background: rgba(208, 59, 59, 0.1);
  border: 1px solid rgba(208, 59, 59, 0.3);
  padding: 1px 9px;
  border-radius: 999px;
}
</style>
