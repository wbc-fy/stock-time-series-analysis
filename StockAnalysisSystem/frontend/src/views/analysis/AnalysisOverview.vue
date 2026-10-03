<template>
  <div>
    <div class="card section">
      <div class="card-title">日 K 线 · {{ live ? 'Flink MA5/10/20' : 'MA5/10/20/60' }} · 成交量 <button @click="load" aria-label="刷新行情">刷新行情</button></div>
      <div class="card-sub">{{ live ? '真实 MySQL stock_daily' : '演示 stock_daily' }} · {{ tsCode }} · 有界返回 {{ bars.length }} 个交易日（红涨绿跌）</div>
      <p v-if="loading" role="status">K 线加载中…</p>
      <p v-if="barError" role="alert">K 线不可用：{{ barError }} <button @click="load">重试</button></p>
      <template v-if="live">
        <p v-if="indicatorLoading" role="status">Flink 指标加载中…</p>
        <p v-else-if="indicatorError" role="alert">MA 不可用：{{ indicatorError }}（不使用演示指标）</p>
        <p v-else class="card-sub">指标覆盖 {{ covered }} / {{ bars.length }} 个 K 线日期 · 最新指标 {{ latestIndicator?.trade_date || '无' }} · 最新 K 线 {{ lastBar?.trade_date || '无' }}</p>
        <p v-if="latestIndicator && lastBar && latestIndicator.trade_date < lastBar.trade_date" role="status">指标尚未覆盖最新 K 线，以下为历史部分覆盖，不代表每日更新。</p>
        <p v-if="latestIndicator" class="num">最新指标 MA5 {{ latestIndicator.ma5 ?? '—' }} · MA10 {{ latestIndicator.ma10 ?? '—' }} · MA20 {{ latestIndicator.ma20 ?? '—' }} · 窗口 {{ latestIndicator.window_size ?? '—' }} · {{ latestIndicator.is_warmup ? '预热中' : '预热完成' }}</p>
        <p v-else-if="!indicatorLoading && !indicatorError">暂无真实指标，MA 保持空值。</p>
      </template>
      <ChartBox v-if="bars.length" :option="option" :height="520" />
      <div v-else-if="!loading && !barError" class="muted loading">暂无 K 线数据</div>
    </div>

    <div class="grid quote-grid section">
      <StatCard label="最新收盘" :value="lastBar ? lastBar.close.toFixed(2) : '—'" raw unit="元" />
      <StatCard
        label="当日涨跌" raw
        :value="lastBar?.pct_chg != null ? (lastBar.pct_chg > 0 ? '+' : '') + lastBar.pct_chg.toFixed(2) + '%' : '—'"
        :tone="lastBar ? (lastBar.pct_chg > 0 ? 'up' : lastBar.pct_chg < 0 ? 'down' : '') : ''"
      />
      <StatCard label="当日成交量" :value="lastBar ? fmtCompact(lastBar.vol) : '—'" raw unit="手" />
      <StatCard label="当日成交额" :value="lastBar ? fmtCompact(lastBar.amount) : '—'" raw unit="千元" />
      <StatCard label="区间最高" :value="bars.length ? hi.toFixed(2) : '—'" raw unit="元" />
      <StatCard label="区间最低" :value="bars.length ? lo.toFixed(2) : '—'" raw unit="元" />
    </div>
  </div>
</template>

<script setup>
import { computed, ref, watch, onBeforeUnmount } from 'vue'
import ChartBox from '../../components/ChartBox.vue'
import StatCard from '../../components/StatCard.vue'
import { getKline, getIndicatorSeries } from '../../api/analysis.js'
import { dataSource } from '../../api/client.js'
import { alignIndicators } from '../../utils/market.js'
import { useThemeStore } from '../../stores/theme.js'
import { fmtCompact } from '../../utils/format.js'
import { baseOption, xAxis, yAxis, lineSeries, updown, chrome, seriesColors } from '../../utils/chart.js'

const props = defineProps({ tsCode: { type: String, required: true } })
const theme = useThemeStore()

const bars = ref([])
const indicators = ref([])
const live = computed(() => dataSource.value === 'hybrid')
const loading = ref(false), indicatorLoading = ref(false), barError = ref(''), indicatorError = ref('')
let generation = 0, controller
async function load() {
  controller?.abort(); controller = new AbortController()
  const run = ++generation, code = props.tsCode
  bars.value = []; indicators.value = []; barError.value = ''; indicatorError.value = ''
  loading.value = true; indicatorLoading.value = live.value
  const options = { signal: controller.signal }
  const tasks = [getKline(code, options).then(data => { if (run === generation) bars.value = data.bars })
    .catch(e => { if (run === generation && e.name !== 'AbortError') barError.value = e.message })
    .finally(() => { if (run === generation) loading.value = false })]
  if (live.value) tasks.push(getIndicatorSeries(code, options).then(data => { if (run === generation) indicators.value = data.indicators })
    .catch(e => { if (run === generation && e.name !== 'AbortError') indicatorError.value = e.message })
    .finally(() => { if (run === generation) indicatorLoading.value = false }))
  await Promise.all(tasks)
}
const latestIndicator = computed(() => indicators.value.at(-1))
const covered = computed(() => bars.value.filter(bar => indicators.value.some(row => row.trade_date === bar.trade_date)).length)
const aligned = computed(() => alignIndicators(bars.value, indicators.value))

watch(() => props.tsCode, load, { immediate: true })
onBeforeUnmount(() => { generation++; controller?.abort() })

const lastBar = computed(() => bars.value[bars.value.length - 1] || null)
const hi = computed(() => (bars.value.length ? Math.max(...bars.value.map((b) => b.high)) : 0))
const lo = computed(() => (bars.value.length ? Math.min(...bars.value.map((b) => b.low)) : 0))

const option = computed(() => {
  const c = chrome(theme.isDark)
  const colors = seriesColors(theme.isDark)
  const dates = bars.value.map((b) => b.trade_date)
  const ohlc = bars.value.map((b) => [b.open, b.close, b.low, b.high])

  // 真实模式使用按交易日对齐的服务端 MA5/10/20；仅演示模式在前端计算均线。
  const ma = (n) => live.value ? aligned.value[`ma${n}`] : bars.value.map((_, i) => {
    if (i < n - 1) return null
    let s = 0
    for (let j = i - n + 1; j <= i; j++) s += bars.value[j].close
    return +(s / n).toFixed(2)
  })

  const vols = bars.value.map((b, i) => ({
    value: b.vol,
    itemStyle: { color: b.close >= b.open ? c.up : c.down, opacity: 0.75 },
  }))

  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis', axisPointer: { type: 'cross' } },
    axisPointer: { link: [{ xAxisIndex: 'all' }] },
    legend: { ...baseOption(theme.isDark).legend, data: live.value ? ['K线', 'MA5', 'MA10', 'MA20'] : ['K线', 'MA5', 'MA10', 'MA20', 'MA60'] },
    grid: [
      { left: 8, right: 18, top: 34, height: '56%', containLabel: true },
      { left: 8, right: 18, top: '74%', height: '15%', containLabel: true },
    ],
    xAxis: [
      xAxis(theme.isDark, { data: dates, boundaryGap: true, axisPointer: { label: { show: false } } }),
      xAxis(theme.isDark, { data: dates, gridIndex: 1, axisLabel: { show: false }, boundaryGap: true }),
    ],
    yAxis: [
      yAxis(theme.isDark, { scale: true, splitNumber: 4 }),
      yAxis(theme.isDark, { gridIndex: 1, splitNumber: 2, axisLabel: { show: false }, splitLine: { show: false } }),
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 55, end: 100 },
      {
        type: 'slider', xAxisIndex: [0, 1], start: 55, end: 100,
        bottom: 2, height: 18,
        borderColor: c.grid, backgroundColor: 'transparent',
        fillerColor: theme.isDark ? 'rgba(57,135,229,0.14)' : 'rgba(42,120,214,0.10)',
        handleStyle: { color: colors[0] },
        textStyle: { color: c.muted, fontSize: 10 },
      },
    ],
    series: [
      {
        name: 'K线', type: 'candlestick', data: ohlc,
        itemStyle: { color: c.up, color0: c.down, borderColor: c.up, borderColor0: c.down },
      },
      lineSeries('MA5', ma(5), { color: colors[0], lineStyle: { width: 1.4 } }),
      lineSeries('MA10', ma(10), { color: colors[1], lineStyle: { width: 1.4 } }),
      lineSeries('MA20', ma(20), { color: colors[2], lineStyle: { width: 1.4 } }),
      ...(!live.value ? [lineSeries('MA60', ma(60), { color: colors[3], lineStyle: { width: 1.4 } })] : []),
      {
        name: '成交量', type: 'bar', xAxisIndex: 1, yAxisIndex: 1,
        data: vols, barMaxWidth: 8,
      },
    ],
  }
})
</script>

<style scoped>
.loading {
  padding: 40px;
  text-align: center;
}

.quote-grid {
  grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
}
</style>
