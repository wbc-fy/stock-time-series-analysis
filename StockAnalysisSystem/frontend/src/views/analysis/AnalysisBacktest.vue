<template>
  <div>
    <!-- 参数 -->
    <div class="card section">
      <div class="card-title">回测参数</div>
      <div class="card-sub">信号驱动全仓轮动：技术指标信号 = MA5/MA10 金叉买入、死叉卖出；模型预测信号 = 概率 &gt;0.55 买入、&lt;0.45 卖出</div>
      <div class="param-row">
        <div class="field">
          <label>回测类型</label>
          <select v-model="type">
            <option>技术指标信号</option>
            <option>模型预测信号</option>
          </select>
        </div>
        <div class="field">
          <label>初始资金（元）</label>
          <input v-model.number="initialCapital" type="number" step="100000" min="10000" />
        </div>
        <div class="field">
          <label>佣金率</label>
          <input v-model.number="commission" type="number" step="0.0001" min="0" max="0.01" />
        </div>
        <div class="field submit-field">
          <label>&nbsp;</label>
          <button class="btn btn-primary" :disabled="running" @click="run">
            {{ running ? '回测中…' : '运行回测' }}
          </button>
        </div>
      </div>
      <div v-if="bt.params" class="muted window-note">回测窗口：{{ bt.params.window }}</div>
    </div>

    <!-- 指标卡 -->
    <div v-if="bt.metrics" class="grid metric-grid section">
      <StatCard label="总收益率" :value="fmtPct(bt.metrics.total_return)" raw :tone="signClass(bt.metrics.total_return)" />
      <StatCard label="年化收益" :value="fmtPct(bt.metrics.annual_return)" raw :tone="signClass(bt.metrics.annual_return)" />
      <StatCard label="年化波动" :value="fmtPct(bt.metrics.annual_volatility)" raw />
      <StatCard label="Sharpe" :value="fmtNum(bt.metrics.sharpe_ratio, 3)" raw :tone="bt.metrics.sharpe_ratio > 1 ? 'good' : ''" />
      <StatCard label="最大回撤" :value="fmtPct(bt.metrics.max_drawdown)" raw tone="critical" />
      <StatCard label="胜率" :value="fmtPct(bt.metrics.win_rate)" raw />
      <StatCard label="盈亏比" :value="bt.metrics.profit_loss_ratio == null ? '—' : fmtNum(bt.metrics.profit_loss_ratio, 3)" raw />
      <StatCard label="交易次数" :value="String(bt.metrics.trade_count)" raw unit="对" />
    </div>

    <!-- 资金曲线 + 回撤 -->
    <div class="card section">
      <div class="card-title">策略 vs 买入持有 · 买卖点标记</div>
      <ChartBox :option="equityOption" :height="320" />
    </div>

    <div class="card section">
      <div class="card-title">策略回撤（%）</div>
      <ChartBox :option="drawdownOption" :height="200" />
    </div>

    <!-- 交易记录 -->
    <div class="card section">
      <div class="card-title">交易记录</div>
      <DataTable :columns="tradeColumns" :rows="bt.trades || []" :page-size="10">
        <template #cell-action="{ row }">
          <StatusTag :status="row.action" />
        </template>
      </DataTable>
    </div>
  </div>
</template>

<script setup>
import { computed, ref, watch } from 'vue'
import ChartBox from '../../components/ChartBox.vue'
import DataTable from '../../components/DataTable.vue'
import StatCard from '../../components/StatCard.vue'
import StatusTag from '../../components/StatusTag.vue'
import { getBacktest } from '../../api/analysis.js'
import { useThemeStore } from '../../stores/theme.js'
import { fmtNum, fmtInt, fmtPct, signClass } from '../../utils/format.js'
import { baseOption, xAxis, yAxis, lineSeries, chrome, seriesColors } from '../../utils/chart.js'

const props = defineProps({ tsCode: { type: String, required: true } })
const theme = useThemeStore()

const type = ref('技术指标信号')
const initialCapital = ref(1000000)
const commission = ref(0.0003)
const running = ref(false)
const bt = ref({})

async function run() {
  running.value = true
  bt.value = await getBacktest(props.tsCode, type.value, {
    initial_capital: initialCapital.value,
    commission: commission.value,
  })
  running.value = false
}

watch(() => props.tsCode, run, { immediate: true })

/* ── 资金曲线（含买卖点） ── */
const equityOption = computed(() => {
  const eq = bt.value.equity
  if (!eq?.length) return baseOption(theme.isDark)
  const c = chrome(theme.isDark)
  const colors = seriesColors(theme.isDark)
  const eqMap = Object.fromEntries(eq.map((e) => [e.date, e.strategy]))
  const marks = (bt.value.trades || []).map((t) => ({
    coord: [t.date.slice(5), eqMap[t.date]],
    value: t.action === 'BUY' ? 'B' : 'S',
    itemStyle: { color: t.action === 'BUY' ? c.up : c.down },
    symbol: t.action === 'BUY' ? 'triangle' : 'pin',
    symbolSize: 13,
    symbolRotate: t.action === 'BUY' ? 0 : 0,
    label: { show: false },
  }))
  return {
    ...baseOption(theme.isDark),
    tooltip: {
      ...baseOption(theme.isDark).tooltip, trigger: 'axis',
      valueFormatter: (v) => (v == null ? '—' : fmtInt(Math.round(v)) + ' 元'),
    },
    grid: { left: 8, right: 16, top: 30, bottom: 2, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: eq.map((e) => e.date.slice(5)), axisLabel: { color: c.muted, fontSize: 10, interval: 25 } }),
    yAxis: yAxis(theme.isDark, { scale: true, axisLabel: { fontSize: 10, formatter: (v) => (v / 10000).toFixed(0) + '万' } }),
    series: [
      lineSeries('strategy', eq.map((e) => e.strategy), { color: colors[0], markPoint: { data: marks, animation: false } }),
      lineSeries('buyhold', eq.map((e) => e.buyhold), { color: colors[3], lineStyle: { width: 1.6, type: 'dashed' } }),
    ],
  }
})

const drawdownOption = computed(() => {
  const dd = bt.value.drawdown
  if (!dd?.length) return baseOption(theme.isDark)
  const c = chrome(theme.isDark)
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis', valueFormatter: (v) => v + '%' },
    grid: { left: 8, right: 16, top: 18, bottom: 2, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: dd.map((e) => e.date.slice(5)), axisLabel: { color: c.muted, fontSize: 10, interval: 25 } }),
    yAxis: yAxis(theme.isDark, { max: 0, axisLabel: { fontSize: 10, formatter: '{value}%' } }),
    series: [{
      ...lineSeries('drawdown', dd.map((e) => e.value)),
      color: c.critical,
      areaStyle: { color: c.critical, opacity: 0.14 },
    }],
  }
})

const tradeColumns = [
  { key: 'date', label: 'date', width: '104px' },
  { key: 'action', label: 'action', width: '90px' },
  { key: 'price', label: 'price', align: 'right', type: 'number' },
  { key: 'shares', label: 'shares', align: 'right', type: 'number', format: (v) => fmtInt(v) },
  { key: 'cost', label: 'cost', align: 'right', type: 'number', format: (v) => (v == null ? '—' : fmtInt(Math.round(v))) },
  { key: 'revenue', label: 'revenue', align: 'right', type: 'number', format: (v) => (v == null ? '—' : fmtInt(Math.round(v))) },
]
</script>

<style scoped>
.param-row {
  display: flex;
  flex-wrap: wrap;
  gap: 14px;
  align-items: flex-end;
}

.param-row .field {
  min-width: 150px;
}

.submit-field .btn {
  height: 34px;
}

.window-note {
  margin-top: 10px;
  font-size: 12px;
}

.metric-grid {
  grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
}
</style>
