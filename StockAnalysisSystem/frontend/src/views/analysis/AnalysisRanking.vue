<template>
  <div>
    <!-- 排名表 + 模型评估 -->
    <div class="grid rank-grid section">
      <div class="card">
        <div class="card-title">全市场选股排名 · {{ ranking.model }}</div>
        <div class="card-sub">
          trade_date {{ ranking.trade_date }} · TopN 展示 {{ ranking.top_n }} ·
          train_window {{ ranking.params?.train_window }}d · rebalance {{ ranking.params?.rebalance_days }}d
        </div>
        <DataTable :columns="rankColumns" :rows="ranking.rows || []" row-key="ts_code" />
      </div>
      <div class="rank-side">
        <StatCard label="模型评估 RMSE" :value="fmtNum(ranking.model_eval?.RMSE, 4)" raw />
        <StatCard label="Direction_Accuracy" :value="fmtPct(ranking.model_eval?.Direction_Accuracy)" raw tone="accent" />
        <div class="card held-card">
          <div class="card-title">当前持仓（Top{{ rankBt.params?.hold_top || 3 }} 等权）</div>
          <div class="held-list">
            <span v-for="code in rankBt.held || []" :key="code" class="held-chip">{{ code }}</span>
          </div>
          <div class="held-metrics">
            <div class="held-metric">
              <span class="muted">策略收益</span>
              <b class="num" :class="signClass(rankBt.metrics?.total_return)">{{ fmtPct(rankBt.metrics?.total_return) }}</b>
            </div>
            <div class="held-metric">
              <span class="muted">基准收益</span>
              <b class="num" :class="signClass(rankBt.metrics?.benchmark_return)">{{ fmtPct(rankBt.metrics?.benchmark_return) }}</b>
            </div>
            <div class="held-metric">
              <span class="muted">超额收益</span>
              <b class="num" :class="signClass(rankBt.metrics?.excess_return)">{{ fmtPct(rankBt.metrics?.excess_return) }}</b>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- 排名回测权益曲线 -->
    <div class="card section">
      <div class="card-title">排名组合回测 · 策略 vs 全池等权基准</div>
      <ChartBox :option="equityOption" :height="280" />
    </div>

    <!-- 探针法特征筛选 -->
    <div class="card section">
      <div class="card-title">探针法特征筛选（probe_selection.py）</div>
      <div class="card-sub">向特征注入噪声训练探针模型，性能不达标者剔除，迭代收紧阈值</div>
      <div class="grid probe-cards">
        <StatCard label="原始特征数" :value="String(probe.original_count ?? '—')" raw />
        <StatCard label="保留特征数" :value="String(probe.final_count ?? '—')" raw tone="good" />
        <StatCard label="剔除率" :value="probe.removal_rate != null ? probe.removal_rate + '%' : '—'" raw tone="warning" />
      </div>

      <div class="grid probe-grid">
        <div>
          <div class="sub-title">筛选迭代过程</div>
          <DataTable :columns="iterColumns" :rows="probe.iterations || []" row-key="iter" />
          <ChartBox :option="iterOption" :height="200" />
        </div>
        <div>
          <div class="sub-title">Top20 特征重要性（Gain）</div>
          <ChartBox :option="gainOption" :height="360" />
        </div>
      </div>

      <div class="sub-title" style="margin-top: 14px">保留特征（{{ (probe.retained_features || []).length }}）</div>
      <div class="retained">
        <code v-for="f in probe.retained_features || []" :key="f" class="retained-chip">{{ f }}</code>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, ref } from 'vue'
import ChartBox from '../../components/ChartBox.vue'
import DataTable from '../../components/DataTable.vue'
import StatCard from '../../components/StatCard.vue'
import { getRanking, getRankingBacktest, getProbeSelection } from '../../api/analysis.js'
import { useThemeStore } from '../../stores/theme.js'
import { fmtNum, fmtPct, signClass } from '../../utils/format.js'
import { baseOption, xAxis, yAxis, lineSeries, barSeries, chrome, seriesColors } from '../../utils/chart.js'

defineProps({ tsCode: { type: String, required: true } })
const theme = useThemeStore()

const ranking = ref({})
const rankBt = ref({})
const probe = ref({})

onMounted(async () => {
  const [r, rb, p] = await Promise.all([getRanking(), getRankingBacktest(), getProbeSelection()])
  ranking.value = r
  rankBt.value = rb
  probe.value = p
})

const rankColumns = [
  { key: 'rank', label: '#', width: '44px', align: 'right', type: 'number', format: (v) => String(v) },
  { key: 'ts_code', label: 'ts_code', width: '96px' },
  { key: 'name', label: '名称', width: '86px' },
  { key: 'industry', label: '行业', width: '80px' },
  { key: 'pred_return', label: '预测收益率(%)', align: 'right', type: 'updown', format: (v) => fmtNum(v, 3) },
  { key: 'score', label: 'score', align: 'right', type: 'number', format: (v) => fmtNum(v, 1) },
  { key: 'trade_date', label: 'trade_date', width: '100px' },
]

const equityOption = computed(() => {
  const eq = rankBt.value.equity
  if (!eq?.length) return baseOption(theme.isDark)
  const c = chrome(theme.isDark)
  const colors = seriesColors(theme.isDark)
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis', valueFormatter: (v) => (v / 10000).toFixed(1) + '万' },
    grid: { left: 8, right: 16, top: 30, bottom: 2, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: eq.map((e) => e.date.slice(5)), axisLabel: { color: c.muted, fontSize: 10, interval: 25 } }),
    yAxis: yAxis(theme.isDark, { scale: true, axisLabel: { fontSize: 10, formatter: (v) => (v / 10000).toFixed(0) + '万' } }),
    series: [
      lineSeries('strategy(Top3)', eq.map((e) => e.strategy), { color: colors[0] }),
      lineSeries('benchmark(全池等权)', eq.map((e) => e.benchmark), { color: colors[3], lineStyle: { width: 1.6, type: 'dashed' } }),
    ],
  }
})

const iterColumns = [
  { key: 'iter', label: 'iter', width: '52px', align: 'right', type: 'number', format: (v) => String(v) },
  { key: 'noise_th_cls', label: 'noise_th_cls', align: 'right', type: 'number' },
  { key: 'noise_th_reg', label: 'noise_th_reg', align: 'right', type: 'number' },
  { key: 'noise_th_dir', label: 'noise_th_dir', align: 'right', type: 'number' },
  { key: 'removed_count', label: 'removed', align: 'right', type: 'number', format: (v) => String(v) },
  { key: 'remaining_count', label: 'remaining', align: 'right', type: 'number', format: (v) => String(v) },
]

const iterOption = computed(() => {
  const its = probe.value.iterations
  if (!its?.length) return baseOption(theme.isDark)
  const c = chrome(theme.isDark)
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis' },
    grid: { left: 8, right: 16, top: 30, bottom: 2, containLabel: true },
    xAxis: xAxis(theme.isDark, { data: its.map((i) => 'iter ' + i.iter), axisLabel: { color: c.muted, fontSize: 10 } }),
    yAxis: [
      yAxis(theme.isDark, { name: '特征数', nameTextStyle: { color: c.muted, fontSize: 10 }, axisLabel: { fontSize: 10 } }),
    ],
    series: [
      barSeries('removed_count', its.map((i) => i.removed_count), { color: chrome(theme.isDark).critical, barMaxWidth: 18 }),
      lineSeries('remaining_count', its.map((i) => i.remaining_count), { color: seriesColors(theme.isDark)[0] }),
    ],
  }
})

const gainOption = computed(() => {
  const rows = [...(probe.value.importance || [])].reverse()
  const c = chrome(theme.isDark)
  return {
    ...baseOption(theme.isDark),
    tooltip: { ...baseOption(theme.isDark).tooltip, trigger: 'axis', axisPointer: { type: 'shadow' } },
    grid: { left: 8, right: 46, top: 8, bottom: 2, containLabel: true },
    xAxis: yAxis(theme.isDark, { axisLabel: { fontSize: 10 } }),
    yAxis: xAxis(theme.isDark, { data: rows.map((r) => r.feature), axisLabel: { color: c.muted, fontSize: 9.5 } }),
    series: [{
      ...barSeries('gain', rows.map((r) => r.gain)),
      barMaxWidth: 11,
      itemStyle: { borderRadius: [0, 3, 3, 0] },
      label: { show: true, position: 'right', fontSize: 9, color: c.muted, formatter: (p) => rows[p.dataIndex].pct + '%' },
    }],
  }
})
</script>

<style scoped>
.rank-grid {
  grid-template-columns: 1.7fr 1fr;
}

@media (max-width: 1100px) {
  .rank-grid { grid-template-columns: 1fr; }
}

.rank-side {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.held-list {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-bottom: 12px;
}

.held-chip {
  font-family: 'Cascadia Code', Consolas, monospace;
  font-size: 11.5px;
  border: 1px solid var(--accent);
  color: var(--accent);
  background: var(--accent-soft);
  border-radius: 999px;
  padding: 2px 10px;
}

.held-metrics {
  display: flex;
  gap: 22px;
  border-top: 1px dashed var(--border);
  padding-top: 10px;
}

.held-metric {
  display: flex;
  flex-direction: column;
  font-size: 11.5px;
  gap: 2px;
}

.held-metric b {
  font-size: 15px;
}

.probe-cards {
  grid-template-columns: repeat(3, minmax(140px, 220px));
  margin-bottom: 16px;
}

.probe-grid {
  grid-template-columns: 1.2fr 1fr;
}

@media (max-width: 1100px) {
  .probe-grid { grid-template-columns: 1fr; }
}

.sub-title {
  font-size: 13px;
  font-weight: 650;
  margin-bottom: 8px;
}

.retained {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
}

.retained-chip {
  font-family: 'Cascadia Code', Consolas, monospace;
  font-size: 10.5px;
  border: 1px solid var(--border);
  background: var(--surface-2);
  color: var(--text-2);
  border-radius: 6px;
  padding: 2px 8px;
}
</style>
