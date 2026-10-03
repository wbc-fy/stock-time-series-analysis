<template>
  <div>
    <div class="card section">
      <div class="card-title">最新估值快照（stock_daily_basic）</div>
      <div class="card-sub">{{ tsCode }} · {{ basic.trade_date }}</div>
      <div v-if="basic.ts_code" class="grid basic-grid">
        <StatCard label="收盘价" :value="String(basic.close ?? '—')" raw unit="元" />
        <StatCard label="换手率" :value="String(basic.turnover_rate ?? '—')" raw unit="%" />
        <StatCard label="PE" :value="String(basic.pe ?? '—')" raw />
        <StatCard label="PE(TTM)" :value="String(basic.pe_ttm ?? '—')" raw />
        <StatCard label="PB" :value="String(basic.pb ?? '—')" raw />
        <StatCard label="PS" :value="String(basic.ps ?? '—')" raw />
        <StatCard label="总市值" :value="fmtWanToYi(basic.total_mv)" raw unit="亿元" />
      </div>
    </div>

    <div class="card section">
      <div class="card-title">日线行情（stock_daily，最近 30 个交易日）</div>
      <DataTable :columns="dailyColumns" :rows="rows" :page-size="12" />
    </div>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue'
import StatCard from '../../components/StatCard.vue'
import DataTable from '../../components/DataTable.vue'
import { getKline, getDailyBasicRow } from '../../api/analysis.js'
import { fmtCompact, fmtWanToYi } from '../../utils/format.js'

const props = defineProps({ tsCode: { type: String, required: true } })

const basic = ref({})
const rows = ref([])

const dailyColumns = [
  { key: 'trade_date', label: 'trade_date', width: '100px' },
  { key: 'open', label: 'open', align: 'right', type: 'number' },
  { key: 'high', label: 'high', align: 'right', type: 'number' },
  { key: 'low', label: 'low', align: 'right', type: 'number' },
  { key: 'close', label: 'close', align: 'right', type: 'number' },
  { key: 'pre_close', label: 'pre_close', align: 'right', type: 'number' },
  { key: 'change', label: 'change', align: 'right', type: 'updown' },
  { key: 'pct_chg', label: 'pct_chg(%)', align: 'right', type: 'updown' },
  { key: 'vol', label: 'vol(手)', align: 'right', format: (v) => fmtCompact(v) },
  { key: 'amount', label: 'amount(千元)', align: 'right', format: (v) => fmtCompact(v) },
]

watch(() => props.tsCode, async (code) => {
  const [kline, b] = await Promise.all([getKline(code), getDailyBasicRow(code)])
  rows.value = [...kline.bars].reverse().slice(0, 30)
  basic.value = b
}, { immediate: true })
</script>

<style scoped>
.basic-grid {
  grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
}
</style>
