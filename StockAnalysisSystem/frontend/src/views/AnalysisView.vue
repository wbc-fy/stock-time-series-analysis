<template>
  <div>
    <!-- 股票选择 + Tab -->
    <div class="analysis-head">
      <div class="field stock-select">
        <label for="stock-code">{{ live ? '股票（真实 stock_basic 股票池）' : '股票（mock 股票池 · seeded 随机游走 260 交易日）' }}</label>
        <select id="stock-code" v-model="tsCode" :disabled="loading || !pool.length">
          <option v-for="s in pool" :key="s.ts_code" :value="s.ts_code">
            {{ s.ts_code }} · {{ s.name }} · {{ s.industry }}
          </option>
        </select>
      </div>
      <div v-if="current" class="stock-meta muted">
        {{ current.area }} · 上市 {{ current.list_date }}
      </div>
    </div>
    <p v-if="loading" role="status">股票池加载中…</p>
    <p v-else-if="error" role="alert">{{ error }} <button @click="loadPool">重试股票池</button></p>
    <p v-else-if="!pool.length" role="status">股票池为空，暂无可查询股票。</p>
    <p v-if="live" class="muted">综合概览与模型预测已接入真实数据；其他分析尚未接入，切换演示模式查看。</p>

    <div class="tabs">
      <button
        v-for="t in TABS" :key="t.key"
        class="tab-btn" :class="{ active: activeTab === t.key }"
        :disabled="live && !['overview', 'prediction'].includes(t.key)" :title="live && !['overview', 'prediction'].includes(t.key) ? '尚未接入，切换演示模式查看' : ''"
        @click="activeTab = t.key"
      >{{ t.label }}</button>
    </div>

    <component v-if="tsCode && !loading && !error" :is="activeComponent" :ts-code="tsCode" />
  </div>
</template>

<script setup>
import { computed, onMounted, onBeforeUnmount, ref, shallowRef, watch } from 'vue'
import { dataSource } from '../api/client.js'
import { getStockPool } from '../api/analysis.js'
import AnalysisOverview from './analysis/AnalysisOverview.vue'
import AnalysisBasic from './analysis/AnalysisBasic.vue'
import AnalysisTechnical from './analysis/AnalysisTechnical.vue'
import AnalysisStats from './analysis/AnalysisStats.vue'
import AnalysisPrediction from './analysis/AnalysisPrediction.vue'
import AnalysisBacktest from './analysis/AnalysisBacktest.vue'
import AnalysisRanking from './analysis/AnalysisRanking.vue'
import AnalysisTransformer from './analysis/AnalysisTransformer.vue'
import AnalysisRisk from './analysis/AnalysisRisk.vue'

const TABS = [
  { key: 'overview', label: '综合概览', comp: AnalysisOverview },
  { key: 'basic', label: '基本行情', comp: AnalysisBasic },
  { key: 'technical', label: '技术指标', comp: AnalysisTechnical },
  { key: 'stats', label: '统计分析', comp: AnalysisStats },
  { key: 'prediction', label: '模型预测', comp: AnalysisPrediction },
  { key: 'backtest', label: '策略回测', comp: AnalysisBacktest },
  { key: 'ranking', label: '选股排名', comp: AnalysisRanking },
  { key: 'transformer', label: 'Transformer', comp: AnalysisTransformer },
  { key: 'risk', label: '风险提示', comp: AnalysisRisk },
]

const pool = ref([])
const tsCode = ref('')
const live = computed(() => dataSource.value === 'hybrid')
const loading = ref(false)
const error = ref('')
let controller
let generation = 0
const activeTab = ref('overview')
const activeComponent = shallowRef(AnalysisOverview)

const current = computed(() => pool.value.find((s) => s.ts_code === tsCode.value))

function syncComponent() {
  activeComponent.value = TABS.find((t) => t.key === activeTab.value)?.comp || AnalysisOverview
}

watch(activeTab, syncComponent, { immediate: true })

async function loadPool() {
  controller?.abort()
  controller = new AbortController()
  const run = ++generation
  loading.value = true; error.value = ''; pool.value = []; tsCode.value = ''
  try {
    const result = await getStockPool({ signal: controller.signal })
    if (run !== generation) return
    pool.value = result
    tsCode.value = result.find(s => s.ts_code === '000001.SZ')?.ts_code || result[0]?.ts_code || ''
  } catch (e) { if (run === generation && e.name !== 'AbortError') error.value = e.message }
  finally { if (run === generation) loading.value = false }
}
onMounted(loadPool)
onBeforeUnmount(() => { generation++; controller?.abort() })
</script>

<style scoped>
.analysis-head {
  display: flex;
  align-items: flex-end;
  gap: 16px;
  margin-bottom: 14px;
  flex-wrap: wrap;
}

.stock-select select {
  min-width: 280px;
}

.stock-meta {
  font-size: 12.5px;
  padding-bottom: 8px;
}
</style>
