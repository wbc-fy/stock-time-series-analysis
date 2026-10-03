<template>
  <div>
    <!-- 架构流程图 -->
    <section class="section">
      <h2>系统架构</h2>
      <div class="card arch-card">
        <div class="arch">
          <div class="arch-col">
            <div class="node source">
              <div class="node-name">Tushare / AkShare</div>
              <div class="node-desc">外部行情数据源</div>
            </div>
          </div>
          <div class="arrow">→</div>
          <div class="arch-col">
            <div class="node collect">
              <div class="node-name">python-collector</div>
              <div class="node-desc">采集 · 校验 · 任务状态机</div>
              <div class="node-tag">支持 mysql / kafka / dual</div>
            </div>
          </div>
          <div class="arrow">→</div>
          <div class="arch-col">
            <div class="node store">
              <div class="node-name">MySQL</div>
              <div class="node-desc">行情 · 基础资料 · 特征 · 分析结果</div>
            </div>
            <div class="node store kafka">
              <div class="node-name">Kafka</div>
              <div class="node-desc">stock.ods.daily.v1 · basic.v1 · dead-letter.v1</div>
            </div>
          </div>
          <div class="arrow">→</div>
          <div class="arch-col">
            <div class="node consume">
              <div class="node-name">kafka-consumer-service</div>
              <div class="node-desc">Java 消费 · 协议校验 · DLT · 监控 API</div>
              <div class="node-tag">原始行情消费者</div>
            </div>
            <div class="node consume">
              <div class="node-name">flink-realtime-job</div>
              <div class="node-desc">20 交易日状态 · 均线 · 量比 · Kafka 指标输出</div>
              <div class="node-tag">V0.5 实时指标链路</div>
            </div>
          </div>
          <div class="arrow">→</div>
          <div class="arch-col">
            <div class="node analysis">
              <div class="node-name">stock-analysis-app</div>
              <div class="node-desc">特征工程 · 模型训练 · 预测 · 回测</div>
              <div class="node-tag">FEATURE_VERSION 2.0.0</div>
            </div>
          </div>
        </div>
        <div class="arch-note muted">
          分析应用从 MySQL 读取数据。Java Consumer 与 Flink 分别消费 Kafka 原始行情；Flink 指标仍输出到 Kafka，尚未接入该页面。下方服务状态和核心指标是演示数据。
        </div>
      </div>
    </section>

    <!-- KPI -->
    <section class="section">
      <h2>核心指标</h2>
      <div v-if="loadingKpi" class="card loading-card muted">加载中…</div>
      <div v-else class="grid kpi-grid">
        <StatCard
          v-for="k in kpis" :key="k.key"
          :label="k.label" :value="k.value" :unit="k.unit" :tone="k.tone || ''"
        />
      </div>
    </section>

    <!-- 服务健康 -->
    <section class="section">
      <h2>服务健康</h2>
      <div v-if="loadingSvc" class="card loading-card muted">加载中…</div>
      <div v-else class="grid svc-grid">
        <div v-for="s in services" :key="s.name" class="card svc-card">
          <div class="svc-head">
            <span class="svc-name">{{ s.name }}</span>
            <StatusTag :status="s.status" />
          </div>
          <div class="svc-desc">{{ s.desc }}</div>
          <div class="svc-detail muted">{{ s.detail }}</div>
        </div>
      </div>
    </section>
  </div>
</template>

<script setup>
import { onMounted, ref } from 'vue'
import StatCard from '../components/StatCard.vue'
import StatusTag from '../components/StatusTag.vue'
import { getServices, getKpis } from '../api/system'

const services = ref([])
const kpis = ref([])
const loadingSvc = ref(true)
const loadingKpi = ref(true)

onMounted(async () => {
  getServices().then((d) => { services.value = d; loadingSvc.value = false })
  getKpis().then((d) => { kpis.value = d; loadingKpi.value = false })
})
</script>

<style scoped>
.arch-card {
  padding: 22px 18px 14px;
  overflow-x: auto;
}

.arch {
  display: flex;
  align-items: center;
  gap: 6px;
  min-width: 1080px;
}

.arch-col {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.arrow {
  color: var(--text-3);
  font-size: 20px;
  flex-shrink: 0;
  padding-bottom: 2px;
}

.node {
  border: 1px solid var(--border);
  border-left: 3px solid var(--series-1);
  border-radius: 10px;
  background: var(--surface-2);
  padding: 10px 12px;
}

.node-name {
  font-weight: 650;
  font-size: 13px;
}

.node-desc {
  font-size: 11.5px;
  color: var(--text-3);
  margin-top: 3px;
  line-height: 1.5;
}

.node-tag {
  display: inline-block;
  margin-top: 7px;
  font-size: 10.5px;
  color: var(--accent);
  background: var(--accent-soft);
  border-radius: 999px;
  padding: 1px 8px;
}

.node.source { border-left-color: var(--series-4); }
.node.collect { border-left-color: var(--series-1); }
.node.store { border-left-color: var(--series-3); }
.node.store.kafka { border-left-color: var(--series-5); }
.node.consume { border-left-color: var(--series-7); }
.node.analysis { border-left-color: var(--series-2); }

.arch-note {
  margin-top: 14px;
  font-size: 11.5px;
  border-top: 1px dashed var(--border);
  padding-top: 10px;
}

.kpi-grid {
  grid-template-columns: repeat(auto-fill, minmax(170px, 1fr));
}

.svc-grid {
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
}

.svc-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.svc-name {
  font-weight: 650;
  font-size: 13.5px;
  font-family: 'Cascadia Code', Consolas, monospace;
}

.svc-desc {
  font-size: 12px;
  color: var(--text-2);
  margin-top: 7px;
}

.svc-detail {
  font-size: 11.5px;
  margin-top: 4px;
}

.loading-card {
  padding: 24px;
  text-align: center;
}
</style>
