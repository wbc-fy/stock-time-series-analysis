<template>
  <template v-if="dataSource === 'hybrid'">
    <div class="prediction-subtabs" aria-label="预测与评估子面板">
      <button class="btn" @click="panel = 'prediction'" :aria-pressed="panel === 'prediction'">单模型预测</button>
      <button class="btn" @click="panel = 'evaluation'" :aria-pressed="panel === 'evaluation'">滚动评估</button>
    </div>
    <AnalysisPredictionLive v-if="panel === 'prediction'" :ts-code="tsCode" />
    <AnalysisEvaluationLive v-else :ts-code="tsCode" />
  </template>
  <AnalysisPredictionDemo v-else :ts-code="tsCode" />
</template>
<script setup>
import { ref } from 'vue'
import { dataSource } from '../../api/client.js'
import AnalysisPredictionDemo from './AnalysisPredictionDemo.vue'
import AnalysisPredictionLive from './AnalysisPredictionLive.vue'
import AnalysisEvaluationLive from './AnalysisEvaluationLive.vue'
const panel = ref('prediction')
defineProps({ tsCode: { type: String, required: true } })
</script>
<style scoped>
.prediction-subtabs { display:flex; flex-wrap:wrap; gap:8px; margin-bottom:16px; }
.prediction-subtabs button[aria-pressed="true"] { border-color:var(--accent); color:var(--accent); background:var(--accent-soft); }
.prediction-subtabs button:focus-visible { outline:2px solid var(--accent); outline-offset:3px; }
</style>
