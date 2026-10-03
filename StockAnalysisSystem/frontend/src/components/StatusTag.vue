<template>
  <span class="status-tag" :class="`tone-${tone}`">
    <span class="dot" :class="{ pulse: status === 'RUNNING' }"></span>
    {{ text }}
  </span>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  status: { type: String, required: true },
})

// 状态 → 色调 + 显示文本（状态色永远伴随图标+文字，不靠颜色单独表意）
const MAP = {
  SUCCESS: ['good', 'SUCCESS'],
  UP: ['good', 'UP'],
  STABLE: ['good', 'STABLE'],
  ACTIVE: ['good', 'ACTIVE'],
  RUNNING: ['accent', 'RUNNING'],
  PENDING: ['muted', 'PENDING'],
  PARTIAL_SUCCESS: ['warning', 'PARTIAL'],
  DEGRADED: ['warning', 'DEGRADED'],
  WARNING: ['warning', 'WARNING'],
  FAILED: ['critical', 'FAILED'],
  DOWN: ['critical', 'DOWN'],
  CRITICAL: ['critical', 'CRITICAL'],
  VALIDATION_FAILURE: ['warning', 'VALIDATION'],
  JSON_PARSE_FAILURE: ['serious', 'JSON_PARSE'],
  UNSUPPORTED_SCHEMA_VERSION: ['serious', 'SCHEMA'],
  DEAD_LETTER_PUBLISH_FAILURE: ['critical', 'DLT_FAIL'],
  BUY: ['up', 'BUY'],
  SELL: ['down', 'SELL'],
}

const tone = computed(() => (MAP[props.status] || ['muted', props.status])[0])
const text = computed(() => (MAP[props.status] || ['muted', props.status])[1])
</script>

<style scoped>
.status-tag {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 11.5px;
  font-weight: 600;
  letter-spacing: 0.3px;
  padding: 2px 9px;
  border-radius: 999px;
  border: 1px solid transparent;
  white-space: nowrap;
}

.dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: currentColor;
  flex-shrink: 0;
}

.dot.pulse {
  animation: pulse 1.2s ease-in-out infinite;
}

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.3; }
}

.tone-good { color: var(--good); background: rgba(12, 163, 12, 0.1); border-color: rgba(12, 163, 12, 0.3); }
.tone-accent { color: var(--accent); background: var(--accent-soft); border-color: rgba(57, 135, 229, 0.35); }
.tone-muted { color: var(--text-3); background: var(--surface-2); border-color: var(--border); }
.tone-warning { color: var(--warning); background: rgba(250, 178, 25, 0.1); border-color: rgba(250, 178, 25, 0.32); }
.tone-serious { color: var(--serious); background: rgba(236, 131, 90, 0.1); border-color: rgba(236, 131, 90, 0.35); }
.tone-critical { color: var(--critical); background: rgba(208, 59, 59, 0.1); border-color: rgba(208, 59, 59, 0.35); }
.tone-up { color: var(--up); background: rgba(231, 76, 60, 0.1); border-color: rgba(231, 76, 60, 0.32); }
.tone-down { color: var(--down); background: rgba(46, 204, 113, 0.1); border-color: rgba(46, 204, 113, 0.32); }
</style>
