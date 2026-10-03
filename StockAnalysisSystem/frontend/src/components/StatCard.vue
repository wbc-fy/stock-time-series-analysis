<template>
  <div class="card stat-card">
    <div class="stat-label">{{ label }}</div>
    <div class="stat-value" :class="toneClass">
      {{ displayValue }}<span v-if="unit && displayValue !== '—'" class="stat-unit">{{ unit }}</span>
    </div>
    <div v-if="sub" class="stat-sub">{{ sub }}</div>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { fmtInt } from '../utils/format.js'

const props = defineProps({
  label: String,
  value: [Number, String],
  unit: String,
  sub: String,
  tone: { type: String, default: '' }, // good | warning | critical | accent | up | down
  raw: { type: Boolean, default: false }, // true: value 已是格式化字符串
})

const displayValue = computed(() =>
  props.raw ? props.value : typeof props.value === 'number' ? fmtInt(props.value) : props.value ?? '—',
)
const toneClass = computed(() => (props.tone ? `tone-${props.tone}` : ''))
</script>

<style scoped>
.stat-card {
  padding: 14px 16px;
}

.stat-label {
  font-size: 12px;
  color: var(--text-3);
  margin-bottom: 6px;
}

.stat-value {
  font-size: 24px;
  font-weight: 700;
  line-height: 1.15;
  font-variant-numeric: tabular-nums;
}

.stat-unit {
  font-size: 12px;
  font-weight: 400;
  color: var(--text-3);
  margin-left: 4px;
}

.stat-sub {
  font-size: 11.5px;
  color: var(--text-3);
  margin-top: 4px;
}

.tone-good { color: var(--good); }
.tone-warning { color: var(--warning); }
.tone-critical { color: var(--critical); }
.tone-accent { color: var(--accent); }
.tone-up { color: var(--up); }
.tone-down { color: var(--down); }
</style>
