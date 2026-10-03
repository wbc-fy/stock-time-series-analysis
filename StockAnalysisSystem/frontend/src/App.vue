<template>
  <div class="layout">
    <aside class="sidebar">
      <div class="logo">
        <div class="logo-mark">S</div>
        <div class="logo-text">
          StockAnalysisSystem
          <small>股票时序分析系统</small>
        </div>
      </div>

      <nav>
        <RouterLink
          v-for="r in navRoutes" :key="r.path"
          class="nav-item" :to="r.path"
        >
          <Icon :name="r.meta.icon" />
          {{ r.meta.title }}
        </RouterLink>
      </nav>

      <div class="sidebar-footer">
        python-collector · java kafka-consumer<br />
        stock-analysis-app · Flink · v0.6
      </div>
    </aside>

    <div class="main">
      <header class="topbar">
        <div class="topbar-title">{{ route.meta.title }}</div>
        <div class="topbar-right">
          <span class="mock-badge">{{ sourceLabel }}</span>
          <label class="source-select">
            <span class="sr-only">数据模式</span>
            <select :value="dataSource" @change="setDataSource($event.target.value)" aria-label="数据模式">
              <option value="mock">演示模式</option>
              <option value="hybrid">接入现有 API</option>
            </select>
          </label>
          <span class="clock num">{{ clock }}</span>
          <button
            class="theme-toggle"
            :title="theme.isDark ? '切换到浅色主题' : '切换到深色主题'"
            @click="theme.toggle()"
          >
            <Icon :name="theme.isDark ? 'sun' : 'moon'" />
          </button>
        </div>
      </header>

      <main class="content">
        <RouterView :key="dataSource + route.path" />
      </main>
    </div>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { RouterLink, RouterView, useRoute, useRouter } from 'vue-router'
import Icon from './components/Icon.vue'
import { dataSource, setDataSource } from './api/client.js'
import { useThemeStore } from './stores/theme.js'
import { fmtTime } from './utils/format.js'

const route = useRoute()
const router = useRouter()
const theme = useThemeStore()

const navRoutes = computed(() => router.getRoutes().filter((r) => r.meta?.title))
const sourceLabel = computed(() => dataSource.value === 'hybrid' && route.name === 'monitor'
  ? '监控接口数据' : dataSource.value === 'hybrid' && route.name === 'analysis' ? '真实行情 · 概览与预测已接入' : '演示数据 · 非真实行情')

// 顶栏时钟
const clock = ref('')
let timer = null
function tick() {
  clock.value = fmtTime(new Date().toISOString())
}
onMounted(() => { tick(); timer = setInterval(tick, 1000) })
onBeforeUnmount(() => clearInterval(timer))
</script>

<style scoped>
nav {
  display: flex;
  flex-direction: column;
  gap: 3px;
}
.source-select select { font-size: 12px; max-width: 150px; }
.sr-only { position: absolute; width: 1px; height: 1px; padding: 0; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; }
@media (max-width: 800px) {
  .layout { flex-direction: column; height: 100dvh; }
  .sidebar { width: 100%; padding: 8px; }
  .logo, .sidebar-footer { display: none; }
  nav { flex-direction: row; justify-content: space-between; overflow-x: auto; }
  .nav-item { flex-shrink: 0; padding: 8px 6px; font-size: 12px; gap: 4px; }
  .main { min-height: 0; }
  .topbar { height: auto; min-height: 56px; padding: 10px 12px; flex-wrap: wrap; gap: 8px; }
  .topbar-title { font-size: 14px; }
  .topbar-right { flex-wrap: wrap; gap: 8px; }
  .clock { display: none; }
  .content { padding: 12px; }
}
</style>
