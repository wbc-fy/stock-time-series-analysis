import { createRouter, createWebHashHistory } from 'vue-router'

const routes = [
  { path: '/', redirect: '/overview' },
  {
    path: '/overview',
    name: 'overview',
    component: () => import('../views/OverviewView.vue'),
    meta: { title: '系统总览', icon: 'grid' },
  },
  {
    path: '/collector',
    name: 'collector',
    component: () => import('../views/CollectorView.vue'),
    meta: { title: '数据采集', icon: 'database' },
  },
  {
    path: '/monitor',
    name: 'monitor',
    component: () => import('../views/MonitorView.vue'),
    meta: { title: 'Kafka 消费监控', icon: 'activity' },
  },
  {
    path: '/analysis',
    name: 'analysis',
    component: () => import('../views/AnalysisView.vue'),
    meta: { title: '分析建模', icon: 'trend' },
  },
]

export default createRouter({
  history: createWebHashHistory(),
  routes,
})
