import { defineConfig, loadEnv } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const target = env.CONSUMER_API_TARGET || 'http://127.0.0.1:8080'
  const marketTarget = env.MARKET_API_TARGET || 'http://127.0.0.1:8083'
  const predictionTarget = env.PREDICTION_API_TARGET || 'http://127.0.0.1:8084'
  return {
  plugins: [vue()],
  server: {
    port: 5173,
    open: false,
    proxy: {
      '^/api/analysis/evaluations(?:\\?|$)': { target: predictionTarget, changeOrigin: true },
      '^/api/analysis/evaluations/[0-9]{6}\\.(?:SZ|SH|BJ)/eval_[0-9a-f]{32}(?:\\?|$)': { target: predictionTarget, changeOrigin: true },
      '^/api/analysis/models(?:\\?|$)': { target: predictionTarget, changeOrigin: true },
      '^/api/analysis/models/[A-Za-z0-9_-]+/importance(?:\\?|$)': { target: predictionTarget, changeOrigin: true },
      '^/api/analysis/(?:prediction|results)/[0-9]{6}\\.(?:SZ|SH|BJ)(?:\\?|$)': { target: predictionTarget, changeOrigin: true },
      '/api/consumer': { target, changeOrigin: true },
      '/actuator/health': { target, changeOrigin: true },
      '^/api/analysis/stocks(?:\\?|$)': { target: marketTarget, changeOrigin: true },
      '^/api/analysis/(?:kline|indicators)/[0-9]{6}\\.(?:SZ|SH|BJ)(?:\\?|$)': { target: marketTarget, changeOrigin: true },
    },
  },
  build: {
    chunkSizeWarningLimit: 1200,
  },
  }
})
