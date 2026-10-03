import { defineStore } from 'pinia'
import { computed, ref } from 'vue'

const STORAGE_KEY = 'sas-theme'

export const useThemeStore = defineStore('theme', () => {
  const theme = ref(
    (typeof localStorage !== 'undefined' && localStorage.getItem(STORAGE_KEY)) || 'dark',
  )
  const isDark = computed(() => theme.value === 'dark')

  function apply() {
    document.documentElement.setAttribute('data-theme', theme.value)
    try {
      localStorage.setItem(STORAGE_KEY, theme.value)
    } catch (e) {
      /* 忽略隐私模式下的存储失败 */
    }
  }

  function init() {
    apply()
  }

  function toggle() {
    theme.value = isDark.value ? 'light' : 'dark'
    apply()
  }

  return { theme, isDark, init, toggle }
})
