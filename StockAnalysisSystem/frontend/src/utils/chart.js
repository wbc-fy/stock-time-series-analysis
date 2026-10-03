/**
 * ECharts 主题工具：所有图表统一从这里取色，深浅主题各一套（dataviz 已校验色板）。
 * A 股习惯红涨绿跌；序列色固定顺序不循环。
 */

export const SERIES_DARK = ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#008300', '#9085e9', '#e66767']
export const SERIES_LIGHT = ['#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#008300', '#4a3aa7', '#e34948']

const FONT = "system-ui, -apple-system, 'Segoe UI', 'PingFang SC', 'Microsoft YaHei', sans-serif"

export function chrome(isDark) {
  return isDark
    ? {
        text1: '#ffffff', text2: '#c3c2b7', muted: '#898781',
        grid: '#2c2c2a', axis: '#383835', surface: '#1a1a19',
        border: 'rgba(255,255,255,0.14)',
        up: '#e74c3c', down: '#2ecc71',
        good: '#0ca30c', warning: '#fab219', critical: '#d03b3b',
      }
    : {
        text1: '#0b0b0b', text2: '#52514e', muted: '#898781',
        grid: '#e1e0d9', axis: '#c3c2b7', surface: '#fcfcfb',
        border: 'rgba(11,11,11,0.14)',
        up: '#e74c3c', down: '#2ecc71',
        good: '#0ca30c', warning: '#fab219', critical: '#d03b3b',
      }
}

export function seriesColors(isDark) {
  return isDark ? SERIES_DARK : SERIES_LIGHT
}

/** 图表基础骨架：view 里 computed 组合自己的 series 后传给 ChartBox */
export function baseOption(isDark) {
  const c = chrome(isDark)
  return {
    color: seriesColors(isDark),
    backgroundColor: 'transparent',
    textStyle: { fontFamily: FONT, color: c.text2, fontSize: 12 },
    tooltip: tooltipStyle(isDark),
    legend: {
      textStyle: { color: c.text2, fontSize: 11 },
      icon: 'roundRect',
      itemWidth: 10,
      itemHeight: 10,
      itemGap: 14,
      top: 0,
    },
    grid: { left: 8, right: 18, top: 32, bottom: 4, containLabel: true },
  }
}

export function tooltipStyle(isDark) {
  const c = chrome(isDark)
  return {
    backgroundColor: c.surface,
    borderColor: c.border,
    borderWidth: 1,
    textStyle: { color: c.text1, fontSize: 12, fontFamily: FONT },
    confine: true,
  }
}

export function xAxis(isDark, extra = {}) {
  const c = chrome(isDark)
  return {
    type: 'category',
    axisLine: { lineStyle: { color: c.axis } },
    axisTick: { show: false },
    axisLabel: { color: c.muted, fontSize: 11 },
    splitLine: { show: false },
    ...extra,
  }
}

export function yAxis(isDark, extra = {}) {
  const c = chrome(isDark)
  return {
    type: 'value',
    axisLine: { show: false },
    axisTick: { show: false },
    axisLabel: { color: c.muted, fontSize: 11 },
    splitLine: { lineStyle: { color: c.grid, type: 'dashed' } },
    ...extra,
  }
}

/** 涨跌配色（K线/柱状按正负着色） */
export function updown(isDark) {
  const c = chrome(isDark)
  return { up: c.up, down: c.down }
}

/** 折线 series 通用样式：2px 线，无符号，hover 显示 */
export function lineSeries(name, data, extra = {}) {
  return {
    name,
    type: 'line',
    data,
    showSymbol: false,
    symbolSize: 7,
    lineStyle: { width: 2 },
    emphasis: { focus: 'series' },
    ...extra,
  }
}

/** 数据端 4px 圆角柱 */
export function barSeries(name, data, extra = {}) {
  return {
    name,
    type: 'bar',
    data,
    barMaxWidth: 14,
    itemStyle: { borderRadius: [3, 3, 0, 0] },
    ...extra,
  }
}
