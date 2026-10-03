<template>
  <div class="table-wrap">
    <table class="data-table">
      <thead>
        <tr>
          <th
            v-for="col in columns" :key="col.key"
            :style="{ width: col.width, textAlign: col.align || 'left' }"
          >{{ col.label }}</th>
        </tr>
      </thead>
      <tbody>
        <tr v-if="!pagedRows.length">
          <td :colspan="columns.length" class="empty">{{ emptyText }}</td>
        </tr>
        <tr v-for="(row, ri) in pagedRows" :key="rowKey ? row[rowKey] : ri">
          <td
            v-for="col in columns" :key="col.key"
            :style="{ textAlign: col.align || 'left' }"
            :class="{ ellipsis: col.ellipsis }"
            :title="col.ellipsis ? String(row[col.key] ?? '') : undefined"
          >
            <slot :name="`cell-${col.key}`" :row="row" :value="row[col.key]">
              <StatusTag v-if="col.type === 'status'" :status="String(row[col.key] ?? '')" />
              <span v-else-if="col.type === 'updown'" class="num" :class="signClass(row[col.key])">
                {{ col.format ? col.format(row[col.key], row) : fmtNum(row[col.key]) }}
              </span>
              <span v-else-if="col.type === 'number'" class="num">
                {{ col.format ? col.format(row[col.key], row) : fmtNum(row[col.key]) }}
              </span>
              <span v-else>{{ col.format ? col.format(row[col.key], row) : row[col.key] ?? '—' }}</span>
            </slot>
          </td>
        </tr>
      </tbody>
    </table>
    <div v-if="pageSize && rows.length > pageSize" class="pager">
      <button class="btn btn-sm" :disabled="page === 1" @click="page--">上一页</button>
      <span class="page-info num">{{ page }} / {{ totalPages }} · 共 {{ rows.length }} 行</span>
      <button class="btn btn-sm" :disabled="page >= totalPages" @click="page++">下一页</button>
    </div>
  </div>
</template>

<script setup>
import { computed, ref, watch } from 'vue'
import StatusTag from './StatusTag.vue'
import { fmtNum, signClass } from '../utils/format.js'

const props = defineProps({
  columns: { type: Array, required: true }, // [{key,label,width,align,type:'status|number|updown',format,ellipsis}]
  rows: { type: Array, default: () => [] },
  pageSize: { type: Number, default: 0 }, // 0 = 不分页
  rowKey: { type: String, default: '' },
  emptyText: { type: String, default: '暂无数据' },
})

const page = ref(1)
watch(() => props.rows, () => { page.value = 1 })

const totalPages = computed(() => Math.max(1, Math.ceil(props.rows.length / props.pageSize)))
const pagedRows = computed(() => {
  if (!props.pageSize) return props.rows
  const start = (page.value - 1) * props.pageSize
  return props.rows.slice(start, start + props.pageSize)
})
</script>

<style scoped>
.table-wrap {
  overflow-x: auto;
}

.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12.5px;
}

.data-table th {
  text-align: left;
  padding: 8px 10px;
  color: var(--text-3);
  font-weight: 550;
  font-size: 11.5px;
  letter-spacing: 0.3px;
  border-bottom: 1px solid var(--border);
  white-space: nowrap;
}

.data-table td {
  padding: 7px 10px;
  border-bottom: 1px solid var(--border);
  color: var(--text-2);
  white-space: nowrap;
}

.data-table tbody tr:hover td {
  background: var(--surface-2);
}

.data-table td.ellipsis {
  max-width: 260px;
  overflow: hidden;
  text-overflow: ellipsis;
}

.empty {
  text-align: center;
  color: var(--text-3);
  padding: 26px 0;
}

.pager {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  padding-top: 10px;
}

.page-info {
  font-size: 12px;
  color: var(--text-3);
}
</style>
