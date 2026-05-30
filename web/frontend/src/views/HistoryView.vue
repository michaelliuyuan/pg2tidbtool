<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import apiClient from '../api'
import type { Task } from '../api'

const router = useRouter()
const tasks = ref<Task[]>([])
const loading = ref(true)

const statusMap: Record<string, { type: string; label: string }> = {
  completed: { type: 'success', label: '已完成' },
  failed: { type: 'danger', label: '失败' },
  cancelled: { type: 'info', label: '已取消' },
}

async function fetchHistory() {
  try {
    const { data } = await apiClient.listTasks()
    tasks.value = (data || []).filter((t: Task) =>
      ['completed', 'failed', 'cancelled'].includes(t.status)
    )
  } catch {
    ElMessage.error('获取历史记录失败')
  } finally {
    loading.value = false
  }
}

async function deleteTask(id: string) {
  try {
    await apiClient.deleteTask(id)
    ElMessage.success('已删除')
    await fetchHistory()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.error || '删除失败')
  }
}

onMounted(fetchHistory)
</script>

<template>
  <div style="max-width: 1200px; margin: 0 auto;">
    <h2 style="margin-bottom: 20px;">迁移历史</h2>
    <el-card v-loading="loading">
      <el-table :data="tasks" style="width: 100%;">
        <el-table-column prop="name" label="任务名称" min-width="200" />
        <el-table-column label="状态" width="120">
          <template #default="{ row }">
            <el-tag :type="(statusMap[row.status]?.type || 'info') as any">
              {{ statusMap[row.status]?.label || row.status }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="表" width="100">
          <template #default="{ row }">{{ row.tables_done }}/{{ row.tables_total }}</template>
        </el-table-column>
        <el-table-column label="行数" width="140">
          <template #default="{ row }">{{ row.rows_done?.toLocaleString() || 0 }}</template>
        </el-table-column>
        <el-table-column label="创建时间" width="180">
          <template #default="{ row }">{{ new Date(row.created_at).toLocaleString() }}</template>
        </el-table-column>
        <el-table-column label="结束时间" width="180">
          <template #default="{ row }">{{ row.finished_at ? new Date(row.finished_at).toLocaleString() : '-' }}</template>
        </el-table-column>
        <el-table-column label="操作" width="180">
          <template #default="{ row }">
            <el-button size="small" @click="router.push(`/tasks/${row.id}`)">详情</el-button>
            <el-button size="small" type="danger" plain @click="deleteTask(row.id)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
      <el-empty v-if="!loading && tasks.length === 0" description="暂无历史记录" />
    </el-card>
  </div>
</template>
