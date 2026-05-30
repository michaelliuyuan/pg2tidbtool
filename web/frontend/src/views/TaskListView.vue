<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import apiClient from '../api'
import type { Task } from '../api'

const router = useRouter()
const tasks = ref<Task[]>([])
const loading = ref(true)

const statusMap: Record<string, { type: string; label: string }> = {
  created: { type: 'info', label: '已创建' },
  running: { type: 'warning', label: '运行中' },
  paused: { type: '', label: '已暂停' },
  completed: { type: 'success', label: '已完成' },
  failed: { type: 'danger', label: '失败' },
  cancelled: { type: 'info', label: '已取消' },
}

async function fetchTasks() {
  try {
    const { data } = await apiClient.listTasks()
    tasks.value = data || []
  } catch {
    ElMessage.error('获取任务列表失败')
  } finally {
    loading.value = false
  }
}

function goToTask(id: string) {
  router.push(`/tasks/${id}`)
}

let timer: any = null
onMounted(() => {
  fetchTasks()
  timer = setInterval(fetchTasks, 5000)
})
onUnmounted(() => {
  if (timer) clearInterval(timer)
})
</script>

<template>
  <div style="max-width: 1200px; margin: 0 auto;">
    <div style="display: flex; justify-content: space-between; margin-bottom: 20px;">
      <h2>任务监控</h2>
      <el-button type="primary" @click="router.push('/wizard')">
        <el-icon><Plus /></el-icon> 新建迁移
      </el-button>
    </div>

    <el-card v-loading="loading">
      <el-table :data="tasks" style="width: 100%;" @row-click="(row: Task) => goToTask(row.id)" cursor: pointer>
        <el-table-column prop="name" label="任务名称" min-width="180" />
        <el-table-column label="状态" width="120">
          <template #default="{ row }">
            <el-tag :type="(statusMap[row.status]?.type || 'info') as any">
              {{ statusMap[row.status]?.label || row.status }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="阶段" width="120">
          <template #default="{ row }">{{ row.phase || '-' }}</template>
        </el-table-column>
        <el-table-column label="进度" width="200">
          <template #default="{ row }">
            <el-progress :percentage="Math.round(row.progress * 100)" :stroke-width="14"
              :status="row.status === 'completed' ? 'success' : row.status === 'failed' ? 'exception' : undefined" />
          </template>
        </el-table-column>
        <el-table-column label="表进度" width="120">
          <template #default="{ row }">{{ row.tables_done }}/{{ row.tables_total }}</template>
        </el-table-column>
        <el-table-column label="行数" width="140">
          <template #default="{ row }">{{ row.rows_done?.toLocaleString() || 0 }}</template>
        </el-table-column>
        <el-table-column label="创建时间" width="180">
          <template #default="{ row }">{{ new Date(row.created_at).toLocaleString() }}</template>
        </el-table-column>
      </el-table>

      <el-empty v-if="!loading && tasks.length === 0" description="暂无任务">
        <el-button type="primary" @click="router.push('/wizard')">创建第一个迁移任务</el-button>
      </el-empty>
    </el-card>
  </div>
</template>
