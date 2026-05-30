<script setup lang="ts">
import { ref, onMounted, onUnmounted, computed, nextTick } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import apiClient from '../api'
import type { Task, TaskLogEntry } from '../api'

const route = useRoute()
const router = useRouter()
const taskId = route.params.id as string
const task = ref<Task | null>(null)
const loading = ref(true)
const ws = ref<WebSocket | null>(null)
const wsProgress = ref<any>(null)

const logDrawerVisible = ref(false)
const logs = ref<TaskLogEntry[]>([])
const logsLoading = ref(false)
const logAutoScroll = ref(true)
const logWs = ref<WebSocket | null>(null)
const logContainer = ref<HTMLElement | null>(null)
const logPhaseFilter = ref('all')

const activePhaseTab = ref('overview')

const statusMap: Record<string, { type: string; label: string }> = {
  created: { type: 'info', label: '已创建' },
  running: { type: 'warning', label: '运行中' },
  paused: { type: '', label: '已暂停' },
  completed: { type: 'success', label: '已完成' },
  failed: { type: 'danger', label: '失败' },
  cancelled: { type: 'info', label: '已取消' },
}

const phaseMap: Record<string, string> = {
  precheck: '预检查',
  schema: 'Schema 迁移',
  data: '数据迁移',
  validate: '数据验证',
  completed: '已完成',
}

const phases = ['precheck', 'schema', 'data', 'validate']

const statusInfo = computed(() => {
  if (!task.value) return { type: 'info', label: '-' }
  return statusMap[task.value.status] || { type: 'info', label: task.value.status }
})

const progressPercent = computed(() => {
  if (!task.value) return 0
  return Math.round(task.value.progress * 100)
})

const elapsed = computed(() => {
  if (!task.value?.started_at) return '-'
  const end = task.value.finished_at ? new Date(task.value.finished_at) : new Date()
  const start = new Date(task.value.started_at)
  const diff = Math.floor((end.getTime() - start.getTime()) / 1000)
  const h = Math.floor(diff / 3600)
  const m = Math.floor((diff % 3600) / 60)
  const s = diff % 60
  return `${h}h ${m}m ${s}s`
})

const rowsPerSec = computed(() => {
  if (!task.value?.started_at || !task.value?.rows_done) return 0
  const end = task.value.finished_at ? new Date(task.value.finished_at) : new Date()
  const start = new Date(task.value.started_at)
  const secs = (end.getTime() - start.getTime()) / 1000
  if (secs <= 0) return 0
  return Math.round(task.value.rows_done / secs)
})

const currentPhaseIndex = computed(() => {
  if (!task.value) return -1
  return phases.indexOf(task.value.phase)
})

function phaseStatus(phase: string): string {
  if (!task.value) return 'pending'
  const idx = phases.indexOf(phase)
  const curIdx = currentPhaseIndex.value
  if (task.value.status === 'completed') return 'completed'
  if (task.value.status === 'failed' && idx === curIdx) return 'failed'
  if (idx < curIdx) return 'completed'
  if (idx === curIdx) return task.value.status === 'running' ? 'running' : 'running'
  return 'pending'
}

function phaseStatusIcon(phase: string): string {
  const s = phaseStatus(phase)
  switch (s) {
    case 'completed': return '✅'
    case 'running': return '⏳'
    case 'failed': return '❌'
    default: return '⬜'
  }
}

const filteredLogs = computed(() => {
  if (logPhaseFilter.value === 'all') return logs.value
  const phaseLabel = `Phase: ${phaseMap[logPhaseFilter.value] || logPhaseFilter.value}`
  const phaseLabelLower = phaseLabel.toLowerCase()
  let inPhase = false
  const result: TaskLogEntry[] = []
  for (const entry of logs.value) {
    if (entry.message.toLowerCase().includes(phaseLabelLower)) {
      inPhase = true
      result.push(entry)
      continue
    }
    if (inPhase && entry.message.includes('Phase:')) {
      inPhase = false
      continue
    }
    if (inPhase) result.push(entry)
  }
  return result
})

const phaseLogs = computed(() => {
  const result: Record<string, TaskLogEntry[]> = {}
  for (const phase of phases) {
    const phaseLabel = `Phase: ${phaseMap[phase]}`
    let inPhase = false
    const entries: TaskLogEntry[] = []
    for (const entry of logs.value) {
      if (entry.message.includes(phaseLabel)) {
        inPhase = true
        entries.push(entry)
        continue
      }
      if (inPhase && entry.message.includes('Phase:')) {
        inPhase = false
        continue
      }
      if (inPhase) entries.push(entry)
    }
    result[phase] = entries
  }
  return result
})

async function fetchTask() {
  try {
    const { data } = await apiClient.getTask(taskId)
    task.value = data
  } catch (e: any) {
    ElMessage.error('获取任务信息失败')
  } finally {
    loading.value = false
  }
}

function connectWS() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:'
  ws.value = new WebSocket(`${proto}//${location.host}/api/v1/ws`)
  ws.value.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data)
      if (data.task_id === taskId && task.value) {
        wsProgress.value = data
        if (data.phase) task.value.phase = data.phase
        if (data.progress !== undefined) task.value.progress = data.progress
        if (data.tables_done !== undefined) task.value.tables_done = data.tables_done
        if (data.tables_total !== undefined) task.value.tables_total = data.tables_total
        if (data.rows_done !== undefined) task.value.rows_done = data.rows_done
        if (data.rows_total !== undefined) task.value.rows_total = data.rows_total
        if (data.status) task.value.status = data.status
      }
    } catch {}
  }
}

let pollTimer: any = null

onMounted(() => {
  fetchTask()
  connectWS()
  pollTimer = setInterval(fetchTask, 5000)
  loadLogs()
})

onUnmounted(() => {
  if (pollTimer) clearInterval(pollTimer)
  if (ws.value) ws.value.close()
  if (logWs.value) logWs.value.close()
})

async function action(actionName: string) {
  try {
    await (apiClient as any)[`${actionName}Task`](taskId)
    ElMessage.success('操作成功')
    await fetchTask()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.error || '操作失败')
  }
}

async function cancelTask() {
  try {
    await apiClient.cancelTask(taskId)
    ElMessage.success('任务已取消')
    await fetchTask()
  } catch (e: any) {
    ElMessage.error('取消失败')
  }
}

async function deleteTask() {
  try {
    await apiClient.deleteTask(taskId)
    ElMessage.success('任务已删除')
    router.push('/tasks')
  } catch (e: any) {
    ElMessage.error(e.response?.data?.error || '删除失败')
  }
}

async function downloadReport() {
  const { data } = await apiClient.getTaskReport(taskId, 'json')
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `report-${taskId}.json`
  a.click()
  URL.revokeObjectURL(url)
}

async function loadLogs() {
  logsLoading.value = true
  try {
    const { data } = await apiClient.getTaskLogs(taskId)
    logs.value = data.logs || []
  } catch {
  } finally {
    logsLoading.value = false
  }
  connectLogWS()
}

function openLogs() {
  logDrawerVisible.value = true
  if (logs.value.length === 0) {
    loadLogs()
  }
  nextTick(() => scrollToBottom())
}

function closeLogs() {
  logDrawerVisible.value = false
}

function connectLogWS() {
  if (logWs.value) {
    logWs.value.close()
  }
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:'
  logWs.value = new WebSocket(`${proto}//${location.host}/api/v1/tasks/${taskId}/logs?ws=true`)
  logWs.value.onmessage = (event) => {
    try {
      const entry: TaskLogEntry = JSON.parse(event.data)
      logs.value.push(entry)
      if (logAutoScroll.value && logDrawerVisible.value) {
        nextTick(() => scrollToBottom())
      }
    } catch {}
  }
}

function scrollToBottom() {
  if (logContainer.value) {
    logContainer.value.scrollTop = logContainer.value.scrollHeight
  }
}

function logLevelClass(level: string): string {
  switch (level) {
    case 'ERROR': case 'FATAL': return 'log-error'
    case 'WARN': return 'log-warn'
    case 'DEBUG': return 'log-debug'
    default: return 'log-info'
  }
}
</script>

<template>
  <div v-loading="loading" style="max-width: 1000px; margin: 0 auto;">
    <el-page-header @back="router.push('/tasks')" style="margin-bottom: 20px;">
      <template #content>
        <span>{{ task?.name || '任务详情' }}</span>
        <el-tag :type="statusInfo.type as any" style="margin-left: 12px;">{{ statusInfo.label }}</el-tag>
      </template>
    </el-page-header>

    <template v-if="task">
      <!-- Status Overview -->
      <el-card style="margin-bottom: 20px;">
        <el-row :gutter="20">
          <el-col :span="4">
            <el-statistic title="阶段" :value="phaseMap[task.phase] || task.phase || '-'" />
          </el-col>
          <el-col :span="4">
            <el-statistic title="表进度" :value="`${task.tables_done}/${task.tables_total}`" />
          </el-col>
          <el-col :span="4">
            <el-statistic title="行数" :value="task.rows_done.toLocaleString()" />
            <div style="font-size: 12px; color: #999;">/ {{ task.rows_total.toLocaleString() }}</div>
          </el-col>
          <el-col :span="4">
            <el-statistic title="吞吐量" :value="rowsPerSec.toLocaleString()" suffix="rows/s" />
          </el-col>
          <el-col :span="4">
            <el-statistic title="耗时" :value="elapsed" />
          </el-col>
          <el-col :span="4">
            <el-statistic title="总进度" :value="progressPercent" suffix="%" />
          </el-col>
        </el-row>
        <el-progress :percentage="progressPercent" :stroke-width="20" style="margin-top: 16px;"
          :status="task.status === 'completed' ? 'success' : task.status === 'failed' ? 'exception' : undefined" />
      </el-card>

      <!-- Actions -->
      <el-card style="margin-bottom: 20px;">
        <template #header>操作</template>
        <el-space>
          <el-button v-if="task.status === 'running'" type="warning" @click="action('pause')">
            <el-icon><VideoPause /></el-icon> 暂停
          </el-button>
          <el-button v-if="task.status === 'paused'" type="success" @click="action('resume')">
            <el-icon><VideoPlay /></el-icon> 恢复
          </el-button>
          <el-button v-if="task.status === 'running' || task.status === 'paused'" type="danger" @click="cancelTask">
            <el-icon><CircleClose /></el-icon> 取消
          </el-button>
          <el-button v-if="task.status === 'completed' || task.status === 'failed' || task.status === 'cancelled'" @click="downloadReport">
            <el-icon><Download /></el-icon> 下载报告
          </el-button>
          <el-button v-if="task.status !== 'running'" type="danger" plain @click="deleteTask">
            <el-icon><Delete /></el-icon> 删除任务
          </el-button>
          <el-button v-if="task.status !== 'created'" type="primary" plain @click="openLogs">
            <el-icon><Document /></el-icon> 查看日志
          </el-button>
        </el-space>
      </el-card>

      <!-- Phase Timeline -->
      <el-card style="margin-bottom: 20px;">
        <template #header>迁移阶段</template>
        <el-tabs v-model="activePhaseTab">
          <el-tab-pane label="阶段概览" name="overview">
            <el-timeline>
              <el-timeline-item
                v-for="phase in phases"
                :key="phase"
                :type="phaseStatus(phase) === 'completed' ? 'success' : phaseStatus(phase) === 'running' ? 'primary' : phaseStatus(phase) === 'failed' ? 'danger' : 'info'"
                :hollow="phaseStatus(phase) === 'pending'"
                :timestamp="phaseMap[phase]"
                placement="top"
              >
                <div style="display: flex; align-items: center; gap: 8px;">
                  <span>{{ phaseStatusIcon(phase) }}</span>
                  <el-tag :type="phaseStatus(phase) === 'completed' ? 'success' : phaseStatus(phase) === 'running' ? '' : phaseStatus(phase) === 'failed' ? 'danger' : 'info'" size="small">
                    {{ phaseStatus(phase) === 'completed' ? '已完成' : phaseStatus(phase) === 'running' ? '进行中' : phaseStatus(phase) === 'failed' ? '失败' : '等待中' }}
                  </el-tag>
                </div>
                <div v-if="phase === task.phase && phase === 'data'" style="margin-top: 8px; color: #606266; font-size: 13px;">
                  表: {{ task.tables_done }}/{{ task.tables_total }} · 行: {{ task.rows_done.toLocaleString() }}/{{ task.rows_total.toLocaleString() }}
                </div>
                <div v-if="phaseLogs[phase]?.length" style="margin-top: 8px;">
                  <el-tag
                    size="small"
                    type="info"
                    style="cursor: pointer;"
                    @click="logPhaseFilter = phase; logDrawerVisible = true"
                  >
                    {{ phaseLogs[phase].length }} 条日志 - 点击查看
                  </el-tag>
                </div>
              </el-timeline-item>
            </el-timeline>
          </el-tab-pane>

          <el-tab-pane
            v-for="phase in phases"
            :key="phase"
            :label="phaseMap[phase]"
            :name="phase"
          >
            <div v-if="phaseLogs[phase]?.length === 0" style="color: #909399; text-align: center; padding: 30px;">
              {{ phaseStatus(phase) === 'pending' ? '该阶段尚未开始' : '暂无日志' }}
            </div>
            <div v-else class="phase-log-container">
              <div v-for="(entry, idx) in phaseLogs[phase]" :key="idx" class="log-line" :class="logLevelClass(entry.level)">
                <span class="log-time">{{ entry.timestamp ? entry.timestamp.replace('T', ' ').substring(11, 19) : '' }}</span>
                <span class="log-level">[{{ entry.level }}]</span>
                <span class="log-msg">{{ entry.message }}</span>
              </div>
            </div>
          </el-tab-pane>
        </el-tabs>
      </el-card>

      <!-- Error -->
      <el-card v-if="task.error" style="margin-bottom: 20px;">
        <el-alert :title="task.error" type="error" :closable="false" show-icon />
      </el-card>

      <!-- Info -->
      <el-card>
        <template #header>任务信息</template>
        <el-descriptions :column="2" border>
          <el-descriptions-item label="任务 ID">{{ task.id }}</el-descriptions-item>
          <el-descriptions-item label="创建时间">{{ task.created_at }}</el-descriptions-item>
          <el-descriptions-item label="开始时间">{{ task.started_at || '-' }}</el-descriptions-item>
          <el-descriptions-item label="结束时间">{{ task.finished_at || '-' }}</el-descriptions-item>
        </el-descriptions>
      </el-card>
    </template>

    <!-- Log Drawer -->
    <el-drawer v-model="logDrawerVisible" title="任务日志" size="60%" :before-close="closeLogs" direction="rtl">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
        <el-checkbox v-model="logAutoScroll">自动滚动</el-checkbox>
        <el-select v-model="logPhaseFilter" size="small" style="width: 160px;">
          <el-option label="全部日志" value="all" />
          <el-option label="预检查" value="precheck" />
          <el-option label="Schema 迁移" value="schema" />
          <el-option label="数据迁移" value="data" />
          <el-option label="数据验证" value="validate" />
        </el-select>
      </div>
      <div ref="logContainer" v-loading="logsLoading" class="log-container">
        <div v-if="filteredLogs.length === 0 && !logsLoading" style="color: #999; text-align: center; padding: 40px;">
          暂无日志
        </div>
        <div v-for="(log, idx) in filteredLogs" :key="idx" class="log-line" :class="logLevelClass(log.level)">
          <span class="log-time">{{ log.timestamp ? log.timestamp.replace('T', ' ').substring(0, 19) : '' }}</span>
          <span class="log-level">[{{ log.level }}]</span>
          <span class="log-msg">{{ log.message }}</span>
          <span v-if="log.caller" class="log-caller">({{ log.caller }})</span>
        </div>
      </div>
    </el-drawer>
  </div>
</template>

<style scoped>
.log-container {
  background: #1e1e1e;
  color: #d4d4d4;
  font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
  font-size: 13px;
  line-height: 1.6;
  padding: 12px;
  border-radius: 6px;
  height: calc(100vh - 240px);
  overflow-y: auto;
}

.phase-log-container {
  background: #1e1e1e;
  color: #d4d4d4;
  font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
  font-size: 13px;
  line-height: 1.6;
  padding: 12px;
  border-radius: 6px;
  max-height: 400px;
  overflow-y: auto;
}

.log-line {
  padding: 2px 0;
  border-bottom: 1px solid rgba(255,255,255,0.05);
}

.log-time {
  color: #6a9955;
  margin-right: 8px;
}

.log-level {
  font-weight: bold;
  margin-right: 8px;
}

.log-msg {
  color: #d4d4d4;
}

.log-caller {
  color: #608b4e;
  margin-left: 8px;
  font-size: 12px;
}

.log-info .log-level { color: #4ec9b0; }
.log-warn .log-level { color: #dcdcaa; }
.log-error .log-level { color: #f44747; }
.log-debug .log-level { color: #608b4e; }
.log-error { background: rgba(244, 71, 71, 0.1); }
.log-warn { background: rgba(220, 220, 170, 0.08); }
</style>
