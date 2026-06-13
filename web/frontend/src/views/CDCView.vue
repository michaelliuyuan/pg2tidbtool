<template>
  <div class="cdc-container">
    <h1>CDC 增量同步</h1>
    <p class="subtitle">PostgreSQL → TiDB 实时增量同步监控</p>

    <!-- Status Card -->
    <div class="status-card" :class="{ running: status.running, stopped: !status.running }">
      <div class="status-indicator">
        <span class="status-dot" :class="{ active: status.running }"></span>
        <span class="status-text">{{ status.running ? '运行中' : '已停止' }}</span>
      </div>
      <div class="status-meta">
        <span v-if="status.running && status.source_lsn">LSN: {{ status.source_lsn }}</span>
        <span v-else>{{ status.message || 'CDC 未运行，使用 pg2tidb cdc 命令启动' }}</span>
      </div>
    </div>

    <!-- Stats Grid -->
    <div class="stats-grid" v-if="stats">
      <div class="stat-item">
        <div class="stat-value">{{ formatNumber(stats.source_events_total) }}</div>
        <div class="stat-label">源端事件</div>
      </div>
      <div class="stat-item">
        <div class="stat-value">{{ formatNumber(stats.applier_events_applied) }}</div>
        <div class="stat-label">已应用</div>
      </div>
      <div class="stat-item">
        <div class="stat-value" :class="{ error: stats.applier_events_failed > 0 }">{{ formatNumber(stats.applier_events_failed) }}</div>
        <div class="stat-label">失败</div>
      </div>
      <div class="stat-item">
        <div class="stat-value">{{ formatNumber(stats.applier_events_skipped) }}</div>
        <div class="stat-label">已跳过</div>
      </div>
      <div class="stat-item">
        <div class="stat-value">{{ stats.events_per_second?.toFixed(1) || '0' }}/s</div>
        <div class="stat-label">吞吐量</div>
      </div>
      <div class="stat-item">
        <div class="stat-value">{{ formatNumber(stats.lag_events) }}</div>
        <div class="stat-label">延迟事件</div>
      </div>
      <div class="stat-item">
        <div class="stat-value">{{ formatUptime(stats.uptime_seconds) }}</div>
        <div class="stat-label">运行时间</div>
      </div>
      <div class="stat-item">
        <div class="stat-value">{{ formatNumber(stats.applier_batches_flushed) }}</div>
        <div class="stat-label">批次数</div>
      </div>
    </div>

    <!-- Checkpoint Card -->
    <div class="detail-card" v-if="checkpoint">
      <h3>📌 检查点</h3>
      <div class="detail-row">
        <span class="detail-label">LSN:</span>
        <code>{{ checkpoint.lsn }}</code>
      </div>
      <div class="detail-row">
        <span class="detail-label">Slot:</span>
        <code>{{ checkpoint.slot_name }}</code>
      </div>
      <div class="detail-row">
        <span class="detail-label">更新时间:</span>
        <span>{{ checkpoint.timestamp ? new Date(checkpoint.timestamp).toLocaleString() : '-' }}</span>
      </div>
    </div>

    <!-- Config Card -->
    <div class="detail-card" v-if="config">
      <h3>⚙️ 配置</h3>
      <div class="detail-row">
        <span class="detail-label">Slot:</span>
        <code>{{ config.slot_name }}</code>
      </div>
      <div class="detail-row">
        <span class="detail-label">Publication:</span>
        <code>{{ config.publication }}</code>
      </div>
      <div class="detail-row">
        <span class="detail-label">冲突策略:</span>
        <span>{{ config.conflict_strategy }}</span>
      </div>
      <div class="detail-row">
        <span class="detail-label">批量大小:</span>
        <span>{{ config.batch_size }}</span>
      </div>
      <div class="detail-row">
        <span class="detail-label">并行度:</span>
        <span>{{ config.parallel }}</span>
      </div>
    </div>

    <!-- Error display -->
    <div class="error-card" v-if="stats?.applier_last_error">
      <h3>⚠️ 最近错误</h3>
      <pre>{{ stats.applier_last_error }}</pre>
    </div>

    <!-- Refresh button -->
    <div class="actions">
      <button @click="refresh" class="btn-refresh">🔄 刷新</button>
      <span class="auto-refresh">自动刷新: {{ refreshInterval }}s</span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'

const API_BASE = '/api/v1/cdc'

interface CDCStatus {
  running: boolean
  source_lsn?: string
  message?: string
  config?: CDCConfig
}

interface CDCConfig {
  slot_name: string
  publication: string
  conflict_strategy: string
  batch_size: number
  parallel: number
}

interface CDCStats {
  source_events_total: number
  applier_events_applied: number
  applier_events_failed: number
  applier_events_skipped: number
  applier_batches_flushed: number
  events_per_second: number
  lag_events: number
  uptime_seconds: number
  applier_last_error?: string
}

interface CDCCheckpoint {
  lsn: string
  slot_name: string
  timestamp: string
}

const status = ref<CDCStatus>({ running: false })
const stats = ref<CDCStats | null>(null)
const checkpoint = ref<CDCCheckpoint | null>(null)
const config = ref<CDCConfig | null>(null)
const refreshInterval = ref(5)
let timer: ReturnType<typeof setInterval> | null = null

async function refresh() {
  try {
    const [statusRes, statsRes, cpRes] = await Promise.all([
      fetch(API_BASE + '/status').then(r => r.json()).catch(() => null),
      fetch(API_BASE + '/stats').then(r => r.json()).catch(() => null),
      fetch(API_BASE + '/checkpoint').then(r => r.json()).catch(() => null),
    ])

    if (statusRes) {
      status.value = statusRes
      if (statusRes.config) config.value = statusRes.config
    }
    if (statsRes && !statsRes.error) stats.value = statsRes
    if (cpRes && !cpRes.error) checkpoint.value = cpRes
  } catch {
    // silently ignore fetch errors
  }
}

function formatNumber(n: number): string {
  if (n === undefined || n === null) return '0'
  if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M'
  if (n >= 1000) return (n / 1000).toFixed(1) + 'K'
  return String(n)
}

function formatUptime(seconds: number): string {
  if (!seconds) return '0s'
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = Math.floor(seconds % 60)
  if (h > 0) return `${h}h ${m}m`
  if (m > 0) return `${m}m ${s}s`
  return `${s}s`
}

onMounted(() => {
  refresh()
  timer = setInterval(refresh, refreshInterval.value * 1000)
})

onUnmounted(() => {
  if (timer) clearInterval(timer)
})
</script>

<style scoped>
.cdc-container {
  max-width: 1000px;
  margin: 0 auto;
  padding: 24px;
  font-family: -apple-system, BlinkMacSystemFont, 'PingFang SC', sans-serif;
}
h1 { font-size: 24px; color: #1a1a2e; margin-bottom: 4px; }
.subtitle { color: #666; font-size: 14px; margin-bottom: 24px; }

.status-card {
  border-radius: 12px; padding: 24px; margin-bottom: 24px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.08);
}
.status-card.running { background: linear-gradient(135deg, #52c41a, #73d13d); color: #fff; }
.status-card.stopped { background: #f5f5f5; color: #666; }
.status-indicator { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
.status-dot { width: 12px; height: 12px; border-radius: 50%; background: #d9d9d9; }
.status-dot.active { background: #fff; animation: pulse 2s infinite; }
.status-text { font-size: 18px; font-weight: 600; }
.status-meta { font-size: 13px; opacity: 0.8; }

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.5; }
}

.stats-grid {
  display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px;
  margin-bottom: 24px;
}
.stat-item {
  background: #fff; border-radius: 12px; padding: 20px; text-align: center;
  box-shadow: 0 2px 8px rgba(0,0,0,0.06);
}
.stat-value { font-size: 28px; font-weight: 700; color: #1a1a2e; }
.stat-value.error { color: #f5222d; }
.stat-label { font-size: 13px; color: #666; margin-top: 4px; }

.detail-card {
  background: #fff; border-radius: 12px; padding: 20px; margin-bottom: 16px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.06);
}
.detail-card h3 { font-size: 16px; margin-bottom: 12px; color: #1a1a2e; }
.detail-row { display: flex; align-items: center; padding: 6px 0; font-size: 14px; }
.detail-label { width: 100px; color: #666; }
code { background: #f0f0f0; padding: 2px 8px; border-radius: 4px; font-size: 13px; }

.error-card {
  background: #fff1f0; border: 1px solid #ffccc7; border-radius: 12px;
  padding: 16px; margin-bottom: 16px;
}
.error-card h3 { font-size: 16px; color: #cf1322; margin-bottom: 8px; }
.error-card pre { font-size: 13px; color: #cf1322; white-space: pre-wrap; word-break: break-all; }

.actions {
  display: flex; align-items: center; gap: 16px; margin-top: 16px;
}
.btn-refresh {
  padding: 8px 20px; border: none; border-radius: 8px;
  background: #1a1a2e; color: #fff; font-size: 14px; cursor: pointer;
}
.btn-refresh:hover { opacity: 0.85; }
.auto-refresh { font-size: 12px; color: #999; }
</style>
