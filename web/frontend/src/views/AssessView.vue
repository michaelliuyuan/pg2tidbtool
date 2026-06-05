<script setup lang="ts">
import { ref, computed } from 'vue'
import { ElMessage } from 'element-plus'

interface Finding {
  dimension: string
  object_type: string
  object_name: string
  level: string
  pg_detail: string
  tidb_detail: string
  suggestion: string
  auto_fix: boolean
}

interface DimResult {
  dimension: string
  total: number
  score: number
  findings: Finding[]
}

interface AssessReport {
  score: number
  level: string
  dimension_results: DimResult[]
  all_findings: Finding[]
  summary: Record<string, number>
}

const loading = ref(false)
const report = ref<AssessReport | null>(null)
const htmlReportUrl = ref('')

const sourceForm = ref({
  host: '',
  port: 5432,
  user: 'postgres',
  password: '',
  database: '',
  schema: 'public'
})

const levelEmoji: Record<string, string> = {
  compatible: '✅',
  convertible: '⚠️',
  manual_needed: '🟡',
  incompatible: '❌'
}

const levelName: Record<string, string> = {
  compatible: '兼容',
  convertible: '可转换',
  manual_needed: '需手动',
  incompatible: '不兼容'
}

const dimName: Record<string, string> = {
  data_type: '数据类型',
  structure: '表结构',
  index: '索引',
  view: '视图',
  function: '函数',
  trigger: '触发器',
  custom_type: '自定义类型',
  extension: '扩展',
  sequence: '序列'
}

const scoreColor = computed(() => {
  if (!report.value) return '#52c41a'
  const s = report.value.score
  if (s >= 90) return '#52c41a'
  if (s >= 70) return '#faad14'
  if (s >= 40) return '#fa8c16'
  return '#f5222d'
})

const problems = computed(() => {
  if (!report.value) return []
  return report.value.all_findings
    .filter(f => f.level !== 'compatible')
    .sort((a, b) => levelOrder(a.level) - levelOrder(b.level))
})

function levelOrder(level: string): number {
  const order: Record<string, number> = { incompatible: 0, manual_needed: 1, convertible: 2 }
  return order[level] ?? 3
}

function badgeClass(level: string): string {
  const map: Record<string, string> = {
    convertible: 'warning',
    manual_needed: 'warning',
    incompatible: 'danger'
  }
  return map[level] || 'info'
}

async function runAssess() {
  if (!sourceForm.value.host || !sourceForm.value.database) {
    ElMessage.warning('请填写主机和数据库名')
    return
  }
  loading.value = true
  report.value = null
  htmlReportUrl.value = ''

  try {
    const resp = await fetch('/api/v1/assess', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(sourceForm.value)
    })
    if (!resp.ok) {
      const err = await resp.json()
      throw new Error(err.error || `HTTP ${resp.status}`)
    }
    report.value = await resp.json()
    ElMessage.success('评估完成')
  } catch (e: any) {
    ElMessage.error(e.message || '评估失败')
  } finally {
    loading.value = false
  }
}

async function downloadHTML() {
  if (!sourceForm.value.host) return
  loading.value = true
  try {
    const resp = await fetch('/api/v1/assess', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...sourceForm.value, format: 'html' })
    })
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
    const html = await resp.text()
    const blob = new Blob([html], { type: 'text/html' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'compatibility-assessment-report.html'
    a.click()
    URL.revokeObjectURL(url)
  } catch (e: any) {
    ElMessage.error(e.message || '下载失败')
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div style="max-width: 1100px; margin: 0 auto;">
    <!-- Connection Form -->
    <el-card shadow="never" style="margin-bottom: 20px;">
      <template #header>
        <span style="font-weight: 600;">🔗 数据源配置</span>
      </template>
      <el-form :model="sourceForm" label-width="80px" size="default" inline>
        <el-form-item label="主机">
          <el-input v-model="sourceForm.host" placeholder="PG 主机地址" style="width: 160px;" />
        </el-form-item>
        <el-form-item label="端口">
          <el-input-number v-model="sourceForm.port" :min="1" :max="65535" style="width: 120px;" />
        </el-form-item>
        <el-form-item label="用户">
          <el-input v-model="sourceForm.user" style="width: 120px;" />
        </el-form-item>
        <el-form-item label="密码">
          <el-input v-model="sourceForm.password" type="password" show-password style="width: 140px;" />
        </el-form-item>
        <el-form-item label="数据库">
          <el-input v-model="sourceForm.database" placeholder="数据库名" style="width: 140px;" />
        </el-form-item>
        <el-form-item label="Schema">
          <el-input v-model="sourceForm.schema" style="width: 100px;" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="runAssess" :loading="loading">
            {{ loading ? '评估中...' : '🔍 开始评估' }}
          </el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <!-- Report -->
    <template v-if="report">
      <!-- Score Card -->
      <el-card shadow="never" style="margin-bottom: 20px;" :body-style="{ padding: '0' }">
        <div :style="{
          background: `linear-gradient(135deg, ${scoreColor}, ${scoreColor}dd)`,
          padding: '32px', textAlign: 'center', color: '#fff', borderRadius: '8px'
        }">
          <div style="font-size: 56px; font-weight: 900; line-height: 1;">{{ report.score.toFixed(1) }}</div>
          <div style="font-size: 16px; margin-top: 8px; opacity: 0.9;">总体兼容性评分</div>
          <div style="margin-top: 10px;">
            <span :style="{
              display: 'inline-block', padding: '4px 16px', borderRadius: '20px',
              fontSize: '14px', background: 'rgba(255,255,255,0.2)'
            }">{{ levelEmoji[report.level] }} {{ levelName[report.level] || report.level }}</span>
          </div>
        </div>
      </el-card>

      <!-- Summary Cards -->
      <el-row :gutter="16" style="margin-bottom: 20px;">
        <el-col :span="6" v-for="(key, idx) in ['compatible', 'convertible', 'manual_needed', 'incompatible']" :key="key">
          <el-card shadow="never" :body-style="{ textAlign: 'center', padding: '20px' }">
            <div :style="{ fontSize: '32px', fontWeight: 700, color: ['#52c41a','#faad14','#fa8c16','#f5222d'][idx] }">
              {{ report.summary[key] || 0 }}
            </div>
            <div style="font-size: 13px; color: #666; margin-top: 4px;">
              {{ ['✅ 兼容', '⚠️ 可转换', '🟡 需手动', '❌ 不兼容'][idx] }}
            </div>
          </el-card>
        </el-col>
      </el-row>

      <!-- Dimension Scores -->
      <el-card shadow="never" style="margin-bottom: 20px;">
        <template #header>
          <span style="font-weight: 600;">📊 维度评分</span>
        </template>
        <el-table :data="report.dimension_results" stripe size="default">
          <el-table-column label="维度" width="140">
            <template #default="{ row }">{{ dimName[row.dimension] || row.dimension }}</template>
          </el-table-column>
          <el-table-column prop="total" label="对象数" width="100" align="center" />
          <el-table-column label="得分" width="220">
            <template #default="{ row }">
              <el-progress
                :percentage="Math.round(row.score)"
                :color="row.score >= 90 ? '#52c41a' : row.score >= 70 ? '#faad14' : row.score >= 40 ? '#fa8c16' : '#f5222d'"
                :stroke-width="10"
              />
            </template>
          </el-table-column>
          <el-table-column label="兼容性" width="120">
            <template #default="{ row }">
              {{ levelEmoji[
                row.score >= 90 ? 'compatible' : row.score >= 70 ? 'convertible' : row.score >= 40 ? 'manual_needed' : 'incompatible'
              ] }}
            </template>
          </el-table-column>
        </el-table>
      </el-card>

      <!-- Problems -->
      <el-card shadow="never" v-if="problems.length > 0">
        <template #header>
          <div style="display: flex; justify-content: space-between; align-items: center;">
            <span style="font-weight: 600;">⚠️ 需要处理的项目（{{ problems.length }} 项）</span>
            <el-button size="small" @click="downloadHTML" :loading="loading">📥 下载 HTML 报告</el-button>
          </div>
        </template>
        <el-table :data="problems" stripe size="small" max-height="500">
          <el-table-column type="index" width="50" />
          <el-table-column prop="object_type" label="类型" width="80" />
          <el-table-column prop="object_name" label="对象" min-width="200" show-overflow-tooltip />
          <el-table-column label="级别" width="110">
            <template #default="{ row }">
              <el-tag :type="badgeClass(row.level)" size="small">
                {{ levelEmoji[row.level] }} {{ levelName[row.level] }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="pg_detail" label="PG" width="140" show-overflow-tooltip />
          <el-table-column prop="tidb_detail" label="TiDB" width="120" show-overflow-tooltip />
          <el-table-column prop="suggestion" label="建议" min-width="250" show-overflow-tooltip />
        </el-table>
      </el-card>
    </template>
  </div>
</template>
