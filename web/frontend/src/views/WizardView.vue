<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, type FormRules } from 'element-plus'
import apiClient from '../api'

const router = useRouter()
const loading = ref(false)
const activeStep = ref(0)

const form = reactive({
  name: '',
  source: {
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: '',
    database: '',
    schema: 'public',
    sslmode: 'disable',
  },
  target: {
    host: 'localhost',
    port: 4000,
    user: 'root',
    password: '',
    database: '',
  },
  opts: {
    parallel: 4,
    batch_size: 100000,
    tables: '',
    exclude_tables: '',
    use_lightning: true,
    skip_precheck: false,
    skip_schema: false,
    skip_data: false,
    skip_validate: false,
  },
})

const sourceTestResult = ref<any>(null)
const targetTestResult = ref<any>(null)
const testingSource = ref(false)
const testingTarget = ref(false)

const savedConnections = ref<Array<{ name: string; source: any; target: any }>>([])
const saveConnName = ref('')
const saveConnDialogVisible = ref(false)
const loadConnDialogVisible = ref(false)

const STORAGE_KEY = 'pg2tidb_saved_connections'

function loadSavedConnections() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (raw) {
      savedConnections.value = JSON.parse(raw)
    }
  } catch {}
}

function saveConnectionsToStorage() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(savedConnections.value))
}

function saveCurrentConnection() {
  if (!saveConnName.value.trim()) {
    ElMessage.warning('请输入连接配置名称')
    return
  }
  const idx = savedConnections.value.findIndex(c => c.name === saveConnName.value.trim())
  const entry = {
    name: saveConnName.value.trim(),
    source: { ...form.source },
    target: { ...form.target },
  }
  if (idx >= 0) {
    savedConnections.value[idx] = entry
  } else {
    savedConnections.value.push(entry)
  }
  saveConnectionsToStorage()
  saveConnDialogVisible.value = false
  saveConnName.value = ''
  ElMessage.success('连接配置已保存')
}

function loadConnection(conn: { source: any; target: any }) {
  Object.assign(form.source, conn.source)
  Object.assign(form.target, conn.target)
  loadConnDialogVisible.value = false
  ElMessage.success('连接配置已加载')
}

function deleteConnection(idx: number) {
  savedConnections.value.splice(idx, 1)
  saveConnectionsToStorage()
}

onMounted(() => {
  loadSavedConnections()
  const last = localStorage.getItem('pg2tidb_last_connection')
  if (last) {
    try {
      const c = JSON.parse(last)
      if (c.source) Object.assign(form.source, c.source)
      if (c.target) Object.assign(form.target, c.target)
    } catch {}
  }
})

const rules: FormRules = {
  'source.host': [{ required: true, message: '请输入源数据库地址', trigger: 'blur' }],
  'source.database': [{ required: true, message: '请输入源数据库名', trigger: 'blur' }],
  'target.host': [{ required: true, message: '请输入目标数据库地址', trigger: 'blur' }],
  'target.database': [{ required: true, message: '请输入目标数据库名', trigger: 'blur' }],
}

async function testConnection(type: 'source' | 'target') {
  if (type === 'source') {
    testingSource.value = true
    sourceTestResult.value = null
  } else {
    testingTarget.value = true
    targetTestResult.value = null
  }

  try {
    const { data } = await apiClient.testConnection({
      type,
      host: type === 'source' ? form.source.host : form.target.host,
      port: type === 'source' ? form.source.port : form.target.port,
      user: type === 'source' ? form.source.user : form.target.user,
      password: type === 'source' ? form.source.password : form.target.password,
      database: type === 'source' ? form.source.database : form.target.database,
      schema: type === 'source' ? form.source.schema : undefined,
      sslmode: type === 'source' ? form.source.sslmode : undefined,
    })
    if (type === 'source') {
      sourceTestResult.value = data
    } else {
      targetTestResult.value = data
    }
    if (data.ok) {
      ElMessage.success(`${type === 'source' ? 'PostgreSQL' : 'TiDB'} 连接成功`)
    } else {
      ElMessage.error(`连接失败: ${data.error}`)
    }
  } catch (e: any) {
    ElMessage.error(`连接测试失败: ${e.message}`)
  } finally {
    testingSource.value = false
    testingTarget.value = false
  }
}

async function submit() {
  loading.value = true
  try {
    localStorage.setItem('pg2tidb_last_connection', JSON.stringify({ source: form.source, target: form.target }))

    const { data } = await apiClient.createTask({
      name: form.name || `Migration ${new Date().toLocaleString()}`,
      source: { ...form.source },
      target: { ...form.target },
      opts: {
        parallel: form.opts.parallel,
        batch_size: form.opts.batch_size,
        tables: form.opts.tables ? form.opts.tables.split(',').map(s => s.trim()).filter(Boolean) : [],
        exclude_tables: form.opts.exclude_tables ? form.opts.exclude_tables.split(',').map(s => s.trim()).filter(Boolean) : [],
        use_lightning: form.opts.use_lightning,
        skip_precheck: form.opts.skip_precheck,
        skip_schema: form.opts.skip_schema,
        skip_data: form.opts.skip_data,
        skip_validate: form.opts.skip_validate,
      },
    })
    ElMessage.success('迁移任务创建成功')
    await apiClient.startTask(data.id)
    router.push(`/tasks/${data.id}`)
  } catch (e: any) {
    ElMessage.error(`创建失败: ${e.response?.data?.error || e.message}`)
  } finally {
    loading.value = false
  }
}

function nextStep() {
  activeStep.value++
}
function prevStep() {
  activeStep.value--
}
</script>

<template>
  <div style="max-width: 900px; margin: 0 auto;">
    <el-card>
      <template #header>
        <div style="display: flex; align-items: center; justify-content: space-between;">
          <div style="display: flex; align-items: center;">
            <el-icon size="24" style="margin-right: 8px;"><Connection /></el-icon>
            <span style="font-size: 18px; font-weight: bold;">新建迁移任务</span>
          </div>
          <el-space>
            <el-button size="small" @click="loadConnDialogVisible = true">
              <el-icon><FolderOpened /></el-icon> 加载连接
            </el-button>
            <el-button size="small" @click="saveConnDialogVisible = true">
              <el-icon><FolderAdd /></el-icon> 保存连接
            </el-button>
          </el-space>
        </div>
      </template>

      <el-steps :active="activeStep" finish-status="success" align-center style="margin-bottom: 30px;">
        <el-step title="源数据库" />
        <el-step title="目标数据库" />
        <el-step title="迁移选项" />
        <el-step title="确认执行" />
      </el-steps>

      <el-form ref="formRef" :model="form" :rules="rules" label-width="120px">
        <!-- Step 0: Source -->
        <div v-show="activeStep === 0">
          <el-form-item label="任务名称">
            <el-input v-model="form.name" placeholder="可选，自动生成" />
          </el-form-item>
          <el-form-item label="主机地址" prop="source.host">
            <el-input v-model="form.source.host" />
          </el-form-item>
          <el-form-item label="端口">
            <el-input-number v-model="form.source.port" :min="1" :max="65535" />
          </el-form-item>
          <el-form-item label="用户名">
            <el-input v-model="form.source.user" />
          </el-form-item>
          <el-form-item label="密码">
            <el-input v-model="form.source.password" type="password" show-password />
          </el-form-item>
          <el-form-item label="数据库名" prop="source.database">
            <el-input v-model="form.source.database" />
          </el-form-item>
          <el-form-item label="Schema">
            <el-input v-model="form.source.schema" />
          </el-form-item>
          <el-form-item label="SSL模式">
            <el-select v-model="form.source.sslmode">
              <el-option label="disable" value="disable" />
              <el-option label="require" value="require" />
              <el-option label="verify-ca" value="verify-ca" />
              <el-option label="verify-full" value="verify-full" />
            </el-select>
          </el-form-item>
          <el-form-item>
            <el-button type="primary" :loading="testingSource" @click="testConnection('source')">
              测试 PostgreSQL 连接
            </el-button>
            <el-tag v-if="sourceTestResult" :type="sourceTestResult.ok ? 'success' : 'danger'" style="margin-left: 12px;">
              {{ sourceTestResult.ok ? `连接成功 (${sourceTestResult.version?.substring(0, 50)})` : sourceTestResult.error }}
            </el-tag>
          </el-form-item>
        </div>

        <!-- Step 1: Target -->
        <div v-show="activeStep === 1">
          <el-form-item label="主机地址" prop="target.host">
            <el-input v-model="form.target.host" />
          </el-form-item>
          <el-form-item label="端口">
            <el-input-number v-model="form.target.port" :min="1" :max="65535" />
          </el-form-item>
          <el-form-item label="用户名">
            <el-input v-model="form.target.user" />
          </el-form-item>
          <el-form-item label="密码">
            <el-input v-model="form.target.password" type="password" show-password />
          </el-form-item>
          <el-form-item label="数据库名" prop="target.database">
            <el-input v-model="form.target.database" />
          </el-form-item>
          <el-form-item>
            <el-button type="primary" :loading="testingTarget" @click="testConnection('target')">
              测试 TiDB 连接
            </el-button>
            <el-tag v-if="targetTestResult" :type="targetTestResult.ok ? 'success' : 'danger'" style="margin-left: 12px;">
              {{ targetTestResult.ok ? `连接成功 (${targetTestResult.version?.substring(0, 50)})` : targetTestResult.error }}
            </el-tag>
          </el-form-item>
        </div>

        <!-- Step 2: Options -->
        <div v-show="activeStep === 2">
          <el-form-item label="并发数">
            <el-input-number v-model="form.opts.parallel" :min="1" :max="32" />
          </el-form-item>
          <el-form-item label="批次大小">
            <el-input-number v-model="form.opts.batch_size" :min="1000" :step="10000" />
          </el-form-item>
          <el-form-item label="指定表">
            <el-input v-model="form.opts.tables" placeholder="逗号分隔，留空迁移所有表" />
          </el-form-item>
          <el-form-item label="排除表">
            <el-input v-model="form.opts.exclude_tables" placeholder="逗号分隔" />
          </el-form-item>
          <el-form-item label="使用 Lightning">
            <el-switch v-model="form.opts.use_lightning" />
          </el-form-item>
          <el-divider>跳过阶段（高级）</el-divider>
          <el-form-item label="跳过预检">
            <el-switch v-model="form.opts.skip_precheck" />
          </el-form-item>
          <el-form-item label="跳过 Schema">
            <el-switch v-model="form.opts.skip_schema" />
          </el-form-item>
          <el-form-item label="跳过数据">
            <el-switch v-model="form.opts.skip_data" />
          </el-form-item>
          <el-form-item label="跳过验证">
            <el-switch v-model="form.opts.skip_validate" />
          </el-form-item>
        </div>

        <!-- Step 3: Confirm -->
        <div v-show="activeStep === 3">
          <el-descriptions title="迁移配置确认" :column="2" border>
            <el-descriptions-item label="任务名称">{{ form.name || '自动生成' }}</el-descriptions-item>
            <el-descriptions-item label="并发数">{{ form.opts.parallel }}</el-descriptions-item>
            <el-descriptions-item label="源数据库">{{ form.source.host }}:{{ form.source.port }}/{{ form.source.database }}</el-descriptions-item>
            <el-descriptions-item label="目标数据库">{{ form.target.host }}:{{ form.target.port }}/{{ form.target.database }}</el-descriptions-item>
            <el-descriptions-item label="使用 Lightning">{{ form.opts.use_lightning ? '是' : '否' }}</el-descriptions-item>
            <el-descriptions-item label="批次大小">{{ form.opts.batch_size }}</el-descriptions-item>
          </el-descriptions>
          <el-alert
            title="点击「开始迁移」将创建任务并立即开始执行迁移"
            type="warning"
            :closable="false"
            style="margin-top: 16px;"
          />
        </div>

        <el-form-item style="margin-top: 24px;">
          <el-button v-if="activeStep > 0" @click="prevStep">上一步</el-button>
          <el-button v-if="activeStep < 3" type="primary" @click="nextStep">下一步</el-button>
          <el-button v-if="activeStep === 3" type="success" :loading="loading" @click="submit">
            开始迁移
          </el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <!-- Save Connection Dialog -->
    <el-dialog v-model="saveConnDialogVisible" title="保存连接配置" width="400px">
      <el-input v-model="saveConnName" placeholder="输入配置名称（如：生产环境）" />
      <template #footer>
        <el-button @click="saveConnDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="saveCurrentConnection">保存</el-button>
      </template>
    </el-dialog>

    <!-- Load Connection Dialog -->
    <el-dialog v-model="loadConnDialogVisible" title="加载连接配置" width="500px">
      <div v-if="savedConnections.length === 0" style="color: #999; text-align: center; padding: 20px;">
        暂无保存的连接配置
      </div>
      <div v-for="(conn, idx) in savedConnections" :key="idx" style="border: 1px solid #ebeef5; border-radius: 8px; padding: 12px; margin-bottom: 10px;">
        <div style="display: flex; justify-content: space-between; align-items: center;">
          <div>
            <strong>{{ conn.name }}</strong>
            <div style="color: #909399; font-size: 12px; margin-top: 4px;">
              PG: {{ conn.source.host }}:{{ conn.source.port }}/{{ conn.source.database }}
              → TiDB: {{ conn.target.host }}:{{ conn.target.port }}/{{ conn.target.database }}
            </div>
          </div>
          <el-space>
            <el-button size="small" type="primary" @click="loadConnection(conn)">加载</el-button>
            <el-button size="small" type="danger" plain @click="deleteConnection(idx)">删除</el-button>
          </el-space>
        </div>
      </div>
    </el-dialog>
  </div>
</template>
