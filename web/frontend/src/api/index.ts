import axios from 'axios'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 30000,
})

export interface Task {
  id: string
  name: string
  status: 'created' | 'running' | 'paused' | 'completed' | 'failed' | 'cancelled'
  config_json: string
  phase: string
  progress: number
  tables_total: number
  tables_done: number
  rows_total: number
  rows_done: number
  error: string
  result_json: string
  created_at: string
  started_at: string | null
  finished_at: string | null
  updated_at: string
}

export interface PhaseTableInfo {
  name: string
  state: string
  rows_done: number
  rows_total: number
}

export interface PhaseInfo {
  name: string
  label: string
  status: string
  sub_label?: string
  tables: PhaseTableInfo[]
  table_count: number
  tables_done: number
  rows_total: number
  rows_done: number
  logs?: { level: string; message: string; timestamp: string }[]
}

export interface TaskPhasesResponse {
  task_id: string
  phase: string
  phases: PhaseInfo[]
}

export interface TaskLogEntry {
  timestamp: string
  level: string
  message: string
  caller?: string
}

export interface TaskLogsResponse {
  task_id: string
  logs: TaskLogEntry[]
  count: number
}

export interface ConnectionTestRequest {
  type: 'source' | 'target'
  host: string
  port: number
  user: string
  password: string
  database: string
  schema?: string
  sslmode?: string
}

export interface ConnectionTestResult {
  ok: boolean
  type: string
  host: string
  port: number
  database: string
  version?: string
  error?: string
  elapsed: string
}

export interface CreateTaskRequest {
  name: string
  source: {
    host: string
    port: number
    user: string
    password: string
    database: string
    schema: string
    sslmode: string
  }
  target: {
    host: string
    port: number
    user: string
    password: string
    database: string
    pd_addr: string
  }
  opts: {
    parallel: number
    batch_size: number
    tables: string[]
    exclude_tables: string[]
    use_lightning: boolean
    skip_precheck: boolean
    skip_schema: boolean
    skip_data: boolean
    skip_validate: boolean
    target_policy: string
  }
}

export const apiClient = {
  health: () => api.get('/health'),

  testConnection: (req: ConnectionTestRequest) =>
    api.post<ConnectionTestResult>('/config/test-connection', req),

  listTables: (req: ConnectionTestRequest) =>
    api.post<{ tables: { name: string; row_estimate: number }[]; count: number }>('/config/list-tables', { ...req, type: 'source' }),

  createTask: (req: CreateTaskRequest) =>
    api.post<Task>('/tasks', req),

  listTasks: () =>
    api.get<Task[]>('/tasks'),

  getTask: (id: string) =>
    api.get<Task>(`/tasks/${id}`),

  startTask: (id: string) =>
    api.post(`/tasks/${id}/start`),

  pauseTask: (id: string) =>
    api.post(`/tasks/${id}/pause`),

  resumeTask: (id: string) =>
    api.post(`/tasks/${id}/resume`),

  cancelTask: (id: string) =>
    api.post(`/tasks/${id}/cancel`),

  deleteTask: (id: string) =>
    api.delete(`/tasks/${id}`),

  getTaskProgress: (id: string) =>
    api.get<Task>(`/tasks/${id}/progress`),

  getTaskReport: (id: string, format?: string) =>
    api.get(`/tasks/${id}/report`, { params: { format } }),

  getTaskLogs: (id: string) =>
    api.get<TaskLogsResponse>(`/tasks/${id}/logs`),

  getTaskPhases: (id: string) =>
    api.get<TaskPhasesResponse>(`/tasks/${id}/phases`),
}

export default apiClient
