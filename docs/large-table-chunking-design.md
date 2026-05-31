# PG2TiDB 大表分片导出设计文档

## 1. 背景

当前 pg2tidb 的数据导出逻辑是 `SELECT * FROM table` 逐行写入单个 CSV 文件。对于大表（百万行以上），存在以下问题：

- **单文件过大**：510 万行表导出 CSV 约 787MB
- **导出超时风险**：之前测试 `large_table` 因导出超时只完成了 595K/5.1M 行
- **无法断点续传**：导出中断后需要重新开始整张表
- **单线程瓶颈**：单表内无法并行，导出速度受限于单连接吞吐

## 2. 设计目标

| 目标 | 说明 |
|------|------|
| **分片导出** | 大表按主键范围切分为多个 CSV 分片文件 |
| **并行导出** | 单表内多个分片可并行导出，充分利用带宽 |
| **断点续传** | 单个分片失败不影响已完成分片，可从断点恢复 |
| **Lightning 兼容** | 多分片 CSV 文件可直接被 TiDB Lightning 导入 |
| **小表无感** | 低于阈值的表走原有单文件路径，零改动 |

## 3. 整体架构

```
                        ┌─────────────────────┐
                        │   判断表是否为大表    │
                        │ (行数 > threshold)   │
                        └──────┬──────────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
              行数 ≤ 阈值             行数 > 阈值
                    │                     │
          ┌─────────▼──────┐    ┌─────────▼──────────┐
          │  原有路径       │    │  分片导出路径        │
          │  SELECT *      │    │                      │
          │  → table.csv   │    │  1. 获取主键列名      │
          │                │    │  2. 计算分片边界      │
          │                │    │  3. 并行导出分片      │
          │                │    │  4. 合并/直接导入     │
          └────────────────┘    └──────────────────────┘
```

## 4. 详细设计

### 4.1 配置扩展

```yaml
migration:
  # ... 现有配置 ...
  
  # 大表分片配置（新增）
  large_table_threshold: 1000000    # 大表阈值（行数），超过此值启用分片导出
  chunk_size: 500000                # 每个分片的行数
  chunk_parallel: 2                 # 单表内并行导出分片数（建议不超过 4，避免对 PG 源端压力过大）
```

配置结构体扩展：

```go
type MigrationConfig struct {
    // ... 现有字段 ...
    LargeTableThreshold int64 `yaml:"large_table_threshold" json:"largeTableThreshold"` // 默认 1000000
    ChunkSize           int64 `yaml:"chunk_size" json:"chunkSize"`                      // 默认 500000
    ChunkParallel       int   `yaml:"chunk_parallel" json:"chunkParallel"`              // 默认 2
}
```

### 4.2 分片边界计算

#### 步骤 1：获取主键信息

```go
func (m *Migrator) getPrimaryKeyInfo(ctx context.Context, schema, table string) (pkColumn string, pkType string, err error) {
    // 查询 information_schema 获取主键列名和类型
    query := `
        SELECT kcu.column_name, c.data_type
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.columns c
            ON kcu.table_schema = c.table_schema
            AND kcu.table_name = c.table_name
            AND kcu.column_name = c.column_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = $1
            AND tc.table_name = $2
        ORDER BY kcu.ordinal_position
        LIMIT 1
    `
    // 如果无主键，使用 ctid 作为分片依据（回退方案）
}
```

#### 步骤 2：计算分片边界

```go
type ChunkBoundary struct {
    Index    int         // 分片序号（从 0 开始）
    MinValue interface{} // 主键最小值（含）
    MaxValue interface{} // 主键最大值（不含，最后一个分片为含）
    IsLast   bool        // 是否为最后一个分片
}

func (m *Migrator) calculateChunkBoundaries(
    ctx context.Context,
    schema, table, pkColumn string,
    totalRows, chunkSize int64,
) ([]ChunkBoundary, error) {
    // 计算分片数量
    numChunks := (totalRows + chunkSize - 1) / chunkSize
    
    // 使用窗口函数获取分片边界
    // SELECT min(pk) AS min_val, max(pk) AS max_val
    // FROM (
    //     SELECT pk, ntile($numChunks) OVER (ORDER BY pk) AS bucket
    //     FROM (SELECT pk FROM schema.table ORDER BY pk) sub
    // ) t
    // GROUP BY bucket ORDER BY bucket
    
    // 对于整数主键的简化方案：
    // 直接按范围均分
    // chunk[0]: pk >= minVal AND pk < minVal + range
    // chunk[1]: pk >= minVal + range AND pk < minVal + 2*range
    // ...
}
```

**整数主键的简化分片方案**（推荐，覆盖大多数场景）：

```sql
-- 获取主键范围
SELECT MIN(pk_col), MAX(pk_col) FROM schema.table;

-- 按范围分片导出
SELECT * FROM schema.table WHERE pk_col >= :min AND pk_col < :max;
```

**非整数主键 / 复合主键 / 无主键方案**：

```sql
-- 使用 CTID 分片（PostgreSQL 物理行标识）
SELECT * FROM schema.table
WHERE ctid >= '(chunk_start,0)' AND ctid < '(chunk_end,0)';

-- 或使用 OFFSET/LIMIT（简单但不够高效）
SELECT * FROM schema.table ORDER BY pk_col LIMIT :chunkSize OFFSET :offset;
```

### 4.3 分片导出流程

```go
func (m *Migrator) exportTableChunked(
    ctx context.Context,
    schema, table string,
    pkColumn string,
    boundaries []ChunkBoundary,
    opts common.DataOpts,
) (int64, int64, error) {
    var totalRows, totalBytes int64
    var mu sync.Mutex
    
    // 信号量控制并行度
    sem := make(chan struct{}, m.cfg.Migration.ChunkParallel)
    
    var wg sync.WaitGroup
    var firstErr error
    
    for i, boundary := range boundaries {
        // 检查断点：跳过已完成的分片
        if m.cpMgr.IsChunkCompleted(table, i) {
            mu.Lock()
            rows, bytes := m.cpMgr.GetChunkProgress(table, i)
            totalRows += rows
            totalBytes += bytes
            mu.Unlock()
            continue
        }
        
        wg.Add(1)
        sem <- struct{}{} // 获取信号量
        
        go func(idx int, b ChunkBoundary) {
            defer wg.Done()
            defer func() { <-sem }() // 释放信号量
            
            // 分片文件名: {table}.{chunk_index}.csv
            chunkFile := filepath.Join(opts.TempDir, fmt.Sprintf("%s.%d.csv", table, idx))
            
            rows, bytes, err := m.exportChunk(ctx, schema, table, pkColumn, b, chunkFile, opts)
            if err != nil {
                mu.Lock()
                if firstErr == nil {
                    firstErr = err
                }
                mu.Unlock()
                return
            }
            
            mu.Lock()
            totalRows += rows
            totalBytes += bytes
            mu.Unlock()
            
            // 更新分片级 checkpoint
            m.cpMgr.MarkChunkCompleted(table, idx, rows, bytes)
            
        }(i, boundary)
    }
    
    wg.Wait()
    return totalRows, totalBytes, firstErr
}
```

### 4.4 单分片导出

```go
func (m *Migrator) exportChunk(
    ctx context.Context,
    schema, table, pkColumn string,
    boundary ChunkBoundary,
    chunkFile string,
    opts common.DataOpts,
) (int64, int64, error) {
    f, err := os.Create(chunkFile)
    if err != nil {
        return 0, 0, err
    }
    defer f.Close()
    
    // 构建带范围条件的查询
    var query string
    if boundary.IsLast {
        query = fmt.Sprintf(
            "SELECT %s FROM %s.%s WHERE %s >= $1 ORDER BY %s",
            buildSelectCols(m, schema, table), schema, table, pkColumn, pkColumn,
        )
    } else {
        query = fmt.Sprintf(
            "SELECT %s FROM %s.%s WHERE %s >= $1 AND %s < $2 ORDER BY %s",
            buildSelectCols(m, schema, table), schema, table, pkColumn, pkColumn, pkColumn,
        )
    }
    
    // 执行查询并逐行写入 CSV
    var rows *pgx.Rows
    if boundary.IsLast {
        rows, err = m.pgPool.Query(ctx, query, boundary.MinValue)
    } else {
        rows, err = m.pgPool.Query(ctx, query, boundary.MinValue, boundary.MaxValue)
    }
    // ... 逐行扫描 + convertValue + 写入 CSV（复用现有逻辑）...
}
```

### 4.5 Checkpoint 扩展

现有的 checkpoint SQLite 表需要增加分片级别跟踪：

```sql
-- 现有表（保持不变）
CREATE TABLE IF NOT EXISTS checkpoint (
    table_name  TEXT PRIMARY KEY,
    schema_name TEXT NOT NULL,
    status      TEXT NOT NULL,
    row_count   BIGINT DEFAULT 0,
    export_path TEXT,
    error_msg   TEXT,
    started_at  DATETIME,
    finished_at DATETIME
);

-- 新增：分片级 checkpoint
CREATE TABLE IF NOT EXISTS chunk_checkpoint (
    table_name  TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',  -- pending/completed/failed
    row_count   BIGINT DEFAULT 0,
    byte_count  BIGINT DEFAULT 0,
    started_at  DATETIME,
    finished_at DATETIME,
    PRIMARY KEY (table_name, chunk_index)
);
```

新增 Checkpoint 方法：

```go
// 分片级别操作
func (m *Manager) IsChunkCompleted(table string, chunkIndex int) bool
func (m *Manager) GetChunkProgress(table string, chunkIndex int) (rows, bytes int64)
func (m *Manager) MarkChunkCompleted(table string, chunkIndex int, rows, bytes int64)
func (m *Manager) MarkChunkFailed(table string, chunkIndex int, errMsg string)
func (m *Manager) GetPendingChunks(table string) []int    // 返回未完成的分片索引
func (m *Manager) ResetChunks(table string)               // 重置所有分片状态
```

### 4.6 Lightning 多文件导入

TiDB Lightning 的 file router 支持同表多文件导入。分片文件命名需满足规则：

```
{database}.{table}.csv          ← 原有单文件
{database}.{table}.001.csv      ← 分片文件 1
{database}.{table}.002.csv      ← 分片文件 2
{database}.{table}.003.csv      ← 分片文件 3
```

Lightning TOML 配置中的 `[mydumper]` 部分会自动识别同一表名前缀的多个 CSV 文件。

**无需额外配置**——Lightning 的 default file router 会将 `{db}.{table}.*.csv` 都路由到同一张目标表。

### 4.7 导入流程调整

```
exportTableChunked()
    │
    ├── table.0.csv  ──┐
    ├── table.1.csv  ──┤── 重命名为 {db}.{table}.001.csv 等
    ├── table.2.csv  ──┤── 交给 Lightning 统一导入
    └── table.3.csv  ──┘
                          │
                          ▼
                  importViaLightning()
                  (Lightning 自动合并同表多文件)
                          │
                  失败？→ importViaSQL()
                  (流式 INSERT 逐分片导入)
```

流式 INSERT 回退路径也需支持分片：

```go
func (m *Migrator) importViaSQL(ctx context.Context, opts common.DataOpts) error {
    // 扫描所有 CSV 文件（含分片文件）
    entries, _ := os.ReadDir(opts.TempDir)
    
    // 按表名分组
    tableFiles := groupFilesByTable(entries)
    
    for table, files := range tableFiles {
        for _, file := range sortFilesByIndex(files) {
            err := m.streamCSVFile(ctx, table, file, opts)
            if err != nil { return err }
        }
    }
}
```

### 4.8 Web UI 进度展示

分片导出时，进度粒度细化到分片级别：

```json
// GET /api/v1/tasks/{id}/progress
{
    "table": "large_table",
    "state": "running",
    "rows_done": 2500000,
    "rows_total": 5100000,
    "progress": 0.49,
    "chunks": {
        "total": 6,
        "completed": 3,
        "running": 2,
        "pending": 1,
        "details": [
            { "index": 0, "state": "completed", "rows": 850000 },
            { "index": 1, "state": "completed", "rows": 850000 },
            { "index": 2, "state": "completed", "rows": 800000 },
            { "index": 3, "state": "running",   "rows": 450000 },
            { "index": 4, "state": "running",   "rows": 200000 },
            { "index": 5, "state": "pending",   "rows": 0 }
        ]
    }
}
```

前端可展示为：

```
large_table [2.5M/5.1M 49%]
  Chunk 0 ████████████ 850K ✅
  Chunk 1 ████████████ 850K ✅
  Chunk 2 ██████████   800K ✅
  Chunk 3 █████░░░░░░  450K 🔄
  Chunk 4 ██░░░░░░░░░  200K 🔄
  Chunk 6 ░░░░░░░░░░░    0K ⏳
```

## 5. 断点续传设计

### 5.1 恢复流程

```
1. 检查 chunk_checkpoint 表
2. 获取该表所有分片状态
3. 已 completed 的分片 → 跳过
4. 已 failed 的分片 → 重新导出
5. pending 的分片 → 正常导出
6. 全部完成后 → 进入导入阶段
```

### 5.2 Checkpoint 清理

- 新任务启动时，清空 `chunk_checkpoint` 中该表的所有记录
- 导入完成后，可选择保留或清理分片 CSV 文件（受 `cleanup_csv` 配置控制）

## 6. 配置默认值

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `large_table_threshold` | 1,000,000 | 超过此行数启用分片导出 |
| `chunk_size` | 500,000 | 每分片行数 |
| `chunk_parallel` | 2 | 单表内并行导出分片数 |

### 推荐配置

| 场景 | threshold | chunk_size | chunk_parallel |
|------|-----------|------------|----------------|
| PG 源端性能良好 | 500,000 | 500,000 | 4 |
| PG 源端性能一般 | 1,000,000 | 500,000 | 2 |
| PG 源端压力大（生产库） | 2,000,000 | 1,000,000 | 1 |
| 网络/磁盘较慢 | 1,000,000 | 200,000 | 2 |

## 7. 无主键表处理

对于无主键的表，使用以下回退方案：

### 方案 A：CTID 分片（推荐）

```sql
-- PostgreSQL 物理行标识，始终可用
-- 获取 CTID 范围
SELECT MIN(ctid), MAX(ctid) FROM schema.table;

-- 按物理块分片
SELECT * FROM schema.table
WHERE ctid >= '(0,0)' AND ctid < '(chunk_end,0)';
```

### 方案 B：OFFSET/LIMIT 分片

```sql
SELECT * FROM schema.table
ORDER BY ctid  -- 使用物理排序
LIMIT :chunkSize OFFSET :offset;
```

### 方案 C：降级为单文件导出

如果分片方案不可行（如复杂类型），回退到原有单文件路径，记录告警日志。

## 8. 风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| 分片边界不均匀（数据倾斜） | 某些分片过大或过小 | 使用 `ntile()` 窗口函数精确均分 |
| 并行导出对 PG 源端压力大 | 影响源端业务 | `chunk_parallel` 默认为 2，可调整为 1 |
| 分片 CSV 文件占用磁盘 | 磁盘满 | 导入完成后立即清理；支持配置 `cleanup_csv` |
| Lightning 不识别分片文件 | 导入失败 | 遵循 `{db}.{table}.*.csv` 命名规则 |
| 复合主键分片 SQL 复杂 | SQL 构建困难 | 仅支持单列主键分片，复合主键降级为单文件 |

## 9. 实现任务拆分

| # | 任务 | 优先级 | 预估 |
|---|------|--------|------|
| 1 | 配置结构体扩展 + 默认值 | P0 | 0.5h |
| 2 | `getPrimaryKeyInfo()` 主键信息获取 | P0 | 1h |
| 3 | `calculateChunkBoundaries()` 分片边界计算 | P0 | 1.5h |
| 4 | `exportChunk()` 单分片导出 | P0 | 1h |
| 5 | `exportTableChunked()` 并行分片调度 | P0 | 1.5h |
| 6 | Checkpoint 扩展（chunk_checkpoint 表 + 方法） | P0 | 1h |
| 7 | Lightning 多文件导入适配（文件重命名） | P0 | 0.5h |
| 8 | 流式 INSERT 回退路径适配 | P1 | 1h |
| 9 | 断点续传恢复逻辑 | P1 | 1h |
| 10 | Web UI 分片进度展示 | P2 | 1h |
| 11 | 单元测试 | P0 | 1.5h |

**总预估：约 11 小时**

## 10. 兼容性说明

- **小表无感**：行数低于 `large_table_threshold` 的表走原有单文件路径，零改动
- **配置向后兼容**：新增配置项有默认值，现有 `config.yaml` 无需修改
- **Lightning 兼容**：分片文件命名遵循 Lightning file router 规则
- **CLI 行为不变**：`pg2tidb data` 命令行为不变，只是内部大表自动走分片路径
