# pg2tidb

一键式 PostgreSQL → TiDB 全量数据迁移工具，覆盖 Schema 迁移、全量数据迁移、数据校验三大能力。

## 特性

- **Schema Migration** — 自动采集 PG schema，类型映射 + DDL AST 转换，生成 TiDB 兼容 DDL
- **Data Migration** — 基于 PG COPY + TiDB Lightning 实现高性能并行数据迁移
- **Data Validation** — 三层校验策略（行数/抽样/Checksum）确保数据一致性
- **Web Monitor** — 可选轻量监控面板，实时查看迁移进度和校验结果
- **Web UI** — 完整 Web 管理界面（`pg2tidb web`），可视化配置、实时监控、迁移历史
- **断点续传** — Checkpoint 机制支持中断后恢复
- **兼容性评估** — 迁移前扫描不兼容对象，输出风险报告

## 安装

### 从源码构建

```bash
make build
# 构建产物: ./build/pg2tidb
```

### Docker

```bash
docker build -t pg2tidb .
docker run pg2tidb --help
```

## 快速开始

### 1. 准备配置文件

```bash
cp configs/config.yaml config.yaml
```

编辑 `config.yaml`，填入 PG 和 TiDB 连接信息：

```yaml
source:
  host: "pg-host"
  port: 5432
  user: "postgres"
  password: ""
  database: "mydb"
  schema: "public"
  sslmode: "disable"

target:
  host: "tidb-host"
  port: 4000
  user: "root"
  password: ""
  database: "mydb"
```

### 2. 一键迁移

```bash
./build/pg2tidb all --config config.yaml
```

执行流程：Pre-check → Schema Migration → Data Migration → Data Validation → Summary Report

### 3. 分步执行

```bash
# 预检（不执行迁移）
./build/pg2tidb precheck --config config.yaml

# 仅迁移 Schema
./build/pg2tidb schema --config config.yaml

# 仅迁移数据
./build/pg2tidb data --config config.yaml

# 仅数据校验
./build/pg2tidb validate --config config.yaml
```

## 命令参考

```
pg2tidb [command]

Commands:
  all                一键执行完整迁移流程
  precheck           预检与兼容性评估
  schema             Schema 迁移
  data               全量数据迁移
  validate           数据校验
  web                启动 Web 管理界面

Flags:
  -c, --config string   配置文件路径 (默认 "configs/config.yaml")
      --log-level string 日志级别: debug, info, warn, error (默认 "info")
      --log-format string 日志格式: console, json (默认 "console")
```

## 配置说明

### 完整配置示例

```yaml
# PostgreSQL 源端配置
source:
  host: "localhost"
  port: 5432
  user: "postgres"
  password: ""               # 支持环境变量注入
  database: "mydb"
  schema: "public"
  sslmode: "disable"         # disable | require | verify-ca | verify-full

# TiDB 目标端配置
target:
  host: "localhost"
  port: 4000
  user: "root"
  password: ""
  database: "mydb"

# 迁移配置
migration:
  parallel: 4                # 并行导出 worker 数
  batch_size: 100000         # 批量大小（行数）
  temp_dir: "/tmp/pg2tidb"   # 临时文件目录
  tables: []                 # 空=全部表，支持正则匹配
  exclude_tables: []         # 排除表，支持正则匹配
  use_lightning: true        # 使用 TiDB Lightning 加速导入
  on_error: "abort"          # 单表失败策略: skip | abort
  checkpoint_dir: ".checkpoint"  # Checkpoint 目录
  read_timeout: "30m"        # 读超时
  write_timeout: "30m"       # 写超时

# 日志配置
logging:
  level: "info"              # debug | info | warn | error
  format: "console"          # console | json
  output: ""                 # 空=stderr，或指定文件路径

# Web 监控面板（可选）
web:
  enable: false              # 默认不启用
  port: 8080                 # 监听端口
  host: "0.0.0.0"            # 监听地址
```

### 关键配置项说明

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `migration.parallel` | 并行导出 worker 数，建议设为 CPU 核数 | 4 |
| `migration.batch_size` | 每批次迁移行数 | 100000 |
| `migration.use_lightning` | 使用 Lightning Local Backend 加速导入 | true |
| `migration.on_error` | 单表失败时 skip 继续或 abort 中止 | abort |
| `logging.level` | 日志级别 | info |
| `web.enable` | 启用 Web 监控面板 | false |

## 类型映射参考

| PostgreSQL | TiDB/MySQL | 备注 |
|---|---|---|
| serial / bigserial | BIGINT AUTO_INCREMENT | 自增主键 |
| varchar(n) | VARCHAR(n) | |
| text | TEXT | |
| jsonb | JSON | |
| boolean | TINYINT(1) | t/f → 1/0 |
| timestamp with tz | TIMESTAMP(3) | 保留毫秒精度 |
| bytea | BLOB | |
| numeric(p,s) | DECIMAL(p,s) | |
| uuid | CHAR(36) / VARCHAR(36) | 转为字符串 |
| array | JSON | 序列化为 JSON 数组 |
| gin/gist 索引 | 跳过 | 记录告警日志 |

## Web 管理界面

### 启动 Web UI

```bash
# 本地启动
./pg2tidb web --port 8080

# Docker 启动（默认启动 Web 模式）
docker run -p 8080:8080 pg2tidb

# 自定义数据目录
./pg2tidb web --data /data/pg2tidb --port 8080
```

浏览器访问 `http://localhost:8080`。

### Web UI 功能

| 页面 | 功能 |
|------|------|
| **配置向导** | 可视化配置 PG/TiDB 连接信息，一键测试连接 |
| **任务监控** | 实时查看运行中任务的进度、吞吐量、表级详情 |
| **迁移历史** | 查看历史迁移任务列表和详情 |
| **报告下载** | 下载 JSON 格式迁移报告 |

### Web UI API 端点

```
GET  /api/v1/health                      # 健康检查
POST /api/v1/config/test-connection      # 测试数据库连接
POST /api/v1/tasks                       # 创建迁移任务
GET  /api/v1/tasks                       # 列出所有任务
GET  /api/v1/tasks/{id}                  # 获取任务详情
POST /api/v1/tasks/{id}/start            # 启动任务
POST /api/v1/tasks/{id}/pause            # 暂停任务
POST /api/v1/tasks/{id}/resume           # 恢复任务
POST /api/v1/tasks/{id}/cancel           # 取消任务
DELETE /api/v1/tasks/{id}                # 删除任务
GET  /api/v1/tasks/{id}/progress         # 获取实时进度
GET  /api/v1/tasks/{id}/report           # 获取迁移报告
GET  /api/v1/ws                          # WebSocket 实时推送
```

### CLI 监控模式（兼容旧版）

在配置文件中启用 `web.enable: true` 后，CLI 命令执行迁移时也会启动监控面板：

启用后在浏览器访问 `http://<host>:8080` 可查看：

- 整体迁移状态和进度
- 各表导出/导入行数和速率
- 数据校验结果
- 最终迁移报告

API 端点（只读）：

```
GET /api/v1/status       # 全局迁移状态
GET /api/v1/tables       # 各表进度
GET /api/v1/validation   # 校验结果
GET /api/v1/report       # 最终报告
```

## 断点续传

迁移过程中断后，重新执行相同命令即可从上次断点继续。Checkpoint 信息保存在 `migration.checkpoint_dir` 目录中。

## 项目结构

```
pg2tidbtool/
├── cmd/                    # CLI 命令入口
│   ├── root.go             # 根命令
│   ├── all.go              # all 命令
│   ├── schema.go           # schema 命令
│   ├── data.go             # data 命令
│   ├── validate.go         # validate 命令
│   └── precheck.go         # precheck 命令
├── internal/
│   ├── schema/             # Schema 迁移模块
│   ├── data/               # 数据迁移模块
│   ├── validator/          # 数据校验模块
│   ├── precheck/           # 预检与兼容性评估
│   ├── orchestrator/       # 任务编排器
│   ├── api/                # Web 监控 API
│   └── common/             # 公共模块
├── configs/                # 配置文件模板
├── main.go                 # 程序入口
├── Makefile                # 构建脚本
└── Dockerfile              # Docker 构建
```

## 测试验证

### 测试环境

| 项目 | 配置 |
|------|------|
| 构建平台 | Windows/amd64, Go 1.26.3 |
| PostgreSQL | 16.13, pepezzzz.synology.me:23654 |
| TiDB | v7.1.9-0.0, pepezzzz.synology.me:23140 |
| 测试数据库 | mydb (20 张表 + 1 视图) |

### Pre-check 结果

```
5 PASS, 3 WARN, 0 FAIL
- pg-connection: PASS (PostgreSQL 16.13)
- tidb-connection: PASS (TiDB v7.1.9)
- disk-space: PASS
- pg-schema-permission: PASS
- collation: PASS (UTF)
- WARN: 1 trigger (trg_update_ts)
- WARN: 1 stored function (update_timestamp)
- WARN: 1 enum type (mood)
```

### Schema 迁移结果

```
20 tables + 1 view 迁移成功
- 19 张表含主键，1 张无主键表 (no_pk_table)
- 3 个索引 (unique, normal, composite)
- 1 个外键 (fk_child → fk_parent)
- 1 个视图 (v_summary)
- 2 个不支持对象: trigger trg_update_ts, function update_timestamp
```

### Data 迁移结果

| 表名 | PG 行数 | TiDB 行数 | 状态 |
|------|---------|----------|------|
| array_types | 4 | 4 | PASS |
| basic_types | 3 | 3 | PASS |
| composite_pk | 3 | 3 | PASS |
| constraint_test | 3 | 3 | PASS |
| custom_type_test | 0 | 0 | PASS |
| empty_table | 0 | 0 | PASS |
| enum_test | 3 | 3 | PASS |
| fk_child | 3 | 3 | PASS |
| fk_parent | 2 | 2 | PASS |
| index_test | 4 | 4 | PASS |
| large_json | 4 | 4 | PASS |
| large_table | 5,100,000 | 595,580 | FAIL (导出中断) |
| no_pk_table | 4 | 4 | PASS |
| null_test | 3 | 3 | PASS |
| order | 3 | 3 | PASS |
| seq_test | 3 | 3 | PASS |
| single_pk | 3 | 3 | PASS |
| single_row | 1 | 1 | PASS |
| special_chars | 3 | 3 | PASS |
| trigger_test | 1 | 1 | PASS |

L1 行数校验: **19/20 PASS**, 1 FAIL (large_table 导出超时导致不完整)

### 发现的 Bug 及修复

1. **Windows 编译失败** — `precheck/checker.go` 使用 `syscall.Statfs_t` (Linux only)，已添加平台特定文件 `disk_windows.go` / `disk_posix.go`
2. **TestTargetDSN 断言错误** — `config_test.go` expected 值缺少 user 前缀
3. **ENUM 注释分号问题** — `ddl.go` BuildEnumDDL 中注释含分号导致 SQL 分割错误，执行 DDL 时触发语法错误
4. **L2 采样校验误报** — `validator.go` 使用 `fmt.Sprintf("%v")` 比较 PG/TiDB 数据，类型差异导致误判 (如 boolean true vs 1)

### 已知限制

- Trigger 和 Stored Function 不会自动迁移，仅在 precheck 阶段告警
- ENUM 类型转为 TEXT，未使用 MySQL ENUM 语法
- L2 校验的值比较需要更精确的类型感知比较逻辑
- 大表 (5M+ rows) 导出时 progress bar 未正确更新

## 开发

```bash
# 格式化
make fmt

# 静态检查
make vet

# 运行测试
make test

# 测试覆盖率
make test-cover

# 构建
make build
```

## 常见问题

**Q: 支持哪些 PG 版本？**
A: PostgreSQL 10+ 版本。

**Q: 支持增量同步吗？**
A: 当前版本仅支持全量迁移，增量能力已预留接口，后续版本支持。

**Q: 大表迁移性能如何？**
A: PG 导出通常 50-200 MB/s，TiDB Lightning 导入 200-500 MB/s，整体瓶颈通常在 PG 导出端。

**Q: 存储过程、触发器会迁移吗？**
A: 不会自动迁移。Pre-check 阶段会扫描并输出兼容性报告，标注不兼容对象和迁移建议。

**Q: 迁移过程中单张表失败怎么办？**
A: 配置 `on_error: skip` 跳过失败表继续迁移，最终报告汇总所有失败表。默认 `abort` 遇错即停。

## 性能调优建议

1. **提高导出并行度** — `migration.parallel` 设为 CPU 核数的 1-2 倍
2. **使用 Lightning** — `migration.use_lightning: true` 是最快导入模式
3. **确保磁盘 IO** — 临时目录使用高速磁盘（SSD/NVMe）
4. **TiDB 集群规模** — Lightning 导入速度与 TiKV 节点数正相关
5. **网络带宽** — PG 到中间机器、中间机器到 TiDB 的网络带宽是关键瓶颈

## License

MIT
