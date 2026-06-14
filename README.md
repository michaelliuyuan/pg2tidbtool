# pg2tidb

一键式 PostgreSQL → TiDB 全量数据迁移工具，覆盖兼容性评估、Schema 迁移、全量数据迁移、数据校验四大能力。支持 CLI 和 Web 管理界面两种使用方式。

## 特性

- **兼容性评估** — 迁移前扫描不兼容对象（Trigger、Stored Function、特殊索引等），输出风险报告
- **Schema Migration** — 自动采集 PG schema，类型映射 + DDL AST 转换，生成 TiDB 兼容 DDL，支持 `--dry-run` 预览
- **Data Migration** — 基于 PG COPY 协议导出 + TiDB Lightning CLI 导入，实现高性能并行数据迁移
- **Data Validation** — 三种校验模式（quick 行数对比 / sample 抽样验证 / checksum 分块哈希）确保数据一致性
- **Web 管理界面** — 可视化配置向导、交互式表选择、实时进度监控、日志流查看、迁移历史、HTML 报告下载
- **断点续传** — Checkpoint 机制支持中断后从断点恢复
- **单二进制部署** — 前端通过 `go:embed` 嵌入，零外部依赖，Docker 一键启动

## 系统要求

| 组件 | 要求 |
|------|------|
| Go | 1.22+ |
| PostgreSQL | 10+ |
| TiDB | 7.1+ |
| TiDB Lightning | 使用 Lightning 导入时需要安装 `tidb-lightning` 二进制 |
| Node.js | 20+（仅前端开发构建时需要） |

## 安装

> 📌 **如果你是第一次使用，推荐从「方式一：下载二进制」开始，最简单。**

### 方式一：下载预编译二进制（推荐，最简单）

从 [GitHub Releases](https://github.com/michaelliuyuan/pg2tidbtool/releases) 下载对应平台的二进制文件。

**Linux（最常见的服务器环境）：**

```bash
# 1. 下载（以 Linux amd64 为例）
wget https://github.com/michaelliuyuan/pg2tidbtool/releases/latest/download/pg2tidb-linux-amd64 -O pg2tidb

# 2. 添加执行权限
chmod +x pg2tidb

# 3. 移到系统路径（可选）
sudo mv pg2tidb /usr/local/bin/

# 4. 验证安装
./pg2tidb version
```

**Windows：**

```powershell
# 下载 pg2tidb-windows-amd64.exe，重命名
Rename-Item pg2tidb-windows-amd64.exe pg2tidb.exe

# 验证安装
.\pg2tidb.exe version
```

### 方式二：从源码构建（适合开发者）

**前置条件：** 安装 [Go 1.22+](https://go.dev/dl/)，如果需要 Web UI 还需要 [Node.js 20+](https://nodejs.org/)

```bash
# 克隆仓库
git clone https://github.com/michaelliuyuan/pg2tidbtool.git
cd pg2tidbtool

# 仅 CLI 版本（不含 Web UI）
make build

# 含 Web UI 版本（需要 Node.js 20+）
make build-web

# 构建产物在 build/ 目录
ls build/pg2tidb
```

**交叉编译（在 Mac/Windows 上编译 Linux 版本）：**

```bash
# Linux amd64
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o pg2tidb .

# Linux arm64
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o pg2tidb .
```

### 方式三：Docker（适合容器化部署）

```bash
# 构建镜像（自动包含前端 + 内置 tidb-lightning）
docker build -t pg2tidb .

# 启动 Web 服务
docker run -d -p 8080:8080 --name pg2tidb pg2tidb

# 浏览器访问 http://localhost:8080
```

---

## 快速开始

### Web 界面模式（推荐）

```bash
# 启动 Web 管理界面
./pg2tidb web

# 自定义端口和数据目录
./pg2tidb web --port 8080 --data /data/pg2tidb
```

浏览器访问 `http://localhost:8080`，通过向导完成：
1. **配置源端** — 填写 PostgreSQL 连接信息，点击「测试连接」
2. **配置目标端** — 填写 TiDB 连接信息（含 PD 地址），点击「测试连接」
3. **选择表** — 查看所有表及预估行数，勾选待迁移表（支持搜索）
4. **迁移选项** — 并发数、批量大小、Lightning 开关、冲突策略
5. **确认执行** — 实时监控迁移进度，查看日志流，下载 HTML 报告

### CLI 命令行模式

#### 1. 准备配置文件

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
  pd_addr: "tidb-host:2379"    # Lightning 导入时必填
```

#### 2. 一键迁移

```bash
./pg2tidb all --config config.yaml
```

执行流程：Pre-check → Schema Migration → Data Migration → Data Validation → Summary Report

#### 3. 分步执行

```bash
# 预检（不执行迁移）
./pg2tidb precheck --config config.yaml

# 仅迁移 Schema（支持 --dry-run 预览）
./pg2tidb schema --config config.yaml
./pg2tidb schema --config config.yaml --dry-run --output schema.sql

# 仅迁移数据
./pg2tidb data --config config.yaml

# 仅数据校验
./pg2tidb validate --config config.yaml
./pg2tidb validate --config config.yaml --level L2 --sample-ratio 0.05  # L2 抽样
./pg2tidb validate --config config.yaml --level L3                       # L3 全量 Checksum
```

---

## 命令参考

```
pg2tidb [command]

Commands:
  all                一键执行完整迁移流程（precheck → schema → data → validate）
  precheck           预检与兼容性评估
  schema             Schema 迁移（--dry-run 仅预览 DDL）
  data               全量数据迁移
  validate           数据校验（L1/L2/L3）
  web                启动 Web 管理界面
  version            显示版本信息

Global Flags:
  -c, --config string     配置文件路径 (默认 "configs/config.yaml")
      --log-level string  日志级别: debug, info, warn, error (默认 "info")
      --log-format string 日志格式: console, json (默认 "console")

Web Command Flags:
  -p, --port int     Web 服务端口 (默认 8080)
      --host string  Web 服务监听地址 (默认 "0.0.0.0")
      --data string  数据存储目录 (默认 ".pg2tidb")
```

---

## 配置说明

### 完整配置示例

```yaml
# PostgreSQL 源端配置
source:
  host: "localhost"
  port: 5432
  user: "postgres"
  password: ""
  database: "mydb"
  schema: "public"
  sslmode: "disable"           # disable | require | verify-ca | verify-full

# TiDB 目标端配置
target:
  host: "localhost"
  port: 4000
  user: "root"
  password: ""
  database: "mydb"
  pd_addr: "localhost:2379"    # PD 地址，Lightning 导入时必填

# 迁移配置
migration:
  parallel: 4                  # 同时迁移的表数量
  batch_size: 100000           # 批量大小（行数）
  temp_dir: "/tmp/pg2tidb"     # CSV 临时文件目录
  tables: []                   # 空=全部表，支持正则匹配
  exclude_tables: []           # 排除表，支持正则匹配
  use_lightning: true          # 使用 TiDB Lightning 加速导入
  on_error: "abort"            # 单表失败策略: skip | abort
  target_policy: "insert"     # 目标表冲突策略: insert | truncate | drop
  checkpoint_dir: ".checkpoint"  # Checkpoint 目录
  read_timeout: "30m"          # PG 读超时
  write_timeout: "30m"         # TiDB 写超时

# 日志配置
logging:
  level: "info"                # debug | info | warn | error
  format: "console"            # console | json
  output: ""                   # 空=stderr，或指定文件路径

# 数据校验配置
compare:
  compare_mode: "sample"       # quick(行数) | sample(抽样,默认) | checksum(分块哈希)
  sample_ratio: 0.01           # sample 模式抽样比例
  checksum_chunk_size: 50000   # checksum 模式每块行数
  checksum_parallel: 4         # checksum 模式并发数
  no_pk_strategy: "auto"       # 无PK表策略: auto | hash_group | bucket | aggregate

# Web 监控面板（CLI 模式可选）
web:
  enable: false                # CLI 模式下是否启动监控面板
  port: 8080                   # 监听端口
  host: "0.0.0.0"              # 监听地址
```

### 关键配置项说明

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `compare.compare_mode` | 校验模式: `quick`(行数) / `sample`(抽样,默认) / `checksum`(分块哈希) | sample |
| `compare.sample_ratio` | sample 模式抽样比例 (0.0-1.0) | 0.01 |
| `migration.parallel` | 并行导出 worker 数 | 4 |
| `migration.batch_size` | 每批次迁移行数 | 100000 |
| `migration.use_lightning` | 使用 TiDB Lightning CLI 物理导入（需安装 tidb-lightning） | true |
| `target.pd_addr` | PD 地址（Lightning local backend 必填） | host:2379 |
| `target.status_port` | TiDB Status 端口（Lightning 使用） | 10080 |
| `migration.target_policy` | 目标表冲突策略: `insert` / `truncate` / `drop` | insert |
| `migration.on_error` | 单表失败时 `skip` 继续或 `abort` 中止 | abort |
| `migration.temp_dir` | CSV 导出临时目录 | /tmp/pg2tidb |
| `logging.level` | 日志级别 | info |
| `web.enable` | CLI 模式下启用 Web 监控面板 | false |

#### target_policy 说明

| 策略 | 行为 |
|------|------|
| `insert` | 直接 INSERT，主键冲突时报错（默认） |
| `truncate` | 先 TRUNCATE 目标表再导入 |
| `drop` | 先 DROP TABLE 再重建并导入 |

---

## TiDB Lightning 集成

pg2tidb 使用 TiDB Lightning 作为数据导入引擎，提供两种后端模式：

| 模式 | 配置 | 速度 | 要求 |
|------|------|------|------|
| **Local Backend**（推荐） | `backend = "local"` | 100–500 GiB/h | 需要访问 PD 地址，目标表必须为空 |
| **TiDB Backend** | `backend = "tidb"` | 10–50 GiB/h | 通过 SQL 执行，目标表可非空 |

### Lightning 使用前提

pg2tidb 支持两种方式获取 tidb-lightning：

1. **内置模式（推荐）**：构建时将 tidb-lightning 二进制嵌入 pg2tidb，运行时自动释放到数据目录，无需手动安装
   ```bash
   # 构建 Web UI + 内置 Lightning
   LIGHTNING_BIN=/path/to/tidb-lightning bash build-web.sh

   # 或使用 Make
   make build-all LIGHTNING_BIN=/path/to/tidb-lightning
   ```

2. **系统 PATH**：如果 pg2tidb 未内置 tidb-lightning，运行时会从系统 PATH 查找。如果都找不到，自动回退到流式 INSERT 模式

> **优先级**：系统 PATH tidb-lightning > 内置 tidb-lightning > 流式 INSERT 回退

### Lightning 导入流程

```
PG COPY 导出 CSV → 生成 lightning.toml → 调用 tidb-lightning CLI → 清理 CSV
```

如果 Lightning 导入失败，工具会自动回退到流式 INSERT 方式完成导入。

### 数据迁移流程

```
PostgreSQL                    pg2tidb                     TiDB
─────────                    ───────                     ─────
    │                            │                           │
    │◄─── PG COPY TO STDOUT ────┤                           │
    │      (并行 N workers)      │                           │
    │──── CSV 数据 ────────────►│                           │
    │                            │  写入 temp_dir/*.csv      │
    │                            │                           │
    │                            │── tidb-lightning ────────►│
    │                            │  (local backend 物理导入)  │
    │                            │                           │
    │                            │◄── 导入完成 ──────────────┤
    │                            │                           │
    │                            │── (回退: 流式 INSERT) ────►│
    │                            │                           │
```

---

## 类型映射参考

| PostgreSQL | TiDB/MySQL | 备注 |
|---|---|---|
| serial / bigserial | BIGINT AUTO_INCREMENT | 自增主键 |
| integer / int | INT | |
| bigint | BIGINT | |
| real / float4 | FLOAT | |
| double precision / float8 | DOUBLE | |
| numeric(p,s) | DECIMAL(p,s) | |
| varchar(n) | VARCHAR(n) | |
| text | TEXT | |
| char(n) | CHAR(n) | |
| boolean | TINYINT(1) | t/f → 1/0 |
| date | DATE | |
| timestamp / timestamptz | DATETIME / TIMESTAMP(3) | 保留毫秒精度 |
| time / timetz | TIME | |
| interval | VARCHAR(64) | 转为字符串 |
| uuid | CHAR(36) | 转为字符串 |
| json / jsonb | JSON | |
| bytea | BLOB | |
| bit / varbit | BIT / VARCHAR | |
| enum | TEXT | 转为 TEXT（非 MySQL ENUM） |
| money | DECIMAL(19,2) | |
| array | JSON | 序列化为 JSON 数组 |
| composite type | JSON | 扁平化为 JSON |
| inet / cidr | VARCHAR(45) | |
| macaddr | VARCHAR(17) | |
| point / line / polygon | VARCHAR | 几何类型存为 WKT |

> **注意**：GIN、GiST 等不兼容索引类型不会自动迁移，仅在 precheck 阶段记录告警日志。

---

## Web 管理界面

### 启动

```bash
# 默认端口 8080
./pg2tidb web

# 自定义配置
./pg2tidb web --port 9090 --host 0.0.0.0 --data /data/pg2tidb

# Docker
docker run -p 8080:8080 -v /data/pg2tidb:/data pg2tidb
```

### 功能页面

| 页面 | 功能说明 |
|------|----------|
| **配置向导** | 5 步向导：源端配置 → 目标端配置 → 选择表（支持搜索/全选） → 迁移选项 → 确认执行 |
| **任务监控** | 实时进度、表级详情（导出/导入行数、百分比）、吞吐量指标 |
| **日志查看** | 实时日志流，按级别过滤（info/warn/error） |
| **迁移历史** | 历史任务列表、详情查看、重新执行 |
| **报告下载** | 生成 HTML 格式完整迁移报告，包含各阶段执行结果 |

### API 端点

#### 任务管理

```
POST /api/v1/config/test-connection      # 测试 PG/TiDB 连接
POST /api/v1/config/list-tables          # 列出源端表及预估行数
POST /api/v1/tasks                       # 创建迁移任务
GET  /api/v1/tasks                       # 列出所有任务（分页）
GET  /api/v1/tasks/{id}                  # 获取任务详情
POST /api/v1/tasks/{id}/start            # 启动任务
POST /api/v1/tasks/{id}/pause            # 暂停任务
POST /api/v1/tasks/{id}/resume           # 恢复任务
POST /api/v1/tasks/{id}/cancel           # 取消任务
DELETE /api/v1/tasks/{id}                # 删除任务
```

#### 进度与监控

```
GET  /api/v1/tasks/{id}/progress         # 获取实时进度
GET  /api/v1/tasks/{id}/phases           # 获取各阶段状态
GET  /api/v1/tasks/{id}/logs             # 获取任务日志
GET  /api/v1/tasks/{id}/report           # 获取迁移报告
GET  /api/v1/ws                          # WebSocket 实时推送（进度+日志）
```

#### 健康检查

```
GET  /api/v1/health                      # 健康检查
```

### CLI 监控模式（兼容）

在配置文件中启用 `web.enable: true` 后，CLI 命令执行迁移时也会启动监控面板。API 端点：

```
GET /api/v1/status       # 全局迁移状态
GET /api/v1/tables       # 各表进度
GET /api/v1/validation   # 校验结果
GET /api/v1/report       # 最终报告
```

---

## 断点续传

迁移过程中断后，重新执行相同命令即可从上次断点继续。Checkpoint 信息保存在 `migration.checkpoint_dir`（默认 `.checkpoint`）目录中。

Web 模式下，可在迁移历史页面点击「重新执行」，工具会自动检测并利用已有 checkpoint。

---

## 部署指南

> 📌 **首次部署建议：先看完「方式一：Web 模式单机部署」，5 分钟即可上手。**

### 方式一：Web 模式单机部署（推荐，最简单）

这是最简单的部署方式，只需要一个二进制文件和一台 Linux 服务器。

**第一步：准备配置文件**

创建 `config.yaml`：

```yaml
source:
  host: "你的PG地址"
  port: 5432
  user: "postgres"
  password: "你的密码"
  database: "要迁移的数据库"
  schema: "public"
  sslmode: "disable"

target:
  host: "你的TiDB地址"
  port: 4000
  user: "root"
  password: "你的密码"
  database: "目标数据库"
  pd_addr: "TiDB-PD地址:2379"

web:
  enable: true
  port: 8080
  host: "0.0.0.0"
```

**第二步：启动服务**

```bash
# 上传 pg2tidb 二进制和 config.yaml 到服务器
scp pg2tidb config.yaml user@your-server:/home/user/pg2tidb/

# SSH 登录服务器
ssh user@your-server

# 启动 Web 服务
cd /home/user/pg2tidb
chmod +x pg2tidb
nohup ./pg2tidb web -c config.yaml -p 8080 > pg2tidb.log 2>&1 &
```

**第三步：浏览器操作**

1. 打开浏览器访问 `http://服务器IP:8080`
2. 按照页面向导操作：配置源端 → 配置目标端 → 选择表 → 设置选项 → 开始迁移
3. 实时查看迁移进度和日志

**验证服务是否正常：**

```bash
# 检查进程是否在运行
ps aux | grep pg2tidb

# 检查 HTTP 服务是否响应
curl http://localhost:8080/api/v1/health
```

**停止服务：**

```bash
# 找到进程并停止
kill $(lsof -ti:8081)
```

### 方式二：CLI 命令行模式

适合自动化脚本或 CI/CD 环境使用。

```bash
# 一键完整迁移（precheck → schema → data → validate）
./pg2tidb all --config config.yaml

# 分步执行
./pg2tidb precheck --config config.yaml   # 第1步：预检
./pg2tidb schema --config config.yaml     # 第2步：Schema 迁移
./pg2tidb data --config config.yaml       # 第3步：数据迁移
./pg2tidb validate --config config.yaml   # 第4步：数据校验
```

### 方式三：Docker 部署

```bash
# 构建镜像
docker build -t pg2tidb .

# 运行（Web 模式，默认启动 8080 端口）
docker run -d \
  --name pg2tidb \
  -p 8080:8080 \
  -v /data/pg2tidb:/data \
  pg2tidb

# 运行（CLI 模式）
docker run --rm \
  -v $(pwd)/config.yaml:/etc/pg2tidb/config.yaml \
  pg2tidb all --config /etc/pg2tidb/config.yaml
```

### Docker Compose（含 PG + TiDB 测试环境）

```yaml
version: '3.8'
services:
  pg2tidb:
    build: .
    ports:
      - "8080:8080"
    volumes:
      - pg2tidb-data:/data
    depends_on:
      - postgres
      - tidb

  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: testdb
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"

  tidb:
    image: pingcap/tidb:v7.1.0
    ports:
      - "4000:4000"
      - "10080:10080"

volumes:
  pg2tidb-data:
```

---

## 项目结构

```
pg2tidbtool/
├── cmd/                        # CLI 命令
│   ├── root.go                 # 根命令 + 全局 flags
│   ├── all.go                  # all — 一键完整流程
│   ├── precheck.go             # precheck — 预检
│   ├── schema.go               # schema — Schema 迁移
│   ├── data.go                 # data — 数据迁移
│   ├── validate.go             # validate — 数据校验
│   ├── web.go                  # web — Web 管理界面
│   └── static/                 # 嵌入式前端资源（go:embed）
├── web/
│   └── frontend/               # Vue 3 前端源码
│       ├── src/
│       │   ├── views/          # 页面组件
│       │   ├── components/     # 通用组件
│       │   └── ...
│       ├── package.json
│       └── vite.config.ts
├── internal/
│   ├── schema/                 # Schema 迁移模块
│   │   ├── reader.go           # PG schema 读取
│   │   ├── converter.go        # DDL 转换
│   │   ├── type_map.go         # 类型映射
│   │   └── applier.go          # DDL 执行
│   ├── data/                   # 数据迁移模块
│   │   ├── exporter.go         # PG COPY 导出 CSV
│   │   ├── importer.go         # TiDB Lightning 导入 + 流式 INSERT 兜底
│   │   ├── scheduler.go        # 并发调度器
│   │   └── migrator.go         # 迁移编排
│   ├── validator/              # 数据校验模块
│   │   ├── validator.go        # L1/L2/L3 校验引擎
│   │   └── reporter.go         # 报告生成
│   ├── precheck/               # 兼容性评估
│   │   └── checker.go          # 预检检查器
│   ├── orchestrator/           # 任务编排器
│   │   └── orchestrator.go     # 4 阶段管道编排
│   ├── webapi/                 # Web API 服务
│   │   ├── server.go           # HTTP + WebSocket 服务（chi 路由）
│   │   ├── dbconn.go           # 数据库连接测试
│   │   ├── logbuffer.go        # 日志缓冲（支持实时订阅）
│   │   └── logcore.go          # Zap 日志采集 Core
│   ├── store/                  # 任务持久化
│   │   ├── store.go            # SQLite 存储（modernc.org/sqlite）
│   │   └── store_test.go
│   ├── api/                    # CLI 监控面板 API（旧版兼容）
│   │   └── server.go
│   └── common/                 # 公共模块
│       ├── config/             # 配置加载
│       ├── logger/             # 日志初始化
│       ├── checkpoint/         # Checkpoint 管理
│       ├── reporter/           # 报告类型定义
│       └── errors/             # 错误类型
├── configs/
│   └── config.yaml             # 配置文件模板
├── main.go                     # 程序入口
├── Makefile                    # 构建脚本
├── Dockerfile                  # 多阶段 Docker 构建
├── go.mod
└── go.sum
```

---

## 测试验证

### 测试环境

| 项目 | 配置 |
|------|------|
| 构建平台 | Windows/amd64, Go 1.26.3 |
| PostgreSQL | 16.13 |
| TiDB | v7.1.9 |
| 测试数据库 | mydb (20 张表 + 1 视图) |
| Lightning | tidb-lightning (local backend) |

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

### Data 迁移 + 校验结果

| 表名 | PG 行数 | TiDB 行数 | 校验模式 | 状态 |
|------|---------|----------|----------|------|
| array_types | 4 | 4 | sample (L2) | ✅ PASS |
| basic_types | 3 | 3 | sample (L2) | ✅ PASS |
| composite_pk | 3 | 3 | sample (L2) | ✅ PASS |
| constraint_test | 3 | 3 | sample (L2) | ✅ PASS |
| custom_type_test | 0 | 0 | sample (L2) | ✅ PASS |
| dec_only_nopk | 3 | 3 | hash group | ✅ PASS |
| duplicate_rows_table | 20 | 20 | hash group | ✅ PASS |
| empty_table | 0 | 0 | sample (L2) | ✅ PASS |
| no_pk_table | 20 | 20 | hash group | ✅ PASS |
| composite_pk (复合主键) | 3 | 3 | sample (L2) | ✅ PASS |

L2 抽样校验: **全部 PASS**（含 DECIMAL 精度、TIMESTAMPTZ 时区、复合主键、无PK表 hash 对比）

---

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

# 构建（仅 CLI）
make build

# 构建（含 Web UI，需 Node.js 20+）
make build-web

# 或使用脚本
bash build-web.sh

# 仅构建前端
make web-frontend

# 前端开发服务器
cd web/frontend && npm run dev
```

---

## 产出文件

| 文件 | 说明 |
|------|------|
| `<temp_dir>/*.csv` | 每张表的导出数据 |
| `<temp_dir>/lightning.toml` | 自动生成的 Lightning 配置 |
| `.checkpoint/` | 断点续传进度 |
| `precheck-report.json` | 预检查报告 |
| `validation-report.json` | 数据校验报告 |
| `unsupported-objects.log` | 不兼容对象清单 |
| `schema.sql`（dry-run 时） | 生成的 DDL 文件 |

---

## 常见问题

**Q: 支持哪些 PG 版本？**
A: PostgreSQL 10+ 版本。

**Q: 支持增量同步吗？**
A: 支持。CDC 增量同步（PG logical replication → TiDB）通过 `pg2tidb cdc` 命令运行，覆盖 INSERT/UPDATE/DELETE、事务顺序性、NULL/特殊字符/TOAST；带 checkpoint-on-failure 防丢数据（at-least-once 重启重读）+ 无 PK 表结构性 halt。Web 端有只读 CDC 监控仪表盘（`pg2tidb web`，读 CDC 状态文件，见 [docs/cdc-web-monitoring-contract.md](docs/cdc-web-monitoring-contract.md)）。

**Q: 大表迁移性能如何？**
A: PG 导出通常 50-200 MB/s，TiDB Lightning 导入 200-500 MB/s，整体瓶颈通常在 PG 导出端。

**Q: 必须安装 tidb-lightning 吗？**
A: 如果使用内置 Lightning 的构建版本（推荐），无需手动安装。否则需要安装 tidb-lightning 到系统 PATH。如果都不可用，工具会自动回退到流式 INSERT 方式。也可以设置 `use_lightning: false` 直接使用流式 INSERT。

**Q: 存储过程、触发器会迁移吗？**
A: 不会自动迁移。Pre-check 阶段会扫描并输出兼容性报告，标注不兼容对象和迁移建议。

**Q: 迁移过程中单张表失败怎么办？**
A: 配置 `on_error: skip` 跳过失败表继续迁移，最终报告汇总所有失败表。默认 `abort` 遇错即停。

**Q: 目标表已有数据怎么处理？**
A: 通过 `target_policy` 配置：`insert`（直接插入，遇主键冲突失败）、`truncate`（先清空目标表再插入）、`drop`（先 DROP 再重建）。

**Q: Web 模式的数据存在哪里？**
A: 任务数据存储在 SQLite 数据库中（默认 `.pg2tidb/tasks.db`），可通过 `--data` 参数指定目录。

---

## 性能调优建议

1. **提高导出并行度** — `migration.parallel` 设为 CPU 核数的 1-2 倍
2. **使用 Lightning Local Backend** — `migration.use_lightning: true` 是最快导入模式
3. **确保 PD 可达** — `target.pd_addr` 正确配置 PD 地址
4. **确保磁盘 IO** — 临时目录使用高速磁盘（SSD/NVMe）
5. **TiDB 集群规模** — Lightning 导入速度与 TiKV 节点数正相关
6. **网络带宽** — PG 到中间机器、中间机器到 TiDB 的网络带宽是关键瓶颈

---

## License

MIT
