# TiMS — PostgreSQL to TiDB Migration Suite

一键式 PostgreSQL → TiDB 全量数据迁移工具，覆盖 Schema 迁移、全量数据迁移、数据校验三大能力，提供可视化 Web 管理界面。

## 功能特性

- **Schema Migration** — 自动采集 PG Schema，类型映射 + DDL 转换，生成 TiDB 兼容 DDL
- **Data Migration** — 基于 PG COPY + TiDB Lightning Local Backend 实现高性能并行数据迁移
- **Data Validation** — 三层校验策略（行数/抽样/Checksum）确保数据一致性
- **Web UI (TiMS)** — 完整 Web 管理界面，可视化配置向导、实时任务监控、迁移历史
- **HTML Report** — 专业的 HTML 迁移报告，包含对象清单、记录数、数据一致性对比
- **断点续传** — Checkpoint 机制支持中断后恢复
- **兼容性评估** — 迁移前扫描不兼容对象，输出风险报告
- **PG 数组支持** — 自动将 PostgreSQL 数组类型转换为 TiDB JSON 格式

## 快速部署

### 方式一：从源码构建

**前置要求**：Go 1.22+, Node.js 18+

```bash
# 克隆代码
git clone https://github.com/michaelliuyuan/pg2tidbtool.git
cd pg2tidbtool

# 构建（含前端）
bash build-web.sh

# 产物: ./build/pg2tidb
```

### 方式二：Docker 部署

```bash
docker build -t tims .
docker run -p 8080:8080 tims web --port 8080
```

### 方式三：直接下载二进制

```bash
# 下载最新 release 后解压
chmod +x pg2tidb
./pg2tidb web --port 8080
```

## 使用方式

### 方式一：Web UI（推荐）

```bash
# 启动 Web 服务
./pg2tidb web --port 8080

# 自定义数据目录
./pg2tidb web --data /data/tims --port 8080
```

浏览器访问 `http://localhost:8080`，通过向导界面完成：

1. **配置源端** — 填入 PostgreSQL 连接信息，测试连接
2. **配置目标端** — 填入 TiDB 连接信息（含 PD 地址和 Status 端口），测试连接
3. **选择表** — 自动列出源端表，勾选要迁移的表
4. **配置选项** — 并发数、批量大小、导入方式（Lightning/Streaming）、目标策略（追加/清空/删除）
5. **启动迁移** — 实时监控进度、日志和吞吐量
6. **下载报告** — 迁移完成后下载 HTML 报告

### 方式二：命令行

```bash
# 准备配置文件
cp configs/config.yaml config.yaml
# 编辑 config.yaml 填入连接信息

# 一键迁移（Pre-check → Schema → Data → Validate → Report）
./pg2tidb all --config config.yaml

# 分步执行
./pg2tidb precheck --config config.yaml   # 预检与兼容性评估
./pg2tidb schema --config config.yaml      # 仅迁移 Schema
./pg2tidb data --config config.yaml        # 仅迁移数据
./pg2tidb validate --config config.yaml    # 仅数据校验
```

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
  sslmode: "disable"         # disable | require | verify-ca | verify-full

# TiDB 目标端配置
target:
  host: "localhost"
  port: 4000
  user: "root"
  password: ""
  database: "mydb"
  pd_addr: "localhost:2379"    # PD 地址（Lightning 导入时需要）
  status_port: 10080           # TiDB Status 端口（默认 10080）

# 迁移配置
migration:
  parallel: 4                # 并行导出 worker 数
  batch_size: 100000         # 批量大小（行数）
  temp_dir: "/tmp/pg2tidb"   # 临时文件目录
  tables: []                 # 空=全部表，支持正则匹配
  exclude_tables: []         # 排除表，支持正则匹配
  use_lightning: true        # 使用 TiDB Lightning 加速导入
  on_error: "abort"          # 单表失败策略: skip | abort
  target_policy: "insert"    # 目标表策略: insert | truncate | drop
  checkpoint_dir: ".checkpoint"  # Checkpoint 目录

# 日志配置
logging:
  level: "info"              # debug | info | warn | error
  format: "console"          # console | json

# Web 监控面板（仅 CLI 模式使用）
web:
  enable: false
  port: 8080
  host: "0.0.0.0"
```

### 关键配置项

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `migration.parallel` | 并行导出 worker 数 | 4 |
| `migration.batch_size` | 每批次迁移行数 | 100000 |
| `migration.use_lightning` | 使用 Lightning Local Backend 加速导入 | true |
| `migration.target_policy` | 目标表策略（insert 直接插入 / truncate 先清空 / drop 先删除） | insert |
| `target.pd_addr` | PD 地址（Lightning 导入时需要） | {host}:2379 |
| `target.status_port` | TiDB Status 端口（Lightning 导入时需要） | 10080 |
| `migration.on_error` | 单表失败策略 | abort |

### Lightning 导入前置条件

使用 Lightning Local Backend 模式（`use_lightning: true`）时需要：

1. **tidb-lightning 二进制** — 服务器上需安装 `tidb-lightning`，并加入 PATH
2. **PD 可达** — 从运行迁移工具的机器能访问 PD 端口（默认 2379）
3. **TiDB Status 端口可达** — 默认 10080
4. **足够磁盘空间** — 临时目录需要存放 CSV 和 sorted KV 文件

```bash
# 安装 tidb-lightning（示例）
wget https://download.pingcap.org/tidb-lightning-v7.1.9-linux-amd64.tar.gz
tar xzf tidb-lightning-*.tar.gz
sudo mv tidb-lightning /usr/local/bin/
```

## 类型映射

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
| uuid | CHAR(36) | 转为字符串 |
| array (int[], text[], etc.) | JSON | 自动转换为 JSON 数组 |
| gin/gist 索引 | 跳过 | 记录告警日志 |

## Web UI 功能

| 页面 | 功能 |
|------|------|
| **配置向导** | 可视化配置 PG/TiDB 连接信息，测试连接，选择迁移表 |
| **任务监控** | 实时查看进度、表级详情、吞吐量、日志流 |
| **迁移历史** | 查看历史任务列表和详情 |
| **报告下载** | 下载 HTML 迁移报告（含表详情、行数对比、数据一致性） |

### Web UI 截图预览

- **Header**: TiMS 品牌 + 导航菜单（新建迁移 / 任务监控 / 迁移历史）
- **配置向导**: 4 步向导（源端 → 目标端 → 选择表 → 选项）
- **目标端配置**: Host、Port、用户名、密码、数据库名、**PD 地址**、**Status 端口**
- **任务详情**: 进度条、表/行统计、吞吐量、实时日志、报告下载

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
  -c, --config string       配置文件路径 (默认 configs/config.yaml)
      --log-level string    日志级别: debug, info, warn, error (默认 info)
      --log-format string   日志格式: console, json (默认 console)

# data 子命令额外参数
      --use-lightning       使用 Lightning 导入 (默认 true)
```

## REST API

```
GET  /api/v1/health                      # 健康检查
POST /api/v1/config/test-connection      # 测试数据库连接
POST /api/v1/config/list-tables          # 列出源端表
POST /api/v1/tasks                       # 创建迁移任务
GET  /api/v1/tasks                       # 列出所有任务
GET  /api/v1/tasks/{id}                  # 获取任务详情
POST /api/v1/tasks/{id}/start            # 启动任务
POST /api/v1/tasks/{id}/pause            # 暂停任务
POST /api/v1/tasks/{id}/resume           # 恢复任务
POST /api/v1/tasks/{id}/cancel           # 取消任务
DELETE /api/v1/tasks/{id}                # 删除任务
GET  /api/v1/tasks/{id}/progress         # 获取实时进度
GET  /api/v1/tasks/{id}/report           # 下载 HTML 报告（默认）
GET  /api/v1/tasks/{id}/report?format=json  # 下载 JSON 报告
GET  /api/v1/tasks/{id}/logs             # 获取任务日志
GET  /api/v1/tasks/{id}/phases           # 获取阶段详情
GET  /api/v1/ws                          # WebSocket 实时推送
```

## 项目结构

```
pg2tidbtool/
├── cmd/                    # CLI 命令入口 + 静态资源嵌入
│   ├── root.go             # 根命令
│   ├── all.go              # all 命令
│   ├── schema.go           # schema 命令
│   ├── data.go             # data 命令
│   ├── validate.go         # validate 命令
│   ├── web.go              # web 命令
│   ├── precheck.go         # precheck 命令
│   └── static/             # 嵌入的前端静态资源
├── internal/
│   ├── schema/             # Schema 迁移模块
│   ├── data/               # 数据迁移模块（CSV 导出 + Lightning/SQL 导入）
│   ├── validator/          # 数据校验模块
│   ├── precheck/           # 预检与兼容性评估
│   ├── orchestrator/       # 任务编排器
│   ├── webapi/             # Web API 服务（任务管理、进度、报告）
│   ├── api/                # CLI 模式 Web 监控
│   └── common/             # 公共模块（config/checkpoint/logger/reporter/errors）
├── web/frontend/           # Vue 3 + Element Plus 前端源码
├── configs/                # 配置文件模板
├── main.go                 # 程序入口
├── Makefile                # 构建脚本
├── build-web.sh            # 前端构建脚本
└── Dockerfile              # Docker 构建
```

## 断点续传

迁移过程中断后，重新执行相同命令即可从断点继续。Checkpoint 信息保存在 `migration.checkpoint_dir` 目录中。

## 常见问题

**Q: 支持哪些 PG 版本？**
A: PostgreSQL 10+ 版本。

**Q: 支持增量同步吗？**
A: 当前仅支持全量迁移，增量能力后续版本支持。

**Q: Lightning 导入报错 "connection refused"？**
A: 检查 PD 地址和 TiDB Status 端口是否可达（Docker 部署需确认端口映射）。如果 PD 不在外网暴露，使用内网地址。

**Q: Lightning 报错 "table(s) are not empty"？**
A: 在任务选项中将目标策略设为 "truncate"（清空表），工具会在 Lightning 导入前自动清空目标表。

**Q: PG 数组类型导入报 JSON 解析错误？**
A: 工具已自动将 PG 数组格式 `{1,2,3}` 转换为 JSON 格式 `[1,2,3]`。如仍有问题，检查 TiDB 目标列是否为 JSON 类型。

**Q: 迁移过程中单张表失败怎么办？**
A: 配置 `on_error: skip` 跳过失败表继续迁移，最终报告汇总所有失败表。

**Q: 大表迁移性能如何？**
A: PG 导出通常 50-200 MB/s，TiDB Lightning 导入 200-500 MB/s，瓶颈通常在 PG 导出端。

**Q: Trigger 和 Stored Function 会迁移吗？**
A: 不会自动迁移。Pre-check 阶段会扫描并标注不兼容对象。

## 性能调优

1. **提高导出并行度** — `migration.parallel` 设为 CPU 核数的 1-2 倍
2. **使用 Lightning** — `use_lightning: true` 使用 Local Backend 物理导入，速度最快
3. **高速磁盘** — 临时目录使用 SSD/NVMe，CSV 和 sorted KV 文件需要大量 IO
4. **网络带宽** — PG 到迁移工具、迁移工具到 TiDB/PD 的网络带宽是关键瓶颈
5. **TiDB 集群规模** — Lightning 导入速度与 TiKV 节点数正相关

## License

MIT
