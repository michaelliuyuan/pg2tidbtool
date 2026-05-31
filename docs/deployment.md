# pg2tidb 部署手册

## 1. 环境要求

| 组件 | 版本要求 | 说明 |
|------|----------|------|
| Go | 1.22+ | 仅源码编译时需要 |
| Node.js | 20+ | 仅构建前端时需要 |
| PostgreSQL | 10+ | 源端数据库 |
| TiDB | 7.1+ | 目标端数据库 |
| tidb-lightning | 可选 | 使用 Lightning 物理导入时需要 |
| 网络 | — | 迁移工具需同时访问 PG 和 TiDB |

## 2. 编译安装

### 方式一：本地编译（仅 CLI）

```bash
# 安装 Go 1.22+
wget https://go.dev/dl/go1.22.0.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.22.0.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin

# 编译
git clone https://github.com/michaelliuyuan/pg2tidbtool.git
cd pg2tidbtool
go mod tidy
make build

# 产物在 build/pg2tidb
./build/pg2tidb --help
```

### 方式二：本地编译（含 Web UI）

```bash
git clone https://github.com/michaelliuyuan/pg2tidbtool.git
cd pg2tidbtool

# 需要安装 Node.js 20+
bash build-web.sh

# 产物在 build/pg2tidb（内嵌前端静态文件）
./build/pg2tidb web --port 8080
```

或使用 Make：

```bash
make build-web
```

### 方式三：Docker

```bash
git clone https://github.com/michaelliuyuan/pg2tidbtool.git
cd pg2tidbtool
docker build -t pg2tidb .

# 启动 Web UI
docker run -p 8080:8080 pg2tidb

# 提取二进制
docker create --name tmp pg2tidb
docker cp tmp:/usr/local/bin/pg2tidb ./pg2tidb
docker rm tmp
```

### 方式四：交叉编译

```bash
# Linux ARM64
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -ldflags="-s -w" -o build/pg2tidb .

# macOS
GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o build/pg2tidb .
```

## 3. 安装 tidb-lightning（Lightning 导入模式）

如果使用 `use_lightning: true`（默认），需要在运行 pg2tidb 的服务器上安装 `tidb-lightning`：

```bash
# 下载 TiDB Lightning（示例版本，请根据需要调整）
wget https://download.pingcap.org/tidb-toolkit-v7.1.0-linux-amd64.tar.gz
tar -xzf tidb-toolkit-v7.1.0-linux-amd64.tar.gz
sudo cp tidb-toolkit-v7.1.0-linux-amd64/bin/tidb-lightning /usr/local/bin/

# 验证
tidb-lightning --version
```

> **注意**：如果不安装 tidb-lightning，数据导入会自动回退为流式 INSERT 模式（速度较慢但功能完整）。

## 4. 配置文件

创建配置文件 `config.yaml`：

```yaml
source:
  host: "your-pg-host"
  port: 5432
  user: "postgres"
  password: "your-password"
  database: "mydb"
  schema: "public"
  sslmode: "disable"

target:
  host: "your-tidb-host"
  port: 4000
  user: "root"
  password: "your-password"
  database: "mydb"
  pd_addr: "your-tidb-host:2379"    # Lightning 导入时必填
  status_port: 10080                  # TiDB Status 端口

migration:
  parallel: 4           # 并行 worker 数
  batch_size: 100000    # 每批行数
  temp_dir: "/tmp/pg2tidb"  # CSV 临时目录
  tables: []            # 空=迁移所有表，或指定表名列表
  exclude_tables: []    # 排除的表
  use_lightning: true   # 使用 TiDB Lightning 物理导入
  on_error: "abort"     # abort 或 skip
  target_policy: "insert"  # insert | truncate | drop
  checkpoint_dir: ".checkpoint"
  read_timeout: "30m"
  write_timeout: "30m"

logging:
  level: "info"
  format: "console"
  output: ""

web:
  enable: false
  port: 8080
  host: "0.0.0.0"
```

### target_policy 说明

| 策略 | 行为 |
|------|------|
| `insert` | 直接 INSERT，主键冲突时报错（默认） |
| `truncate` | 先 TRUNCATE 目标表再导入 |
| `drop` | 先 DROP TABLE 再重建并导入 |

## 5. 使用方法

### 5.1 Web UI 模式（推荐）

```bash
# 启动 Web 服务
./pg2tidb web --port 8080

# 指定数据目录（存储迁移历史、SQLite 数据库）
./pg2tidb web --port 8080 --data /data/pg2tidb
```

访问 `http://<host>:8080`，按向导操作：

1. 填写 PostgreSQL 连接信息 → 测试连接
2. 填写 TiDB 连接信息 → 测试连接
3. 查看并勾选待迁移的表（支持搜索）
4. 配置迁移参数（并发数、Lightning 开关、冲突策略）
5. 确认并启动，实时监控进度

### 5.2 预检查

```bash
./pg2tidb precheck -c config.yaml --report precheck-report.json
```

检查项：
- PG/TiDB 连接可用性
- 磁盘空间是否充足
- PG 权限是否足够
- 不兼容对象扫描（触发器/函数/枚举/扩展）
- 字符集校验

### 5.3 Schema 迁移

```bash
# 直接在 TiDB 执行 DDL
./pg2tidb schema -c config.yaml

# 仅生成 DDL 文件（不执行）
./pg2tidb schema -c config.yaml --dry-run --output schema.sql

# 排除特定表
./pg2tidb schema -c config.yaml --exclude-tables log_table,temp_table
```

### 5.4 数据迁移

```bash
# 默认参数
./pg2tidb data -c config.yaml

# 自定义参数
./pg2tidb data -c config.yaml \
  --parallel 8 \
  --batch-size 200000 \
  --temp-dir /data/tmp \
  --tables users,orders,products
```

数据迁移流程：
1. 通过 PG `COPY TO STDOUT` 协议并行导出为 CSV（tab 分隔）
2. boolean `t/f` → `1/0`，NULL → `\N`
3. 如果 `use_lightning: true`：调用 `tidb-lightning` 物理导入（100~500 GiB/h）
4. 如果 Lightning 不可用或失败：自动回退为流式 INSERT
5. 支持 checkpoint 断点续传

### 5.5 数据校验

```bash
# L1: 行数对比
./pg2tidb validate -c config.yaml --level L1

# L2: 抽样对比（默认 1%）
./pg2tidb validate -c config.yaml --level L2 --sample-ratio 0.05

# L3: 全量 Checksum
./pg2tidb validate -c config.yaml --level L3

# 输出报告
./pg2tidb validate -c config.yaml --level L2 --report validation-report.json
```

### 5.6 全流程一键执行

```bash
# 完整流程：precheck → schema → data → validate
./pg2tidb all -c config.yaml

# 跳过特定阶段
./pg2tidb all -c config.yaml --skip-precheck --skip-validate

# 遇到非致命错误继续
./pg2tidb all -c config.yaml --on-error-continue
```

## 6. 断点续传

迁移进度自动保存在 `.checkpoint/` 目录。中断后重新执行相同命令，已完成的表会自动跳过。

```bash
# 中断后重新执行
./pg2tidb data -c config.yaml
# 输出：skipping completed table xxx
```

## 7. 产出文件

| 文件 | 说明 |
|------|------|
| `<temp_dir>/*.csv` | 每张表的导出数据 |
| `<temp_dir>/lightning.toml` | 自动生成的 Lightning 配置 |
| `.checkpoint/` | 断点续传进度 |
| `precheck-report.json` | 预检查报告 |
| `validation-report.json` | 数据校验报告 |
| `unsupported-objects.log` | 不兼容对象清单 |
| `schema.sql`（dry-run 时） | 生成的 DDL 文件 |

## 8. 性能调优

| 参数 | 建议值 | 说明 |
|------|--------|------|
| `migration.parallel` | CPU 核数 | 并行导出 worker 数 |
| `migration.batch_size` | 100000-500000 | 大表可增大 |
| `migration.temp_dir` | SSD 磁盘 | 临时文件放 SSD |
| `migration.use_lightning` | true | 物理导入速度最快 |
| `migration.read_timeout` | 30m-60m | 大表调整 |
| `logging.level` | warn | 生产环境降低日志级别 |
| `logging.format` | json | 生产环境用 JSON 格式 |
