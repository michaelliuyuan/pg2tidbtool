# pg2tidb-migrator 部署手册

## 1. 环境要求

| 组件 | 版本要求 |
|------|----------|
| Go | 1.22+ |
| PostgreSQL | 12+（源端） |
| TiDB | 6.0+（目标端） |
| 网络 | 迁移工具需同时访问 PG 和 TiDB |

## 2. 编译安装

### 方式一：本地编译

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

### 方式二：Docker 编译

```bash
git clone https://github.com/michaelliuyuan/pg2tidbtool.git
cd pg2tidbtool
docker build -t pg2tidb-builder .

# 提取二进制
docker create --name tmp pg2tidb-builder
docker cp tmp:/usr/local/bin/pg2tidb ./pg2tidb
docker rm tmp
```

## 3. 配置文件

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

migration:
  parallel: 4           # 并行 worker 数
  batch_size: 100000    # 每批行数
  temp_dir: "/tmp/pg2tidb"  # CSV 临时目录
  tables: []            # 空=迁移所有表，或指定表名列表
  exclude_tables: []    # 排除的表
  use_lightning: true   # 使用 LOAD DATA 导入
  on_error: "abort"     # abort 或 skip
  checkpoint_dir: ".checkpoint"
  read_timeout: "30m"
  write_timeout: "30m"

logging:
  level: "info"         # debug/info/warn/error
  format: "console"     # console 或 json
  output: ""            # 空=仅 stderr，或指定日志文件路径

web:
  enable: false         # 启用 Web 监控面板
  port: 8080
  host: "0.0.0.0"
```

## 4. 使用方法

### 4.1 预检查

迁移前先运行 precheck 确认环境兼容性：

```bash
./pg2tidb precheck -c config.yaml --report precheck-report.json
```

检查项：
- PG/TiDB 连接可用性
- 磁盘空间是否充足
- PG 权限是否足够
- 不兼容对象扫描（触发器/函数/枚举/扩展）
- 字符集校验

### 4.2 Schema 迁移

```bash
# 直接在 TiDB 执行 DDL
./pg2tidb schema -c config.yaml

# 仅生成 DDL 文件（不执行）
./pg2tidb schema -c config.yaml --dry-run --output schema.sql

# 排除特定表
./pg2tidb schema -c config.yaml --exclude-tables log_table,temp_table
```

类型映射示例：
| PostgreSQL | TiDB/MySQL |
|-----------|-----------|
| integer | INT |
| bigint | BIGINT |
| serial | INT AUTO_INCREMENT |
| varchar(n) | VARCHAR(n) |
| text | TEXT |
| boolean | TINYINT(1) |
| bytea | BLOB |
| jsonb | JSON |
| uuid | CHAR(36) |
| timestamp with tz | TIMESTAMP |
| numeric(p,s) | DECIMAL(p,s) |

### 4.3 数据迁移

```bash
./pg2tidb data -c config.yaml

# 自定义参数
./pg2tidb data -c config.yaml \
  --parallel 8 \
  --batch-size 200000 \
  --temp-dir /data/tmp \
  --tables users,orders,products
```

数据导出流程：
1. 通过 PG `COPY TO STDOUT` 协议并行导出
2. 每张表生成一个 tab 分隔的 CSV 文件
3. boolean `t/f` → `1/0`，NULL → `\N`
4. 支持 checkpoint 断点续传

数据导入流程：
1. 使用 `LOAD DATA LOCAL INFILE` 导入 TiDB
2. 文件格式：tab 分隔，`\N` 表示 NULL
3. 导入前需 `mysql.RegisterLocalFile` 注册文件

实际执行的 SQL：
```sql
LOAD DATA LOCAL INFILE '/tmp/pg2tidb/users.csv' 
INTO TABLE `users` 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n'
```

### 4.4 数据校验

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

### 4.5 全流程一键执行

```bash
# 完整流程：precheck → schema → data → validate
./pg2tidb all -c config.yaml

# 跳过特定阶段
./pg2tidb all -c config.yaml --skip-precheck --skip-validate

# 遇到非致命错误继续
./pg2tidb all -c config.yaml --on-error-continue
```

## 5. Web 监控面板

```yaml
# config.yaml 中启用
web:
  enable: true
  port: 8080
  host: "0.0.0.0"
```

启动后访问 `http://<host>:8080`

REST API：
- `GET /api/v1/status` — 全局迁移状态
- `GET /api/v1/tables` — 各表进度
- `GET /api/v1/validation` — 校验结果
- `GET /api/v1/report` — 最终报告

## 6. 断点续传

迁移进度自动保存在 `.checkpoint/checkpoint.json`。中断后重新执行相同命令，已完成的表会自动跳过。

```bash
# 中断后重新执行
./pg2tidb data -c config.yaml
# 输出：skipping completed table xxx
```

## 7. 产出文件

| 文件 | 说明 |
|------|------|
| `<temp_dir>/*.csv` | 每张表的导出数据 |
| `.checkpoint/checkpoint.json` | 断点续传进度 |
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
| `migration.read_timeout` | 30m-60m | 大表调整 |
| `logging.level` | warn | 生产环境降低日志级别 |
| `logging.format` | json | 生产环境用 JSON 格式 |
