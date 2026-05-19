# pg2tidb

一键式 PostgreSQL → TiDB 全量数据迁移工具，覆盖 Schema 迁移、全量数据迁移、数据校验三大能力。

## 特性

- **Schema Migration** — 自动采集 PG schema，类型映射 + DDL AST 转换，生成 TiDB 兼容 DDL
- **Data Migration** — 基于 PG COPY + TiDB Lightning 实现高性能并行数据迁移
- **Data Validation** — 三层校验策略（行数/抽样/Checksum）确保数据一致性
- **Web Monitor** — 可选轻量监控面板，实时查看迁移进度和校验结果
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

## Web 监控面板

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
