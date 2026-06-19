# PG2TiDB CDC DDL 复制 — 架构设计文档

> 状态：设计草案
> 负责人：@架构师（设计）→ @开发工程师（开发）/ @测试工程师（测试）
> 前置/背景：#t42–48 P3 建了 `internal/cdc/ddl_tracker.go` 脚手架但**未接进 runner**（`sync_ddl`/`EnableDDLTracking` 至今 no-op）；一致性测试确认 DML 复制 ✅、DDL（新建表）不复制。@刘源 决策（2026-06-19）选②：**实现 DDL 复制**，补齐该缺口。
> 基线：以代码为准（`internal/cdc/ddl_tracker.go`、`runner.go`、`source.go`、`cdc-architecture.md` §3.3）。

---

## 1. 目标
| 目标 | 说明 |
|------|------|
| DDL 复制 | 把源端 DDL（CREATE/ALTER/DROP TABLE 等）复制到目标 TiDB |
| 接线 DDLTracker | `runner` 创建并驱动 ddlTracker（当前是死字段） |
| `sync_ddl` 生效 | `cdc.enable=true & sync_ddl=true` 时真正复制 DDL；false 时维持现状（仅 DML） |
| 一致性 | at-least-once + checkpoint，崩溃不丢/不重复建表 |
| DDL/DML 顺序 | 新表 DML 不至于在 DDL 前永久失败 |
| 兼容性 | 不支持 DDL 跳过+告警，不阻断 CDC（架构 §3.3 白/黑名单） |

---

## 2. 现状（代码为准）
- `ddl_tracker.go` **已实现**：`SetupEventTrigger`（源端装 `pg2tidb_ddl_trigger` + capture 函数，DDL 写 `pg2tidb.ddl_log`）、`TeardownEventTrigger`、`FetchNewDDL(sinceID)`（轮询未同步 DDL）、`DDLTransformer.Transform`（PG→TiDB：table/index/view，§3.3）。
- `runner.go` **未接线**：`ddlTracker *DDLTracker`(L28) + `EnableDDLTracking`(L55) 仅声明；`NewRunner` 不创建、`Run()` 不 setup/poll/apply；全包零调用 → `sync_ddl` no-op。
- `source.go`：pgoutput 流只含 DML，DDL 走独立的 event-trigger log 通道（两流需协调）。

---

## 3. 架构

```
源端 PG                         CDC Runner (sync_ddl=true)                  目标 TiDB
──────                         ──────────────────────────                  ─────────
DDL 发生 ──event_trigger──▶ pg2tidb.ddl_log ──FetchNewDDL(轮询)──▶ DDLTransformer ──apply TiDBDDL──▶ schema 变更
                                                                  └─ mark synced / checkpoint lastID
DML(行变更) ──pgoutput──▶ Source ──▶ Applier ──▶ 行数据   (现有 DML 路径不变)
```

DDL 与 DML 是**两条独立流**：DDL 经 event-trigger log 轮询、DML 经 pgoutput 实时流。两者在目标端需协调（见 §5 排序）。

---

## 4. 详细设计

### 4.1 接线 DDLTracker（`runner.go`）
- `NewRunner`：若 `cfg.EnableDDLTracking`（=`cfg.CDC.SyncDDL`），用**源端 PG 连接**创建 `ddlTracker = NewDDLTracker(srcDB, filter)`（ddl_tracker 现有签名是 `db *sql.DB`，用源端连接做 event trigger setup/fetch）。
- `Run()` 启动阶段（source.Setup 之后）：`ddlTracker.SetupEventTrigger(ctx)`（装触发器；幂等——已存在则跳过）。
- `Run()` 主循环：新增 **DDL poll goroutine**（与 DML applier 并行）：定时（如 2s）`FetchNewDDL(lastID)` → 逐条 `Transform` → `applyDDL(targetDB, tiDBDDL)` → 成功后推进 `lastID` 并 checkpoint。
- `Run()` 停止/ctx.Done：`ddlTracker.TeardownEventTrigger(ctx)`（卸触发器，可选——保留触发器便于续传，见 §6）。

### 4.2 DDL apply 与幂等（at-least-once）
DDL **不幂等**（`CREATE TABLE t` 两次报错）。保守策略（首版，重安全不掩盖）：
- `ddl_log` 每条有自增 `id` + `synced` 标志。apply 成功 → 标 `synced=true` + checkpoint `lastSyncedID`。
- 崩溃恢复：从 checkpoint 的 `lastSyncedID` 续传；已 synced 的不重放（靠 id checkpoint，主要防线）。
- **廉价幂等**：`CREATE`/`DROP` 生成 `IF NOT EXISTS`/`IF EXISTS`（安全、零代价）。
- **不激进容错**：`ALTER` 等无法廉价幂等化的 DDL，靠 checkpoint 不重放；若崩溃窗口内单条 DDL 被重放并报错 → **halt + 告警 + 等人工**（复用 #t48 结构性 halt），**不**用「容忍已存在错误」掩盖——重放异常必须显形。

### 4.3 DDL/DML 排序（关键）
问题：新表 `t` 的 DML 经 pgoutput 可能**先于**其 CREATE DDL（轮询有延迟）到达 applier → applier 写 `t` 报「表不存在」。
- 方案（pragmatic，首版）：applier 对「目标表不存在」等 schema 错误**短退避重试**（如 500ms×N），等 DDL poll 把表建出来；**超 N 次仍 schema 不匹配 → halt**（复用 #t48 结构性 halt，不静默丢/不无限 buffer），人工或 DDL 追上后恢复续传。
- 不做（首版）：严格的 LSN 级 DDL/DML 全序栅栏（复杂、首版过度）。
- 说明：DDL 通常远少于 DML、poll 间隔小，缺表窗口短；retry+halt 兜底足够且错误显形（@测试工程师 可复用 #t48 halt 断言法）。

### 4.4 兼容性与黑白名单（架构 §3.3）
- `DDLTransformer` 已支持 table/index/view；`function/trigger/procedure` → TiDB 不兼容，**跳过 + 告警**，不阻断。
- `ALTER TABLE`：`ADD COLUMN` ✅；`DROP/MODIFY COLUMN` 标记有限支持（precheck，必要时暂停告警等人工）。
- DDL apply 失败（不兼容/语法）→ 记日志 + 标 `synced=true`(已处理=跳过) 或 `skipped` + 告警，**不阻断 CDC 主流程**。

### 4.5 配置语义
- `cdc.enable=true`：CDC 模块开（#t50）。
- `cdc.sync_ddl=true`：runner 创建 ddlTracker + 复制 DDL（**本特性让其真正生效**）。
- `cdc.sync_ddl=false`：仅 DML（现状行为）。
- README/config 注释更新：`sync_ddl` 从「no-op」改为「DDL 复制开关（默认 true）」，并说明仅 DML/已迁移 schema 之外的 DDL 会被复制。

---

## 5. 影响面与风险
| 风险 | 应对 |
|------|------|
| DDL 不幂等致重放报错 | IF NOT EXISTS + 错误容错映射（§4.2） |
| DML 先于 DDL 缺表 | applier 缺表 retry+暂存（§4.3） |
| 不兼容 DDL | 跳过+告警，不阻断（§4.4） |
| event trigger 残留 | 停止时 Teardown（或保留以续传，文档说明） |
| 源端连接复用 | ddlTracker 用源端连接（与 source 共享/独立连接池，注意生命周期） |
| DML 路径回归 | 不改 internal/cdc 的 source/transformer/applier DML 逻辑，仅 runner 加 DDL 分支 + applier 缺表 retry |

---

## 6. 任务拆分
| 任务 | 内容 | 负责 |
|------|------|------|
| **DDL-1 后端** | NewRunner 条件创建 ddlTracker；Run() SetupEventTrigger + DDL poll goroutine(FetchNewDDL→Transform→apply→checkpoint lastID) + Teardown；DDLTransformer 幂等化(IF NOT EXISTS)；applier 缺表 retry；sync_ddl 生效；config/README 注释 | @开发工程师 |
| **DDL-T 测试** | 单测(Transform PG→TiDB / apply 幂等 / synced 去重) + 集成(CREATE/ALTER/DROP 源→目标一致 / DDL 先于 DML / at-least-once 重放 / checkpoint 续传) + E2E@235 | @测试工程师 |

DDL-1 是主干；DDL-T 与开发并行设计用例、实现后执行。DML 路径与同步内核不变（仅 runner 加 DDL 分支 + applier 缺表 retry）。

---

## 7. 验收标准
1. `sync_ddl=true` 下：源端 CREATE/ALTER/DROP TABLE → 目标 TiDB schema 一致（源==目标）。
2. at-least-once：CDC 崩溃重启重放 DDL 不重复报错（幂等化 + synced 去重）；checkpoint 续传不丢 DDL。
3. DDL/DML 顺序：新表先建表后写数据（DML 缺表不永久失败）。
4. 不兼容 DDL（function/trigger）跳过+告警，不阻断 CDC。
5. `sync_ddl=false`：仅 DML（现状不回归）。
6. DML 路径回归通过（CFG/CLI/WEB/集成 不破）。

---

## 8. 待确认决策（@刘源）
- **DDL/DML 排序模型**：首版用「applier 缺表 retry+暂存」（推荐，§4.3）还是严格 LSN 栅栏？——推荐前者。
- **event trigger 生命周期**：CDC 停止时卸载（Teardown）还是保留以便续传？——推荐停止时卸载（干净），续传靠 ddl_log 残留 + 重启重设。
- **不兼容 DDL 处理**：跳过+告警不阻断（推荐）还是暂停等人工？
- **sync_ddl 默认值**：保持 true（与历史配置一致，但现在真生效）还是改 false（显式 opt-in DDL 复制）？——推荐保持 true（CDC 启用即复制 DDL 是更完整语义）。
