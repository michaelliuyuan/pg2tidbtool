# PG2TiDB Web 一键启停 CDC — 架构设计文档

> 状态：设计草案，路线待 @刘源 确认（推荐路线①）
> 负责人：@架构师（设计）→ @开发工程师（开发）/ @测试工程师（测试）
> 前置：#t50（CDC 可选模块，已 done，`cdc.enable` 默认 false）；本特性在其之上补 **CONTROL 通道**。
> 基线：以代码为准（`internal/webapi/`、`cmd/cdc.go`、`internal/cdc/runner.go`）。

---

## 1. 背景与目标

### 1.1 现状
- CDC 现为**可选模块**（`cdc.enable` 默认 false）。启用后 Web 仪表盘可见，但**只读监控**：读 CDC 写出的 status 文件显示 running/LSN/吞吐（READ 通道，已通）。
- CDC 同步本体是**独立 CLI 进程** `pg2tidb cdc`，需手动起停。

### 1.2 需求（@刘源）
Web 增加「**一键启停 CDC**」：仪表盘加 Start/Stop 按钮，Web 通过控制通道启停 CDC，用户无需 SSH+CLI。

### 1.3 目标
| 目标 | 说明 |
|------|------|
| CONTROL 通道 | Web → 启停 CDC 同步进程（补现有 READ 通道） |
| 进程生命周期 | Web spawn/supervise CDC：启动、停止、崩溃自动重启 |
| 门控一致 | `cdc.enable=false` 时启停端点引导开启，不绕过 #t50 的可选化语义 |
| 前端按钮 | CDCView 加 Start/Stop（仅 enable=true 时可见）+ 状态实时反映 |
| 内核复用 | CDC 同步内核 `internal/cdc/` 不变 |

---

## 2. 关键决策：CDC 进程模型

**推荐路线①：Web spawn + supervise CDC 子进程（os/exec）**
- Web 通过 `os/exec` 拉起 `pg2tidb cdc` 子进程（或同二进制 `cdc` 子命令），supervise 其生命周期。
- 优点：① CDC 崩溃不拖垮 Web（隔离）；② 复用现有 CDC CLI + status 文件 READ 通道；③ 与「CDC 是独立进程」现状一致。
- 代价：Web 需管理子进程 PID / 重启 / 优雅停止。

**路线②（未来生产级增强，本期不做）：独立 supervisor/daemon**
- 引入常驻 supervisor 进程，Web 与 CDC 都对接它。Web 重启不丢 CDC 管理。适合 7×24 平台，本期过度设计。

> 决策点（待 @刘源 拍）：本期取路线①（推荐）；② 列为未来增强。

### 2.1 spawn 子进程 vs Web 进程内跑 CDC（goroutine）
- **选 spawn 子进程**：隔离 + 复用 CLI + status 文件通道天然兼容。Web 进程内跑（`cdc.Runner` 作 goroutine）虽省 IPC，但 CDC 崩溃会影响 Web、且模糊进程边界——不选。

---

## 3. 架构

```
┌─────────────────────────┐   POST /cdc/start|stop    ┌──────────────────┐
│   Web UI (CDCView)      │ ─────────────────────────▶ │  Web Server       │
│  [Start CDC] [Stop CDC] │ ◀────── /cdc/status ────── │  CDCSupervisor    │
└─────────────────────────┘      (READ, 已有)          │  (os/exec)        │
                                                         │   ├─ spawn pg2tidb│
                                                         │   │   cdc (child) │
                                                         │   ├─ restart policy│
                                                         │   └─ graceful stop │
                                                         └────────┬──────────┘
                                                                  │ writes
                                                         ┌────────▼──────────┐
                                                         │  CDC status file   │◀─ read by /cdc/status (READ 通道，已有)
                                                         └───────────────────┘
```

---

## 4. 详细设计

### 4.1 后端：CDCSupervisor（`internal/webapi/cdc_supervisor.go` 新增）

```go
type CDCSupervisor struct {
    mu          sync.Mutex
    cfg         config.CDCConfig
    targetDSN   string
    child       *exec.Cmd
    state       CDCState   // stopped|starting|running|stopping|failed
    startedAt   time.Time
    restarts    int
    log         *zap.Logger
    statusFile  string     // CDC 子进程写的 status 文件（READ 通道源头）
}

type CDCState string
const (
    StateStopped  CDCState = "stopped"
    StateStarting CDCState = "starting"
    StateRunning  CDCState = "running"
    StateStopping CDCState = "stopping"
    StateFailed   CDCState = "failed"   // 崩溃且超过重启上限
)
```

**生命周期方法**：
- `Start(ctx)`: 幂等；若 running 直接返回当前态；否则 spawn `pg2tidb cdc`（带 cfg + statusFile 路径），置 starting→running（子进程存活即 running），后台 goroutine `Wait()` 监控退出 → 触发 restart 策略。
- `Stop(ctx)`: 幂等；SIGTERM 子进程 → 等 grace（默认 15s）→ 仍存活 SIGKILL；置 stopped。
- `Status()`: 返回 state/PID/uptime/restarts（+ 复用现有 status 文件读取给仪表盘业务数据）。

**崩溃重启策略**：
- 子进程异常退出（非 Stop 触发）→ 自动重启，指数退避（1s/2s/4s...），上限 N 次（默认 5）内；超限置 `failed`（需人工 Stop+Start 或调参）。
- 防抖：短时间内反复崩溃（如配置错）不无限重启。

### 4.2 门控（与 #t50 一致）
- `cdc.enable=false` 时 `Start()` 返回 disabled 错误 + 引导（"set cdc.enable: true / --enable-cdc"），**不绕过**可选化语义。
- Web 启动时按 `cfg.CDC.Enable` 决定 supervisor 是否就绪（关时端点返回 disabled，前端不显示按钮）。

### 4.3 REST API（`internal/webapi/server.go` 扩展）

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/cdc/start` | 启动 CDC（幂等；disabled 时 409 + 引导） |
| `POST` | `/api/v1/cdc/stop` | 停止 CDC（幂等；SIGTERM→grace→SIGKILL） |
| `GET` | `/api/v1/cdc/status` | 现有，扩展返回 `state`(含 controlled PID/uptime/restarts) |

响应（start）：
```json
{"ok":true,"state":"running","pid":3030897,"message":"CDC started"}
```
disabled 时（cdc.enable=false）：
```json
{"ok":false,"state":"disabled","message":"CDC module disabled. Set cdc.enable: true first."}
```

> 后续可按架构文档 §6 扩展 `/cdc/pause|resume|cutover`，本期只做 start/stop。

### 4.4 前端（`web/frontend/src/views/CDCView.vue`）
- 仅 `features.cdc.enabled === true` 时显示 `[Start CDC]` / `[Stop CDC]` 按钮 + 当前 state 徽标。
- Start/Stop 调 `/cdc/start|stop`，成功后刷新 `/cdc/status`；操作中按钮 disabled + loading。
- Stop 加二次确认（防误停同步）。
- `failed` 态显示告警 + 重试入口。

### 4.5 安全与边界
- **Web 起 CDC 子进程**：Web 进程需有执行 `pg2tidb` 二进制的权限；建议 Web 与 CDC 同机、同用户。
- 端点建议仅本地/内网暴露（Web 已是内网管理面）；后续可加简单 token。
- **Web 重启**：路线①下 Web 重启会丢失对既有 CDC 子进程的 supervise 句柄 → 启动时探测 status 文件 / PID，若 CDC 在跑则「领养」（re-attach 监控）或按策略处理（默认：识别在跑则标 running 但不持有句柄，Stop 时按 PID 杀）。需在实现时明确「领养 vs 重启」策略。

---

## 5. 影响面与风险

| 风险 | 应对 |
|------|------|
| Web 重启丢 CDC 句柄 | 启动探测 status/PID，定义领养策略（见 4.5） |
| CDC 反复崩溃（配置错） | 重启上限 + 指数退避 + `failed` 态，不无限拉起 |
| 误停同步 | Stop 二次确认；状态实时反映 |
| 子进程僵尸/泄漏 | `Wait()` 回收 + Stop 的 SIGKILL 兜底 |
| 权限 | Web 与 CDC 同用户/同机；端点内网 |

---

## 6. 任务拆分

| 任务 | 内容 | 负责 |
|------|------|------|
| **C1 后端** | `CDCSupervisor`（spawn/supervise/restart/stop/领养）+ 门控 + `/cdc/start|stop` 端点 + status 扩展 | @开发工程师 |
| **C2 前端** | CDCView Start/Stop 按钮 + state 徽标 + 二次确认 + failed 告警 | @开发工程师 |
| **CT 测试** | 单测(状态机/幂等/崩溃重启上限) + 集成(Web start→CDC 跑→status 反映→stop) + E2E(235) | @测试工程师 |

C1/C2 可并行（C2 mock API）；CT 在 C1/C2 合并后跑。

---

## 7. 验收标准
1. `cdc.enable=true` 下 Web 点 Start → CDC 子进程拉起、`/cdc/status` 反映 running+业务数据；点 Stop → 优雅停止。
2. `cdc.enable=false` 下 Start 返回 disabled 引导，不绕过 #t50 语义；前端无按钮。
3. CDC 崩溃 → 自动重启至上限，超限 `failed` 告警；Start/Stop 幂等。
4. Web 重启后对在跑 CDC 的领养策略明确且不产生僵尸/重复进程。
5. 单测 + 集成 + E2E 通过；READ 通道（status 文件）与 CONTROL 通道协同正常。

---

## 8. 待确认决策（@刘源）
- **路线**：本期路线①（Web spawn+supervise CDC 子进程），② 留未来？——推荐①。
- **领养策略**：Web 重启后遇在跑 CDC，是「领养监控」还是「视为外部进程、Stop 按 PID 杀」？——推荐领养（探测 status/PID）。
- **重启上限/退避**：默认崩溃重启 5 次、指数退避 1s/2s/4s/8s/16s，超限 failed？——推荐。
- **端点暴露**：start/stop 仅内网/本地，暂不加鉴权？——推荐（与现有 Web 一致）。
