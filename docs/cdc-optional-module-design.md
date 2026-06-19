# PG2TiDB CDC 增量同步 — 改为「可选功能模块」设计方案

> 状态：决策已确认（@刘源 2026-06-19）；D1（#t51）已合并上 main，D2/D3 进行中
> 负责人：@架构师（设计）→ @开发工程师（开发）/ @测试工程师（测试）
> 关联：`docs/cdc-architecture.md`（CDC 模块本身架构，本文件只讲「可选化」改造）
> 基线：以代码为准（`internal/common/config/config.go`、`cmd/cdc.go`、`cmd/all.go`、`internal/webapi/server.go`、`internal/cdc/`），设计文档状态标注可能滞后。

---

## 1. 背景与目标

### 1.1 需求（@刘源）
当前版本中 CDC 增量同步「看起来是一个默认打开的功能」。希望把**增量同步作为一个可选的功能模块**，支持用户选择开启或关闭。

### 1.2 业务现实
- 现有两个 PG 迁移项目（国元证券、星图金融）均**有停机时间窗口，只需要全量迁移，暂不需要 CDC**。
- 因此工具的默认形态应是「全量迁移工具」，CDC 作为「需要零停机迁移时才开启」的可选增强。

### 1.3 设计目标
| 目标 | 说明 |
|------|------|
| 单一开关 | 一个 `cdc.enable` 开关决定 CDC 模块是否对外暴露，用户一处控制 |
| 默认关闭 | 出厂默认 `cdc.enable: false`，工具默认是纯全量迁移形态 |
| 非破坏性 | 已有 `pg2tidb cdc` 显式调用路径不中断（显式即视为 opt-in） |
| 全层一致 | 配置 / CLI / Web API / Web UI 四层对 CDC 的可见性与开关保持一致 |
| 可裁剪（可选） | 支持构建期完全裁掉 CDC 包，产出最小全量迁移二进制（高级选项） |

---

## 2. 现状分析（以代码为准）

> 结论：CDC **没有进 `all` 流水线**，precheck **不检查** CDC 前置条件；「默认打开」实际来自 **CLI 子命令始终注册、Web 路由/仪表盘始终挂载、配置里完全没有开关、`EnableDDLTracking` 硬编码 true**。

| 触点 | 文件 | 当前行为 | 是否「默认打开」来源 |
|------|------|----------|----------------------|
| 配置结构体 | `internal/common/config/config.go` | `Config` 无 `cdc` 字段；仅有 `Web.Enable` 这个开关范式可参考 | ❌ 无开关 |
| 出厂配置 | `configs/config.yaml` | 无 `cdc:` 段 | ❌ |
| CLI `all` | `cmd/all.go` → `orchestrator.Pipeline` | precheck→schema→data→validate，**不含 CDC** | ✅ 本来就没 CDC |
| CLI `cdc` | `cmd/cdc.go` | 独立子命令，`rootCmd.AddCommand(cdcCmd)` 始终注册；`EnableDDLTracking: true` 硬编码；所有参数来自 flag + `cdc.DefaultSourceConfig()` | ⚠️ 始终可用 |
| 流水线编排 | `internal/orchestrator/pipeline.go` | `Phase` 枚举无 CDC 阶段 | ✅ 无 CDC |
| 前置检查 | `internal/precheck/` | grep 无任何 CDC 检查 | ✅ 本就与 CDC 无关 |
| Web API | `internal/webapi/server.go:127` | `/api/v1/cdc/status` **无条件挂载** | ⚠️ 始终暴露 |
| Web 处理器 | `internal/webapi/cdc_handler.go` | `defaultCDCReader` 永远返回「CDC not running」占位 | ⚠️ |
| Web UI | `web/.../CDCView.vue` + 嵌入资源 | CDC 仪表盘**始终渲染** | ⚠️ 始终可见 |

**一句话**：CDC 不是一个被「自动启动」的功能，而是一个**始终被「对外暴露/可见」**的功能（命令、路由、页面都在）。本次改造的本质是：**给「CDC 模块是否对外可见/可用」加一个统一开关，默认关**。

---

## 3. 设计原则

1. **复用既有范式**：完全模仿已有的 `web.enable` 开关模式（`WebConfig` + `Validate` + `DefaultConfig`），不引入新机制。
2. **单一事实源**：`cdc.enable` 是唯一开关；CLI flag / 环境变量只是它的覆盖入口，最终都归并到同一个布尔值。
3. **默认安全**：默认 `false`。需要零停机迁移的用户显式打开。
4. **显式调用即 opt-in**：用户手动执行 `pg2tidb cdc` 等价于明确声明「我要用 CDC」，不应被默认 `false` 硬拦死——给提示而非报错退出（见 §5.2 兼容性）。
5. **分层、可独立开发测试**：配置层 / CLI 层 / Web 层互不强耦合，可并行开发。

---

## 4. 总体方案：分层开关

```
                    ┌─────────────────────────────────────┐
                    │   单一开关  cdc.enable (默认 false)    │
                    └───────────────┬─────────────────────┘
                                    │ 归并
        ┌───────────────┬───────────┴───────────┬──────────────────┐
        ▼               ▼                       ▼                  ▼
  ① 配置层         ② CLI 层               ③ Web 层           ④ 构建层(可选)
  CDCConfig 结构    cdc 子命令受控         路由/仪表盘受控      build tag 裁剪包
  + Validate       + --enable-cdc flag    + 模块禁用状态码     (最小全量二进制)
  + Default        + 环境变量覆盖         + UI 条件渲染
```

优先级：**① ② ③ 为本期必做**；④ 为可选增强，单独立项，不阻塞主路径。

---

## 5. 详细设计

### 5.1 配置层（核心）

**新增 `CDCConfig` 结构体**（`internal/common/config/config.go`），把目前散落在 flag 里的 CDC 参数一并收进配置，使「开关 + 参数」集中一处：

```go
// Config 顶层新增字段
type Config struct {
    Source    SourceConfig    `yaml:"source" json:"source"`
    Target    TargetConfig    `yaml:"target" json:"target"`
    Migration MigrationConfig `yaml:"migration" json:"migration"`
    Compare   CompareConfig   `yaml:"compare" json:"compare"`
    Logging   LoggingConfig   `yaml:"logging" json:"logging"`
    Web       WebConfig       `yaml:"web" json:"web"`
    CDC       CDCConfig       `yaml:"cdc" json:"cdc"`   // ★ 新增
}

// CDCConfig 控制 CDC 增量同步模块是否启用及其行为。
// 默认 Enable=false：工具以纯全量迁移形态运行，CDC 子命令/Web 仪表盘不对外暴露。
type CDCConfig struct {
    // Enable 是 CDC 模块的总开关。false 时 CDC 命令/路由/仪表盘均隐藏或拒绝。
    Enable bool `yaml:"enable" json:"enable"`

    // 以下参数仅在 Enable=true 时生效；将 cmd/cdc.go 中 flag 默认值迁入此处。
    Mode             string   `yaml:"mode" json:"mode"`                         // full_incr | incr_only
    SlotName         string   `yaml:"slot_name" json:"slotName"`                // 默认 pg2tidb_cdc
    Publication      string   `yaml:"publication_name" json:"publicationName"`  // 默认 pg2tidb_pub
    BatchSize        int      `yaml:"batch_size" json:"batchSize"`              // 默认 1000
    Parallel         int      `yaml:"parallel" json:"parallel"`                // 默认 1（串行正确性优先；#t48 Bug#8 partition 修复后 >1 安全，是吞吐 opt-in）
    ConflictStrategy string   `yaml:"conflict_strategy" json:"conflictStrategy"`// replace|insert_ignore|upsert|skip
    SyncDDL          bool     `yaml:"sync_ddl" json:"syncDdl"`                  // 默认 true（取代硬编码）
    Tables           []string `yaml:"tables" json:"tables"`
    ExcludeTables    []string `yaml:"exclude_tables" json:"excludeTables"`
    CheckpointFile   string   `yaml:"checkpoint_file" json:"checkpointFile"`    // 默认 .cdc_checkpoint.json
}
```

**`DefaultConfig()` 增补**（默认全 false / 安全值）：

```go
CDC: CDCConfig{
    Enable:           false,   // ★ 默认关闭
    Mode:             "full_incr",
    SlotName:         "pg2tidb_cdc",
    Publication:      "pg2tidb_pub",
    BatchSize:        1000,
    Parallel:         1,
    ConflictStrategy: "replace",
    SyncDDL:          true,
    CheckpointFile:   ".cdc_checkpoint.json",
},
```

> **Parallel 默认值（2026-06-19 定）**：取 `1`（D1 #t51 已实现）。契合 §3.3「默认安全」+ 与 origin/main `c41b567`（#t48 Bug#8）落地一致。Bug#8 根因：applier 多 worker 共享 channel 致同表同行事件乱序、丢更新；`c41b567` 用 per-worker channel + FNV-1a 按表固定路由（同表串行、跨表并行）修复，并把默认 4→1。partition 修复后 `>1` 也正确（经测试工程师干净环境实测），故 `>1` 是安全的吞吐 opt-in——要吞吐的用户显式调高即可，默认 1 是保守项，非正确性限制。

**`Validate()` 增补**（仅在启用时校验 CDC 专属参数）：

```go
if c.CDC.Enable {
    if c.CDC.Mode != "full_incr" && c.CDC.Mode != "incr_only" {
        return fmt.Errorf("cdc.mode must be 'full_incr' or 'incr_only'")
    }
    if c.CDC.BatchSize <= 0 { return fmt.Errorf("cdc.batch_size must be positive") }
    if c.CDC.Parallel <= 0  { return fmt.Errorf("cdc.parallel must be positive") }
    // conflict_strategy 白名单校验
}
```

**`configs/config.yaml` 增补**（注释清晰说明默认关）：

```yaml
cdc:
  enable: false        # 可选模块，默认关闭。需要零停机迁移时设为 true。
  mode: "full_incr"    # full_incr(全量+增量) | incr_only(仅增量，需已有全量基线)
  sync_ddl: true
  batch_size: 1000
  parallel: 1
  conflict_strategy: "replace"
  slot_name: "pg2tidb_cdc"
  publication_name: "pg2tidb_pub"
  checkpoint_file: ".cdc_checkpoint.json"
```

**`LoadWithOverrides()` 增补**：支持 `cdc.enable` 覆盖键，供 Web/API 注入。

---

### 5.2 CLI 层（`cmd/cdc.go` + `cmd/root.go`）

**覆盖入口（优先级：flag > env > 配置文件）**：

- 全局 flag：`--enable-cdc` / `--disable-cdc`（布尔，作用于本次运行）。
- 环境变量：`PG2TIDB_CDC_ENABLE=true|false`（12-factor 友好，便于容器化部署）。
- 配置文件：`cdc.enable`。

**`cmd/cdc.go` 行为变更**：

```go
// 归并出最终 enable 值
enabled := resolveCDCEnable(cfg.CDC.Enable, cmd)  // flag > env > yaml

if !enabled {
    // 显式执行 cdc 子命令 = 用户明确要用 CDC → 不硬拦死，而是提示并询问是否本次开启
    fmt.Fprintln(os.Stderr,
        "CDC 模块当前未启用（cdc.enable=false）。")
    if !cmd.Flags().Changed("enable-cdc") && os.Getenv("PG2TIDB_CDC_FORCE") != "1" {
        fmt.Fprintln(os.Stderr,
            "如需本次运行，加 --enable-cdc；如需持久开启，在 config.yaml 设 cdc.enable: true。")
        return fmt.Errorf("cdc module disabled")
    }
    // 用户显式 --enable-cdc 或 PG2TIDB_CDC_FORCE=1 → 放行
}

// EnableDDLTracking 不再硬编码 true，改读 cfg.CDC.SyncDDL
runnerCfg := cdc.RunnerConfig{
    ...
    EnableDDLTracking: cfg.CDC.SyncDDL,
}
```

**参数读取顺序**：flag > 配置文件 > 默认值。即 `cmd/cdc.go` 中所有 `cmd.Flags().GetXxx()` 的「空则回退」目标，从硬编码默认改为 `cfg.CDC.Xxx`。

**`cmd/root.go`**：在 root 的 Long 描述里注明「CDC 为可选模块，默认关闭」，避免命令列表让人误以为 CDC 是默认能力。

> **非破坏性说明**：默认 `false` 只影响「未显式 opt-in」的场景。用户手动 `pg2tidb cdc` 是强意图，给出明确提示和一行开启方式即可，不静默吞掉。已有 CDC 用户加一次 `--enable-cdc` 或在配置里写 `cdc.enable: true` 即恢复原行为。

---

### 5.3 Web 层（`internal/webapi/` + `web/`）

**API（`server.go`）**：

```go
// 构造 Server 时传入 cdcEnabled（来自 config）
if !cdcEnabled {
    // 方案 A（推荐）：路由不挂载，/api/v1/cdc/* 返回 404
    // 方案 B：仍挂载 status，但返回稳定的「模块未启用」状态
    r.Get("/cdc/status", s.handleCDCDisabled)   // {"enabled": false, "message": "CDC module disabled"}
} else {
    r.Get("/cdc/status", s.handleCDCStatus)
}
```

新增一个 `GET /api/v1/features`（或复用 `/health`）暴露各模块开关，前端据此决定是否渲染 CDCView：

```json
{ "cdc": { "enabled": false } }
```

**前端（`CDCView.vue`）**：根据 `/features` 或 `/cdc/status` 的 `enabled` 字段，**条件渲染**——关闭时不显示 CDC 仪表盘入口，迁移向导里也不出现「CDC 追赶 / 稳态同步」阶段（本来 `all` 流水线也没有，主要是导航/菜单层面隐藏）。

**`webapi.NewServer` 签名**：增加 `cdcEnabled bool`（或传入整个 `CDCConfig`），由 `cmd/web.go` 从 `cfg.CDC.Enable` 注入。

---

### 5.4 构建层（可选增强，独立立项）

用 Go build tag 把 `internal/cdc/` 整包编译排除，产出不含 pgreplication 依赖的最小全量迁移二进制：

- tag `cdc`（默认包含）/ tag `nocdc`（裁剪）。
- `cmd/cdc.go` 在 `nocdc` 构建下不注册子命令。
- Makefile 增加 `make build-nocdc` 目标。

> 此项**非本期必做**，仅当需要最小化交付物（如瘦身镜像、降低供应链面）时再做。先以运行时开关满足需求。

---

## 6. 默认值与向后兼容矩阵

| 场景 | 旧行为 | 新行为（默认 cdc.enable=false） | 兼容处理 |
|------|--------|------------------------------|----------|
| `pg2tidb all` | 无 CDC | 无 CDC | 无变化 ✅ |
| `pg2tidb cdc`（老用户） | 直接跑 | 提示未启用 + 给一行开启方式；加 `--enable-cdc` 即恢复 | 一次性 opt-in |
| Web 仪表盘 | CDC 页常驻 | 默认隐藏 | 需 CDC 的用户开 `cdc.enable` |
| `/api/v1/cdc/status` | 常驻占位 | 默认 404 或返回 disabled | 前端按 features 开关渲染 |
| precheck | 无 CDC 检查 | 无 CDC 检查（启用时可选加 CDC 前置检查） | 无变化 |

---

## 7. 影响面与风险

| 风险 | 影响 | 应对 |
|------|------|------|
| 老 CDC 用户升级后 `pg2tidb cdc` 被拦 | 体验下降 | 提示明确 + `--enable-cdc` 一行恢复；发版说明写清 |
| Web 前端缓存旧 features | 仪表盘状态不一致 | features 接口禁缓存；前端启动即拉取 |
| `EnableDDLTracking` 由硬编码改为配置 | 默认值须保持 true | DefaultConfig 里 `SyncDDL: true`，行为不变 |
| 配置兼容 | 老 config.yaml 无 cdc 段 | yaml 反序列化缺省即用 DefaultConfig 的 false，向后兼容 ✅ |

---

## 8. 开发任务拆分（@开发工程师，可并行）

> 依赖：D1（配置层）先行 0.5 天，D2/D3 在 D1 合并后并行。建议一个父任务 + 三个子任务。

| 任务 | 内容 | 依赖 | 预估 |
|------|------|------|------|
| **D1 配置层** | `CDCConfig` 结构 + `Validate`/`Default`/`LoadWithOverrides` + `config.yaml` + `config_test.go`（开/关解析与校验） | 无 | 0.5d |
| **D2 CLI 层** | `cmd/cdc.go` 读 `cdc.enable`、flag/env 覆盖、`EnableDDLTracking` 改读配置、禁用提示；`root.go` 描述更新 | D1 | 0.5d |
| **D3 Web 层** | `NewServer` 注入 `cdcEnabled`、`/cdc/status` 禁用态、新增 `/features`、`CDCView.vue` 条件渲染、`cmd/web.go` 注入 | D1 | 0.5–1d |
| D4 构建裁剪（可选） | build tag `nocdc`、Makefile 目标、cmd 注册条件编译 | D1–D3 | 0.5d（独立立项） |

---

## 9. 测试任务拆分（@测试工程师，可与开发并行设计用例）

| 任务 | 内容 |
|------|------|
| **T1 单元测试** | ① `cdc.enable` 默认 false、yaml 开/关解析正确；② 启用态参数 Validate 通过、非法值拒绝；③ CLI `resolveCDCEnable` 优先级（flag>env>yaml）；④ Web 禁用态返回 disabled / 404。 |
| **T2 集成/E2E** | ① 默认配置下 `pg2tidb all` 全流程不受影响、无 CDC 残留；② 默认配置下 Web 仪表盘不显示 CDC、`/features.cdc.enabled=false`；③ 设 `cdc.enable=true` 后 CDC 子命令可跑、仪表盘可见；④ 老路径 `pg2tidb cdc --enable-cdc` 在默认关时仍可恢复运行。 |
| **T3 回归** | ① 已有 CDC 功能（增量同步、断点续传、DDL 同步）在 enable=true 下行为与改造前一致；② `config.yaml` 缺 cdc 段时按默认 false 加载、不报错。 |
| **T4 文档/Release** | README/CLI help 标注「CDC 可选模块、默认关闭」；升级说明写明 opt-in 方式。 |

---

## 10. 验收标准

1. 出厂 `configs/config.yaml` 中 `cdc.enable: false`，`pg2tidb all` 行为与改造前完全一致。
2. `cdc.enable=false` 时：Web 不渲染 CDC 仪表盘，`/api/v1/cdc/status` 返回禁用态（或 404），`pg2tidb cdc` 给出明确开启指引。
3. `cdc.enable=true` 时：CDC 子命令、Web 仪表盘、`/cdc/status` 全部恢复可用，功能与改造前等价（回归通过）。
4. `EnableDDLTracking` 默认值仍为 true（行为不变）。
5. 配置层单测、CLI/Web 禁用态测试、E2E、回归全部通过。

---

## 11. 决策（已确认 @刘源，2026-06-19）

- ✅ **默认值**：`cdc.enable` 默认 `false`。
- ✅ **CLI 拦截策略**：默认关时 `pg2tidb cdc`「提示 + `--enable-cdc` 放行」（非破坏）；`PG2TIDB_CDC_FORCE=1` 同样放行。
- ✅ **构建裁剪**：本期**不做** `nocdc`，仅运行时开关（§5.4 保留为未来可选项）。

> 方案锁定。开发按 D1 →（D2 ∥ D3）推进；测试 TE1 可与开发并行设计用例。
