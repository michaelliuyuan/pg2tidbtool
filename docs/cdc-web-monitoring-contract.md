# CDC→Web Monitoring Contract (single source of truth)

The CDC incremental-sync process and the web UI are **separate processes**. This
document is the contract for how the web observes the running CDC. Implementation
must match it; if behavior changes, update this file first. (#t48 B)

## Topology (v1)

File-based, **same machine, different processes**: CDC writes a status JSON to a
shared path; the web reads it. True cross-machine (CDC on another node) is **v2**
(CDC exposes an HTTP `/status`, web fetches) — not blocking v1.

## Status file

- Path: configurable, default `cdc/status.json` (relative to each process's cwd).
  Same-machine deployment must point CDC (`pg2tidb cdc --status-file`) and web
  (`pg2tidb web --cdc-status-file`) at the **same path** (same cwd or absolute).
- **Atomic write** (temp + rename) — readers never see a half-written file.
- Cadence: rides CDC's checkpoint ticker (~10s) + a final write on shutdown. No
  extra lifecycle/goroutine.

### Schema (CDC writes)

```json
{
  "schema": 1,
  "timestamp": "2026-06-14T13:41:00+08:00",
  "pid": 12345,
  "slot": "pg2tidb_xxx",
  "publication": "pg2tidb_xxx",
  "lsn": "0/E2FCD270",
  "state": "running",
  "fatal_error": null,
  "stats": {
    "source_events": 0, "applied": 0, "failed": 0, "skipped": 0,
    "throughput_rps": 0, "lag_seconds": 0, "uptime_seconds": 0, "batches": 0
  },
  "checkpoint": { "lsn": "0/E2FC...", "updated_at": "..." }
}
```

`state`/`fatal_error` reflect Part A's `setFatal` — `halted` + the error when the
CDC halts on a parse failure. Fields come from `runner.Stats()` + applier stats +
checkpoint.

## Liveness (contract core — freshness, not the self-reported state)

The web does **not** trust `state`/`running` for liveness — it uses timestamp
freshness (+ pid cross-check), because a crashed CDC leaves a stale file still
claiming "running".

- `not_running` — file missing/unreadable → "Start with: pg2tidb cdc".
- `running` — `now - timestamp < stale_threshold` (default 30s ≈ 2–3× cadence) and pid alive.
- `stale` — file present but over threshold, **or** pid gone → show last-known
  lsn/stats (grayed) + "process may have crashed; check process/logs".
- `halted` — CDC self-reports `state:halted` (Part A fatal); honored **even when
  fresh** so the operator sees `fatal_error`.

`stale_threshold` is bound to the write cadence (≈2–3×) to avoid flap; both are
configurable. kill→stale worst case ≈ one tick (≤10s) + threshold.

CDC does **not** clean the file on crash — the stale file is intentional (lets
the web report stale + last-known state).

## Web API (replaces the prior 404s)

- `GET /api/v1/cdc/status` → `{ available, running, state, message, lsn, slot, publication, pid, uptime_seconds, fatal_error }`
- `GET /api/v1/cdc/stats` → the `stats` block (last-known; empty object if not_running)
- `GET /api/v1/cdc/checkpoint` → the `checkpoint` block (last-known; empty if not_running)

Read/parse failure → `not_running` (never HTTP 500).

## Acceptance

- CDC running → `/status` running + `/stats`/`/checkpoint` return real data; CDCView renders.
- kill CDC → within `stale_threshold` (worst case ~tick + threshold) the web shows `stale` with last-known values.
- Halt (Part A, e.g. injected parse failure) → `state:halted` + `fatal_error` even while fresh.
- pid in file but process gone → `stale`.

## Implementation

- CDC write: `internal/cdc/statusfile.go` (`WriteStatusFile`, `CDCStatusFile`),
  written from `runner.writeStatus()` (rides `cpTicker` + shutdown). Flag
  `pg2tidb cdc --status-file`.
- Web read: `internal/webapi/cdc_handler.go` (`fileCDCStatusProvider`,
  `handleCDCStatus/Stats/Checkpoint`), `pidalive_{unix,windows}.go`. Flags
  `pg2tidb web --cdc-status-file`, `--cdc-stale-threshold`.
- Commits: `e5a7708` (CDC write side), `04611c2` (web read side + endpoints).

## Known limitations (not full at-least-once)

This channel is **observability only** (read-only dashboard). CDC data-safety is
#t48 step 2 (Part A checkpoint-on-failure + Part B structural halt), which closes
*deterministic* silent-data-loss; the *probabilistic* "applier-lag + crash"
window remains until apply-driven ACK (near-term follow-up).
