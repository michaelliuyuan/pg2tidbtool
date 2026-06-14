package cdc

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// This file implements the CDC→Web cross-process status channel (#t48 B).
// Contract: docs/cdc-web-monitoring-contract.md. The CDC process atomically
// writes a status JSON; the web UI reads it. Liveness is judged by timestamp
// FRESHNESS (not the self-reported state) so a crashed CDC is detected.

// CDCSelfState is the CDC process's own reported state in the status file.
type CDCSelfState string

const (
	CDCSelfRunning CDCSelfState = "running"
	CDCSelfHalted  CDCSelfState = "halted" // setFatal (Part A) — honored by web even when fresh
)

// CDCStatusFile is the JSON the CDC process writes and the web UI reads.
type CDCStatusFile struct {
	Schema      int                  `json:"schema"`
	Timestamp   time.Time            `json:"timestamp"`
	PID         int                  `json:"pid"`
	Slot        string               `json:"slot"`
	Publication string               `json:"publication"`
	LSN         string               `json:"lsn"`
	State       CDCSelfState         `json:"state"`
	FatalError  string               `json:"fatal_error,omitempty"`
	Stats       CDCStatusStats       `json:"stats"`
	Checkpoint  CDCStatusCheckpoint  `json:"checkpoint"`
}

// CDCStatusStats holds the apply-side counters the dashboard renders.
type CDCStatusStats struct {
	SourceEvents  int64   `json:"source_events"`
	Applied       int64   `json:"applied"`
	Failed        int64   `json:"failed"`
	Skipped       int64   `json:"skipped"`
	Batches       int64   `json:"batches"`
	ThroughputRPS float64 `json:"throughput_rps"`
	LagSeconds    float64 `json:"lag_seconds"`
	UptimeSeconds float64 `json:"uptime_seconds"`
	LastError     string  `json:"last_error,omitempty"`
}

// CDCStatusCheckpoint is the last-saved checkpoint snapshot.
type CDCStatusCheckpoint struct {
	LSN       string    `json:"lsn"`
	UpdatedAt time.Time `json:"updated_at"`
}

// WriteStatusFile atomically writes the status JSON (temp + rename) so a reader
// never sees a half-written file. A nil/empty path is a no-op (CDC disabled).
func WriteStatusFile(path string, st CDCStatusFile) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("cdc status: mkdir: %w", err)
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("cdc status: marshal: %w", err)
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".cdc-status-*.json.tmp")
	if err != nil {
		return fmt.Errorf("cdc status: temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("cdc status: write: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cdc status: close: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("cdc status: rename: %w", err)
	}
	cleanup = false
	return nil
}

// ReadStatusFile reads and parses the status JSON. Missing/unreadable/unparseable
// all return an error (the web reader treats any error as not_running).
func ReadStatusFile(path string) (CDCStatusFile, error) {
	var st CDCStatusFile
	data, err := os.ReadFile(path)
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return st, err
	}
	return st, nil
}

// LivenessState is the web-computed CDC liveness — NOT the self-reported state.
type LivenessState string

const (
	LivenessNotRunning LivenessState = "not_running" // no file / unreadable
	LivenessRunning    LivenessState = "running"     // fresh + pid alive
	LivenessStale      LivenessState = "stale"       // over threshold OR pid gone
	LivenessHalted     LivenessState = "halted"      // self-reported halt, honored even when fresh
)

// ComputeLiveness determines the web-facing liveness from a status record.
// Contract core: trust timestamp freshness + pid, NOT the self-reported `state`
// — except halted, which is honored even when fresh so the operator sees the
// fatal error (Part A).
func ComputeLiveness(st CDCStatusFile, now time.Time, staleThreshold time.Duration, pidAlive func(int) bool) LivenessState {
	if st.State == CDCSelfHalted {
		return LivenessHalted
	}
	if now.Sub(st.Timestamp) > staleThreshold {
		return LivenessStale
	}
	if pidAlive != nil && st.PID > 0 && !pidAlive(st.PID) {
		return LivenessStale
	}
	return LivenessRunning
}
