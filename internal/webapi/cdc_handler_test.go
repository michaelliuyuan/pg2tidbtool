package webapi

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/pg2tidb/pg2tidb-migrator/internal/cdc"
)

func writeCDCStatusFile(t *testing.T, path string, st cdc.CDCStatusFile) {
	t.Helper()
	if err := cdc.WriteStatusFile(path, st); err != nil {
		t.Fatal(err)
	}
}

// TestFileCDCStatusProvider guards the CDC dashboard's read path (#t48 B): read
// the status file, compute liveness via freshness/pid, surface stats/checkpoint.
func TestFileCDCStatusProvider(t *testing.T) {
	path := filepath.Join(t.TempDir(), "status.json")
	prov := &fileCDCStatusProvider{
		path:           path,
		staleThreshold: 30 * time.Second,
		pidAlive:       func(pid int) bool { return pid != 999 }, // 999 = dead
	}

	// 1. No file => not_running (never an error / 500).
	if v := prov.StatusView(); v.State != string(cdc.LivenessNotRunning) {
		t.Errorf("no file: state=%s, want not_running", v.State)
	}

	// 2. Fresh + running => running, stats/checkpoint populated.
	now := time.Now()
	writeCDCStatusFile(t, path, cdc.CDCStatusFile{
		State: cdc.CDCSelfRunning, Timestamp: now, PID: 1, LSN: "0/E1", Slot: "s",
		Stats:      cdc.CDCStatusStats{SourceEvents: 7, Applied: 6},
		Checkpoint: cdc.CDCStatusCheckpoint{LSN: "0/E1"},
	})
	v := prov.StatusView()
	if v.State != string(cdc.LivenessRunning) || !v.Running {
		t.Errorf("fresh: state=%s running=%v, want running", v.State, v.Running)
	}
	if v.Stats == nil || v.Stats.SourceEvents != 7 {
		t.Errorf("fresh: stats=%+v, want SourceEvents=7", v.Stats)
	}
	if v.Checkpoint == nil || v.Checkpoint.LSN != "0/E1" {
		t.Errorf("fresh: checkpoint=%+v, want LSN=0/E1", v.Checkpoint)
	}

	// 3. Halted (fresh) => halted + fatal_error, honored even when fresh.
	writeCDCStatusFile(t, path, cdc.CDCStatusFile{
		State: cdc.CDCSelfHalted, Timestamp: now, PID: 1, FatalError: "parse failed",
	})
	if v := prov.StatusView(); v.State != string(cdc.LivenessHalted) || v.FatalError != "parse failed" {
		t.Errorf("halted: state=%s fatal=%q, want halted/parse failed", v.State, v.FatalError)
	}

	// 4. Stale (over threshold) => stale, still returns last-known stats.
	writeCDCStatusFile(t, path, cdc.CDCStatusFile{
		State: cdc.CDCSelfRunning, Timestamp: now.Add(-2 * time.Minute), PID: 1, LSN: "0/E2",
		Stats: cdc.CDCStatusStats{SourceEvents: 9}, Checkpoint: cdc.CDCStatusCheckpoint{LSN: "0/E2"},
	})
	v = prov.StatusView()
	if v.State != string(cdc.LivenessStale) {
		t.Errorf("stale: state=%s, want stale", v.State)
	}
	if v.Stats == nil || v.Stats.SourceEvents != 9 {
		t.Errorf("stale: must still surface last-known stats, got %+v", v.Stats)
	}

	// 5. pid dead (fresh file) => stale.
	writeCDCStatusFile(t, path, cdc.CDCStatusFile{
		State: cdc.CDCSelfRunning, Timestamp: now, PID: 999,
	})
	if v := prov.StatusView(); v.State != string(cdc.LivenessStale) {
		t.Errorf("pid-dead: state=%s, want stale", v.State)
	}
}
