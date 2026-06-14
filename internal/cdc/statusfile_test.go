package cdc

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteReadStatusFile_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cdc", "status.json")
	st := CDCStatusFile{
		Schema: 1, Timestamp: time.Now(), PID: 123, Slot: "s", Publication: "p",
		LSN: "0/E1", State: CDCSelfRunning,
		Stats:      CDCStatusStats{SourceEvents: 5, Applied: 4, Failed: 1},
		Checkpoint: CDCStatusCheckpoint{LSN: "0/E1"},
	}
	if err := WriteStatusFile(path, st); err != nil {
		t.Fatalf("WriteStatusFile: %v", err)
	}
	got, err := ReadStatusFile(path)
	if err != nil {
		t.Fatalf("ReadStatusFile: %v", err)
	}
	if got.LSN != "0/E1" || got.Stats.SourceEvents != 5 || got.Slot != "s" || got.PID != 123 {
		t.Errorf("round-trip mismatch: %+v", got)
	}
}

func TestWriteStatusFile_EmptyPathNoop(t *testing.T) {
	if err := WriteStatusFile("", CDCStatusFile{}); err != nil {
		t.Errorf("empty path should be a no-op, got %v", err)
	}
}

func TestReadStatusFile_MissingIsError(t *testing.T) {
	if _, err := ReadStatusFile(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Error("missing file must return an error (web treats any error as not_running)")
	}
}

func TestWriteStatusFile_AtomicOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "status.json")
	if err := WriteStatusFile(path, CDCStatusFile{LSN: "0/1"}); err != nil {
		t.Fatal(err)
	}
	if err := WriteStatusFile(path, CDCStatusFile{LSN: "0/2"}); err != nil {
		t.Fatal(err)
	}
	got, _ := ReadStatusFile(path)
	if got.LSN != "0/2" {
		t.Errorf("after two writes LSN=%s, want 0/2", got.LSN)
	}
	// No leftover temp files from the temp+rename write.
	for _, e := range mustList(t, dir) {
		if e != "status.json" {
			t.Errorf("leftover temp file: %s", e)
		}
	}
}

func mustList(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// TestComputeLiveness guards the contract core: liveness comes from timestamp
// freshness + pid, NOT the self-reported state — except halted (Part A), which
// is honored even when fresh so the operator sees the fatal error.
func TestComputeLiveness(t *testing.T) {
	now := time.Now()
	pidAlive := func(pid int) bool { return pid != 999 } // 999 = dead process

	cases := []struct {
		name string
		st   CDCStatusFile
		want LivenessState
	}{
		{"running fresh", CDCStatusFile{State: CDCSelfRunning, Timestamp: now, PID: 1}, LivenessRunning},
		{"halted honored even when fresh", CDCStatusFile{State: CDCSelfHalted, Timestamp: now, PID: 1}, LivenessHalted},
		{"stale over threshold", CDCStatusFile{State: CDCSelfRunning, Timestamp: now.Add(-40 * time.Second), PID: 1}, LivenessStale},
		{"stale pid gone", CDCStatusFile{State: CDCSelfRunning, Timestamp: now, PID: 999}, LivenessStale},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ComputeLiveness(c.st, now, 30*time.Second, pidAlive); got != c.want {
				t.Errorf("got %s, want %s", got, c.want)
			}
		})
	}
}
