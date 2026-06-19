package webapi

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pg2tidb/pg2tidb-migrator/internal/cdc"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
)

// fakeProc is a controllable supervisedProcess for unit tests. Wait blocks until
// the exit channel is closed (Terminate/Kill close it).
type fakeProc struct {
	pid      int
	exit     chan struct{}
	exitOnce sync.Once
	startErr error
	exitErr  error
	mu       sync.Mutex
	started  bool
	term     bool
	killed   bool
}

func newFakeProc(pid int) *fakeProc {
	return &fakeProc{pid: pid, exit: make(chan struct{})}
}
func (f *fakeProc) Start() error {
	f.mu.Lock()
	f.started = true
	f.mu.Unlock()
	return f.startErr
}
func (f *fakeProc) Wait() error { <-f.exit; return f.exitErr }
func (f *fakeProc) PID() int    { return f.pid }
func (f *fakeProc) Terminate() error {
	f.mu.Lock()
	f.term = true
	f.mu.Unlock()
	f.exitOnce.Do(func() { close(f.exit) })
	return nil
}
func (f *fakeProc) Kill() error {
	f.mu.Lock()
	f.killed = true
	f.mu.Unlock()
	f.exitOnce.Do(func() { close(f.exit) })
	return nil
}

// crashingProc exits immediately on Wait (simulates a crash loop).
type crashingProc struct{ pid int }

func (c *crashingProc) Start() error     { return nil }
func (c *crashingProc) Wait() error      { return nil }
func (c *crashingProc) PID() int         { return c.pid }
func (c *crashingProc) Terminate() error { return nil }
func (c *crashingProc) Kill() error      { return nil }

func newTestSupervisor(t *testing.T, enable bool) *CDCSupervisor {
	t.Helper()
	s := NewCDCSupervisor(config.CDCConfig{Enable: enable}, "/fake/bin", "test.yaml", "status.json", nil)
	s.SetBackoff(func(int) time.Duration { return 0 }) // instant restarts for fast tests
	return s
}

// waitForState polls until state matches want or the deadline elapses.
func waitForState(t *testing.T, s *CDCSupervisor, want CDCState, d time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if s.Status().State == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return s.Status().State == want
}

func TestNextRestartBackoff(t *testing.T) {
	cases := []struct {
		n    int
		want time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 16 * time.Second}, // clamped at the last step
		{99, 16 * time.Second},
	}
	for _, c := range cases {
		if got := nextRestartBackoff(c.n); got != c.want {
			t.Errorf("nextRestartBackoff(%d) = %v, want %v", c.n, got, c.want)
		}
	}
}

func TestStartDisabledGating(t *testing.T) {
	s := newTestSupervisor(t, false) // cdc.enable=false
	calls := 0
	s.SetFactory(func() (supervisedProcess, error) { calls++; return newFakeProc(1), nil })
	_, err := s.Start(context.Background())
	if !errors.Is(err, ErrCDCDisabled) {
		t.Fatalf("expected ErrCDCDisabled, got %v", err)
	}
	if calls != 0 {
		t.Errorf("disabled Start must not spawn, calls=%d", calls)
	}
	if st := s.Status(); st.State != StateStopped {
		t.Errorf("state=%s, want stopped", st.State)
	}
}

func TestStartIdempotent(t *testing.T) {
	s := newTestSupervisor(t, true)
	calls := 0
	s.SetFactory(func() (supervisedProcess, error) { calls++; return newFakeProc(calls), nil })
	st1, err := s.Start(context.Background())
	if err != nil || st1.State != StateRunning {
		t.Fatalf("first Start: %+v err=%v", st1, err)
	}
	st2, err := s.Start(context.Background())
	if err != nil || st2.State != StateRunning {
		t.Fatalf("second Start: %+v err=%v", st2, err)
	}
	if calls != 1 {
		t.Errorf("idempotent Start spawned %d times, want 1", calls)
	}
	if st := s.Stop(context.Background()); st.State != StateStopped {
		t.Errorf("cleanup Stop state=%s, want stopped", st.State)
	}
}

func TestStopIdempotent(t *testing.T) {
	s := newTestSupervisor(t, true)
	s.SetFactory(func() (supervisedProcess, error) { return newFakeProc(1), nil })
	if _, err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := s.Stop(context.Background()); st.State != StateStopped {
		t.Fatalf("first Stop state=%s, want stopped", st.State)
	}
	if st := s.Stop(context.Background()); st.State != StateStopped {
		t.Errorf("second Stop state=%s, want stopped (idempotent)", st.State)
	}
}

func TestCrashRestartCap(t *testing.T) {
	s := newTestSupervisor(t, true)
	calls := 0
	s.SetFactory(func() (supervisedProcess, error) {
		calls++
		return &crashingProc{pid: calls}, nil
	})
	if _, err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The supervisor respawns up to maxRestarts then goes failed.
	if !waitForState(t, s, StateFailed, 3*time.Second) {
		t.Fatalf("expected failed after crash cap, state=%s calls=%d", s.Status().State, calls)
	}
	// 1 initial spawn + maxRestarts respawns = 6 process creations.
	if calls != 1+maxRestarts {
		t.Errorf("crash loop spawned %d procs, want %d (1+maxRestarts)", calls, 1+maxRestarts)
	}
}

// TestAdoptRunningCDC: a fresh status file from a CDC we didn't spawn is adopted
// (领养监控). alive=nil trusts freshness (portable; no real PID probe).
func TestAdoptRunningCDC(t *testing.T) {
	dir := t.TempDir()
	statusPath := filepath.Join(dir, "status.json")
	if err := cdc.WriteStatusFile(statusPath, cdc.CDCStatusFile{
		Schema:    1,
		PID:       12345,
		Timestamp: time.Now(),
		State:     cdc.CDCSelfRunning,
	}); err != nil {
		t.Fatal(err)
	}
	s := newTestSupervisor(t, true)
	s.Adopt(statusPath, 30*time.Second, nil)
	if st := s.Status(); st.State != StateAdopted {
		t.Errorf("Adopt state=%s, want adopted", st.State)
	} else if st.PID != 12345 {
		t.Errorf("Adopt pid=%d, want 12345", st.PID)
	}
}
