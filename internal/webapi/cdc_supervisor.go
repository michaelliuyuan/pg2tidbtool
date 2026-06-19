package webapi

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/pg2tidb/pg2tidb-migrator/internal/cdc"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	"go.uber.org/zap"
)

// CDCSupervisor implements the CONTROL channel (#t55): the Web server spawns,
// supervises, restarts, and stops the CDC child process (`pg2tidb cdc`) on
// behalf of the UI, complementing the existing READ channel (status file →
// dashboard). The CDC sync internals (internal/cdc/) are unchanged — only the
// process lifecycle is managed here. Design: docs/cdc-web-control-design.md.

// CDCState is the supervisor's lifecycle state for the controlled CDC process.
type CDCState string

const (
	StateStopped  CDCState = "stopped"  // not running
	StateStarting CDCState = "starting" // spawning / backing off before restart
	StateRunning  CDCState = "running"  // child alive
	StateStopping CDCState = "stopping" // SIGTERM sent, awaiting exit / grace
	StateFailed   CDCState = "failed"   // crashed past the restart cap; needs human action
	StateAdopted  CDCState = "adopted"  // a CDC detected at web startup, monitored but not spawned
)

// Restart policy (locked by @刘源): exp backoff 1/2/4/8/16s, cap 5 attempts.
const (
	maxRestarts = 5
	stopGrace   = 15 * time.Second
)

// nextRestartBackoff returns the delay before restart attempt n (0-indexed:
// 0→1s, 1→2s, 2→4s, 3→8s, 4→16s). Past the last step it stays at 16s.
func nextRestartBackoff(n int) time.Duration {
	if n < 0 {
		n = 0
	}
	if n > 4 {
		n = 4
	}
	return time.Duration(1<<uint(n)) * time.Second
}

// supervisedProcess abstracts the CDC child so the supervisor is unit-testable
// without spawning real OS processes. The exec implementation wraps *exec.Cmd.
type supervisedProcess interface {
	Start() error
	Wait() error      // blocks until the process exits
	PID() int         // 0 before Start
	Terminate() error // graceful (SIGTERM on Unix; Kill on Windows)
	Kill() error      // forceful
}

// execProcess wraps a ready *exec.Cmd as a supervisedProcess.
type execProcess struct {
	cmd *exec.Cmd
}

func (p *execProcess) Start() error { return p.cmd.Start() }
func (p *execProcess) Wait() error  { return p.cmd.Wait() }
func (p *execProcess) PID() int {
	if p.cmd.Process != nil {
		return p.cmd.Process.Pid
	}
	return 0
}
func (p *execProcess) Terminate() error {
	if p.cmd.Process == nil {
		return nil
	}
	return terminateProc(p.cmd.Process)
}
func (p *execProcess) Kill() error {
	if p.cmd.Process == nil {
		return nil
	}
	return p.cmd.Process.Kill()
}

// ErrCDCDisabled is returned by Start when cdc.enable is false (does not bypass
// the #t50 optional-module semantics; the caller surfaces a 409 + guidance).
var ErrCDCDisabled = errors.New("cdc module disabled")

// CDCControlStatus is the supervisor's controlled-process view, surfaced via
// /cdc/status alongside the READ-channel business data.
type CDCControlStatus struct {
	State         CDCState `json:"state"`
	PID           int      `json:"pid"`
	Restarts      int      `json:"restarts"`
	UptimeSeconds float64  `json:"uptime_seconds"`
	Adopted       bool     `json:"adopted"`
}

// CDCSupervisor owns the CDC child lifecycle.
type CDCSupervisor struct {
	mu sync.Mutex

	cfg        config.CDCConfig
	binaryPath string // os.Executable(): the pg2tidb binary to re-exec as `cdc`
	cfgFile    string // -c passed to the spawned cdc so it loads the same config
	statusFile string // --status-file (must match the web's READ path)
	log        *zap.Logger

	// injectable for tests
	factory func() (supervisedProcess, error)
	backoff func(restart int) time.Duration

	proc          supervisedProcess
	state         CDCState
	startedAt     time.Time
	pid           int
	restarts      int
	stopRequested bool
	adopted       bool
	done          chan struct{} // closed when the supervise loop exits
	stopCh        chan struct{} // closed by Stop to interrupt backoff/restart
	stopOnce      sync.Once     // guards stopCh close across repeated Stop calls
}

// NewCDCSupervisor builds a supervisor. binaryPath/cfgFile/statusFile configure
// the spawned `pg2tidb cdc` child; statusFile must equal the path the web reads.
func NewCDCSupervisor(cfg config.CDCConfig, binaryPath, cfgFile, statusFile string, log *zap.Logger) *CDCSupervisor {
	s := &CDCSupervisor{
		cfg:        cfg,
		binaryPath: binaryPath,
		cfgFile:    cfgFile,
		statusFile: statusFile,
		log:        log,
		state:      StateStopped,
		backoff:    nextRestartBackoff,
	}
	s.factory = s.spawnExec
	return s
}

// SetFactory overrides the child factory (tests). Must be called before Start.
func (s *CDCSupervisor) SetFactory(f func() (supervisedProcess, error)) { s.factory = f }

// SetBackoff overrides the restart backoff (tests speed it up).
func (s *CDCSupervisor) SetBackoff(b func(int) time.Duration) { s.backoff = b }

// spawnExec is the default factory: re-execs this binary as `pg2tidb cdc`,
// forcing enable (the user clicked Start) and pinning the shared status file.
func (s *CDCSupervisor) spawnExec() (supervisedProcess, error) {
	if s.binaryPath == "" {
		return nil, fmt.Errorf("cdc supervisor: binary path unknown")
	}
	args := []string{"cdc", "--enable-cdc", "--status-file", s.statusFile}
	if s.cfgFile != "" {
		args = append(args, "-c", s.cfgFile)
	}
	cmd := exec.Command(s.binaryPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return &execProcess{cmd: cmd}, nil
}

// snapshot builds the current control status (caller may or may not hold mu;
// only call under lock or on a quiesced supervisor).
func (s *CDCSupervisor) snapshot() CDCControlStatus {
	st := CDCControlStatus{
		State:    s.state,
		PID:      s.pid,
		Restarts: s.restarts,
		Adopted:  s.adopted,
	}
	if (s.state == StateRunning || s.state == StateAdopted) && !s.startedAt.IsZero() {
		st.UptimeSeconds = time.Since(s.startedAt).Seconds()
	}
	return st
}

// Status returns the current control status (thread-safe).
func (s *CDCSupervisor) Status() CDCControlStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot()
}

// Start brings up the CDC child. Idempotent: a no-op if already running/starting.
// Returns ErrCDCDisabled (→ 409) when cdc.enable is false.
func (s *CDCSupervisor) Start(ctx context.Context) (CDCControlStatus, error) {
	if !s.cfg.Enable {
		return s.Status(), ErrCDCDisabled
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == StateRunning || s.state == StateStarting || s.state == StateAdopted {
		return s.snapshot(), nil // idempotent
	}
	proc, err := s.factory()
	if err != nil {
		s.state = StateFailed
		return s.snapshot(), fmt.Errorf("spawn cdc: %w", err)
	}
	if err := proc.Start(); err != nil {
		s.state = StateFailed
		return s.snapshot(), fmt.Errorf("start cdc: %w", err)
	}
	s.proc = proc
	s.pid = proc.PID()
	s.state = StateRunning
	s.startedAt = time.Now()
	s.restarts = 0
	s.stopRequested = false
	s.adopted = false
	s.done = make(chan struct{})
	s.stopCh = make(chan struct{})
	s.stopOnce = sync.Once{}
	go s.supervise(s.stopCh)
	return s.snapshot(), nil
}

// supervise watches the child: a graceful Stop exit → stopped; an unexpected
// exit → restart with backoff up to maxRestarts, else failed.
func (s *CDCSupervisor) supervise(stopCh chan struct{}) {
	for {
		proc := func() supervisedProcess {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.proc
		}()
		if proc == nil {
			return
		}
		_ = proc.Wait()

		s.mu.Lock()
		if s.stopRequested {
			s.toStoppedLocked()
			s.mu.Unlock()
			s.finish()
			return
		}
		if s.restarts >= maxRestarts {
			s.state = StateFailed
			s.proc = nil
			s.pid = 0
			s.mu.Unlock()
			s.finish()
			if s.log != nil {
				s.log.Error("cdc supervisor: child crashed past restart cap; state=failed")
			}
			return
		}
		attempt := s.restarts
		s.restarts++
		s.state = StateStarting
		s.proc = nil
		s.mu.Unlock()

		// back off before respawning (debounce crash loops, e.g. bad config)
		select {
		case <-time.After(s.backoff(attempt)):
		case <-stopCh:
			s.mu.Lock()
			s.toStoppedLocked()
			s.mu.Unlock()
			s.finish()
			return
		}

		s.mu.Lock()
		if s.stopRequested {
			s.toStoppedLocked()
			s.mu.Unlock()
			s.finish()
			return
		}
		proc2, err := s.factory()
		if err != nil {
			if s.restarts >= maxRestarts {
				s.state = StateFailed
				s.mu.Unlock()
				s.finish()
				return
			}
			s.mu.Unlock()
			continue
		}
		if err := proc2.Start(); err != nil {
			if s.restarts >= maxRestarts {
				s.state = StateFailed
				s.mu.Unlock()
				s.finish()
				return
			}
			s.mu.Unlock()
			continue
		}
		s.proc = proc2
		s.pid = proc2.PID()
		s.state = StateRunning
		s.mu.Unlock()
	}
}

func (s *CDCSupervisor) toStoppedLocked() {
	s.state = StateStopped
	s.proc = nil
	s.pid = 0
	s.stopRequested = false
}

// finish closes the done channel exactly once.
func (s *CDCSupervisor) finish() {
	s.mu.Lock()
	ch := s.done
	s.done = nil
	s.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// Stop gracefully stops the controlled CDC. Idempotent. SIGTERM → grace (15s)
// → SIGKILL. For an adopted (not-owned) process it terminates by PID.
func (s *CDCSupervisor) Stop(ctx context.Context) CDCControlStatus {
	s.mu.Lock()
	if s.state == StateStopped || s.state == StateFailed {
		s.mu.Unlock()
		return s.snapshot()
	}
	proc := s.proc
	pid := s.pid
	adopted := s.adopted
	done := s.done
	stopCh := s.stopCh
	s.stopRequested = true
	s.state = StateStopping
	s.mu.Unlock()

	if done != nil {
		// A supervise loop owns the lifecycle: signal it (abort any backoff) and
		// wait for it to clean up. Escalate to Kill after the grace period.
		if stopCh != nil {
			s.stopOnce.Do(func() { close(stopCh) })
		}
		if proc != nil {
			_ = proc.Terminate()
		}
		select {
		case <-done:
		case <-time.After(stopGrace):
			if proc != nil {
				_ = proc.Kill()
			}
			<-done
		}
	} else if adopted && pid > 0 {
		// Adopted process with no supervise loop: terminate by PID.
		terminatePID(pid)
		s.mu.Lock()
		s.toStoppedLocked()
		s.mu.Unlock()
	}
	return s.Status()
}

// terminatePID gracefully terminates a process by PID that the supervisor did
// not spawn (an adopted CDC). Cross-platform via terminateProc.
func terminatePID(pid int) {
	if pid <= 0 {
		return
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return
	}
	_ = terminateProc(p)
}

// Adopt probes for a CDC already running at web startup (e.g. web restarted
// while CDC was up). If the status file reports a live PID we did not spawn,
// mark the supervisor adopted/running and monitor it via the READ channel;
// Stop will terminate it by PID. Locked decision: adopt (领养监控).
func (s *CDCSupervisor) Adopt(statusPath string, stale time.Duration, alive func(int) bool) {
	st, err := cdc.ReadStatusFile(statusPath)
	if err != nil || st.PID <= 0 {
		return
	}
	if alive != nil && !alive(st.PID) {
		return
	}
	// A halted CDC is honored (operator should see it); a stale non-halted
	// record means the process is likely gone — don't adopt.
	if st.State != cdc.CDCSelfHalted && time.Since(st.Timestamp) > stale {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StateStopped {
		return // don't clobber an active supervised child
	}
	s.adopted = true
	s.pid = st.PID
	s.state = StateAdopted
	s.startedAt = time.Now()
}
