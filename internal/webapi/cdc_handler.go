package webapi

import (
	"errors"
	"net/http"
	"time"

	"github.com/pg2tidb/pg2tidb-migrator/internal/cdc"
)

// CDCStatusResponse is returned by GET /api/v1/cdc/status.
type CDCStatusResponse struct {
	Available     bool    `json:"available"`
	Enabled       bool    `json:"enabled"` // false when the CDC module is off (cdc.enable=false)
	Running       bool    `json:"running"`
	State         string  `json:"state"` // not_running | running | stale | halted
	Message       string  `json:"message,omitempty"`
	LSN           string  `json:"lsn,omitempty"`
	Slot          string  `json:"slot,omitempty"`
	Publication   string  `json:"publication,omitempty"`
	PID           int     `json:"pid,omitempty"`
	UptimeSeconds float64 `json:"uptime_seconds,omitempty"`
	FatalError    string  `json:"fatal_error,omitempty"`

	// Control is the supervisor's lifecycle view (CONTROL channel, #t55):
	// state/pid/restarts/uptime/adopted. Omitted when CDC control isn't wired.
	Control *CDCControlStatus `json:"control,omitempty"`
}

// cdcStatusView is the web's computed view of the CDC process, read from the
// status file. Liveness is computed via timestamp freshness, NOT the
// self-reported state. Contract: docs/cdc-web-monitoring-contract.md (#t48 B).
type cdcStatusView struct {
	State         string // not_running | running | stale | halted
	Running       bool
	LSN           string
	Slot          string
	Publication   string
	PID           int
	UptimeSeconds float64
	FatalError    string
	Stats         *cdc.CDCStatusStats // nil when there is no status file
	Checkpoint    *cdc.CDCStatusCheckpoint
}

// cdcStatusProvider abstracts how the web reads CDC state (a file reader by
// default; a mock in tests).
type cdcStatusProvider interface {
	StatusView() cdcStatusView
}

// fileCDCStatusProvider reads the CDC status JSON and computes liveness.
type fileCDCStatusProvider struct {
	path           string
	staleThreshold time.Duration
	pidAlive       func(int) bool
}

// NewFileCDCStatusProvider builds a provider that reads the CDC status file.
func NewFileCDCStatusProvider(path string, staleThreshold time.Duration) *fileCDCStatusProvider {
	return &fileCDCStatusProvider{path: path, staleThreshold: staleThreshold, pidAlive: pidAlive}
}

func (p *fileCDCStatusProvider) StatusView() cdcStatusView {
	st, err := cdc.ReadStatusFile(p.path)
	if err != nil {
		// Missing / unreadable / unparseable => not_running (never a 500).
		return cdcStatusView{State: string(cdc.LivenessNotRunning)}
	}
	live := cdc.ComputeLiveness(st, time.Now(), p.staleThreshold, p.pidAlive)
	stats := st.Stats
	cp := st.Checkpoint
	return cdcStatusView{
		State:         string(live),
		Running:       live == cdc.LivenessRunning,
		LSN:           st.LSN,
		Slot:          st.Slot,
		Publication:   st.Publication,
		PID:           st.PID,
		UptimeSeconds: stats.UptimeSeconds,
		FatalError:    st.FatalError,
		Stats:         &stats,
		Checkpoint:    &cp,
	}
}

// SetCDCStatusProvider wires the CDC status provider (called from cmd/web).
func (s *Server) SetCDCStatusProvider(p cdcStatusProvider) {
	s.cdcProvider = p
}

// cdcStatus returns the current CDC view via the configured provider (defaults
// to not_running when no provider is wired).
func (s *Server) cdcStatus() cdcStatusView {
	if s.cdcProvider == nil {
		return cdcStatusView{State: string(cdc.LivenessNotRunning)}
	}
	return s.cdcProvider.StatusView()
}

func cdcMessage(v cdcStatusView) string {
	switch v.State {
	case string(cdc.LivenessRunning):
		return "CDC running (LSN: " + v.LSN + ")"
	case string(cdc.LivenessHalted):
		if v.FatalError != "" {
			return "CDC halted: " + v.FatalError
		}
		return "CDC halted"
	case string(cdc.LivenessStale):
		return "CDC status is stale — the process may have crashed; showing last-known state. Check the process/logs."
	default:
		return "CDC not running. Start with: pg2tidb cdc"
	}
}

// handleCDCStatus handles GET /api/v1/cdc/status.
func (s *Server) handleCDCStatus(w http.ResponseWriter, r *http.Request) {
	// Optional module disabled (cdc.enable=false): return a stable disabled
	// state instead of probing the CDC process (D3 #t53).
	if !s.cdcEnabled {
		s.writeJSON(w, http.StatusOK, CDCStatusResponse{
			Available: false,
			Enabled:   false,
			State:     "disabled",
			Message:   "CDC module disabled. Set cdc.enable: true in config to enable.",
		})
		return
	}
	v := s.cdcStatus()
	resp := CDCStatusResponse{
		Available:     true,
		Enabled:       true,
		Running:       v.Running,
		State:         v.State,
		Message:       cdcMessage(v),
		LSN:           v.LSN,
		Slot:          v.Slot,
		Publication:   v.Publication,
		PID:           v.PID,
		UptimeSeconds: v.UptimeSeconds,
		FatalError:    v.FatalError,
	}
	if s.cdcSupervisor != nil {
		cs := s.cdcSupervisor.Status()
		resp.Control = &cs
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// handleCDCStart starts the CDC child via the supervisor (CONTROL channel, #t55).
// Idempotent. Returns 409 + guidance when cdc.enable is false (does not bypass #t50).
func (s *Server) handleCDCStart(w http.ResponseWriter, r *http.Request) {
	if s.cdcSupervisor == nil {
		s.writeJSON(w, http.StatusConflict, map[string]interface{}{
			"ok": false, "state": "disabled", "message": "CDC control is not wired on this server.",
		})
		return
	}
	st, err := s.cdcSupervisor.Start(r.Context())
	if errors.Is(err, ErrCDCDisabled) {
		s.writeJSON(w, http.StatusConflict, map[string]interface{}{
			"ok": false, "state": "disabled", "message": "CDC module disabled. Set cdc.enable: true first.",
		})
		return
	}
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "start cdc: "+err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok": true, "state": string(st.State), "pid": st.PID, "message": "CDC started",
	})
}

// handleCDCStop gracefully stops the CDC child (CONTROL channel, #t55). Idempotent.
func (s *Server) handleCDCStop(w http.ResponseWriter, r *http.Request) {
	if s.cdcSupervisor == nil {
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"ok": true, "state": "stopped", "message": "CDC control is not wired on this server.",
		})
		return
	}
	st := s.cdcSupervisor.Stop(r.Context())
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok": true, "state": string(st.State), "pid": st.PID, "message": "CDC stopped",
	})
}

// handleCDCStats handles GET /api/v1/cdc/stats (last-known stats; empty when not_running).
func (s *Server) handleCDCStats(w http.ResponseWriter, r *http.Request) {
	if !s.cdcEnabled {
		s.writeJSON(w, http.StatusOK, struct{}{})
		return
	}
	v := s.cdcStatus()
	if v.Stats == nil {
		s.writeJSON(w, http.StatusOK, struct{}{})
		return
	}
	s.writeJSON(w, http.StatusOK, v.Stats)
}

// handleCDCCheckpoint handles GET /api/v1/cdc/checkpoint.
func (s *Server) handleCDCCheckpoint(w http.ResponseWriter, r *http.Request) {
	if !s.cdcEnabled {
		s.writeJSON(w, http.StatusOK, struct{}{})
		return
	}
	v := s.cdcStatus()
	if v.Checkpoint == nil {
		s.writeJSON(w, http.StatusOK, struct{}{})
		return
	}
	s.writeJSON(w, http.StatusOK, v.Checkpoint)
}
