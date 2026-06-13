package cdc

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
)

// CDCState represents the current CDC pipeline state for the Web API.
type CDCState struct {
	Running    bool              `json:"running"`
	SourceLSN  string            `json:"source_lsn"`
	Checkpoint Checkpoint        `json:"checkpoint"`
	Stats      ApplierStats      `json:"stats"`
	Filter     TableFilterConfig `json:"filter"`
	Config     CDCConfigSummary  `json:"config"`
}

// TableFilterConfig is the serializable filter configuration.
type TableFilterConfig struct {
	IncludeTables   []string `json:"include_tables,omitempty"`
	ExcludeTables   []string `json:"exclude_tables,omitempty"`
	IncludeSchemas  []string `json:"include_schemas,omitempty"`
	ExcludeSchemas  []string `json:"exclude_schemas,omitempty"`
}

// CDCConfigSummary is a summary of CDC config for the API.
type CDCConfigSummary struct {
	SlotName         string `json:"slot_name"`
	Publication      string `json:"publication"`
	ConflictStrategy string `json:"conflict_strategy"`
	BatchSize        int    `json:"batch_size"`
	Parallel         int    `json:"parallel"`
}

// CDCAPI provides HTTP handlers for CDC status and control.
type CDCAPI struct {
	mu      sync.RWMutex
	runner  *Runner
	metrics *MetricsCollector

	// Control
	pauseCh  chan struct{}
	resumeCh chan struct{}
	paused   bool
}

// NewCDCAPI creates a new CDC API handler.
func NewCDCAPI() *CDCAPI {
	return &CDCAPI{
		pauseCh:  make(chan struct{}, 1),
		resumeCh: make(chan struct{}, 1),
	}
}

// SetRunner sets the CDC runner for status queries.
func (api *CDCAPI) SetRunner(runner *Runner) {
	api.mu.Lock()
	defer api.mu.Unlock()
	api.runner = runner
}

// SetMetrics sets the metrics collector.
func (api *CDCAPI) SetMetrics(mc *MetricsCollector) {
	api.mu.Lock()
	defer api.mu.Unlock()
	api.metrics = mc
}

// RegisterRoutes registers CDC API routes on the given mux.
func (api *CDCAPI) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/cdc/status", api.handleStatus)
	mux.HandleFunc("/api/v1/cdc/pause", api.handlePause)
	mux.HandleFunc("/api/v1/cdc/resume", api.handleResume)
	mux.HandleFunc("/api/v1/cdc/stats", api.handleStats)
	mux.HandleFunc("/api/v1/cdc/checkpoint", api.handleCheckpoint)
	mux.HandleFunc("/api/v1/cdc/filter", api.handleFilter)
}

func (api *CDCAPI) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	api.mu.RLock()
	defer api.mu.RUnlock()

	state := CDCState{
		Running: api.runner != nil && api.runner.source.IsRunning(),
	}

	if api.runner != nil {
		state.SourceLSN = api.runner.source.CurrentLSN().String()
		state.Checkpoint = api.runner.checkpoint.GetCheckpoint()
		state.Stats = api.runner.applier.Stats()

		// Config summary
		state.Config = CDCConfigSummary{
			SlotName:         api.runner.srcCfg.SlotName,
			Publication:      api.runner.srcCfg.Publication,
			ConflictStrategy: string(api.runner.batchCfg.ConflictStrategy),
			BatchSize:        api.runner.batchCfg.BatchSize,
			Parallel:         api.runner.batchCfg.Parallel,
		}

		// Filter config
		api.runner.filter.mu.RLock()
		state.Filter = TableFilterConfig{
			IncludeTables:  api.runner.filter.IncludeTables,
			ExcludeTables:  api.runner.filter.ExcludeTables,
			IncludeSchemas: api.runner.filter.IncludeSchemas,
			ExcludeSchemas: api.runner.filter.ExcludeSchemas,
		}
		api.runner.filter.mu.RUnlock()
	}

	writeJSON(w, http.StatusOK, state)
}

func (api *CDCAPI) handlePause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	api.mu.Lock()
	if api.paused {
		api.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]string{"status": "already_paused"})
		return
	}
	api.paused = true
	api.mu.Unlock()

	// Signal pause
	select {
	case api.pauseCh <- struct{}{}:
	default:
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

func (api *CDCAPI) handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	api.mu.Lock()
	if !api.paused {
		api.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]string{"status": "already_running"})
		return
	}
	api.paused = false
	api.mu.Unlock()

	// Signal resume
	select {
	case api.resumeCh <- struct{}{}:
	default:
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "resumed"})
}

func (api *CDCAPI) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	api.mu.RLock()
	defer api.mu.RUnlock()

	if api.metrics != nil {
		m := api.metrics.Snapshot()
		writeJSON(w, http.StatusOK, m)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"error": "metrics not available"})
}

func (api *CDCAPI) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		api.mu.RLock()
		defer api.mu.RUnlock()

		if api.runner != nil {
			cp := api.runner.checkpoint.GetCheckpoint()
			writeJSON(w, http.StatusOK, cp)
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "checkpoint not available"})

	case http.MethodPost:
		// Force save checkpoint
		api.mu.RLock()
		defer api.mu.RUnlock()

		if api.runner != nil {
			api.runner.checkpoint.Update(api.runner.source.CurrentLSN())
			if err := api.runner.checkpoint.Save(); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			cp := api.runner.checkpoint.GetCheckpoint()
			writeJSON(w, http.StatusOK, cp)
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "checkpoint not available"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (api *CDCAPI) handleFilter(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		api.mu.RLock()
		defer api.mu.RUnlock()

		if api.runner != nil {
			api.runner.filter.mu.RLock()
			cfg := TableFilterConfig{
				IncludeTables:  api.runner.filter.IncludeTables,
				ExcludeTables:  api.runner.filter.ExcludeTables,
				IncludeSchemas: api.runner.filter.IncludeSchemas,
				ExcludeSchemas: api.runner.filter.ExcludeSchemas,
			}
			api.runner.filter.mu.RUnlock()
			writeJSON(w, http.StatusOK, cfg)
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "filter not available"})

	case http.MethodPut:
		// Update filter at runtime
		var cfg TableFilterConfig
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		api.mu.RLock()
		defer api.mu.RUnlock()

		if api.runner != nil {
			api.runner.filter.WithWhitelist(cfg.IncludeTables).
				WithBlacklist(cfg.ExcludeTables).
				WithSchemas(cfg.IncludeSchemas, cfg.ExcludeSchemas)
			writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "filter not available"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// PauseCh returns the pause signal channel.
func (api *CDCAPI) PauseCh() <-chan struct{} {
	return api.pauseCh
}

// ResumeCh returns the resume signal channel.
func (api *CDCAPI) ResumeCh() <-chan struct{} {
	return api.resumeCh
}

// IsPaused returns whether CDC is currently paused.
func (api *CDCAPI) IsPaused() bool {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.paused
}

// writeJSON is a helper to write JSON responses.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// Ensure context import is used
var _ context.Context
