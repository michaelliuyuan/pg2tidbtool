package webapi

import (
	"encoding/json"
	"net/http"
)

// CDCStatusResponse is returned by the CDC status API endpoint.
type CDCStatusResponse struct {
	Available bool   `json:"available"`
	Running   bool   `json:"running"`
	Message   string `json:"message,omitempty"`
}

// cdcStateReader is an interface for reading CDC state.
// Implemented by cdc.CDCAPI or a mock.
type cdcStateReader interface {
	ReadStatus() (running bool, lsn string, err error)
}

// defaultCDCReader provides a no-op CDC state reader when CDC is not running.
type defaultCDCReader struct{}

func (r *defaultCDCReader) ReadStatus() (bool, string, error) {
	return false, "", nil
}

// SetCDCStateReader sets the CDC state reader for status queries.
var cdcReader cdcStateReader = &defaultCDCReader{}

// SetCDCReader replaces the current CDC state reader (called from main or orchestrator).
func SetCDCReader(r cdcStateReader) {
	cdcReader = r
}

// handleCDCStatus handles GET /api/v1/cdc/status
func (s *Server) handleCDCStatus(w http.ResponseWriter, r *http.Request) {
	running, lsn, err := cdcReader.ReadStatus()
	if err != nil {
		s.writeJSON(w, http.StatusOK, CDCStatusResponse{
			Available: true,
			Running:   false,
			Message:   err.Error(),
		})
		return
	}

	resp := CDCStatusResponse{
		Available: true,
		Running:   running,
	}
	if running {
		resp.Message = "LSN: " + lsn
	} else {
		resp.Message = "CDC not running. Start with: pg2tidb cdc"
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// writeJSON is a helper for JSON responses (used by cdc_handler).
func writeCDCJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
