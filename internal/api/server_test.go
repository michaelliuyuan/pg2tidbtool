package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type mockReader struct {
	phase  string
	tables map[string]TableState
}

func (m *mockReader) GetPhase() string { return m.phase }
func (m *mockReader) GetAllTables() map[string]TableState { return m.tables }
func (m *mockReader) Summary() (int, int, int, int) {
	c, f, p, r := 0, 0, 0, 0
	for _, t := range m.tables {
		switch t.State {
		case "completed":
			c++
		case "failed":
			f++
		case "running":
			r++
		default:
			p++
		}
	}
	return c, f, p, r
}

func TestHandleStatus(t *testing.T) {
	reader := &mockReader{
		phase: "data-migration",
		tables: map[string]TableState{
			"users":  {TableName: "users", State: "completed", RowsDone: 1000, RowsTotal: 1000},
			"orders": {TableName: "orders", State: "running", RowsDone: 500, RowsTotal: 2000},
		},
	}

	s := NewServer(reader, "0.0.0.0", 8080)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	w := httptest.NewRecorder()
	s.handleStatus(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["phase"] != "data-migration" {
		t.Errorf("expected data-migration, got %v", resp["phase"])
	}
}

func TestHandleTables(t *testing.T) {
	reader := &mockReader{
		phase: "data-migration",
		tables: map[string]TableState{
			"users":  {TableName: "users", State: "completed", RowsDone: 1000, RowsTotal: 1000},
			"orders": {TableName: "orders", State: "running", RowsDone: 500, RowsTotal: 2000},
		},
	}

	s := NewServer(reader, "0.0.0.0", 8080)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tables", nil)
	w := httptest.NewRecorder()
	s.handleTables(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var tables []TableState
	json.NewDecoder(w.Body).Decode(&tables)
	if len(tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(tables))
	}
}

func TestHandleUI(t *testing.T) {
	reader := &mockReader{phase: "idle"}
	s := NewServer(reader, "0.0.0.0", 8080)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	s.handleUI(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if w.Body.Len() == 0 {
		t.Error("UI should return HTML content")
	}
}

func TestHandleValidation(t *testing.T) {
	reader := &mockReader{phase: "idle"}
	s := NewServer(reader, "0.0.0.0", 8080)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/validation", nil)
	w := httptest.NewRecorder()
	s.handleValidation(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestServerStartStop(t *testing.T) {
	reader := &mockReader{phase: "idle"}
	s := NewServer(reader, "127.0.0.1", 0)

	if err := s.Stop(); err != nil {
		t.Error("stopping non-started server should not error")
	}
}
