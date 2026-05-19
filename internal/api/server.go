package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type StateReader interface {
	GetPhase() string
	GetAllTables() map[string]TableState
	Summary() (completed, failed, pending, running int)
}

type TableState struct {
	TableName string  `json:"table_name"`
	State     string  `json:"state"`
	RowsDone  int64   `json:"rows_done"`
	RowsTotal int64   `json:"rows_total"`
	Progress  float64 `json:"progress"`
	Error     string  `json:"error,omitempty"`
}

type Server struct {
	reader   StateReader
	addr     string
	server   *http.Server
	started  time.Time
}

func NewServer(reader StateReader, host string, port int) *Server {
	addr := fmt.Sprintf("%s:%d", host, port)
	return &Server{
		reader:  reader,
		addr:    addr,
		started: time.Now(),
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/status", s.handleStatus)
	mux.HandleFunc("/api/v1/tables", s.handleTables)
	mux.HandleFunc("/api/v1/validation", s.handleValidation)
	mux.HandleFunc("/api/v1/report", s.handleReport)
	mux.HandleFunc("/", s.handleUI)

	s.server = &http.Server{
		Addr:         s.addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("web server error: %v\n", err)
		}
	}()

	return nil
}

func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	c, f, p, running := s.reader.Summary()
	total := c + f + p + running

	resp := map[string]interface{}{
		"phase":       s.reader.GetPhase(),
		"started_at":  s.started.Format(time.RFC3339),
		"elapsed":     time.Since(s.started).String(),
		"total":       total,
		"completed":   c,
		"failed":      f,
		"pending":     p,
		"running":     running,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleTables(w http.ResponseWriter, r *http.Request) {
	tables := s.reader.GetAllTables()

	var result []TableState
	for _, t := range tables {
		progress := float64(0)
		if t.RowsTotal > 0 {
			progress = float64(t.RowsDone) / float64(t.RowsTotal)
			if progress > 1.0 {
				progress = 1.0
			}
		}
		result = append(result, TableState{
			TableName: t.TableName,
			State:     t.State,
			RowsDone:  t.RowsDone,
			RowsTotal: t.RowsTotal,
			Progress:  progress,
			Error:     t.Error,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (s *Server) handleValidation(w http.ResponseWriter, r *http.Request) {
	resp := map[string]interface{}{
		"status":  "not_available",
		"message": "validation results available after data validation phase",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleReport(w http.ResponseWriter, r *http.Request) {
	c, f, p, running := s.reader.Summary()
	resp := map[string]interface{}{
		"phase":     s.reader.GetPhase(),
		"completed": c,
		"failed":    f,
		"pending":   p,
		"running":   running,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(indexHTML))
}

const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>pg2tidb Migration Monitor</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f5f5; padding: 20px; }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 20px; }
        .status-bar { background: #fff; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); display: flex; justify-content: space-between; }
        .status-item { text-align: center; }
        .status-item .value { font-size: 24px; font-weight: bold; color: #1a73e8; }
        .status-item .label { font-size: 12px; color: #666; margin-top: 4px; }
        .tables { background: #fff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f8f9fa; font-weight: 600; color: #333; }
        .progress-bar { background: #e0e0e0; border-radius: 4px; height: 20px; overflow: hidden; }
        .progress-fill { height: 100%; background: #1a73e8; transition: width 0.5s; }
        .state-completed { color: #34a853; font-weight: bold; }
        .state-failed { color: #ea4335; font-weight: bold; }
        .state-running { color: #fbbc04; font-weight: bold; }
        .state-pending { color: #999; }
        #refresh-info { color: #999; font-size: 12px; margin-top: 10px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>pg2tidb Migration Monitor</h1>
        <div class="status-bar" id="status-bar">
            <div class="status-item"><div class="value" id="phase">-</div><div class="label">Phase</div></div>
            <div class="status-item"><div class="value" id="total">-</div><div class="label">Total Tables</div></div>
            <div class="status-item"><div class="value" id="completed">-</div><div class="label">Completed</div></div>
            <div class="status-item"><div class="value" id="running">-</div><div class="label">Running</div></div>
            <div class="status-item"><div class="value" id="failed">-</div><div class="label">Failed</div></div>
            <div class="status-item"><div class="value" id="elapsed">-</div><div class="label">Elapsed</div></div>
        </div>
        <div class="tables">
            <h2 style="margin-bottom:15px">Table Progress</h2>
            <table>
                <thead><tr><th>Table</th><th>State</th><th>Progress</th><th>Rows</th><th>Error</th></tr></thead>
                <tbody id="table-body"></tbody>
            </table>
        </div>
        <div id="refresh-info">Auto-refreshing every 2s</div>
    </div>
    <script>
        async function refresh() {
            try {
                const statusResp = await fetch('/api/v1/status');
                const status = await statusResp.json();
                document.getElementById('phase').textContent = status.phase || '-';
                document.getElementById('total').textContent = status.total || 0;
                document.getElementById('completed').textContent = status.completed || 0;
                document.getElementById('running').textContent = status.running || 0;
                document.getElementById('failed').textContent = status.failed || 0;
                document.getElementById('elapsed').textContent = status.elapsed || '-';

                const tablesResp = await fetch('/api/v1/tables');
                const tables = await tablesResp.json();
                const tbody = document.getElementById('table-body');
                tbody.innerHTML = '';
                (tables || []).forEach(t => {
                    const pct = (t.progress * 100).toFixed(1);
                    const cls = 'state-' + (t.state || 'pending').toLowerCase();
                    tbody.innerHTML += '<tr><td>' + t.table_name + '</td><td class="' + cls + '">' + t.state + '</td><td><div class="progress-bar"><div class="progress-fill" style="width:' + pct + '%"></div></div>' + pct + '%</td><td>' + t.rows_done + '/' + t.rows_total + '</td><td>' + (t.error || '') + '</td></tr>';
                });
            } catch(e) { console.error(e); }
        }
        refresh();
        setInterval(refresh, 2000);
    </script>
</body>
</html>`
