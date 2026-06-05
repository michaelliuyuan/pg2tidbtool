package webapi

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/checkpoint"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/logger"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/reporter"
	"github.com/pg2tidb/pg2tidb-migrator/internal/orchestrator"
	"github.com/pg2tidb/pg2tidb-migrator/internal/assess"
	"github.com/pg2tidb/pg2tidb-migrator/internal/store"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Server struct {
	router       chi.Router
	store        *store.Store
	addr         string
	hub          *Hub
	dataDir      string

	// running tasks
	runningTasks map[string]context.CancelFunc

	// log collection
	logCollector *LogCollector
	logCores     map[string]*TaskLogCore
}

type Hub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan []byte
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case conn := <-h.register:
			h.clients[conn] = true
		case conn := <-h.unregister:
			delete(h.clients, conn)
			conn.Close()
		case msg := <-h.broadcast:
			for conn := range h.clients {
				if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
					delete(h.clients, conn)
					conn.Close()
				}
			}
		}
	}
}

func NewServer(store *store.Store, host string, port int, dataDir string, staticFS embed.FS) *Server {
	s := &Server{
		store:        store,
		addr:         fmt.Sprintf("%s:%d", host, port),
		hub:          newHub(),
		runningTasks: make(map[string]context.CancelFunc),
		dataDir:      dataDir,
		logCollector: NewLogCollector(),
		logCores:     make(map[string]*TaskLogCore),
	}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/health", s.handleHealth)
		r.Post("/config/test-connection", s.handleTestConnection)
		r.Post("/config/list-tables", s.handleListTables)
		r.Post("/tasks", s.handleCreateTask)
		r.Get("/tasks", s.handleListTasks)
		r.Route("/tasks/{taskID}", func(r chi.Router) {
			r.Get("/", s.handleGetTask)
			r.Post("/start", s.handleStartTask)
			r.Post("/pause", s.handlePauseTask)
			r.Post("/resume", s.handleResumeTask)
			r.Post("/cancel", s.handleCancelTask)
			r.Delete("/", s.handleDeleteTask)
			r.Get("/progress", s.handleTaskProgress)
			r.Get("/report", s.handleTaskReport)
			r.Get("/logs", s.handleTaskLogs)
			r.Get("/phases", s.handleTaskPhases)
		})
		r.Get("/ws", s.handleWebSocket)
			r.Post("/assess", s.handleAssess)
	})

	if staticFS != (embed.FS{}) {
		staticContent, err := fs.Sub(staticFS, "static")
		if err == nil {
			fileServer := http.FileServer(http.FS(staticContent))
			r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
				path := r.URL.Path
				if path != "/" && !strings.HasPrefix(path, "/api/") && !strings.HasPrefix(path, "/assets/") {
					if !fileExists(staticContent, path) {
						r.URL.Path = "/"
					}
				}
				fileServer.ServeHTTP(w, r)
			})
		}
	}

	s.router = r
	return s
}

func fileExists(fsys fs.FS, path string) bool {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return false
	}
	f, err := fsys.Open(path)
	if err != nil {
		return false
	}
	f.Close()
	return true
}

func (s *Server) Start() error {
	go s.hub.Run()
	log := zap.L()
	log.Info("starting web server", zap.String("addr", s.addr))

	server := &http.Server{
		Addr:         s.addr,
		Handler:      s.router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	return server.ListenAndServe()
}

func (s *Server) BroadcastProgress(taskID string, progress map[string]interface{}) {
	progress["task_id"] = taskID
	data, err := json.Marshal(progress)
	if err != nil {
		return
	}
	select {
	case s.hub.broadcast <- data:
	default:
	}
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (s *Server) writeError(w http.ResponseWriter, status int, msg string) {
	s.writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "ok",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

type TestConnectionRequest struct {
	Type     string `json:"type"` // "source" or "target"
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
	Schema   string `json:"schema,omitempty"`
	SSLMode  string `json:"sslmode,omitempty"`
}

func (s *Server) handleTestConnection(w http.ResponseWriter, r *http.Request) {
	var req TestConnectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	var result map[string]interface{}
	switch req.Type {
	case "source":
		result = s.testPGConnection(r.Context(), &req)
	case "target":
		result = s.testTiDBConnection(r.Context(), &req)
	default:
		s.writeError(w, http.StatusBadRequest, "type must be 'source' or 'target'")
		return
	}
	s.writeJSON(w, http.StatusOK, result)
}

func (s *Server) testPGConnection(ctx context.Context, req *TestConnectionRequest) map[string]interface{} {
	cfg := config.SourceConfig{
		Host:     req.Host,
		Port:     req.Port,
		User:     req.User,
		Password: req.Password,
		Database: req.Database,
		Schema:   req.Schema,
		SSLMode:  req.SSLMode,
	}
	if cfg.Schema == "" {
		cfg.Schema = "public"
	}
	if cfg.SSLMode == "" {
		cfg.SSLMode = "disable"
	}

	start := time.Now()
	dsn := cfg.DSN()
	pgConn, err := openPGTestConn(dsn)
	elapsed := time.Since(start)

	result := map[string]interface{}{
		"type":     "source",
		"host":     cfg.Host,
		"port":     cfg.Port,
		"database": cfg.Database,
		"elapsed":  elapsed.String(),
	}

	if err != nil {
		result["ok"] = false
		result["error"] = err.Error()
		return result
	}
	defer pgConn.Close()

	var version string
	pgConn.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	result["ok"] = true
	result["version"] = version
	return result
}

func (s *Server) testTiDBConnection(ctx context.Context, req *TestConnectionRequest) map[string]interface{} {
	cfg := config.TargetConfig{
		Host:     req.Host,
		Port:     req.Port,
		User:     req.User,
		Password: req.Password,
		Database: req.Database,
	}

	start := time.Now()
	mysqlConn, err := openMySQLTestConn(cfg.DSN())
	elapsed := time.Since(start)

	result := map[string]interface{}{
		"type":     "target",
		"host":     cfg.Host,
		"port":     cfg.Port,
		"database": cfg.Database,
		"elapsed":  elapsed.String(),
	}

	if err != nil {
		result["ok"] = false
		result["error"] = err.Error()
		return result
	}
	defer mysqlConn.Close()

	var version string
	mysqlConn.QueryRowContext(ctx, "SELECT tidb_version()").Scan(&version)
	result["ok"] = true
	result["version"] = version
	return result
}

func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	var req TestConnectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Type != "source" {
		s.writeError(w, http.StatusBadRequest, "type must be 'source'")
		return
	}

	cfg := config.SourceConfig{
		Host:     req.Host,
		Port:     req.Port,
		User:     req.User,
		Password: req.Password,
		Database: req.Database,
		Schema:   req.Schema,
		SSLMode:  req.SSLMode,
	}
	if cfg.Schema == "" {
		cfg.Schema = "public"
	}
	if cfg.SSLMode == "" {
		cfg.SSLMode = "disable"
	}

	pgConn, err := openPGTestConn(cfg.DSN())
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "connect failed: "+err.Error())
		return
	}
	defer pgConn.Close()

	rows, err := pgConn.QueryContext(r.Context(), `
		SELECT table_name,
		       (SELECT reltuples::bigint FROM pg_class WHERE oid = (quote_ident($1)||'.'||quote_ident(t.table_name))::regclass) AS row_estimate
		FROM information_schema.tables t
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name`, cfg.Schema)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "query tables: "+err.Error())
		return
	}
	defer rows.Close()

	type TableInfo struct {
		Name       string `json:"name"`
		RowEstimate int64  `json:"row_estimate"`
	}
	var tables []TableInfo
	for rows.Next() {
		var ti TableInfo
		if err := rows.Scan(&ti.Name, &ti.RowEstimate); err != nil {
			continue
		}
		tables = append(tables, ti)
	}
	if tables == nil {
		tables = []TableInfo{}
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"tables": tables,
		"count":  len(tables),
	})
}

type CreateTaskRequest struct {
	Name   string         `json:"name"`
	Source config.SourceConfig   `json:"source"`
	Target config.TargetConfig   `json:"target"`
	Opts   MigrationOptsBody `json:"opts"`
}

type MigrationOptsBody struct {
	Parallel      int      `json:"parallel"`
	BatchSize     int      `json:"batch_size"`
	Tables        []string `json:"tables"`
	ExcludeTables []string `json:"exclude_tables"`
	UseLightning  bool     `json:"use_lightning"`
	SkipPrecheck  bool     `json:"skip_precheck"`
	SkipSchema    bool     `json:"skip_schema"`
	SkipData      bool     `json:"skip_data"`
	SkipValidate        bool    `json:"skip_validate"`
	TargetPolicy        string  `json:"target_policy"`
	CompareMode         string  `json:"compare_mode"`
	SampleRatio         float64 `json:"sample_ratio"`
	ChecksumChunkSize   int64   `json:"checksum_chunk_size"`
	ChecksumParallel    int     `json:"checksum_parallel"`
}

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Source.Host == "" || req.Target.Host == "" {
		s.writeError(w, http.StatusBadRequest, "source and target host are required")
		return
	}
	if req.Name == "" {
		req.Name = fmt.Sprintf("Migration %s", time.Now().Format("2006-01-02 15:04:05"))
	}

	task := &store.Task{
		ID:   uuid.New().String()[:8],
		Name: req.Name,
	}

	cfg := &config.Config{
		Source: req.Source,
		Target: req.Target,
		Migration: config.MigrationConfig{
			Parallel:            req.Opts.Parallel,
			BatchSize:           req.Opts.BatchSize,
			Tables:              req.Opts.Tables,
			ExcludeTables:       req.Opts.ExcludeTables,
			UseLightning:        req.Opts.UseLightning,
			TempDir:             "/tmp/pg2tidb",
			CheckpointDir:       fmt.Sprintf(".checkpoint/%s", task.ID),
			OnError:             "abort",
			TargetPolicy:        req.Opts.TargetPolicy,
			SkipPrecheck:        req.Opts.SkipPrecheck,
			SkipSchema:          req.Opts.SkipSchema,
			SkipData:            req.Opts.SkipData,
			SkipValidate:        req.Opts.SkipValidate,
		},
		Logging: config.LoggingConfig{Level: "info", Format: "console"},
		Compare: config.CompareConfig{
			CompareMode:       req.Opts.CompareMode,
			SampleRatio:       req.Opts.SampleRatio,
			ChecksumChunkSize: req.Opts.ChecksumChunkSize,
			ChecksumParallel:  req.Opts.ChecksumParallel,
		},
	}
	if cfg.Migration.Parallel <= 0 {
		cfg.Migration.Parallel = 4
	}
	if cfg.Migration.BatchSize <= 0 {
		cfg.Migration.BatchSize = 100000
	}

	cfgBytes, _ := json.Marshal(cfg)
	task.ConfigJSON = string(cfgBytes)

	if err := s.store.CreateTask(task); err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to create task: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusCreated, task)
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := s.store.ListTasks(50, 0)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tasks == nil {
		tasks = []*store.Task{}
	}
	s.writeJSON(w, http.StatusOK, tasks)
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	task, err := s.store.GetTask(taskID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if task == nil {
		s.writeError(w, http.StatusNotFound, "task not found")
		return
	}
	s.writeJSON(w, http.StatusOK, task)
}

func (s *Server) handleStartTask(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	task, err := s.store.GetTask(taskID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if task == nil {
		s.writeError(w, http.StatusNotFound, "task not found")
		return
	}
	if task.Status == store.TaskStatusRunning {
		s.writeError(w, http.StatusConflict, "task is already running")
		return
	}

	var cfg config.Config
	if err := json.Unmarshal([]byte(task.ConfigJSON), &cfg); err != nil {
		s.writeError(w, http.StatusInternalServerError, "invalid task config")
		return
	}

	if err := s.store.UpdateTaskStatus(taskID, store.TaskStatusRunning); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Reset all progress fields for a fresh run
	_ = s.store.ResetTaskForRerun(taskID)

	// Clear old logs and checkpoint data
	s.logCollector.RemoveBuffer(taskID)
	os.RemoveAll(fmt.Sprintf(".checkpoint/%s", taskID))

	ctx, cancel := context.WithCancel(context.Background())
	s.runningTasks[taskID] = cancel

	go s.runMigration(ctx, taskID, cfg)

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "started", "task_id": taskID})
}

func (s *Server) runMigration(ctx context.Context, taskID string, cfg config.Config) {
	logCore := NewTaskLogCore(s.logCollector, taskID, nil)
	s.logCores[taskID] = logCore

	defer func() {
		logCore.Disable()
		delete(s.logCores, taskID)
		logger.UnregisterExtraCore()
	}()

	s.logCollector.GetBuffer(taskID)
	s.logCollector.Append(taskID, "INFO", "Migration task started", "")

	logger.RegisterExtraCore(logCore)

	progressCtx, progressCancel := context.WithCancel(ctx)
	defer progressCancel()
	go s.pollProgress(progressCtx, taskID, cfg.Migration.CheckpointDir)

	o := orchestrator.NewOrchestrator(cfg)
	pipeCfg := orchestrator.PipelineConfig{
		SkipPrecheck: cfg.Migration.SkipPrecheck,
		SkipSchema:   cfg.Migration.SkipSchema,
		SkipData:     cfg.Migration.SkipData,
		SkipValidate: cfg.Migration.SkipValidate,
	}

	results, err := o.Run(ctx, pipeCfg)

	progressCancel()

	if ctx.Err() == context.Canceled {
		s.logCollector.Append(taskID, "WARN", "Migration task cancelled", "")
		s.store.UpdateTaskStatus(taskID, store.TaskStatusCancelled)
		return
	}

	if err != nil {
		s.logCollector.Append(taskID, "ERROR", "Migration failed: "+err.Error(), "")
		s.store.SetTaskError(taskID, err.Error())
		return
	}

	// Final progress sync from checkpoint
	if cpMgr, cpErr := checkpoint.NewReadOnlyManager(cfg.Migration.CheckpointDir); cpErr == nil {
		cpPhase := cpMgr.GetPhase()
		if cpPhase == "data-migration" || cpPhase == "data-export" || cpPhase == "data-import" {
			cpPhase = "data"
		}
		cpTables := cpMgr.GetAllTables()
		var tDone, tTotal int
		var rDone, rTotal int64
		for _, tc := range cpTables {
			tTotal++
			rTotal += tc.RowsTotal
			rDone += tc.RowsDone
			if tc.State == checkpoint.StateCompleted || tc.State == checkpoint.StateFailed {
				tDone++
			}
		}
		zap.L().Info("[DEBUG] final progress sync",
			zap.String("phase", cpPhase),
			zap.Int("tables_total", tTotal),
			zap.Int("tables_done", tDone),
			zap.Int64("rows_total", rTotal),
			zap.Int64("rows_done", rDone))
		var prog float64
		if rTotal > 0 {
			prog = float64(rDone) / float64(rTotal)
			if prog > 1.0 {
				prog = 1.0
			}
		} else if tTotal > 0 {
			prog = float64(tDone) / float64(tTotal)
		}
		_ = s.store.UpdateTaskProgress(taskID, cpPhase, prog, tDone, tTotal, rDone, rTotal)
		s.BroadcastProgress(taskID, map[string]interface{}{
			"phase":        cpPhase,
			"progress":     prog,
			"tables_done":  tDone,
			"tables_total": tTotal,
			"rows_done":    rDone,
			"rows_total":   rTotal,
		})
	}

	resultData, _ := json.Marshal(results)
	s.store.SetTaskResult(taskID, string(resultData))

	allSuccess := true
	for _, r := range results {
		if !r.Success {
			allSuccess = false
			break
		}
	}
	if allSuccess {
		s.logCollector.Append(taskID, "INFO", "Migration completed successfully", "")
		s.store.UpdateTaskStatus(taskID, store.TaskStatusCompleted)
	} else {
		s.logCollector.Append(taskID, "WARN", "Migration completed with errors", "")
		s.store.UpdateTaskStatus(taskID, store.TaskStatusFailed)
	}
}

func (s *Server) pollProgress(ctx context.Context, taskID string, checkpointDir string) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cpMgr, err := checkpoint.NewReadOnlyManager(checkpointDir)
			if err != nil {
				continue
			}

			phase := cpMgr.GetPhase()
			if phase == "data-migration" || phase == "data-export" || phase == "data-import" {
				phase = "data"
			}
			tables := cpMgr.GetAllTables()

			var tablesDone, tablesTotal int
			var rowsDone, rowsTotal int64
			for _, tc := range tables {
				tablesTotal++
				rowsTotal += tc.RowsTotal
				rowsDone += tc.RowsDone
				if tc.State == checkpoint.StateCompleted || tc.State == checkpoint.StateFailed {
					tablesDone++
				}
			}

			var progress float64
			if rowsTotal > 0 {
				progress = float64(rowsDone) / float64(rowsTotal)
				if progress > 1.0 {
					progress = 1.0
				}
			} else if tablesTotal > 0 {
				progress = float64(tablesDone) / float64(tablesTotal)
			}

			_ = s.store.UpdateTaskProgress(taskID, phase, progress, tablesDone, tablesTotal, rowsDone, rowsTotal)

			s.BroadcastProgress(taskID, map[string]interface{}{
				"phase":        phase,
				"progress":     progress,
				"tables_done":  tablesDone,
				"tables_total": tablesTotal,
				"rows_done":    rowsDone,
				"rows_total":   rowsTotal,
			})
		}
	}
}

func (s *Server) handlePauseTask(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	task, _ := s.store.GetTask(taskID)
	if task == nil {
		s.writeError(w, http.StatusNotFound, "task not found")
		return
	}
	if task.Status != store.TaskStatusRunning {
		s.writeError(w, http.StatusConflict, "task is not running")
		return
	}
	s.store.UpdateTaskStatus(taskID, store.TaskStatusPaused)
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

func (s *Server) handleResumeTask(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	task, _ := s.store.GetTask(taskID)
	if task == nil {
		s.writeError(w, http.StatusNotFound, "task not found")
		return
	}
	if task.Status != store.TaskStatusPaused {
		s.writeError(w, http.StatusConflict, "task is not paused")
		return
	}

	var cfg config.Config
	json.Unmarshal([]byte(task.ConfigJSON), &cfg)

	s.store.UpdateTaskStatus(taskID, store.TaskStatusRunning)
	ctx, cancel := context.WithCancel(context.Background())
	s.runningTasks[taskID] = cancel
	go s.runMigration(ctx, taskID, cfg)

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "resumed"})
}

func (s *Server) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if cancel, ok := s.runningTasks[taskID]; ok {
		cancel()
		delete(s.runningTasks, taskID)
	}
	s.store.UpdateTaskStatus(taskID, store.TaskStatusCancelled)
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

func (s *Server) handleDeleteTask(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	task, _ := s.store.GetTask(taskID)
	if task == nil {
		s.writeError(w, http.StatusNotFound, "task not found")
		return
	}
	if task.Status == store.TaskStatusRunning {
		s.writeError(w, http.StatusConflict, "cannot delete running task, cancel first")
		return
	}
	s.store.DeleteTask(taskID)
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *Server) handleTaskProgress(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	task, err := s.store.GetTask(taskID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if task == nil {
		s.writeError(w, http.StatusNotFound, "task not found")
		return
	}
	s.writeJSON(w, http.StatusOK, task)
}

func (s *Server) handleTaskReport(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	task, err := s.store.GetTask(taskID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if task == nil {
		s.writeError(w, http.StatusNotFound, "task not found")
		return
	}

	format := r.URL.Query().Get("format")
	switch format {
	case "html":
		report := s.buildTaskReport(task)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=report-%s.html", taskID))
		w.Write([]byte(report.ToHTML()))
	case "json":
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=report-%s.json", taskID))
		w.Write([]byte(task.ResultJSON))
	default:
		report := s.buildTaskReport(task)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=report-%s.html", taskID))
		w.Write([]byte(report.ToHTML()))
	}
}

func (s *Server) buildTaskReport(task *store.Task) *reporter.Report {
	report := reporter.NewReport("Migration Report")
	report.Tool = "TiMS"
	report.Version = "1.0.0"

	if task.StartedAt != nil {
		report.StartTime = *task.StartedAt
	}
	if task.FinishedAt != nil {
		report.EndTime = *task.FinishedAt
	}
	report.Duration = reporter.FormatDuration(report.EndTime.Sub(report.StartTime))

	if task.Status == store.TaskStatusCompleted {
		report.Status = reporter.StatusPass
	} else if task.Status == store.TaskStatusFailed {
		report.Status = reporter.StatusFail
	} else {
		report.Status = reporter.StatusWarn
	}

	// Build table reports from checkpoint data
	cfg := config.Config{}
	if task.ConfigJSON != "" {
		json.Unmarshal([]byte(task.ConfigJSON), &cfg)
	}
	cpDir := cfg.Migration.CheckpointDir
	if cpDir == "" {
		cpDir = fmt.Sprintf(".checkpoint/%s", task.ID)
	}
	if cpMgr, err := checkpoint.NewReadOnlyManager(cpDir); err == nil {
		for _, tc := range cpMgr.GetAllTables() {
			tr := reporter.TableReport{
				TableName:  tc.TableName,
				SourceRows: tc.RowsTotal,
				TargetRows: tc.RowsDone,
				Duration:   "",
			}
			if !tc.FinishedAt.IsZero() && !tc.StartedAt.IsZero() {
				tr.Duration = reporter.FormatDuration(tc.FinishedAt.Sub(tc.StartedAt))
			}
			switch tc.State {
			case checkpoint.StateCompleted:
				tr.Status = reporter.StatusPass
			case checkpoint.StateFailed:
				tr.Status = reporter.StatusFail
				tr.Error = tc.Error
			default:
				tr.Status = reporter.StatusSkip
			}
			if tr.SourceRows > 0 && tr.TargetRows > 0 {
				tr.DiffRows = tr.SourceRows - tr.TargetRows
			}
			report.AddTableReport(tr)
		}
	}

	// Build summary
	var summaryParts []string
	summaryParts = append(summaryParts, fmt.Sprintf("Source: %s:%d/%s", cfg.Source.Host, cfg.Source.Port, cfg.Source.Database))
	summaryParts = append(summaryParts, fmt.Sprintf("Target: %s:%d/%s", cfg.Target.Host, cfg.Target.Port, cfg.Target.Database))
	summaryParts = append(summaryParts, fmt.Sprintf("Tables: %d/%d completed", task.TablesDone, task.TablesTotal))
	summaryParts = append(summaryParts, fmt.Sprintf("Rows: %d/%d", task.RowsDone, task.RowsTotal))
	if task.Error != "" {
		summaryParts = append(summaryParts, fmt.Sprintf("Error: %s", task.Error))
	}
	report.Summary = strings.Join(summaryParts, " | ")

	report.Finish(report.Status, report.Summary)
	// Restore the actual times after Finish overwrites them
	if task.StartedAt != nil {
		report.StartTime = *task.StartedAt
	}
	if task.FinishedAt != nil {
		report.EndTime = *task.FinishedAt
	}
	report.Duration = reporter.FormatDuration(report.EndTime.Sub(report.StartTime))

	return report
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.hub.register <- conn

	defer func() {
		s.hub.unregister <- conn
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func (s *Server) handleTaskLogs(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	task, err := s.store.GetTask(taskID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if task == nil {
		s.writeError(w, http.StatusNotFound, "task not found")
		return
	}

	upgrade := r.URL.Query().Get("ws")
	if upgrade == "true" {
		s.handleLogStream(w, r, taskID)
		return
	}

	logs := s.logCollector.GetBuffer(taskID).GetAll()
	if logs == nil {
		logs = []TaskLogEntry{}
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"task_id": taskID,
		"logs":    logs,
		"count":   len(logs),
	})
}

func (s *Server) handleLogStream(w http.ResponseWriter, r *http.Request, taskID string) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	buf := s.logCollector.GetBuffer(taskID)
	ch := buf.Subscribe()
	defer buf.Unsubscribe(ch)

	existingLogs := buf.GetAll()
	for _, entry := range existingLogs {
		data, _ := json.Marshal(entry)
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			return
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case entry, ok := <-ch:
			if !ok {
				return
			}
			data, _ := json.Marshal(entry)
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}

func (s *Server) handleTaskPhases(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	task, err := s.store.GetTask(taskID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if task == nil {
		s.writeError(w, http.StatusNotFound, "task not found")
		return
	}

	type PhaseInfo struct {
		Name        string                   `json:"name"`
		Label       string                   `json:"label"`
		Status      string                   `json:"status"`
		SubLabel    string                   `json:"sub_label,omitempty"`
		Tables      []map[string]interface{} `json:"tables,omitempty"`
		TableCount  int                      `json:"table_count"`
		TablesDone  int                      `json:"tables_done"`
		RowsTotal   int64                    `json:"rows_total"`
		RowsDone    int64                    `json:"rows_done"`
		Logs        []map[string]interface{} `json:"logs,omitempty"`
	}

	phaseNames := []struct{ name, label string }{
		{"precheck", "预检查"},
		{"schema", "Schema 迁移"},
		{"data", "数据迁移"},
		{"validate", "数据验证"},
	}

	var phases []PhaseInfo
	for i, p := range phaseNames {
		pi := PhaseInfo{
			Name:   p.name,
			Label:  p.label,
			Status: "pending",
		}

		if task.Status == "completed" {
			pi.Status = "completed"
		} else if task.Phase == p.name {
			if task.Status == "running" {
				pi.Status = "running"
			} else if task.Status == "failed" {
				pi.Status = "failed"
			}
		} else if task.Phase != "" {
			curIdx := -1
			for j, pp := range phaseNames {
				if pp.name == task.Phase {
					curIdx = j
					break
				}
			}
			if curIdx >= 0 && i < curIdx {
				pi.Status = "completed"
			}
		}

		if p.name == "data" {
			cpMgr, cpErr := checkpoint.NewReadOnlyManager(fmt.Sprintf(".checkpoint/%s", taskID))
			if cpErr == nil {
				cpPhase := cpMgr.GetPhase()
				switch cpPhase {
				case "data-export":
					pi.SubLabel = "数据导出"
				case "data-import":
					pi.SubLabel = "数据导入"
				}
				tables := cpMgr.GetAllTables()
				if len(tables) > 0 {
					for _, tc := range tables {
						tableInfo := map[string]interface{}{
							"name":       tc.TableName,
							"state":      string(tc.State),
							"rows_done":  tc.RowsDone,
							"rows_total": tc.RowsTotal,
						}
						pi.Tables = append(pi.Tables, tableInfo)
						pi.TableCount++
						pi.RowsTotal += tc.RowsTotal
						pi.RowsDone += tc.RowsDone
						if tc.State == checkpoint.StateCompleted || tc.State == checkpoint.StateFailed {
							pi.TablesDone++
						}
					}
				}
			}
		}

		logs := s.logCollector.GetBuffer(taskID).GetAll()
		phaseLabel := p.label
		inPhase := false
		for _, entry := range logs {
			if strings.Contains(entry.Message, "Phase: "+phaseLabel) {
				inPhase = true
				continue
			}
			if inPhase {
				nextPhaseIdx := -1
				for _, pp := range phaseNames {
					if pp.label != phaseLabel && strings.Contains(entry.Message, "Phase: "+pp.label) {
						nextPhaseIdx = 1
						break
					}
				}
				if nextPhaseIdx >= 0 || strings.Contains(entry.Message, "Migration task started") || strings.Contains(entry.Message, "migration pipeline completed") {
					break
				}
				pi.Logs = append(pi.Logs, map[string]interface{}{
					"level":     entry.Level,
					"message":   entry.Message,
					"timestamp": entry.Timestamp,
				})
			}
		}

		phases = append(phases, pi)
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"task_id": taskID,
		"phase":   task.Phase,
		"phases":  phases,
	})
}

// handleAssess runs a compatibility assessment and returns JSON for the frontend.
func (s *Server) handleAssess(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		Database string `json:"database"`
		Schema   string `json:"schema"`
		Format   string `json:"format"` // "json" (default) or "html"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Host == "" || req.Database == "" {
		s.writeError(w, http.StatusBadRequest, "host and database are required")
		return
	}
	if req.Schema == "" {
		req.Schema = "public"
	}
	if req.Port == 0 {
		req.Port = 5432
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		req.User, req.Password, req.Host, req.Port, req.Database)

	pgDB, err := sql.Open("pgx", dsn)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "connect failed: "+err.Error())
		return
	}
	defer pgDB.Close()

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	// Scan and assess
	scanner := assess.NewScanner(pgDB, req.Schema)
	result, err := scanner.ScanAll(ctx)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "scan failed: "+err.Error())
		return
	}

	assessor := assess.NewAssessor()
	dims := assessor.Assess(result)

	rg := assess.NewReportGenerator(dims)

	// If HTML format requested, return HTML
	if req.Format == "html" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		rg.WriteHTML(w)
		return
	}

	// Default: return JSON
	report := rg.Report()
	s.writeJSON(w, http.StatusOK, report)
}
