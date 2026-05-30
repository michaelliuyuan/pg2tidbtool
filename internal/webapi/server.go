package webapi

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	"github.com/pg2tidb/pg2tidb-migrator/internal/orchestrator"
	"github.com/pg2tidb/pg2tidb-migrator/internal/store"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Server struct {
	router chi.Router
	store  *store.Store
	addr   string
	hub    *Hub

	// running tasks
	runningTasks map[string]context.CancelFunc
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

func NewServer(store *store.Store, host string, port int, staticFS embed.FS) *Server {
	s := &Server{
		store:        store,
		addr:         fmt.Sprintf("%s:%d", host, port),
		hub:          newHub(),
		runningTasks: make(map[string]context.CancelFunc),
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
		})
		r.Get("/ws", s.handleWebSocket)
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
	SkipValidate  bool     `json:"skip_validate"`
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
			Parallel:      req.Opts.Parallel,
			BatchSize:     req.Opts.BatchSize,
			Tables:        req.Opts.Tables,
			ExcludeTables: req.Opts.ExcludeTables,
			UseLightning:  req.Opts.UseLightning,
			TempDir:       "/tmp/pg2tidb",
			CheckpointDir: fmt.Sprintf(".checkpoint/%s", task.ID),
			OnError:       "abort",
		},
		Logging: config.LoggingConfig{Level: "info", Format: "console"},
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

	ctx, cancel := context.WithCancel(context.Background())
	s.runningTasks[taskID] = cancel

	go s.runMigration(ctx, taskID, cfg)

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "started", "task_id": taskID})
}

func (s *Server) runMigration(ctx context.Context, taskID string, cfg config.Config) {
	_ = zap.L()
	o := orchestrator.NewOrchestrator(cfg)

	results, err := o.Run(ctx, orchestrator.PipelineConfig{})

	if ctx.Err() == context.Canceled {
		s.store.UpdateTaskStatus(taskID, store.TaskStatusCancelled)
		return
	}

	if err != nil {
		s.store.SetTaskError(taskID, err.Error())
		return
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
		s.store.UpdateTaskStatus(taskID, store.TaskStatusCompleted)
	} else {
		s.store.UpdateTaskStatus(taskID, store.TaskStatusFailed)
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
	case "json":
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=report-%s.json", taskID))
		w.Write([]byte(task.ResultJSON))
	default:
		report := map[string]interface{}{
			"task_id":     task.ID,
			"name":        task.Name,
			"status":      task.Status,
			"phase":       task.Phase,
			"progress":    task.Progress,
			"tables_done": task.TablesDone,
			"tables_total": task.TablesTotal,
			"rows_done":   task.RowsDone,
			"rows_total":  task.RowsTotal,
			"created_at":  task.CreatedAt,
			"started_at":  task.StartedAt,
			"finished_at": task.FinishedAt,
			"error":       task.Error,
		}
		if task.ResultJSON != "" {
			var results interface{}
			json.Unmarshal([]byte(task.ResultJSON), &results)
			report["results"] = results
		}
		s.writeJSON(w, http.StatusOK, report)
	}
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
