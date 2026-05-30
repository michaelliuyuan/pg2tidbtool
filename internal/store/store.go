package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type TaskStatus string

const (
	TaskStatusCreated   TaskStatus = "created"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusPaused    TaskStatus = "paused"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

type Task struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Status      TaskStatus `json:"status"`
	ConfigJSON  string     `json:"-"` // hidden from API, contains passwords
	Phase       string     `json:"phase"`
	Progress    float64    `json:"progress"`
	TablesTotal int        `json:"tables_total"`
	TablesDone  int        `json:"tables_done"`
	RowsTotal   int64      `json:"rows_total"`
	RowsDone    int64      `json:"rows_done"`
	Error       string     `json:"error,omitempty"`
	ResultJSON  string     `json:"result_json,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

type Store struct {
	mu sync.Mutex
	db *sql.DB
}

func NewStore(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	dbPath := filepath.Join(dataDir, "pg2tidb.db")
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	db.SetMaxOpenConns(1)

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS tasks (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'created',
			config_json TEXT NOT NULL DEFAULT '{}',
			phase TEXT NOT NULL DEFAULT '',
			progress REAL NOT NULL DEFAULT 0,
			tables_total INTEGER NOT NULL DEFAULT 0,
			tables_done INTEGER NOT NULL DEFAULT 0,
			rows_total INTEGER NOT NULL DEFAULT 0,
			rows_done INTEGER NOT NULL DEFAULT 0,
			error TEXT NOT NULL DEFAULT '',
			result_json TEXT NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			started_at DATETIME,
			finished_at DATETIME,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
		CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at DESC);
	`)
	return err
}

func (s *Store) CreateTask(task *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	task.CreatedAt = now
	task.UpdatedAt = now
	if task.Status == "" {
		task.Status = TaskStatusCreated
	}

	_, err := s.db.Exec(`
		INSERT INTO tasks (id, name, status, config_json, phase, progress, tables_total, tables_done,
			rows_total, rows_done, error, result_json, created_at, started_at, finished_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		task.ID, task.Name, task.Status, task.ConfigJSON, task.Phase, task.Progress,
		task.TablesTotal, task.TablesDone, task.RowsTotal, task.RowsDone,
		task.Error, task.ResultJSON, task.CreatedAt, task.StartedAt, task.FinishedAt, task.UpdatedAt)
	return err
}

func (s *Store) GetTask(id string) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t := &Task{}
	var startedAt, finishedAt sql.NullTime
	err := s.db.QueryRow(`
		SELECT id, name, status, config_json, phase, progress, tables_total, tables_done,
			rows_total, rows_done, error, result_json, created_at, started_at, finished_at, updated_at
		FROM tasks WHERE id = ?`, id).
		Scan(&t.ID, &t.Name, &t.Status, &t.ConfigJSON, &t.Phase, &t.Progress,
			&t.TablesTotal, &t.TablesDone, &t.RowsTotal, &t.RowsDone,
			&t.Error, &t.ResultJSON, &t.CreatedAt, &startedAt, &finishedAt, &t.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if startedAt.Valid {
		t.StartedAt = &startedAt.Time
	}
	if finishedAt.Valid {
		t.FinishedAt = &finishedAt.Time
	}
	return t, nil
}

func (s *Store) UpdateTaskStatus(id string, status TaskStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	switch status {
	case TaskStatusRunning:
		_, err := s.db.Exec(`UPDATE tasks SET status=?, started_at=?, updated_at=? WHERE id=?`,
			status, now, now, id)
		return err
	case TaskStatusCompleted, TaskStatusFailed, TaskStatusCancelled:
		_, err := s.db.Exec(`UPDATE tasks SET status=?, finished_at=?, updated_at=? WHERE id=?`,
			status, now, now, id)
		return err
	default:
		_, err := s.db.Exec(`UPDATE tasks SET status=?, updated_at=? WHERE id=?`,
			status, now, id)
		return err
	}
}

func (s *Store) UpdateTaskProgress(id string, phase string, progress float64, tablesDone, tablesTotal int, rowsDone, rowsTotal int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec(`
		UPDATE tasks SET phase=?, progress=?, tables_done=?, tables_total=?,
			rows_done=?, rows_total=?, updated_at=? WHERE id=?`,
		phase, progress, tablesDone, tablesTotal, rowsDone, rowsTotal, time.Now(), id)
	return err
}

func (s *Store) SetTaskError(id string, taskErr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	_, err := s.db.Exec(`UPDATE tasks SET status='failed', error=?, finished_at=?, updated_at=? WHERE id=?`,
		taskErr, now, now, id)
	return err
}

func (s *Store) SetTaskResult(id string, result interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`UPDATE tasks SET result_json=?, updated_at=? WHERE id=?`,
		string(data), time.Now(), id)
	return err
}

func (s *Store) ListTasks(limit, offset int) ([]*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if limit <= 0 {
		limit = 50
	}

	rows, err := s.db.Query(`
		SELECT id, name, status, config_json, phase, progress, tables_total, tables_done,
			rows_total, rows_done, error, result_json, created_at, started_at, finished_at, updated_at
		FROM tasks ORDER BY created_at DESC LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*Task
	for rows.Next() {
		t := &Task{}
		var startedAt, finishedAt sql.NullTime
		if err := rows.Scan(&t.ID, &t.Name, &t.Status, &t.ConfigJSON, &t.Phase, &t.Progress,
			&t.TablesTotal, &t.TablesDone, &t.RowsTotal, &t.RowsDone,
			&t.Error, &t.ResultJSON, &t.CreatedAt, &startedAt, &finishedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		if startedAt.Valid {
			t.StartedAt = &startedAt.Time
		}
		if finishedAt.Valid {
			t.FinishedAt = &finishedAt.Time
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (s *Store) DeleteTask(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM tasks WHERE id=?`, id)
	return err
}
