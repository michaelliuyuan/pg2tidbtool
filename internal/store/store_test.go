package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewStore(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	dbPath := filepath.Join(dir, "pg2tidb.db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("database file not created")
	}
}

func TestCreateAndGetTask(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	task := &Task{
		ID:         "test001",
		Name:       "test task",
		Status:     TaskStatusCreated,
		ConfigJSON: `{"source":{"host":"localhost"}}`,
	}
	if err := s.CreateTask(task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	got, err := s.GetTask("test001")
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got == nil {
		t.Fatal("GetTask returned nil")
	}
	if got.Name != "test task" {
		t.Errorf("Name = %q, want %q", got.Name, "test task")
	}
	if got.Status != TaskStatusCreated {
		t.Errorf("Status = %q, want %q", got.Status, TaskStatusCreated)
	}
	if got.ConfigJSON != `{"source":{"host":"localhost"}}` {
		t.Errorf("ConfigJSON = %q, unexpected", got.ConfigJSON)
	}
}

func TestGetTaskNotFound(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	got, err := s.GetTask("nonexistent")
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got != nil {
		t.Error("expected nil for nonexistent task")
	}
}

func TestUpdateTaskStatus(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	task := &Task{ID: "test002", Name: "status test", Status: TaskStatusCreated}
	s.CreateTask(task)

	if err := s.UpdateTaskStatus("test002", TaskStatusRunning); err != nil {
		t.Fatalf("UpdateTaskStatus: %v", err)
	}

	got, _ := s.GetTask("test002")
	if got.Status != TaskStatusRunning {
		t.Errorf("Status = %q, want %q", got.Status, TaskStatusRunning)
	}
	if got.StartedAt == nil {
		t.Error("StartedAt should be set for running status")
	}

	if err := s.UpdateTaskStatus("test002", TaskStatusCompleted); err != nil {
		t.Fatalf("UpdateTaskStatus completed: %v", err)
	}

	got, _ = s.GetTask("test002")
	if got.Status != TaskStatusCompleted {
		t.Errorf("Status = %q, want %q", got.Status, TaskStatusCompleted)
	}
	if got.FinishedAt == nil {
		t.Error("FinishedAt should be set for completed status")
	}
}

func TestUpdateTaskProgress(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	task := &Task{ID: "test003", Name: "progress test", Status: TaskStatusRunning}
	s.CreateTask(task)

	if err := s.UpdateTaskProgress("test003", "data", 0.5, 5, 10, 50000, 100000); err != nil {
		t.Fatalf("UpdateTaskProgress: %v", err)
	}

	got, _ := s.GetTask("test003")
	if got.Phase != "data" {
		t.Errorf("Phase = %q, want %q", got.Phase, "data")
	}
	if got.Progress != 0.5 {
		t.Errorf("Progress = %f, want %f", got.Progress, 0.5)
	}
	if got.TablesDone != 5 || got.TablesTotal != 10 {
		t.Errorf("Tables = %d/%d, want 5/10", got.TablesDone, got.TablesTotal)
	}
	if got.RowsDone != 50000 || got.RowsTotal != 100000 {
		t.Errorf("Rows = %d/%d, want 50000/100000", got.RowsDone, got.RowsTotal)
	}
}

func TestSetTaskError(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	task := &Task{ID: "test004", Name: "error test", Status: TaskStatusRunning}
	s.CreateTask(task)

	if err := s.SetTaskError("test004", "connection refused"); err != nil {
		t.Fatalf("SetTaskError: %v", err)
	}

	got, _ := s.GetTask("test004")
	if got.Status != TaskStatusFailed {
		t.Errorf("Status = %q, want %q", got.Status, TaskStatusFailed)
	}
	if got.Error != "connection refused" {
		t.Errorf("Error = %q, want %q", got.Error, "connection refused")
	}
}

func TestListTasks(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	for i := 0; i < 5; i++ {
		task := &Task{
			ID:     fmt.Sprintf("list%03d", i),
			Name:   fmt.Sprintf("task %d", i),
			Status: TaskStatusCreated,
		}
		s.CreateTask(task)
		time.Sleep(10 * time.Millisecond)
	}

	tasks, err := s.ListTasks(10, 0)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 5 {
		t.Errorf("len(tasks) = %d, want 5", len(tasks))
	}

	// Should be ordered by created_at DESC
	if tasks[0].Name != "task 4" {
		t.Errorf("first task = %q, want %q", tasks[0].Name, "task 4")
	}
}

func TestDeleteTask(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	task := &Task{ID: "del001", Name: "delete me", Status: TaskStatusCreated}
	s.CreateTask(task)

	if err := s.DeleteTask("del001"); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}

	got, _ := s.GetTask("del001")
	if got != nil {
		t.Error("task should be deleted")
	}
}

func TestSetTaskResult(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer s.Close()

	task := &Task{ID: "result001", Name: "result test", Status: TaskStatusRunning}
	s.CreateTask(task)

	result := []map[string]interface{}{
		{"phase": "schema", "success": true},
		{"phase": "data", "success": true},
	}
	if err := s.SetTaskResult("result001", result); err != nil {
		t.Fatalf("SetTaskResult: %v", err)
	}

	got, _ := s.GetTask("result001")
	if got.ResultJSON == "" {
		t.Error("ResultJSON should not be empty")
	}
}
