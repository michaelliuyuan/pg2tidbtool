package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewManager(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "checkpoint")
	m, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m == nil {
		t.Fatal("manager should not be nil")
	}
}

func TestGetOrCreateTable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "checkpoint")
	m, _ := NewManager(dir)

	tc := m.GetOrCreateTable("users", 1000)
	if tc.TableName != "users" {
		t.Errorf("expected users, got %s", tc.TableName)
	}
	if tc.RowsTotal != 1000 {
		t.Errorf("expected 1000, got %d", tc.RowsTotal)
	}
	if tc.State != StatePending {
		t.Errorf("expected pending, got %s", tc.State)
	}

	tc2 := m.GetOrCreateTable("users", 2000)
	if tc2.RowsTotal != 1000 {
		t.Error("should return existing table, not create new")
	}
}

func TestMarkTableRunning(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "checkpoint")
	m, _ := NewManager(dir)
	m.GetOrCreateTable("orders", 5000)

	if err := m.MarkTableRunning("orders"); err != nil {
		t.Fatal(err)
	}
	tc, _ := m.GetTable("orders")
	if tc.State != StateRunning {
		t.Errorf("expected running, got %s", tc.State)
	}
}

func TestMarkTableCompleted(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "checkpoint")
	m, _ := NewManager(dir)
	m.GetOrCreateTable("orders", 5000)
	m.MarkTableRunning("orders")

	m.MarkTableCompleted("orders", 5000)
	tc, _ := m.GetTable("orders")
	if tc.State != StateCompleted {
		t.Errorf("expected completed, got %s", tc.State)
	}
	if tc.RowsDone != 5000 {
		t.Errorf("expected 5000, got %d", tc.RowsDone)
	}
}

func TestIsTableCompleted(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "checkpoint")
	m, _ := NewManager(dir)
	m.GetOrCreateTable("t1", 100)

	if m.IsTableCompleted("t1") {
		t.Error("should not be completed")
	}
	m.MarkTableCompleted("t1", 100)
	if !m.IsTableCompleted("t1") {
		t.Error("should be completed")
	}
}

func TestSummary(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "checkpoint")
	m, _ := NewManager(dir)
	m.GetOrCreateTable("t1", 100)
	m.GetOrCreateTable("t2", 100)
	m.GetOrCreateTable("t3", 100)

	m.MarkTableCompleted("t1", 100)
	m.MarkTableRunning("t2")
	m.MarkTableFailed("t3", "error")

	c, f, p, r := m.Summary()
	if c != 1 || f != 1 || r != 1 {
		t.Errorf("expected 1 completed, 1 failed, 0 pending, 1 running; got c=%d f=%d p=%d r=%d", c, f, p, r)
	}
}

func TestPersistence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "checkpoint")
	m1, _ := NewManager(dir)
	m1.GetOrCreateTable("users", 1000)
	m1.MarkTableCompleted("users", 1000)

	m2, _ := NewManager(dir)
	if !m2.IsTableCompleted("users") {
		t.Error("should persist and reload completed state")
	}
}

func TestCheckpointFileExists(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "checkpoint")
	m, _ := NewManager(dir)
	m.GetOrCreateTable("t1", 100)

	fp := filepath.Join(dir, "checkpoint.json")
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		t.Error("checkpoint file should exist after write")
	}
}
