package reporter

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewReport(t *testing.T) {
	r := NewReport("schema")
	if r.Phase != "schema" {
		t.Errorf("expected schema, got %s", r.Phase)
	}
	if len(r.Tables) != 0 {
		t.Error("should start with no tables")
	}
}

func TestAddTableReport(t *testing.T) {
	r := NewReport("data")
	r.AddTableReport(TableReport{
		TableName:  "users",
		Status:     StatusPass,
		SourceRows: 1000,
		TargetRows: 1000,
	})
	if len(r.Tables) != 1 {
		t.Error("should have 1 table")
	}
	if r.Tables[0].TableName != "users" {
		t.Errorf("expected users, got %s", r.Tables[0].TableName)
	}
}

func TestFinish(t *testing.T) {
	r := NewReport("validate")
	r.AddTableReport(TableReport{TableName: "t1", Status: StatusPass})
	r.AddTableReport(TableReport{TableName: "t2", Status: StatusFail})
	r.Finish(StatusFail, "1 table failed")

	if r.Status != StatusFail {
		t.Errorf("expected fail, got %s", r.Status)
	}
	if r.Stats.PassTables != 1 || r.Stats.FailTables != 1 {
		t.Errorf("expected 1 pass 1 fail, got %d pass %d fail", r.Stats.PassTables, r.Stats.FailTables)
	}
}

func TestOverallStatus(t *testing.T) {
	r := NewReport("test")
	r.AddTableReport(TableReport{TableName: "t1", Status: StatusPass})
	r.AddTableReport(TableReport{TableName: "t2", Status: StatusPass})
	r.Finish(StatusPass, "")
	if r.OverallStatus() != StatusPass {
		t.Error("all pass should be pass")
	}

	r2 := NewReport("test")
	r2.AddTableReport(TableReport{TableName: "t1", Status: StatusWarn})
	r2.Finish(StatusWarn, "")
	if r2.OverallStatus() != StatusWarn {
		t.Error("warn should be warn")
	}

	r3 := NewReport("test")
	r3.AddTableReport(TableReport{TableName: "t1", Status: StatusPass})
	r3.AddTableReport(TableReport{TableName: "t2", Status: StatusFail})
	r3.Finish(StatusFail, "")
	if r3.OverallStatus() != StatusFail {
		t.Error("fail should override pass")
	}
}

func TestSaveJSON(t *testing.T) {
	dir := t.TempDir()
	r := NewReport("schema")
	r.AddTableReport(TableReport{TableName: "users", Status: StatusPass, SourceRows: 100})
	r.Finish(StatusPass, "all good")

	path := filepath.Join(dir, "report.json")
	if err := r.SaveJSON(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("report file should exist")
	}
}

func TestSaveText(t *testing.T) {
	dir := t.TempDir()
	r := NewReport("data")
	r.AddTableReport(TableReport{TableName: "orders", Status: StatusPass, SourceRows: 5000, TargetRows: 5000})
	r.Finish(StatusPass, "migrated successfully")

	path := filepath.Join(dir, "report.txt")
	if err := r.SaveText(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Error("report should not be empty")
	}
}

func TestSaveAutoFormat(t *testing.T) {
	dir := t.TempDir()
	r := NewReport("test")
	r.Finish(StatusPass, "")

	jsonPath := filepath.Join(dir, "report.json")
	if err := r.Save(jsonPath); err != nil {
		t.Fatal(err)
	}

	txtPath := filepath.Join(dir, "report.txt")
	if err := r.Save(txtPath); err != nil {
		t.Fatal(err)
	}
}
