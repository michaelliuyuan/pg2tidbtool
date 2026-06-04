package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Source.Port != 5432 {
		t.Errorf("expected source port 5432, got %d", cfg.Source.Port)
	}
	if cfg.Target.Port != 4000 {
		t.Errorf("expected target port 4000, got %d", cfg.Target.Port)
	}
	if cfg.Migration.Parallel != 4 {
		t.Errorf("expected parallel 4, got %d", cfg.Migration.Parallel)
	}
	if cfg.Web.Enable != false {
		t.Error("web should be disabled by default")
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	content := []byte(`
source:
  host: "pg-host"
  port: 5433
  user: "admin"
  database: "testdb"
target:
  host: "tidb-host"
  port: 4001
  database: "testdb"
`)
	if err := os.WriteFile(cfgPath, content, 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Source.Host != "pg-host" {
		t.Errorf("expected source host pg-host, got %s", cfg.Source.Host)
	}
	if cfg.Source.Port != 5433 {
		t.Errorf("expected source port 5433, got %d", cfg.Source.Port)
	}
	if cfg.Target.Host != "tidb-host" {
		t.Errorf("expected target host tidb-host, got %s", cfg.Target.Host)
	}
}

func TestLoadMissingFile(t *testing.T) {
	cfg, err := Load("/nonexistent/config.yaml")
	if err != nil {
		t.Fatal("should return default config for missing file")
	}
	if cfg.Source.Port != 5432 {
		t.Error("should return defaults")
	}
}

func TestValidate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Source.Database = "testdb"
	cfg.Target.Database = "testdb"
	if err := cfg.Validate(); err != nil {
		t.Errorf("valid config should pass: %v", err)
	}

	cfg.Source.Host = ""
	if err := cfg.Validate(); err == nil {
		t.Error("missing source host should fail")
	}

	cfg.Source.Host = "localhost"
	cfg.Source.Database = ""
	if err := cfg.Validate(); err == nil {
		t.Error("missing source database should fail")
	}
}

func TestSourceDSN(t *testing.T) {
	cfg := SourceConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "pass",
		Database: "testdb",
		SSLMode:  "disable",
	}
	expected := "postgresql://postgres:pass@localhost:5432/testdb?sslmode=disable"
	if dsn := cfg.DSN(); dsn != expected {
		t.Errorf("expected %s, got %s", expected, dsn)
	}
}

func TestTargetDSN(t *testing.T) {
	cfg := TargetConfig{
		Host:     "127.0.0.1",
		Port:     4000,
		User:     "root",
		Password: "",
		Database: "testdb",
	}
	expected := "root:@tcp(127.0.0.1:4000)/testdb?charset=utf8mb4&parseTime=true&time_zone=%2B00%3A00&timeout=30s&readTimeout=300s&writeTimeout=300s"
	if dsn := cfg.DSN(); dsn != expected {
		t.Errorf("expected %s, got %s", expected, dsn)
	}
}

func TestLoadWithOverrides(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	content := []byte(`
source:
  host: "original"
  port: 5432
  database: "db"
target:
  host: "original"
  port: 4000
  database: "db"
`)
	os.WriteFile(cfgPath, content, 0644)

	overrides := map[string]string{
		"source.host": "overridden",
		"target.host": "overridden",
	}

	cfg, err := LoadWithOverrides(cfgPath, overrides)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Source.Host != "overridden" {
		t.Errorf("expected overridden, got %s", cfg.Source.Host)
	}
	if cfg.Target.Host != "overridden" {
		t.Errorf("expected overridden, got %s", cfg.Target.Host)
	}
}

func TestWebConfigValidation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Source.Database = "db"
	cfg.Target.Database = "db"
	cfg.Web.Enable = true
	cfg.Web.Port = 99999
	if err := cfg.Validate(); err == nil {
		t.Error("invalid web port should fail validation")
	}
}
