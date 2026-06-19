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
	if cfg.CDC.Enable != false {
		t.Error("cdc should be disabled by default")
	}
}

func TestCDCConfigDefault(t *testing.T) {
	cdc := DefaultConfig().CDC
	if cdc.Enable {
		t.Error("cdc.enable should default to false")
	}
	if cdc.Mode != "full_incr" {
		t.Errorf("cdc.mode default = %q, want full_incr", cdc.Mode)
	}
	if cdc.SlotName != "pg2tidb_cdc" {
		t.Errorf("cdc.slot_name default = %q, want pg2tidb_cdc", cdc.SlotName)
	}
	if cdc.Publication != "pg2tidb_pub" {
		t.Errorf("cdc.publication default = %q, want pg2tidb_pub", cdc.Publication)
	}
	if cdc.BatchSize != 1000 {
		t.Errorf("cdc.batch_size default = %d, want 1000", cdc.BatchSize)
	}
	if cdc.Parallel != 1 {
		t.Errorf("cdc.parallel default = %d, want 1 (serial, correctness-first)", cdc.Parallel)
	}
	if cdc.ConflictStrategy != "replace" {
		t.Errorf("cdc.conflict_strategy default = %q, want replace", cdc.ConflictStrategy)
	}
	if !cdc.SyncDDL {
		t.Error("cdc.sync_ddl should default to true (preserve EnableDDLTracking behavior)")
	}
	if cdc.CheckpointFile != ".cdc_checkpoint.json" {
		t.Errorf("cdc.checkpoint_file default = %q, want .cdc_checkpoint.json", cdc.CheckpointFile)
	}
}

// When CDC is disabled, Validate must NOT check CDC params — so even garbage
// values must pass. This is the core "optional module" guarantee.
func TestCDCDisabledSkipsValidation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Source.Database = "db"
	cfg.Target.Database = "db"
	// Deliberately invalid CDC values, but module is off → validate passes.
	cfg.CDC.Enable = false
	cfg.CDC.Mode = "nonsense"
	cfg.CDC.BatchSize = -1
	cfg.CDC.Parallel = 0
	cfg.CDC.ConflictStrategy = "bogus"
	if err := cfg.Validate(); err != nil {
		t.Errorf("disabled CDC should skip validation, got: %v", err)
	}
}

func TestCDCEnabledValidation(t *testing.T) {
	base := func() *Config {
		c := DefaultConfig()
		c.Source.Database = "db"
		c.Target.Database = "db"
		c.CDC.Enable = true
		return c
	}

	// Valid enabled config passes.
	if err := base().Validate(); err != nil {
		t.Errorf("valid enabled CDC should pass, got: %v", err)
	}

	// Invalid mode.
	c := base()
	c.CDC.Mode = "full"
	if err := c.Validate(); err == nil {
		t.Error("invalid cdc.mode should fail validation when enabled")
	}

	// Invalid batch_size.
	c = base()
	c.CDC.BatchSize = 0
	if err := c.Validate(); err == nil {
		t.Error("non-positive cdc.batch_size should fail validation when enabled")
	}

	// Invalid parallel.
	c = base()
	c.CDC.Parallel = -1
	if err := c.Validate(); err == nil {
		t.Error("non-positive cdc.parallel should fail validation when enabled")
	}

	// Invalid conflict_strategy.
	c = base()
	c.CDC.ConflictStrategy = "overwrite"
	if err := c.Validate(); err == nil {
		t.Error("invalid cdc.conflict_strategy should fail validation when enabled")
	}
}

func TestCDCConfigLoad(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	content := []byte(`
source:
  host: "pg-host"
  database: "db"
target:
  host: "tidb-host"
  database: "db"
cdc:
  enable: true
  mode: "full_incr"
  slot_name: "custom_slot"
  publication_name: "custom_pub"
  batch_size: 500
  parallel: 2
  conflict_strategy: "upsert"
  sync_ddl: false
  tables: ["public.t1"]
  exclude_tables: ["public.t2"]
  checkpoint_file: "/var/run/cp.json"
`)
	if err := os.WriteFile(cfgPath, content, 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.CDC.Enable {
		t.Error("cdc.enable should be true from yaml")
	}
	if cfg.CDC.Mode != "full_incr" {
		t.Errorf("cdc.mode = %q, want full_incr", cfg.CDC.Mode)
	}
	if cfg.CDC.SlotName != "custom_slot" {
		t.Errorf("cdc.slot_name = %q, want custom_slot", cfg.CDC.SlotName)
	}
	if cfg.CDC.BatchSize != 500 {
		t.Errorf("cdc.batch_size = %d, want 500", cfg.CDC.BatchSize)
	}
	if cfg.CDC.Parallel != 2 {
		t.Errorf("cdc.parallel = %d, want 2", cfg.CDC.Parallel)
	}
	if cfg.CDC.ConflictStrategy != "upsert" {
		t.Errorf("cdc.conflict_strategy = %q, want upsert", cfg.CDC.ConflictStrategy)
	}
	if cfg.CDC.SyncDDL {
		t.Error("cdc.sync_ddl should be false from yaml")
	}
	if cfg.CDC.CheckpointFile != "/var/run/cp.json" {
		t.Errorf("cdc.checkpoint_file = %q", cfg.CDC.CheckpointFile)
	}
	if len(cfg.CDC.Tables) != 1 || cfg.CDC.Tables[0] != "public.t1" {
		t.Errorf("cdc.tables = %v, want [public.t1]", cfg.CDC.Tables)
	}
	// Loaded enabled config must also validate clean.
	if err := cfg.Validate(); err != nil {
		t.Errorf("loaded enabled CDC config should validate, got: %v", err)
	}
}

// Backward compatibility: a config file with NO cdc section must load with the
// CDC module disabled (defaults), and must validate.
func TestCDCConfigLoadNoSectionBackwardCompat(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	content := []byte(`
source:
  host: "pg-host"
  database: "db"
target:
  host: "tidb-host"
  database: "db"
`)
	if err := os.WriteFile(cfgPath, content, 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CDC.Enable {
		t.Error("missing cdc section should load as disabled")
	}
	if cfg.CDC.Mode != "full_incr" {
		t.Errorf("missing cdc section should default mode to full_incr, got %q", cfg.CDC.Mode)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("config without cdc section should validate, got: %v", err)
	}
}

func TestCDCEnableOverride(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	content := []byte(`
source:
  host: "pg-host"
  database: "db"
target:
  host: "tidb-host"
  database: "db"
cdc:
  enable: false
`)
	os.WriteFile(cfgPath, content, 0644)

	// Flip the switch on via override key.
	cfg, err := LoadWithOverrides(cfgPath, map[string]string{
		"cdc.enable":            "true",
		"cdc.parallel":          "8",
		"cdc.conflict_strategy": "skip",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.CDC.Enable {
		t.Error("cdc.enable override 'true' should enable the module")
	}
	if cfg.CDC.Parallel != 8 {
		t.Errorf("cdc.parallel override = %d, want 8", cfg.CDC.Parallel)
	}
	if cfg.CDC.ConflictStrategy != "skip" {
		t.Errorf("cdc.conflict_strategy override = %q, want skip", cfg.CDC.ConflictStrategy)
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
	expected := "root:@tcp(127.0.0.1:4000)/testdb?charset=utf8mb4&parseTime=true&timeout=30s&readTimeout=300s&writeTimeout=300s"
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
