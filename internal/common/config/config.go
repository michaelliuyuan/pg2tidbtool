package config

import (
	"fmt"
	"net/url"
	"os"
	"time"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Source    SourceConfig    `yaml:"source" json:"source"`
	Target    TargetConfig    `yaml:"target" json:"target"`
	Migration MigrationConfig `yaml:"migration" json:"migration"`
	Compare   CompareConfig   `yaml:"compare" json:"compare"`
	Logging   LoggingConfig   `yaml:"logging" json:"logging"`
	Web       WebConfig       `yaml:"web" json:"web"`
}

// CompareConfig controls data comparison/validation behavior, especially for
// tables without primary keys.
type CompareConfig struct {
	// NoPKStrategy selects the validation strategy for tables without primary keys:
	//   "auto"       — auto-select based on table structure and row count (default)
	//   "hash_group" — full row hash group counting (exact, detects missing/extra rows)
	//   "aggregate"  — full table aggregate hash (fast yes/no check)
	//   "bucket"     — hash-based bucketed comparison (for very large tables)
	NoPKStrategy string `yaml:"no_pk_strategy" json:"noPkStrategy"`

	// NoPKBucketCount is the number of buckets for the bucket strategy.
	NoPKBucketCount int `yaml:"no_pk_bucket_count" json:"noPkBucketCount"`

	// NoPKTableThreshold is the row count above which "auto" mode will choose
	// bucket or aggregate instead of hash_group.
	NoPKTableThreshold int64 `yaml:"no_pk_table_threshold" json:"noPkTableThreshold"`
}

type SourceConfig struct {
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"`
	User     string `yaml:"user" json:"user"`
	Password string `yaml:"password" json:"password"`
	Database string `yaml:"database" json:"database"`
	Schema   string `yaml:"schema" json:"schema"`
	SSLMode  string `yaml:"sslmode" json:"sslmode"`
}

func (s SourceConfig) DSN() string {
	sslmode := s.SSLMode
	if sslmode == "" {
		sslmode = "disable"
	}
	return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=%s",
		url.QueryEscape(s.User), url.QueryEscape(s.Password), s.Host, s.Port, s.Database, sslmode)
}

type TargetConfig struct {
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"`
	User     string `yaml:"user" json:"user"`
	Password string `yaml:"password" json:"password"`
	Database string `yaml:"database" json:"database"`
	PDAddr     string `yaml:"pd_addr" json:"pd_addr"`
	StatusPort int    `yaml:"status_port" json:"status_port"`
}

func (t TargetConfig) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&timeout=30s&readTimeout=300s&writeTimeout=300s",
		t.User, t.Password, t.Host, t.Port, t.Database)
}

type MigrationConfig struct {
	Parallel            int      `yaml:"parallel"`
	BatchSize           int      `yaml:"batch_size"`
	TempDir             string   `yaml:"temp_dir"`
	Tables              []string `yaml:"tables"`
	ExcludeTables       []string `yaml:"exclude_tables"`
	UseLightning        bool     `yaml:"use_lightning"`
	OnError             string   `yaml:"on_error"`
	CheckpointDir       string   `yaml:"checkpoint_dir"`
	ReadTimeout         string   `yaml:"read_timeout"`
	WriteTimeout        string   `yaml:"write_timeout"`
	TargetPolicy        string   `yaml:"target_policy"` // insert, truncate, drop
	LargeTableThreshold int64    `yaml:"large_table_threshold" json:"largeTableThreshold"`
	ChunkSize           int64    `yaml:"chunk_size" json:"chunkSize"`
	ChunkParallel       int      `yaml:"chunk_parallel" json:"chunkParallel"`
	SkipPrecheck        bool     `yaml:"skip_precheck" json:"skipPrecheck"`
	SkipSchema          bool     `yaml:"skip_schema" json:"skipSchema"`
	SkipData            bool     `yaml:"skip_data" json:"skipData"`
	SkipValidate        bool     `yaml:"skip_validate" json:"skipValidate"`
}

func (m MigrationConfig) ReadTimeoutDuration() time.Duration {
	if m.ReadTimeout == "" {
		return 30 * time.Minute
	}
	d, _ := time.ParseDuration(m.ReadTimeout)
	return d
}

func (m MigrationConfig) WriteTimeoutDuration() time.Duration {
	if m.WriteTimeout == "" {
		return 30 * time.Minute
	}
	d, _ := time.ParseDuration(m.WriteTimeout)
	return d
}

type LoggingConfig struct {
	Level  string `yaml:"level" json:"level"`
	Format string `yaml:"format" json:"format"`
	Output string `yaml:"output" json:"output"`
}

type WebConfig struct {
	Enable bool   `yaml:"enable" json:"enable"`
	Port   int    `yaml:"port" json:"port"`
	Host   string `yaml:"host" json:"host"`
}

func DefaultConfig() *Config {
	return &Config{
		Source: SourceConfig{
			Host:    "localhost",
			Port:    5432,
			User:    "postgres",
			Schema:  "public",
			SSLMode: "disable",
		},
		Target: TargetConfig{
			Host: "localhost",
			Port: 4000,
			User: "root",
		},
		Migration: MigrationConfig{
			Parallel:            4,
			BatchSize:           100000,
			TempDir:             "/tmp/pg2tidb",
			UseLightning:        true,
			OnError:             "abort",
			CheckpointDir:       ".checkpoint",
			ReadTimeout:         "30m",
			WriteTimeout:        "30m",
			LargeTableThreshold: 1000000,
			ChunkSize:           500000,
			ChunkParallel:       4,
			},
		Compare: CompareConfig{
			NoPKStrategy:       "auto",
			NoPKBucketCount:    100,
			NoPKTableThreshold: 1000000,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "console",
		},
		Web: WebConfig{
			Enable: false,
			Port:   8080,
			Host:   "0.0.0.0",
		},
	}
}

func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	if _, err := os.Stat(path); err == nil {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config file: %w", err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config file: %w", err)
		}
	}

	return cfg, nil
}

func LoadWithOverrides(path string, overrides map[string]string) (*Config, error) {
	cfg, err := Load(path)
	if err != nil {
		return nil, err
	}

	if v, ok := overrides["source.host"]; ok {
		cfg.Source.Host = v
	}
	if v, ok := overrides["source.port"]; ok {
		fmt.Sscanf(v, "%d", &cfg.Source.Port)
	}
	if v, ok := overrides["source.user"]; ok {
		cfg.Source.User = v
	}
	if v, ok := overrides["source.password"]; ok {
		cfg.Source.Password = v
	}
	if v, ok := overrides["source.database"]; ok {
		cfg.Source.Database = v
	}
	if v, ok := overrides["target.host"]; ok {
		cfg.Target.Host = v
	}
	if v, ok := overrides["target.port"]; ok {
		fmt.Sscanf(v, "%d", &cfg.Target.Port)
	}
	if v, ok := overrides["target.user"]; ok {
		cfg.Target.User = v
	}
	if v, ok := overrides["target.password"]; ok {
		cfg.Target.Password = v
	}
	if v, ok := overrides["target.database"]; ok {
		cfg.Target.Database = v
	}
	if v, ok := overrides["target.pd_addr"]; ok {
		cfg.Target.PDAddr = v
	}
	if v, ok := overrides["migration.parallel"]; ok {
		fmt.Sscanf(v, "%d", &cfg.Migration.Parallel)
	}
	if v, ok := overrides["migration.batch_size"]; ok {
		fmt.Sscanf(v, "%d", &cfg.Migration.BatchSize)
	}
	if v, ok := overrides["logging.level"]; ok {
		cfg.Logging.Level = v
	}
	if v, ok := overrides["logging.format"]; ok {
		cfg.Logging.Format = v
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.Source.Host == "" {
		return fmt.Errorf("source.host is required")
	}
	if c.Source.Database == "" {
		return fmt.Errorf("source.database is required")
	}
	if c.Source.Port <= 0 {
		return fmt.Errorf("source.port must be positive")
	}
	if c.Target.Host == "" {
		return fmt.Errorf("target.host is required")
	}
	if c.Target.Database == "" {
		return fmt.Errorf("target.database is required")
	}
	if c.Target.Port <= 0 {
		return fmt.Errorf("target.port must be positive")
	}
	if c.Migration.Parallel <= 0 {
		return fmt.Errorf("migration.parallel must be positive")
	}
	if c.Migration.BatchSize <= 0 {
		return fmt.Errorf("migration.batch_size must be positive")
	}
	if c.Migration.OnError != "abort" && c.Migration.OnError != "skip" {
		return fmt.Errorf("migration.on_error must be 'abort' or 'skip'")
	}
	if c.Web.Enable {
		if c.Web.Port <= 0 || c.Web.Port > 65535 {
			return fmt.Errorf("web.port must be between 1 and 65535")
		}
	}
	return nil
}

func init() {
	_ = zap.L()
}
