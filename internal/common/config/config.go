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
	Source    SourceConfig    `yaml:"source"`
	Target    TargetConfig    `yaml:"target"`
	Migration MigrationConfig `yaml:"migration"`
	Logging   LoggingConfig   `yaml:"logging"`
	Web       WebConfig       `yaml:"web"`
}

type SourceConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	Schema   string `yaml:"schema"`
	SSLMode  string `yaml:"sslmode"`
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
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

func (t TargetConfig) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&timeout=30s&readTimeout=300s&writeTimeout=300s",
		t.User, t.Password, t.Host, t.Port, t.Database)
}

type MigrationConfig struct {
	Parallel       int      `yaml:"parallel"`
	BatchSize      int      `yaml:"batch_size"`
	TempDir        string   `yaml:"temp_dir"`
	Tables         []string `yaml:"tables"`
	ExcludeTables  []string `yaml:"exclude_tables"`
	UseLightning   bool     `yaml:"use_lightning"`
	OnError        string   `yaml:"on_error"`
	CheckpointDir  string   `yaml:"checkpoint_dir"`
	ReadTimeout    string   `yaml:"read_timeout"`
	WriteTimeout   string   `yaml:"write_timeout"`
	TargetPolicy   string   `yaml:"target_policy"` // insert, truncate, drop
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
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	Output string `yaml:"output"`
}

type WebConfig struct {
	Enable bool   `yaml:"enable"`
	Port   int    `yaml:"port"`
	Host   string `yaml:"host"`
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
			Parallel:      4,
			BatchSize:     100000,
			TempDir:       "/tmp/pg2tidb",
			UseLightning:  true,
			OnError:       "abort",
			CheckpointDir: ".checkpoint",
			ReadTimeout:   "30m",
			WriteTimeout:  "30m",
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
