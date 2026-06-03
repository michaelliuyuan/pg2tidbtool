package common

import (
	"context"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common/reporter"
)

type SchemaMigrator interface {
	Run(ctx context.Context, opts SchemaOpts) error
}

type DataMigrator interface {
	Run(ctx context.Context, opts DataOpts) (*DataResult, error)
}

type DataValidator interface {
	Run(ctx context.Context, opts ValidateOpts) (*reporter.Report, error)
}

type Prechecker interface {
	Run(ctx context.Context, opts PrecheckOpts) (*reporter.Report, error)
}

type SchemaOpts struct {
	DryRun        bool
	OutputFile    string
	Schemas       []string
	ExcludeTables []string
}

type DataOpts struct {
	Parallel      int
	BatchSize     int
	Tables        []string
	ExcludeTables []string
	UseLightning  bool
	LightningCfg  string
	TempDir       string
}

type DataResult struct {
	TotalRows    int64
	TotalTables  int
	TotalBytes   int64
	Duration     string
	ExportPath   string
}

type ValidateOpts struct {
	Level       string
	Mode        string // quick, sample, checksum, full
	SampleRatio float64
	Tables      []string
	ReportFile  string
}

type PrecheckOpts struct {
	ReportFile string
}
