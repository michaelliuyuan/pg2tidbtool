package precheck

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/reporter"
	"go.uber.org/zap"
)

type Checker struct {
	cfg config.Config
}

func NewChecker(cfg config.Config) *Checker {
	return &Checker{cfg: cfg}
}

type CheckItem struct {
	Name    string
	Status  reporter.Status
	Message string
	Detail  string
}

func (c *Checker) Run(ctx context.Context, opts common.PrecheckOpts) (*reporter.Report, error) {
	logger := zap.L()
	logger.Info("starting pre-check")

	rpt := reporter.NewReport("pre-check")

	var items []CheckItem
	var itemsMu sync.Mutex
	var wg sync.WaitGroup

	collect := func(newItems ...CheckItem) {
		itemsMu.Lock()
		items = append(items, newItems...)
		itemsMu.Unlock()
	}

	wg.Add(6)
	go func() { defer wg.Done(); collect(c.checkPGConnection(ctx)) }()
	go func() { defer wg.Done(); collect(c.checkTiDBConnection(ctx)) }()
	go func() { defer wg.Done(); collect(c.checkDiskSpace(ctx)) }()
	go func() { defer wg.Done(); collect(c.checkPGPermissions(ctx)...) }()
	go func() { defer wg.Done(); collect(c.checkIncompatibleObjects(ctx)...) }()
	go func() { defer wg.Done(); collect(c.checkCollation(ctx)) }()
	wg.Wait()

	for _, item := range items {
		tr := reporter.TableReport{
			TableName: item.Name,
			Status:    item.Status,
			Error:     item.Message,
			Suggestion: item.Detail,
		}
		rpt.AddTableReport(tr)

		switch item.Status {
		case reporter.StatusFail:
			logger.Error("check failed", zap.String("check", item.Name), zap.String("error", item.Message))
		case reporter.StatusWarn:
			logger.Warn("check warning", zap.String("check", item.Name), zap.String("message", item.Message))
		default:
			logger.Info("check passed", zap.String("check", item.Name))
		}
	}

	rpt.Finish(rpt.OverallStatus(), fmt.Sprintf("%d checks: %d passed, %d warnings, %d failed",
		len(items), rpt.Stats.PassTables, rpt.Stats.WarnTables, rpt.Stats.FailTables))

	if opts.ReportFile != "" {
		if err := rpt.Save(opts.ReportFile); err != nil {
			logger.Warn("failed to save precheck report", zap.Error(err))
		}
	}

	if rpt.OverallStatus() == reporter.StatusFail {
		return rpt, fmt.Errorf("pre-check failed: %d critical issues found", rpt.Stats.FailTables)
	}

	return rpt, nil
}

func (c *Checker) checkPGConnection(ctx context.Context) CheckItem {
	item := CheckItem{Name: "pg-connection", Status: reporter.StatusPass, Message: "PostgreSQL connection OK"}

	db, err := sql.Open("pgx", c.cfg.Source.DSN())
	if err != nil {
		item.Status = reporter.StatusFail
		item.Message = fmt.Sprintf("failed to open PG connection: %v", err)
		return item
	}
	defer db.Close()

	start := time.Now()
	if err := db.PingContext(ctx); err != nil {
		item.Status = reporter.StatusFail
		item.Message = fmt.Sprintf("failed to ping PG: %v", err)
		return item
	}
	item.Detail = fmt.Sprintf("latency: %v", time.Since(start))

	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err == nil {
		item.Detail = fmt.Sprintf("%s, version: %s", item.Detail, version)
	}

	return item
}

func (c *Checker) checkTiDBConnection(ctx context.Context) CheckItem {
	item := CheckItem{Name: "tidb-connection", Status: reporter.StatusPass, Message: "TiDB connection OK"}

	db, err := sql.Open("mysql", c.cfg.Target.DSN())
	if err != nil {
		item.Status = reporter.StatusFail
		item.Message = fmt.Sprintf("failed to open TiDB connection: %v", err)
		return item
	}
	defer db.Close()

	start := time.Now()
	if err := db.PingContext(ctx); err != nil {
		item.Status = reporter.StatusFail
		item.Message = fmt.Sprintf("failed to ping TiDB: %v", err)
		return item
	}
	item.Detail = fmt.Sprintf("latency: %v", time.Since(start))

	var version string
	if err := db.QueryRowContext(ctx, "SELECT tidb_version()").Scan(&version); err == nil {
		if len(version) > 100 {
			version = version[:100]
		}
		item.Detail = fmt.Sprintf("%s, version: %s", item.Detail, version)
	}

	return item
}

func (c *Checker) checkDiskSpace(ctx context.Context) CheckItem {
	item := CheckItem{Name: "disk-space", Status: reporter.StatusPass, Message: "Disk space OK"}

	tempDir := c.cfg.Migration.TempDir
	if tempDir == "" {
		tempDir = "/tmp/pg2tidb"
	}

	var availGB float64
	if stat, err := getDiskUsage(tempDir); err != nil {
		item.Status = reporter.StatusWarn
		item.Message = fmt.Sprintf("cannot check disk space for %s: %v", tempDir, err)
		return item
	} else {
		availGB = stat
	}

	if availGB < 1.0 {
		item.Status = reporter.StatusFail
		item.Message = fmt.Sprintf("insufficient disk space: %.1f GB available in %s", availGB, tempDir)
	} else if availGB < 10.0 {
		item.Status = reporter.StatusWarn
		item.Message = fmt.Sprintf("low disk space: %.1f GB available in %s", availGB, tempDir)
	}
	item.Detail = fmt.Sprintf("%.1f GB available in %s", availGB, tempDir)

	return item
}

func (c *Checker) checkPGPermissions(ctx context.Context) []CheckItem {
	var items []CheckItem

	db, err := sql.Open("pgx", c.cfg.Source.DSN())
	if err != nil {
		items = append(items, CheckItem{Name: "pg-permissions", Status: reporter.StatusFail,
			Message: fmt.Sprintf("cannot connect: %v", err)})
		return items
	}
	defer db.Close()

	schema := c.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	var hasUsage bool
	err = db.QueryRowContext(ctx,
		"SELECT has_schema_privilege($1, 'usage')", schema).Scan(&hasUsage)
	if err != nil || !hasUsage {
		items = append(items, CheckItem{
			Name:    "pg-schema-permission",
			Status:  reporter.StatusFail,
			Message: fmt.Sprintf("no USAGE privilege on schema %s", schema),
		})
	} else {
		items = append(items, CheckItem{
			Name:    "pg-schema-permission",
			Status:  reporter.StatusPass,
			Message: fmt.Sprintf("USAGE privilege on schema %s OK", schema),
		})
	}

	var canRead bool
	err = db.QueryRowContext(ctx,
		"SELECT has_database_privilege(current_database(), 'read')").Scan(&canRead)
	if err == nil && !canRead {
		items = append(items, CheckItem{
			Name:    "pg-read-permission",
			Status:  reporter.StatusWarn,
			Message: "may not have full read access to all tables",
		})
	}

	return items
}

func (c *Checker) checkIncompatibleObjects(ctx context.Context) []CheckItem {
	var items []CheckItem

	db, err := sql.Open("pgx", c.cfg.Source.DSN())
	if err != nil {
		return items
	}
	defer db.Close()

	schema := c.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	triggerCount := 0
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema = $1", schema).Scan(&triggerCount)
	if err == nil && triggerCount > 0 {
		items = append(items, CheckItem{
			Name:    "pg-triggers",
			Status:  reporter.StatusWarn,
			Message: fmt.Sprintf("found %d triggers (not migrated)", triggerCount),
			Detail:  "triggers will be skipped during migration",
		})
	}

	funcCount := 0
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema = $1 AND routine_type = 'FUNCTION'", schema).Scan(&funcCount)
	if err == nil && funcCount > 0 {
		items = append(items, CheckItem{
			Name:    "pg-functions",
			Status:  reporter.StatusWarn,
			Message: fmt.Sprintf("found %d stored functions (not migrated)", funcCount),
			Detail:  "stored functions/procedures will be skipped during migration",
		})
	}

	enumCount := 0
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(DISTINCT t.typname) FROM pg_type t JOIN pg_enum e ON t.oid = e.enumtypid JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname = $1",
		schema).Scan(&enumCount)
	if err == nil && enumCount > 0 {
		items = append(items, CheckItem{
			Name:    "pg-enums",
			Status:  reporter.StatusWarn,
			Message: fmt.Sprintf("found %d enum types (need manual mapping)", enumCount),
			Detail:  "enum types will be converted to VARCHAR or ENUM in TiDB",
		})
	}

	extCount := 0
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pg_extension WHERE extnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)", schema).Scan(&extCount)
	if err == nil && extCount > 0 {
		items = append(items, CheckItem{
			Name:    "pg-extensions",
			Status:  reporter.StatusWarn,
			Message: fmt.Sprintf("found %d extensions (review compatibility)", extCount),
		})
	}

	return items
}

func (c *Checker) checkCollation(ctx context.Context) CheckItem {
	item := CheckItem{Name: "collation", Status: reporter.StatusPass, Message: "Collation check OK"}

	db, err := sql.Open("pgx", c.cfg.Source.DSN())
	if err != nil {
		return item
	}
	defer db.Close()

	schema := c.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	var dbCollation string
	err = db.QueryRowContext(ctx,
		"SELECT datcollate FROM pg_database WHERE datname = current_database()").Scan(&dbCollation)
	if err == nil {
		item.Detail = fmt.Sprintf("PG collation: %s", dbCollation)
		if !strings.Contains(strings.ToLower(dbCollation), "utf") {
			item.Status = reporter.StatusWarn
			item.Message = fmt.Sprintf("non-UTF collation detected: %s, may cause encoding issues", dbCollation)
		}
	}

	return item
}
