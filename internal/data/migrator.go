package data

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/checkpoint"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	cerrors "github.com/pg2tidb/pg2tidb-migrator/internal/common/errors"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/progress"
	"go.uber.org/zap"
)

type Migrator struct {
	cfg       config.Config
	pgDB      *sql.DB
	cpMgr     *checkpoint.Manager
	display   *progress.Display
}

func NewMigrator(cfg config.Config) *Migrator {
	return &Migrator{cfg: cfg}
}

func (m *Migrator) Run(ctx context.Context, opts common.DataOpts) (*common.DataResult, error) {
	logger := zap.L()
	startTime := time.Now()

	logger.Info("starting data migration",
		zap.Int("parallel", opts.Parallel),
		zap.Int("batch_size", opts.BatchSize),
		zap.Bool("use_lightning", opts.UseLightning))

	var err error
	m.pgDB, err = sql.Open("pgx", m.cfg.Source.DSN())
	if err != nil {
		return nil, cerrors.Wrap(cerrors.ErrSourceConnect, "connect to PostgreSQL", err)
	}
	defer m.pgDB.Close()

	if err := m.pgDB.PingContext(ctx); err != nil {
		return nil, cerrors.Wrap(cerrors.ErrSourceConnect, "ping PostgreSQL", err)
	}

	cpDir := m.cfg.Migration.CheckpointDir
	if opts.TempDir != "" {
		cpDir = filepath.Join(opts.TempDir, ".checkpoint")
	}
	m.cpMgr, err = checkpoint.NewManager(cpDir)
	if err != nil {
		return nil, cerrors.Wrap(cerrors.ErrCheckpointLoad, "init checkpoint", err)
	}
	m.cpMgr.SetPhase("data-migration")

	if err := os.MkdirAll(opts.TempDir, 0755); err != nil {
		return nil, cerrors.Wrap(cerrors.ErrDataExport, "create temp dir", err)
	}

	tables, err := m.getTables(ctx, opts.Tables, opts.ExcludeTables)
	if err != nil {
		return nil, cerrors.Wrap(cerrors.ErrDataExport, "get table list", err)
	}

	logger.Info("migrating tables", zap.Int("count", len(tables)))

	m.display = progress.NewDisplay()
	m.display.Start()

	var totalRows atomic.Int64
	var totalBytes atomic.Int64

	sem := make(chan struct{}, opts.Parallel)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	for _, table := range tables {
		if m.cpMgr.IsTableCompleted(table) {
			logger.Info("skipping completed table", zap.String("table", table))
			continue
		}

		rowCount, err := m.getRowCount(ctx, table)
		if err != nil {
			logger.Warn("failed to get row count", zap.String("table", table), zap.Error(err))
			rowCount = 0
		}

		m.cpMgr.GetOrCreateTable(table, rowCount)
		bar := m.display.AddBar(table, rowCount)

		sem <- struct{}{}
		wg.Add(1)

		go func(tableName string, bar *progress.Bar) {
			defer wg.Done()
			defer func() { <-sem }()

			m.cpMgr.MarkTableRunning(tableName)

			rows, bytes, err := m.exportTable(ctx, tableName, opts)
			if err != nil {
				m.cpMgr.MarkTableFailed(tableName, err.Error())
				m.display.RemoveBar(tableName)
				errMu.Lock()
				if firstErr == nil {
					firstErr = cerrors.WithTable(
						cerrors.Wrap(cerrors.ErrDataExport, "export table", err),
						tableName)
				}
				errMu.Unlock()
				return
			}

			bar.Set(rows)
			totalRows.Add(rows)
			totalBytes.Add(bytes)
			m.cpMgr.MarkTableCompleted(tableName, rows)
			m.display.RemoveBar(tableName)

			logger.Info("table exported",
				zap.String("table", tableName),
				zap.Int64("rows", rows),
				zap.Int64("bytes", bytes))
		}(table, bar)
	}

	wg.Wait()
	m.display.Stop()

	if firstErr != nil && m.cfg.Migration.OnError != "skip" {
		return nil, firstErr
	}

	if opts.UseLightning {
		if err := m.importViaLightning(ctx, opts); err != nil {
			return nil, cerrors.Wrap(cerrors.ErrLightningExec, "lightning import", err)
		}
	} else {
		if err := m.importViaSQL(ctx, opts); err != nil {
			return nil, cerrors.Wrap(cerrors.ErrDataImport, "sql import", err)
		}
	}

	duration := time.Since(startTime)
	result := &common.DataResult{
		TotalRows:   totalRows.Load(),
		TotalTables: len(tables),
		TotalBytes:  totalBytes.Load(),
		Duration:    duration.String(),
		ExportPath:  opts.TempDir,
	}

	logger.Info("data migration completed",
		zap.Int64("total_rows", result.TotalRows),
		zap.Int("tables", result.TotalTables),
		zap.String("duration", result.Duration))

	return result, nil
}

func (m *Migrator) getTables(ctx context.Context, include, exclude []string) ([]string, error) {
	if len(include) > 0 {
		return include, nil
	}

	schema := m.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`
	rows, err := m.pgDB.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if !contains(exclude, name) {
			tables = append(tables, name)
		}
	}
	return tables, nil
}

func (m *Migrator) getRowCount(ctx context.Context, table string) (int64, error) {
	schema := m.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}
	var count int64
	err := m.pgDB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", quotePG(schema), quotePG(table))).Scan(&count)
	return count, err
}

func (m *Migrator) exportTable(ctx context.Context, table string, opts common.DataOpts) (int64, int64, error) {
	schema := m.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	outputPath := filepath.Join(opts.TempDir, table+".csv")
	f, err := os.Create(outputPath)
	if err != nil {
		return 0, 0, fmt.Errorf("create csv file: %w", err)
	}
	defer f.Close()

	var totalRows int64

	copyQuery := fmt.Sprintf("COPY %s.%s TO STDOUT WITH (FORMAT csv, NULL '\\N', HEADER false)",
		quotePG(schema), quotePG(table))

	conn, err := m.pgDB.Conn(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("get connection: %w", err)
	}
	defer conn.Close()

	err = conn.Raw(func(driverConn interface{}) error {
		pgConn, ok := driverConn.(interface {
			CopyTo(context.Context, string, string) (int64, error)
		})
		if !ok {
			return m.exportTableFallback(ctx, schema, table, f, opts)
		}
		n, copyErr := pgConn.CopyTo(ctx, copyQuery, "")
		totalRows = n
		return copyErr
	})

	if err != nil {
		return totalRows, 0, fmt.Errorf("copy export: %w", err)
	}

	fi, _ := f.Stat()
	var totalBytes int64
	if fi != nil {
		totalBytes = fi.Size()
	}

	return totalRows, totalBytes, nil
}

func (m *Migrator) exportTableFallback(ctx context.Context, schema, table string, f *os.File, opts common.DataOpts) error {
	query := fmt.Sprintf("SELECT * FROM %s.%s", quotePG(schema), quotePG(table))
	rows, err := m.pgDB.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query table: %w", err)
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	var rowCount int64
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
		record := make([]string, len(cols))
		for i, val := range values {
			record[i] = convertValue(val)
		}
		line := strings.Join(record, "\t") + "\n"
		if _, err := f.WriteString(line); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
		rowCount++
		if rowCount%int64(opts.BatchSize) == 0 {
			m.cpMgr.UpdateTableProgress(table, rowCount, 0)
		}
	}

	return nil
}

func (m *Migrator) importViaLightning(ctx context.Context, opts common.DataOpts) error {
	logger := zap.L()
	logger.Info("TiDB Lightning import starting", zap.String("dir", opts.TempDir))

	tidbDB, err := sql.Open("mysql", m.cfg.Target.DSN())
	if err != nil {
		return err
	}
	defer tidbDB.Close()

	entries, err := os.ReadDir(opts.TempDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".csv") {
			continue
		}

		tableName := strings.TrimSuffix(entry.Name(), ".csv")
		csvPath := filepath.Join(opts.TempDir, entry.Name())

		logger.Info("loading CSV into TiDB", zap.String("table", tableName))

		if err := m.loadCSVToTiDB(ctx, tidbDB, tableName, csvPath); err != nil {
			if m.cfg.Migration.OnError != "skip" {
				return err
			}
			logger.Warn("failed to load table", zap.String("table", tableName), zap.Error(err))
		}
	}

	return nil
}

func (m *Migrator) loadCSVToTiDB(ctx context.Context, db *sql.DB, table, csvPath string) error {
	mysql.RegisterLocalFile(csvPath)
	defer mysql.DeregisterLocalFile(csvPath)

	query := fmt.Sprintf("LOAD DATA LOCAL INFILE ? INTO TABLE %s FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'",
		quoteMySQL(table))

	_, err := db.ExecContext(ctx, query, csvPath)
	return err
}

func (m *Migrator) importViaSQL(ctx context.Context, opts common.DataOpts) error {
	return m.importViaLightning(ctx, opts)
}

func convertValue(val interface{}) string {
	if val == nil {
		return "\\N"
	}
	switch v := val.(type) {
	case bool:
		if v {
			return "1"
		}
		return "0"
	case []byte:
		return string(v)
	case time.Time:
		return v.Format("2006-01-02 15:04:05.999999")
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func quotePG(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteMySQL(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
