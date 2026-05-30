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

	m.pgDB.SetMaxOpenConns(opts.Parallel + 2)
	m.pgDB.SetConnMaxLifetime(10 * time.Minute)
	m.pgDB.SetConnMaxIdleTime(5 * time.Minute)

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
			logger.Warn("LOAD DATA import failed, falling back to streaming INSERT", zap.Error(err))
			if err := m.importViaSQL(ctx, opts); err != nil {
				return nil, cerrors.Wrap(cerrors.ErrDataImport, "sql import", err)
			}
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

	exportErr := m.exportTableFallback(ctx, schema, table, f, opts, &totalRows)
	if exportErr != nil {
		err = conn.Raw(func(driverConn interface{}) error {
			pgConn, ok := driverConn.(interface {
				CopyTo(context.Context, string, string) (int64, error)
			})
			if !ok {
				return exportErr
			}
			n, copyErr := pgConn.CopyTo(ctx, copyQuery, "")
			totalRows = n
			return copyErr
		})
		if err != nil {
			return totalRows, 0, fmt.Errorf("copy export: %w", err)
		}
	}

	fi, _ := f.Stat()
	var totalBytes int64
	if fi != nil {
		totalBytes = fi.Size()
	}

	return totalRows, totalBytes, nil
}

func (m *Migrator) exportTableFallback(ctx context.Context, schema, table string, f *os.File, opts common.DataOpts, totalRows *int64) error {
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
			*totalRows = rowCount
			m.cpMgr.UpdateTableProgress(table, rowCount, 0)
		}
	}

	*totalRows = rowCount
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

	tidbDB.SetConnMaxLifetime(5 * time.Minute)
	tidbDB.SetConnMaxIdleTime(2 * time.Minute)
	tidbDB.SetMaxOpenConns(4)

	if err := tidbDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping TiDB: %w", err)
	}

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

	safePath := strings.ReplaceAll(csvPath, "\\", "\\\\")
	safePath = strings.ReplaceAll(safePath, "'", "\\'")

	query := fmt.Sprintf("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'",
		safePath, quoteMySQL(table))

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}
		conn, err := db.Conn(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		err = conn.QueryRowContext(ctx, "SELECT 1").Scan(new(int))
		if err != nil {
			conn.Close()
			lastErr = err
			continue
		}
		_, err = conn.ExecContext(ctx, query)
		conn.Close()
		if err == nil {
			return nil
		}
		lastErr = err
		if !isBadConnection(err) {
			return err
		}
		logger := zap.L()
		logger.Warn("bad connection, retrying LOAD DATA", zap.Int("attempt", attempt+1), zap.String("table", table))
	}
	return lastErr
}

func isBadConnection(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "bad connection") ||
		strings.Contains(msg, "invalid connection") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "EOF")
}

func (m *Migrator) importViaSQL(ctx context.Context, opts common.DataOpts) error {
	logger := zap.L()
	logger.Info("starting streaming SQL import (batch INSERT)")

	schema := m.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	tables, err := m.getTables(ctx, opts.Tables, opts.ExcludeTables)
	if err != nil {
		return fmt.Errorf("get table list: %w", err)
	}

	tidbDB, err := sql.Open("mysql", m.cfg.Target.DSN())
	if err != nil {
		return err
	}
	defer tidbDB.Close()

	tidbDB.SetConnMaxLifetime(5 * time.Minute)
	tidbDB.SetConnMaxIdleTime(2 * time.Minute)
	tidbDB.SetMaxOpenConns(2)

	if err := tidbDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping TiDB: %w", err)
	}

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 5000
	}
	if batchSize > 5000 {
		batchSize = 5000
	}

	for _, table := range tables {
		logger.Info("streaming table to TiDB", zap.String("table", table))

		selectQuery := fmt.Sprintf("SELECT * FROM %s.%s", quotePG(schema), quotePG(table))
		rows, err := m.pgDB.QueryContext(ctx, selectQuery)
		if err != nil {
			if m.cfg.Migration.OnError != "skip" {
				return fmt.Errorf("query %s: %w", table, err)
			}
			logger.Warn("failed to query table", zap.String("table", table), zap.Error(err))
			continue
		}

		cols, err := rows.ColumnTypes()
		if err != nil {
			rows.Close()
			return fmt.Errorf("get columns for %s: %w", table, err)
		}

		colNames := make([]string, len(cols))
		for i, col := range cols {
			colNames[i] = quoteMySQL(col.Name())
		}
		colList := strings.Join(colNames, ", ")
		placeholders := strings.Repeat("?,", len(cols))
		placeholders = placeholders[:len(placeholders)-1]

		insertBase := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			quoteMySQL(table), colList, placeholders)

		var batch [][]interface{}
		totalRows := 0

		for rows.Next() {
			values := make([]interface{}, len(cols))
			valuePtrs := make([]interface{}, len(cols))
			for i := range values {
				valuePtrs[i] = &values[i]
			}
			if err := rows.Scan(valuePtrs...); err != nil {
				logger.Warn("scan error", zap.String("table", table), zap.Error(err))
				continue
			}

			converted := make([]interface{}, len(cols))
			for i, v := range values {
				converted[i] = convertSQLValue(v)
			}
			batch = append(batch, converted)

			if len(batch) >= batchSize {
				if err := m.execBatch(ctx, tidbDB, insertBase, batch, len(cols)); err != nil {
					rows.Close()
					if m.cfg.Migration.OnError != "skip" {
						return fmt.Errorf("insert batch for %s: %w", table, err)
					}
					logger.Warn("batch insert error", zap.String("table", table), zap.Error(err))
					batch = batch[:0]
					continue
				}
				totalRows += len(batch)
				batch = batch[:0]
			}
		}
		rows.Close()

		if len(batch) > 0 {
			if err := m.execBatch(ctx, tidbDB, insertBase, batch, len(cols)); err != nil {
				if m.cfg.Migration.OnError != "skip" {
					return fmt.Errorf("insert final batch for %s: %w", table, err)
				}
				logger.Warn("final batch error", zap.String("table", table), zap.Error(err))
			} else {
				totalRows += len(batch)
			}
		}

		logger.Info("table imported", zap.String("table", table), zap.Int("rows", totalRows))
	}

	return nil
}

func (m *Migrator) execBatch(ctx context.Context, db *sql.DB, insertBase string, batch [][]interface{}, colCount int) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			lastErr = err
			continue
		}

		stmt, err := tx.PrepareContext(ctx, insertBase)
		if err != nil {
			tx.Rollback()
			lastErr = err
			continue
		}

		batchErr := func() error {
			for _, row := range batch {
				if _, err := stmt.ExecContext(ctx, row...); err != nil {
					return err
				}
			}
			return nil
		}()

		stmt.Close()

		if batchErr != nil {
			tx.Rollback()
			lastErr = batchErr
			if !isBadConnection(batchErr) {
				return batchErr
			}
			zap.L().Warn("bad connection in batch, retrying", zap.Int("attempt", attempt+1))
			continue
		}

		if err := tx.Commit(); err != nil {
			lastErr = err
			if !isBadConnection(err) {
				return err
			}
			continue
		}
		return nil
	}
	return lastErr
}

func convertSQLValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case []byte:
		return string(v)
	case time.Time:
		return v.Format("2006-01-02 15:04:05.999999")
	default:
		return v
	}
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
