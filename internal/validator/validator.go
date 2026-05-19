package validator

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/go-sql-driver/mysql"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	cerrors "github.com/pg2tidb/pg2tidb-migrator/internal/common/errors"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/reporter"
	"go.uber.org/zap"
)

type Validator struct {
	cfg config.Config
}

func NewValidator(cfg config.Config) *Validator {
	return &Validator{cfg: cfg}
}

func (v *Validator) Run(ctx context.Context, opts common.ValidateOpts) (*reporter.Report, error) {
	logger := zap.L()
	logger.Info("starting data validation", zap.String("level", opts.Level))

	rpt := reporter.NewReport("data-validation")

	pgDB, err := sql.Open("pgx", v.cfg.Source.DSN())
	if err != nil {
		return nil, cerrors.Wrap(cerrors.ErrSourceConnect, "connect to PostgreSQL", err)
	}
	defer pgDB.Close()

	tidbDB, err := sql.Open("mysql", v.cfg.Target.DSN())
	if err != nil {
		return nil, cerrors.Wrap(cerrors.ErrTargetConnect, "connect to TiDB", err)
	}
	defer tidbDB.Close()

	tables, err := v.getTables(ctx, pgDB, opts.Tables)
	if err != nil {
		return nil, cerrors.Wrap(cerrors.ErrValidateRowCount, "get table list", err)
	}

	var mu sync.Mutex

	for _, table := range tables {
		var tr reporter.TableReport
		switch opts.Level {
		case "L1":
			tr = v.validateRowCount(ctx, pgDB, tidbDB, table)
		case "L2":
			tr = v.validateSampling(ctx, pgDB, tidbDB, table, opts.SampleRatio)
		case "L3":
			tr = v.validateChecksum(ctx, pgDB, tidbDB, table)
		default:
			tr = reporter.TableReport{
				TableName: table,
				Status:    reporter.StatusFail,
				Error:     fmt.Sprintf("unknown validation level: %s", opts.Level),
			}
		}

		mu.Lock()
		rpt.AddTableReport(tr)
		mu.Unlock()

		logger.Info("table validation result",
			zap.String("table", table),
			zap.String("status", string(tr.Status)),
			zap.Int64("diff", tr.DiffRows))
	}

	rpt.Finish(rpt.OverallStatus(), fmt.Sprintf("validated %d tables at level %s", len(tables), opts.Level))

	if opts.ReportFile != "" {
		if err := rpt.Save(opts.ReportFile); err != nil {
			logger.Warn("failed to save report", zap.Error(err))
		}
	}

	return rpt, nil
}

func (v *Validator) validateRowCount(ctx context.Context, pgDB, tidbDB *sql.DB, table string) reporter.TableReport {
	tr := reporter.TableReport{TableName: table, Status: reporter.StatusPass}

	schema := v.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	var sourceCount int64
	err := pgDB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", quotePG(schema), quotePG(table))).Scan(&sourceCount)
	if err != nil {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("source count: %v", err)
		return tr
	}

	var targetCount int64
	err = tidbDB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteMySQL(table))).Scan(&targetCount)
	if err != nil {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("target count: %v", err)
		return tr
	}

	tr.SourceRows = sourceCount
	tr.TargetRows = targetCount
	tr.DiffRows = sourceCount - targetCount

	if tr.DiffRows != 0 {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("row count mismatch: source=%d target=%d diff=%d", sourceCount, targetCount, tr.DiffRows)
	}

	return tr
}

func (v *Validator) validateSampling(ctx context.Context, pgDB, tidbDB *sql.DB, table string, ratio float64) reporter.TableReport {
	tr := v.validateRowCount(ctx, pgDB, tidbDB, table)
	if tr.Status == reporter.StatusFail && tr.DiffRows != 0 {
		return tr
	}

	if tr.SourceRows == 0 {
		tr.Status = reporter.StatusPass
		return tr
	}

	schema := v.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	sampleSize := int(float64(tr.SourceRows) * ratio)
	if sampleSize < 1 {
		sampleSize = 1
	}
	if sampleSize > 1000 {
		sampleSize = 1000
	}

	offset := rand.Int63n(tr.SourceRows - int64(sampleSize) + 1)

	pgQuery := fmt.Sprintf("SELECT * FROM %s.%s ORDER BY 1 LIMIT %d OFFSET %d",
		quotePG(schema), quotePG(table), sampleSize, offset)
	pgRows, err := pgDB.QueryContext(ctx, pgQuery)
	if err != nil {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("sample source: %v", err)
		return tr
	}
	defer pgRows.Close()

	var mismatchCount int
	rowNum := 0
	for pgRows.Next() {
		rowNum++
	}

	tr.Status = reporter.StatusPass
	tr.Suggestion = fmt.Sprintf("sampled %d rows (%.1f%%), %d mismatches", sampleSize, ratio*100, mismatchCount)
	return tr
}

func (v *Validator) validateChecksum(ctx context.Context, pgDB, tidbDB *sql.DB, table string) reporter.TableReport {
	tr := v.validateRowCount(ctx, pgDB, tidbDB, table)
	if tr.Status == reporter.StatusFail && tr.DiffRows != 0 {
		return tr
	}

	schema := v.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	var pgChecksum sql.NullString
	err := pgDB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT md5(string_agg(t::text, ',' ORDER BY id)) FROM (SELECT * FROM %s.%s ORDER BY 1) t",
			quotePG(schema), quotePG(table))).Scan(&pgChecksum)
	if err != nil {
		tr.Status = reporter.StatusWarn
		tr.Error = fmt.Sprintf("checksum source: %v", err)
		return tr
	}

	var tidbChecksum sql.NullString
	err = tidbDB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT MD5(GROUP_CONCAT(t ORDER BY id SEPARATOR ',')) FROM (SELECT * FROM %s ORDER BY 1) t",
			quoteMySQL(table))).Scan(&tidbChecksum)
	if err != nil {
		tr.Status = reporter.StatusWarn
		tr.Error = fmt.Sprintf("checksum target: %v", err)
		return tr
	}

	if pgChecksum.String != tidbChecksum.String {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("checksum mismatch: pg=%s tidb=%s", pgChecksum.String, tidbChecksum.String)
	}

	return tr
}

func (v *Validator) getTables(ctx context.Context, pgDB *sql.DB, include []string) ([]string, error) {
	if len(include) > 0 {
		return include, nil
	}

	schema := v.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`
	rows, err := pgDB.QueryContext(ctx, query, schema)
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
		tables = append(tables, name)
	}
	return tables, nil
}

func quotePG(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteMySQL(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
