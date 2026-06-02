package validator

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"

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

	pgDB.SetMaxOpenConns(8)
	pgDB.SetConnMaxLifetime(5 * time.Minute)
	tidbDB.SetMaxOpenConns(8)
	tidbDB.SetConnMaxLifetime(5 * time.Minute)

	tables, err := v.getTables(ctx, pgDB, opts.Tables)
	if err != nil {
		return nil, cerrors.Wrap(cerrors.ErrValidateRowCount, "get table list", err)
	}

	parallel := v.cfg.Migration.Parallel
	if parallel <= 0 {
		parallel = 4
	}

	var mu sync.Mutex
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup

	for _, table := range tables {
		wg.Add(1)
		sem <- struct{}{}
		go func(tableName string) {
			defer wg.Done()
			defer func() { <-sem }()

			var tr reporter.TableReport
			switch opts.Level {
			case "L1":
				tr = v.validateRowCount(ctx, pgDB, tidbDB, tableName)
			case "L2":
				tr = v.validateSampling(ctx, pgDB, tidbDB, tableName, opts.SampleRatio)
			case "L3":
				tr = v.validateChecksum(ctx, pgDB, tidbDB, tableName)
			default:
				tr = reporter.TableReport{
					TableName: tableName,
					Status:    reporter.StatusFail,
					Error:     fmt.Sprintf("unknown validation level: %s", opts.Level),
				}
			}

			mu.Lock()
			rpt.AddTableReport(tr)
			mu.Unlock()

			logger.Info("table validation result",
				zap.String("table", tableName),
				zap.String("status", string(tr.Status)),
				zap.Int64("diff", tr.DiffRows))
		}(table)
	}

	wg.Wait()

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

	pgCols, _ := pgRows.ColumnTypes()
	if pgCols == nil {
		tr.Status = reporter.StatusFail
		tr.Error = "failed to get PG column types"
		return tr
	}

	// Build sets of column indices to skip or trim in comparison.
	// Floating point types have inherent precision differences between PG and TiDB.
	// CHAR/VARCHAR/TEXT types may differ in trailing spaces (MySQL auto-trims CHAR).
	skipCols := make(map[int]bool)
	trimCols := make(map[int]bool)
	for i, c := range pgCols {
		dt := strings.ToLower(c.DatabaseTypeName())
		if dt == "real" || dt == "float4" || dt == "float8" || dt == "double" || dt == "double precision" || dt == "numeric" || dt == "decimal" ||
				dt == "json" || dt == "jsonb" {
			skipCols[i] = true
		}
		if dt == "character" || dt == "char" || dt == "bpchar" || dt == "character varying" || dt == "varchar" || dt == "text" {
			trimCols[i] = true
		}
	}

	pgValues := make([]interface{}, len(pgCols))
	pgPtrs := make([]interface{}, len(pgCols))
	for i := range pgValues {
		pgPtrs[i] = &pgValues[i]
	}

	var pgData [][]string
	for pgRows.Next() {
		if err := pgRows.Scan(pgPtrs...); err != nil {
			tr.Status = reporter.StatusFail
			tr.Error = fmt.Sprintf("scan PG row: %v", err)
			return tr
		}
		row := make([]string, len(pgCols))
		for i, val := range pgValues {
			row[i] = normalizeValue(val)
		}
		pgData = append(pgData, row)
	}

	tidbQuery := fmt.Sprintf("SELECT * FROM %s ORDER BY 1 LIMIT %d OFFSET %d",
		quoteMySQL(table), sampleSize, offset)
	tidbRows, err := tidbDB.QueryContext(ctx, tidbQuery)
	if err != nil {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("sample target: %v", err)
		return tr
	}
	defer tidbRows.Close()

	tidbCols, _ := tidbRows.ColumnTypes()
	if tidbCols == nil {
		tr.Status = reporter.StatusFail
		tr.Error = "failed to get TiDB column types"
		return tr
	}
	tidbValues := make([]interface{}, len(tidbCols))
	tidbPtrs := make([]interface{}, len(tidbCols))
	for i := range tidbValues {
		tidbPtrs[i] = &tidbValues[i]
	}

	var mismatchCount int
	var mismatchDetails []string
	rowIdx := 0
	for tidbRows.Next() {
		if err := tidbRows.Scan(tidbPtrs...); err != nil {
			tr.Status = reporter.StatusFail
			tr.Error = fmt.Sprintf("scan TiDB row: %v", err)
			return tr
		}

		if rowIdx < len(pgData) {
			for colIdx, val := range tidbValues {
				if skipCols[colIdx] {
					continue
				}
				pgVal := ""
				if colIdx < len(pgData[rowIdx]) {
					pgVal = pgData[rowIdx][colIdx]
				}
				tidbVal := normalizeValue(val)
				if trimCols[colIdx] {
					pgVal = strings.TrimRight(pgVal, " ")
					tidbVal = strings.TrimRight(tidbVal, " ")
				}
				if pgVal != tidbVal {
					mismatchCount++
					colName := tidbCols[colIdx].Name()
					mismatchDetails = append(mismatchDetails, fmt.Sprintf("row %d col %q: PG=%q TiDB=%q", rowIdx+int(offset)+1, colName, truncate(pgVal, 80), truncate(tidbVal, 80)))
					break
				}
			}
		}
		rowIdx++
	}

	if mismatchCount > 0 {
		tr.Status = reporter.StatusFail
		maxShow := 10
		if len(mismatchDetails) > maxShow {
			mismatchDetails = mismatchDetails[:maxShow]
		}
		detailStr := strings.Join(mismatchDetails, "; ")
		tr.Error = fmt.Sprintf("%d/%d rows mismatch in sampling (%s)", mismatchCount, len(pgData), detailStr)
	} else {
		tr.Status = reporter.StatusPass
	}
	tr.Suggestion = fmt.Sprintf("sampled %d rows (%.1f%%), %d mismatches", len(pgData), ratio*100, mismatchCount)
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

func normalizeValue(val interface{}) string {
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
		return normalizeString(string(v))
	case time.Time:
		return v.Format("2006-01-02 15:04:05")
	case string:
		return normalizeString(v)
	case fmt.Stringer:
		return normalizeString(v.String())
	default:
		return normalizeString(fmt.Sprintf("%v", v))
	}
}

var uuidRe = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
var pgArrayRe = regexp.MustCompile(`^\{.*\}$`)

func normalizeString(s string) string {
	// Normalize UUID to lowercase
	s = uuidRe.ReplaceAllStringFunc(s, func(m string) string {
		return strings.ToLower(m)
	})
	// Normalize PG array format {1,2,3} -> [1,2,3] (must be before JSON check)
	if pgArrayRe.MatchString(s) {
		return normalizePGArray(s)
	}
	// Normalize JSON whitespace
	if strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[") {
		return normalizeJSON(s)
	}
	return s
}

func normalizePGArray(s string) string {
	// Replace PG array delimiters with JSON array format
	s = strings.ReplaceAll(s, "{", "[")
	s = strings.ReplaceAll(s, "}", "]")
	// PG arrays use " to quote elements with special chars — these are already valid JSON string delimiters
	// PG escapes embedded " as "" — convert to JSON \" for proper comparison
	s = strings.ReplaceAll(s, `""`, `\"`)
	return s
}

func normalizeJSON(s string) string {
	var buf strings.Builder
	buf.Grow(len(s))
	inString := false
	escaped := false
	for _, r := range s {
		if escaped {
			buf.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' && inString {
			buf.WriteRune(r)
			escaped = true
			continue
		}
		if r == '"' {
			inString = !inString
			buf.WriteRune(r)
			continue
		}
		if inString {
			buf.WriteRune(r)
			continue
		}
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			continue
		}
		buf.WriteRune(r)
	}
	return buf.String()
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
