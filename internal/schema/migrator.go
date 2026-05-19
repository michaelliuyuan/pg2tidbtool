package schema

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	cerrors "github.com/pg2tidb/pg2tidb-migrator/internal/common/errors"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/reporter"
	"go.uber.org/zap"
)

type Migrator struct {
	cfg config.Config
}

func NewMigrator(cfg config.Config) *Migrator {
	return &Migrator{cfg: cfg}
}

func (m *Migrator) Run(ctx context.Context, opts common.SchemaOpts) error {
	logger := zap.L()
	logger.Info("starting schema migration")

	pgDB, err := sql.Open("pgx", m.cfg.Source.DSN())
	if err != nil {
		return cerrors.Wrap(cerrors.ErrSourceConnect, "connect to PostgreSQL", err)
	}
	defer pgDB.Close()

	if err := pgDB.PingContext(ctx); err != nil {
		return cerrors.Wrap(cerrors.ErrSourceConnect, "ping PostgreSQL", err)
	}
	logger.Info("connected to PostgreSQL", zap.String("host", m.cfg.Source.Host))

	collector := NewCollector(pgDB)
	schema := m.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	tables, err := collector.CollectTables(ctx, schema, opts.ExcludeTables)
	if err != nil {
		return cerrors.Wrap(cerrors.ErrSchemaFetch, "collect tables", err)
	}
	logger.Info("collected tables", zap.Int("count", len(tables)))

	views, err := collector.CollectViews(ctx, schema)
	if err != nil {
		logger.Warn("failed to collect views", zap.Error(err))
	}

	enums, err := collector.CollectEnums(ctx, schema)
	if err != nil {
		logger.Warn("failed to collect enums", zap.Error(err))
	}

	unsupported, err := collector.CollectUnsupported(ctx, schema)
	if err != nil {
		logger.Warn("failed to collect unsupported objects", zap.Error(err))
	}

	schemaInfo := &SchemaInfo{
		Tables:      tables,
		Views:       views,
		Enums:       enums,
		Unsupported: unsupported,
	}

	builder := NewDDLBuilder()
	for _, enum := range schemaInfo.Enums {
		ddl := builder.BuildEnumDDL(enum)
		builder.statements = append(builder.statements, ddl)
	}

	rpt := reporter.NewReport("schema-migration")

	for _, table := range schemaInfo.Tables {
		tableStart := fmt.Sprintf("-- Table: %s", table.Name)
		builder.statements = append(builder.statements, tableStart)

		if err := builder.BuildTableDDL(table); err != nil {
			logger.Error("failed to build table DDL", zap.String("table", table.Name), zap.Error(err))
			rpt.AddTableReport(reporter.TableReport{
				TableName: table.Name,
				Status:    reporter.StatusFail,
				Error:     err.Error(),
			})
			continue
		}

		builder.BuildPrimaryKeyDDL(table)

		for _, idx := range table.Indexes {
			if idx.IsPrimary {
				continue
			}
			idxDDL := builder.BuildIndexDDL(idx)
			if idxDDL != "" {
				builder.statements = append(builder.statements, idxDDL)
			}
		}

		for _, fk := range table.ForeignKeys {
			fkDDL := builder.BuildForeignKeyDDL(fk)
			builder.statements = append(builder.statements, fkDDL)
		}

		rpt.AddTableReport(reporter.TableReport{
			TableName: table.Name,
			Status:    reporter.StatusPass,
			SourceRows: int64(len(table.Columns)),
		})
	}

	for _, view := range schemaInfo.Views {
		viewDDL := builder.BuildViewDDL(view)
		builder.statements = append(builder.statements, viewDDL)
	}

	for _, obj := range schemaInfo.Unsupported {
		logger.Warn("unsupported object",
			zap.String("type", string(obj.Type)),
			zap.String("name", obj.Name),
			zap.String("note", obj.Note))
		builder.statements = append(builder.statements,
			fmt.Sprintf("-- UNSUPPORTED: %s %s - %s", obj.Type, obj.Name, obj.Note))
	}

	sql := builder.JoinSQL()

	if opts.OutputFile != "" {
		if err := os.WriteFile(opts.OutputFile, []byte(sql), 0644); err != nil {
			return cerrors.Wrap(cerrors.ErrSchemaApply, "write DDL file", err)
		}
		logger.Info("DDL written to file", zap.String("path", opts.OutputFile))
	}

	if !opts.DryRun && opts.OutputFile == "" {
		if err := m.executeDDL(ctx, sql); err != nil {
			return cerrors.Wrap(cerrors.ErrSchemaApply, "execute DDL", err)
		}
		logger.Info("DDL executed on TiDB")
	}

	if len(schemaInfo.Unsupported) > 0 {
		m.writeUnsupportedLog(schemaInfo.Unsupported)
	}

	rpt.Finish(rpt.OverallStatus(), fmt.Sprintf("migrated %d tables, %d views, %d unsupported objects",
		len(tables), len(views), len(unsupported)))

	logger.Info("schema migration completed",
		zap.Int("tables", len(tables)),
		zap.Int("views", len(views)),
		zap.Int("unsupported", len(unsupported)))

	return nil
}

func (m *Migrator) executeDDL(ctx context.Context, sql string) error {
	tidbDB, err := sql.Open("mysql", m.cfg.Target.DSN())
	if err != nil {
		return fmt.Errorf("connect to TiDB: %w", err)
	}
	defer tidbDB.Close()

	statements := strings.Split(sql, ";")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := tidbDB.ExecContext(ctx, stmt); err != nil {
			zap.L().Warn("DDL statement failed", zap.Error(err), zap.String("stmt", truncate(stmt, 200)))
			if m.cfg.Migration.OnError != "skip" {
				return fmt.Errorf("execute DDL: %w", err)
			}
		}
	}
	return nil
}

func (m *Migrator) writeUnsupportedLog(objects []Object) {
	var lines []string
	for _, obj := range objects {
		lines = append(lines, fmt.Sprintf("%s\t%s\t%s", obj.Type, obj.Name, obj.Note))
	}
	content := "Type\tName\tNote\n" + strings.Join(lines, "\n") + "\n"
	_ = os.WriteFile("unsupported-objects.log", []byte(content), 0644)
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
