package cdc

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// Transformer converts raw PG logical replication events into TiDB-compatible
// SQL statements. It handles type mapping, quoting, and SQL generation.
type Transformer struct {
	cfg TransformerConfig
	log *zap.Logger
}

// NewTransformer creates a new event transformer.
func NewTransformer(cfg TransformerConfig) *Transformer {
	return &Transformer{
		cfg: cfg,
		log: zap.NewNop(),
	}
}

// SetLogger sets the logger.
func (t *Transformer) SetLogger(log *zap.Logger) {
	t.log = log
}

// TransformEvent converts a CDCEvent into a SQL statement suitable for TiDB.
// Returns the SQL string and any error.
func (t *Transformer) TransformEvent(event *CDCEvent) (string, error) {
	switch event.Kind {
	case EventInsert:
		return t.transformInsert(event)
	case EventUpdate:
		return t.transformUpdate(event)
	case EventDelete:
		return t.transformDelete(event)
	case EventTruncate:
		return t.transformTruncate(event)
	case EventDDL:
		return t.transformDDL(event)
	default:
		return "", fmt.Errorf("cdc transformer: unknown event kind %q", event.Kind)
	}
}

func (t *Transformer) transformInsert(event *CDCEvent) (string, error) {
	tableName := t.quotedTable(event.Schema, event.Table)

	columns := make([]string, 0, len(event.Columns))
	values := make([]string, 0, len(event.Columns))
	for _, col := range event.Columns {
		columns = append(columns, quoteMySQLIdent(col.Name))
		values = append(values, t.formatValue(col))
	}

	sql := fmt.Sprintf("REPLACE INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(values, ", "),
	)
	return sql, nil
}

func (t *Transformer) transformUpdate(event *CDCEvent) (string, error) {
	tableName := t.quotedTable(event.Schema, event.Table)

	setClauses := make([]string, 0, len(event.Columns))
	for _, col := range event.Columns {
		setClauses = append(setClauses,
			fmt.Sprintf("%s = %s", quoteMySQLIdent(col.Name), t.formatValue(col)))
	}

	// Build WHERE from old columns (primary key / all old values)
	whereClauses := t.buildWhere(event)
	if len(whereClauses) == 0 {
		return "", fmt.Errorf("cdc transformer: UPDATE without key columns for %s", tableName)
	}

	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		tableName,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)
	return sql, nil
}

func (t *Transformer) transformDelete(event *CDCEvent) (string, error) {
	tableName := t.quotedTable(event.Schema, event.Table)

	whereClauses := t.buildWhere(event)
	if len(whereClauses) == 0 {
		return "", fmt.Errorf("cdc transformer: DELETE without key columns for %s", tableName)
	}

	sql := fmt.Sprintf("DELETE FROM %s WHERE %s",
		tableName,
		strings.Join(whereClauses, " AND "),
	)
	return sql, nil
}

func (t *Transformer) transformTruncate(event *CDCEvent) (string, error) {
	tableName := t.quotedTable(event.Schema, event.Table)
	return fmt.Sprintf("TRUNCATE TABLE %s", tableName), nil
}

func (t *Transformer) transformDDL(event *CDCEvent) (string, error) {
	// DDL transformation is handled by the DDL tracker (P3).
	// For now, return the DDL as-is with a note.
	if event.DDL == "" {
		return "", fmt.Errorf("cdc transformer: DDL event without DDL text")
	}
	return event.DDL, nil
}

// buildWhere constructs WHERE clauses from the event's columns.
// Prefers old columns (UPDATE) for the WHERE to match the original row.
func (t *Transformer) buildWhere(event *CDCEvent) []string {
	source := event.Columns
	if len(event.OldColumns) > 0 {
		source = event.OldColumns
	}

	var clauses []string
	for _, col := range source {
		if col.Value == nil {
			clauses = append(clauses,
				fmt.Sprintf("%s IS NULL", quoteMySQLIdent(col.Name)))
		} else {
			clauses = append(clauses,
				fmt.Sprintf("%s = %s", quoteMySQLIdent(col.Name), t.formatValue(col)))
		}
	}
	return clauses
}

// formatValue formats a column value for MySQL/TiDB SQL.
func (t *Transformer) formatValue(col ColumnValue) string {
	if col.Value == nil {
		return "NULL"
	}

	str := fmt.Sprintf("%v", col.Value)

	// Truncate if configured
	if t.cfg.MaxColumnValueLength > 0 && len(str) > t.cfg.MaxColumnValueLength {
		str = str[:t.cfg.MaxColumnValueLength]
	}

	// Handle special cases
	if str == "" {
		return "''"
	}

	// Escape single quotes
	escaped := strings.ReplaceAll(str, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `''`)

	return fmt.Sprintf("'%s'", escaped)
}

// quotedTable returns a fully-qualified table name with MySQL quoting.
func (t *Transformer) quotedTable(schema, table string) string {
	if schema == "" || schema == "public" {
		return quoteMySQLIdent(table)
	}
	return quoteMySQLIdent(schema) + "." + quoteMySQLIdent(table)
}

// quoteMySQLIdent quotes a MySQL/TiDB identifier with backticks.
func quoteMySQLIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
