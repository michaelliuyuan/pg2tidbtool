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

	// SET only changed columns. Unchanged TOAST ('u') columns carry no value and
	// must be dropped — rendering them as '' / NULL would corrupt the row.
	setClauses := make([]string, 0, len(event.Columns))
	for _, col := range event.Columns {
		if col.Unchanged {
			continue
		}
		setClauses = append(setClauses,
			fmt.Sprintf("%s = %s", quoteMySQLIdent(col.Name), t.formatValue(col)))
	}
	if len(setClauses) == 0 {
		return "", fmt.Errorf("cdc transformer: UPDATE with no settable columns for %s (all unchanged/missing)", tableName)
	}

	// Build WHERE from the PK
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

// buildWhere constructs a PK-based WHERE clause for UPDATE/DELETE.
//
// Per #t48 Bug#5: the WHERE must target the row identity (PK / replica
// identity) only — never the full new-image row. Under REPLICA IDENTITY DEFAULT
// a non-key UPDATE carries no old tuple, so we fall back to the NEW image's PK
// columns (the PK is unchanged for a non-key update). With no usable PK (table
// without a replica identity) this returns empty and the caller errors out
// rather than emitting a silent 0-row no-op.
func (t *Transformer) buildWhere(event *CDCEvent) []string {
	keys := keyColumns(event.OldColumns) // prefer old image (UPDATE-of-PK, FULL identity)
	if len(keys) == 0 {
		keys = keyColumns(event.Columns) // fall back to new image PK (DEFAULT non-key update, DELETE key image)
	}

	var clauses []string
	for _, col := range keys {
		if col.Unchanged {
			continue // 'u': no value present, cannot anchor a WHERE predicate
		}
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

// keyColumns returns only the PK / replica-identity columns from a column slice.
func keyColumns(cols []ColumnValue) []ColumnValue {
	var out []ColumnValue
	for _, c := range cols {
		if c.IsKey {
			out = append(out, c)
		}
	}
	return out
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
