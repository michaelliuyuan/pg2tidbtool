package schema

import (
	"fmt"
	"strings"
)

type DDLBuilder struct {
	statements []string
}

func NewDDLBuilder() *DDLBuilder {
	return &DDLBuilder{}
}

func (b *DDLBuilder) BuildTableDDL(table TableInfo) error {
	var cols []string
	pk := table.PrimaryKey()
	pkColSet := make(map[string]bool)
	if pk != nil {
		for _, c := range pk.Columns {
			pkColSet[c] = true
		}
	}

	for _, col := range table.Columns {
		colDDL, err := b.buildColumnDDL(col)
		if err != nil {
			return fmt.Errorf("column %s.%s: %w", table.Name, col.ColumnName, err)
		}
		cols = append(cols, colDDL)
	}

	if pk != nil {
		pkCols := make([]string, len(pk.Columns))
		for i, c := range pk.Columns {
			pkCols[i] = QuoteIdentifier(c)
		}
		cols = append(cols, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkCols, ", ")))
	}

	var tableSuffix string
	if table.Comment != "" {
		tableSuffix = fmt.Sprintf(" COMMENT '%s'", escapeSQLString(table.Comment))
	}

	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)%s",
		QuoteIdentifier(table.Name),
		strings.Join(cols, ",\n  "),
		tableSuffix)

	b.statements = append(b.statements, ddl)

	return nil
}

func (b *DDLBuilder) buildColumnDDL(col Column) (string, error) {
	// For character types, MaxLength holds character_maximum_length;
	// for numeric types, NumericPrec/NumericScale hold the precision.
	// Pass the correct precision based on column type.
	precision := col.NumericPrec
	if col.PGType == PGVarchar || col.PGType == PGChar {
		precision = col.MaxLength
	}
	mysqlType := MapTypeWithPrecision(col.PGType, precision, col.NumericScale)
	if mysqlType == "" {
		mysqlType = "TEXT"
	}

	if col.ColumnName == "" {
		return "", fmt.Errorf("empty column name")
	}

	parts := []string{
		QuoteIdentifier(col.ColumnName),
		mysqlType,
	}

	if !col.IsNullable {
		parts = append(parts, "NOT NULL")
	} else {
		parts = append(parts, "NULL")
	}

	if col.IsAutoIncr {
		parts = append(parts, "AUTO_INCREMENT")
	} else if col.DefaultValue != "" {
		def := convertDefaultValue(col.DefaultValue, col.PGType)
		if def != "" {
			parts = append(parts, "DEFAULT "+def)
		}
	}

	if col.Comment != "" {
		parts = append(parts, fmt.Sprintf("COMMENT '%s'", escapeSQLString(col.Comment)))
	}

	return strings.Join(parts, " "), nil
}

func (b *DDLBuilder) BuildPrimaryKeyDDL(table TableInfo) {
	pk := table.PrimaryKey()
	if pk == nil {
		return
	}
	cols := make([]string, len(pk.Columns))
	for i, c := range pk.Columns {
		cols[i] = QuoteIdentifier(c)
	}
	b.statements = append(b.statements, fmt.Sprintf(
		"ALTER TABLE %s ADD PRIMARY KEY (%s)",
		QuoteIdentifier(table.Name),
		strings.Join(cols, ", "),
	))
}

func (b *DDLBuilder) BuildIndexDDL(idx Index) string {
	cols := make([]string, len(idx.Columns))
	for i, c := range idx.Columns {
		cols[i] = QuoteIdentifier(c)
	}

	switch idx.IndexType {
	case "btree", "":
		// Supported natively
	case "gin", "gist", "hash", "spgist", "brin":
		return fmt.Sprintf("-- WARNING: index %s uses %s which is not supported in TiDB, converted to regular index",
			idx.IndexName, idx.IndexType)
	default:
		return fmt.Sprintf("-- WARNING: unknown index type %s for %s", idx.IndexType, idx.IndexName)
	}

	if idx.IsPrimary {
		return ""
	}

	var unique string
	if idx.IsUnique {
		unique = "UNIQUE "
	}

	return fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
		unique,
		QuoteIdentifier(idx.IndexName),
		QuoteIdentifier(idx.TableName),
		strings.Join(cols, ", "))
}

func (b *DDLBuilder) BuildForeignKeyDDL(fk ForeignKey) string {
	cols := make([]string, len(fk.Columns))
	for i, c := range fk.Columns {
		cols[i] = QuoteIdentifier(c)
	}
	refCols := make([]string, len(fk.RefColumns))
	for i, c := range fk.RefColumns {
		refCols[i] = QuoteIdentifier(c)
	}
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE %s ON UPDATE %s",
		QuoteIdentifier(fk.TableName),
		QuoteIdentifier(fk.ConstraintName),
		strings.Join(cols, ", "),
		QuoteIdentifier(fk.RefTable),
		strings.Join(refCols, ", "),
		fk.OnDelete,
		fk.OnUpdate)
}

func (b *DDLBuilder) BuildViewDDL(view View) string {
	def := view.Definition
	def = strings.TrimSpace(def)
	if !strings.HasPrefix(strings.ToUpper(def), "SELECT") {
		return fmt.Sprintf("-- WARNING: view %s has complex definition, manual review needed\n-- %s", view.Name, def)
	}
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", QuoteIdentifier(view.Name), def)
}

func (b *DDLBuilder) BuildEnumDDL(enum EnumType) string {
	values := make([]string, len(enum.Values))
	for i, v := range enum.Values {
		values[i] = fmt.Sprintf("'%s'", escapeSQLString(v))
	}
	return fmt.Sprintf("-- ENUM %s: TiDB does not support CREATE TYPE AS ENUM, using VARCHAR or explicit ENUM\n-- Values: %s",
		enum.Name, strings.Join(values, ", "))
}

func (b *DDLBuilder) Statements() []string {
	return b.statements
}

func (b *DDLBuilder) JoinSQL() string {
	return strings.Join(b.statements, ";\n\n") + ";"
}

func convertDefaultValue(pgDefault string, pgType PGType) string {
	d := strings.TrimSpace(pgDefault)

	switch {
	case d == "", strings.ToUpper(d) == "NULL":
		return ""
	case strings.Contains(strings.ToUpper(d), "NEXTVAL"):
		return ""
	case strings.Contains(strings.ToUpper(d), "CURRENT_TIMESTAMP"):
		if pgType == PGTimestamp || pgType == PGTimestampTZ {
			return "CURRENT_TIMESTAMP(6)"
		}
		return "CURRENT_TIMESTAMP"
	case strings.ToUpper(d) == "TRUE":
		return "1"
	case strings.ToUpper(d) == "FALSE":
		return "0"
	}

	if idx := strings.Index(d, "::"); idx > 0 {
		raw := strings.TrimSpace(d[:idx])
		if strings.ToUpper(raw) == "NULL" {
			return ""
		}
		if strings.HasPrefix(raw, "'") && strings.HasSuffix(raw, "'") {
			return raw
		}
		if isSimpleLiteral(raw) {
			return raw
		}
		return ""
	}

	if strings.HasPrefix(d, "'") && strings.HasSuffix(d, "'") {
		return d
	}

	if isSimpleLiteral(d) {
		return d
	}

	return ""
}

func isSimpleLiteral(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
		case c == '.' || c == '-' || c == '+':
		default:
			return false
		}
	}
	return true
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
