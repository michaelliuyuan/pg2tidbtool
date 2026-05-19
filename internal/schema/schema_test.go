package schema

import (
	"strings"
	"testing"
)

func TestMapType(t *testing.T) {
	tests := []struct {
		pgType   PGType
		expected string
		support  SupportLevel
	}{
		{PGInteger, "INT", Supported},
		{PGBigint, "BIGINT", Supported},
		{PGVarchar, "VARCHAR", Supported},
		{PGText, "TEXT", Supported},
		{PGBoolean, "TINYINT(1)", Convert},
		{PGBytea, "BLOB", Convert},
		{PGJSONB, "JSON", Convert},
		{PGUUID, "CHAR(36)", Convert},
		{PGTSVector, "", Unsupported},
		{PGPoint, "", Unsupported},
	}

	for _, tt := range tests {
		m, ok := MapType(tt.pgType)
		if !ok {
			t.Errorf("type %s not found in map", tt.pgType)
			continue
		}
		if m.MySQLType != tt.expected {
			t.Errorf("type %s: expected %s, got %s", tt.pgType, tt.expected, m.MySQLType)
		}
		if m.SupportLevel != tt.support {
			t.Errorf("type %s: expected support %s, got %s", tt.pgType, tt.support, m.SupportLevel)
		}
	}
}

func TestMapTypeWithPrecision(t *testing.T) {
	tests := []struct {
		pgType    PGType
		prec      int
		scale     int
		expected  string
	}{
		{PGNumeric, 10, 2, "DECIMAL(10,2)"},
		{PGNumeric, 10, 0, "DECIMAL(10)"},
		{PGNumeric, 0, 0, "DECIMAL"},
		{PGVarchar, 255, 0, "VARCHAR(255)"},
		{PGChar, 10, 0, "CHAR(10)"},
		{PGInteger, 0, 0, "INT"},
	}

	for _, tt := range tests {
		result := MapTypeWithPrecision(tt.pgType, tt.prec, tt.scale)
		if result != tt.expected {
			t.Errorf("MapTypeWithPrecision(%s, %d, %d) = %s, want %s", tt.pgType, tt.prec, tt.scale, result, tt.expected)
		}
	}
}

func TestIsArray(t *testing.T) {
	if !IsArray("_int4") {
		t.Error("_int4 should be array")
	}
	if !IsArray("int[]") {
		t.Error("int[] should be array")
	}
	if IsArray("integer") {
		t.Error("integer should not be array")
	}
}

func TestBaseArrayType(t *testing.T) {
	if BaseArrayType("_int4") != "int4" {
		t.Error("expected int4")
	}
	if BaseArrayType("int[]") != "int" {
		t.Error("expected int")
	}
}

func TestBuildTableDDL(t *testing.T) {
	table := TableInfo{
		Schema: "public",
		Name:   "users",
		Columns: []Column{
			{ColumnName: "id", PGType: PGBigint, IsNullable: false, IsAutoIncr: true},
			{ColumnName: "name", PGType: PGVarchar, MaxLength: 255, IsNullable: false},
			{ColumnName: "email", PGType: PGVarchar, MaxLength: 255, IsNullable: true},
			{ColumnName: "active", PGType: PGBoolean, IsNullable: false, DefaultValue: "true"},
		},
	}

	builder := NewDDLBuilder()
	err := builder.BuildTableDDL(table)
	if err != nil {
		t.Fatal(err)
	}

	sql := builder.JoinSQL()
	if !strings.Contains(sql, "CREATE TABLE") {
		t.Error("should contain CREATE TABLE")
	}
	if !strings.Contains(sql, "`id`") {
		t.Error("should contain id column")
	}
	if !strings.Contains(sql, "BIGINT") {
		t.Error("should contain BIGINT type")
	}
	if !strings.Contains(sql, "AUTO_INCREMENT") {
		t.Error("should contain AUTO_INCREMENT")
	}
	if !strings.Contains(sql, "NOT NULL") {
		t.Error("should contain NOT NULL")
	}
}

func TestBuildIndexDDL(t *testing.T) {
	idx := Index{
		TableName: "users",
		IndexName: "idx_email",
		Columns:   []string{"email"},
		IsUnique:  true,
		IndexType: "btree",
	}

	builder := NewDDLBuilder()
	ddl := builder.BuildIndexDDL(idx)
	if !strings.Contains(ddl, "UNIQUE") {
		t.Error("should be unique index")
	}
	if !strings.Contains(ddl, "idx_email") {
		t.Error("should contain index name")
	}
}

func TestBuildUnsupportedIndexDDL(t *testing.T) {
	idx := Index{
		TableName: "docs",
		IndexName: "idx_content",
		Columns:   []string{"content"},
		IndexType: "gin",
	}

	builder := NewDDLBuilder()
	ddl := builder.BuildIndexDDL(idx)
	if !strings.Contains(ddl, "WARNING") {
		t.Error("should warn about unsupported index type")
	}
}

func TestBuildForeignKeyDDL(t *testing.T) {
	fk := ForeignKey{
		ConstraintName: "fk_orders_user",
		TableName:      "orders",
		Columns:        []string{"user_id"},
		RefTable:       "users",
		RefColumns:     []string{"id"},
		OnDelete:       "CASCADE",
		OnUpdate:       "NO ACTION",
	}

	builder := NewDDLBuilder()
	ddl := builder.BuildForeignKeyDDL(fk)
	if !strings.Contains(ddl, "FOREIGN KEY") {
		t.Error("should contain FOREIGN KEY")
	}
	if !strings.Contains(ddl, "CASCADE") {
		t.Error("should contain CASCADE")
	}
}

func TestBuildViewDDL(t *testing.T) {
	view := View{
		Schema:     "public",
		Name:       "active_users",
		Definition: "SELECT id, name FROM users WHERE active = true",
	}

	builder := NewDDLBuilder()
	ddl := builder.BuildViewDDL(view)
	if !strings.Contains(ddl, "CREATE OR REPLACE VIEW") {
		t.Error("should contain CREATE OR REPLACE VIEW")
	}
}

func TestBuildEnumDDL(t *testing.T) {
	enum := EnumType{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive", "pending"},
	}

	builder := NewDDLBuilder()
	ddl := builder.BuildEnumDDL(enum)
	if !strings.Contains(ddl, "active") {
		t.Error("should contain enum values")
	}
}

func TestConvertDefaultValue(t *testing.T) {
	tests := []struct {
		input    string
		pgType   PGType
		expected string
	}{
		{"true", PGBoolean, "1"},
		{"false", PGBoolean, "0"},
		{"'hello'", PGText, "'hello'"},
	}

	for _, tt := range tests {
		result := convertDefaultValue(tt.input, tt.pgType)
		if result != tt.expected {
			t.Errorf("convertDefaultValue(%s, %s) = %s, want %s", tt.input, tt.pgType, result, tt.expected)
		}
	}
}

func TestQuoteIdentifier(t *testing.T) {
	if QuoteIdentifier("table") != "`table`" {
		t.Error("should backtick-quote identifier")
	}
	if QuoteIdentifier("ta`ble") != "`ta``ble`" {
		t.Error("should escape backticks")
	}
}

func TestTableInfoPrimaryKey(t *testing.T) {
	table := TableInfo{
		Indexes: []Index{
			{IndexName: "idx_name", Columns: []string{"name"}, IsPrimary: false},
			{IndexName: "pk_id", Columns: []string{"id"}, IsPrimary: true},
		},
	}
	pk := table.PrimaryKey()
	if pk == nil || pk.IndexName != "pk_id" {
		t.Error("should find primary key")
	}

	table2 := TableInfo{Indexes: []Index{}}
	if table2.PrimaryKey() != nil {
		t.Error("should return nil when no PK")
	}
}

func TestEscapeSQLString(t *testing.T) {
	if escapeSQLString("it's") != "it''s" {
		t.Error("should escape single quotes")
	}
}
