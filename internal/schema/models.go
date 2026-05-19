package schema

import (
	"strings"
)

type Object struct {
	Schema      string
	Name        string
	Type        ObjectType
	DDL         string
	Unsupported bool
	Note        string
}

type ObjectType string

const (
	ObjTable       ObjectType = "table"
	ObjColumn      ObjectType = "column"
	ObjIndex       ObjectType = "index"
	ObjConstraint  ObjectType = "constraint"
	ObjView        ObjectType = "view"
	ObjSequence    ObjectType = "sequence"
	ObjEnum        ObjectType = "enum"
	ObjFunction    ObjectType = "function"
	ObjTrigger     ObjectType = "trigger"
	ObjExtension   ObjectType = "extension"
	ObjCustomType  ObjectType = "custom_type"
)

type Column struct {
	TableName     string
	ColumnName    string
	OrdinalPos    int
	DataType      string
	PGType        PGType
	MaxLength     int
	NumericPrec   int
	NumericScale  int
	IsNullable    bool
	DefaultValue  string
	IsPrimaryKey  bool
	IsAutoIncr    bool
	Comment       string
}

type Index struct {
	TableName  string
	IndexName  string
	Columns    []string
	IsUnique   bool
	IsPrimary  bool
	IndexType  string
	Where      string
}

type ForeignKey struct {
	ConstraintName string
	TableName      string
	Columns        []string
	RefTable       string
	RefColumns     []string
	OnDelete       string
	OnUpdate       string
}

type View struct {
	Schema     string
	Name       string
	Definition string
}

type Sequence struct {
	Schema    string
	Name      string
	DataType  string
	StartVal  int64
	Increment int64
	MaxVal    int64
	MinVal    int64
	CacheVal  int64
	CycleOpt  bool
	OwnedBy   string
}

type EnumType struct {
	Schema string
	Name   string
	Values []string
}

type SchemaInfo struct {
	Tables      []TableInfo
	Views       []View
	Sequences   []Sequence
	Enums       []EnumType
	Unsupported []Object
}

type TableInfo struct {
	Schema     string
	Name       string
	Columns    []Column
	Indexes    []Index
	ForeignKeys []ForeignKey
	Comment    string
}

func (t *TableInfo) PrimaryKey() *Index {
	for _, idx := range t.Indexes {
		if idx.IsPrimary {
			return &idx
		}
	}
	return nil
}

func (t *TableInfo) HasSerialColumn() bool {
	for _, col := range t.Columns {
		if col.IsAutoIncr {
			return true
		}
	}
	for _, col := range t.Columns {
		upper := strings.ToUpper(col.DefaultValue)
		if strings.Contains(upper, "NEXTVAL") {
			return true
		}
	}
	return false
}

func QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
