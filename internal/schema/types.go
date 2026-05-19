package schema

import (
	"fmt"
	"strings"
)

type PGType string

const (
	PGSmallint    PGType = "smallint"
	PGInteger     PGType = "integer"
	PGBigint      PGType = "bigint"
	PGSerial      PGType = "serial"
	PGBigSerial   PGType = "bigserial"
	PGSmallSerial PGType = "smallserial"
	PGReal        PGType = "real"
	PGDouble      PGType = "double precision"
	PGNumeric     PGType = "numeric"
	PGDecimal     PGType = "decimal"
	PGMoney       PGType = "money"
	PGChar        PGType = "character"
	PGVarchar     PGType = "character varying"
	PGText        PGType = "text"
	PGBytea       PGType = "bytea"
	PGBoolean     PGType = "boolean"
	PGDate        PGType = "date"
	PGTime        PGType = "time"
	PGTimeTZ      PGType = "time with time zone"
	PGTimestamp   PGType = "timestamp without time zone"
	PGTimestampTZ PGType = "timestamp with time zone"
	PGInterval    PGType = "interval"
	PGJSON        PGType = "json"
	PGJSONB       PGType = "jsonb"
	PGUUID        PGType = "uuid"
	PGMacaddr     PGType = "macaddr"
	PGMacaddr8    PGType = "macaddr8"
	PGInet        PGType = "inet"
	PGCidr        PGType = "cidr"
	PGBit         PGType = "bit"
	PGVarbit      PGType = "bit varying"
	PGPoint       PGType = "point"
	PGLine        PGType = "line"
	PGLseg        PGType = "lseg"
	PGBox         PGType = "box"
	PGPath        PGType = "path"
	PGPolygon     PGType = "polygon"
	PGCircle      PGType = "circle"
	PGTSVector    PGType = "tsvector"
	PGTSQuery     PGType = "tsquery"
	PGXML         PGType = "xml"
	PGArray       PGType = "ARRAY"
	PGUserDefined PGType = "USER-DEFINED"
	PGEnum        PGType = "enum"
)

type TypeMapping struct {
	MySQLType    string
	SupportLevel SupportLevel
	Note         string
}

type SupportLevel string

const (
	Supported    SupportLevel = "supported"
	Convert      SupportLevel = "convert"
	Unsupported  SupportLevel = "unsupported"
)

var typeMap = map[PGType]TypeMapping{
	PGSmallint:    {MySQLType: "SMALLINT", SupportLevel: Supported},
	PGInteger:     {MySQLType: "INT", SupportLevel: Supported},
	PGBigint:      {MySQLType: "BIGINT", SupportLevel: Supported},
	PGSerial:      {MySQLType: "INT AUTO_INCREMENT", SupportLevel: Convert, Note: "serial -> INT AUTO_INCREMENT"},
	PGBigSerial:   {MySQLType: "BIGINT AUTO_INCREMENT", SupportLevel: Convert, Note: "bigserial -> BIGINT AUTO_INCREMENT"},
	PGSmallSerial: {MySQLType: "SMALLINT AUTO_INCREMENT", SupportLevel: Convert, Note: "smallserial -> SMALLINT AUTO_INCREMENT"},
	PGReal:        {MySQLType: "FLOAT", SupportLevel: Supported},
	PGDouble:      {MySQLType: "DOUBLE", SupportLevel: Supported},
	PGNumeric:     {MySQLType: "DECIMAL", SupportLevel: Supported},
	PGDecimal:     {MySQLType: "DECIMAL", SupportLevel: Supported},
	PGMoney:       {MySQLType: "DECIMAL(19,2)", SupportLevel: Convert, Note: "money -> DECIMAL(19,2)"},
	PGChar:        {MySQLType: "CHAR", SupportLevel: Supported},
	PGVarchar:     {MySQLType: "VARCHAR", SupportLevel: Supported},
	PGText:        {MySQLType: "TEXT", SupportLevel: Supported},
	PGBytea:       {MySQLType: "BLOB", SupportLevel: Convert, Note: "bytea -> BLOB, data conversion needed"},
	PGBoolean:     {MySQLType: "TINYINT(1)", SupportLevel: Convert, Note: "boolean true/false -> 1/0"},
	PGDate:        {MySQLType: "DATE", SupportLevel: Supported},
	PGTime:        {MySQLType: "TIME", SupportLevel: Supported},
	PGTimeTZ:      {MySQLType: "TIME", SupportLevel: Convert, Note: "timezone info lost"},
	PGTimestamp:   {MySQLType: "DATETIME", SupportLevel: Supported},
	PGTimestampTZ: {MySQLType: "TIMESTAMP", SupportLevel: Convert, Note: "converted to UTC"},
	PGInterval:    {MySQLType: "VARCHAR(64)", SupportLevel: Convert, Note: "interval -> VARCHAR, human readable"},
	PGJSON:        {MySQLType: "JSON", SupportLevel: Supported},
	PGJSONB:       {MySQLType: "JSON", SupportLevel: Convert, Note: "jsonb -> JSON, index lost"},
	PGUUID:        {MySQLType: "CHAR(36)", SupportLevel: Convert, Note: "uuid -> CHAR(36)"},
	PGMacaddr:     {MySQLType: "VARCHAR(17)", SupportLevel: Convert},
	PGMacaddr8:    {MySQLType: "VARCHAR(23)", SupportLevel: Convert},
	PGInet:        {MySQLType: "VARCHAR(43)", SupportLevel: Convert, Note: "inet -> VARCHAR"},
	PGCidr:        {MySQLType: "VARCHAR(43)", SupportLevel: Convert},
	PGBit:         {MySQLType: "BIT", SupportLevel: Supported},
	PGVarbit:      {MySQLType: "BIT", SupportLevel: Convert, Note: "variable bit -> BIT, may truncate"},
	PGXML:         {MySQLType: "LONGTEXT", SupportLevel: Convert},
	PGTSVector:    {MySQLType: "", SupportLevel: Unsupported, Note: "full-text search not supported"},
	PGTSQuery:     {MySQLType: "", SupportLevel: Unsupported, Note: "full-text search not supported"},
	PGPoint:       {MySQLType: "", SupportLevel: Unsupported, Note: "geometric types not supported"},
	PGLine:        {MySQLType: "", SupportLevel: Unsupported},
	PGLseg:        {MySQLType: "", SupportLevel: Unsupported},
	PGBox:         {MySQLType: "", SupportLevel: Unsupported},
	PGPath:        {MySQLType: "", SupportLevel: Unsupported},
	PGPolygon:     {MySQLType: "", SupportLevel: Unsupported},
	PGCircle:      {MySQLType: "", SupportLevel: Unsupported},
	PGArray:       {MySQLType: "JSON", SupportLevel: Convert, Note: "array -> JSON"},
	PGEnum:        {MySQLType: "ENUM", SupportLevel: Convert, Note: "needs explicit ENUM definition"},
	PGUserDefined: {MySQLType: "", SupportLevel: Unsupported, Note: "user-defined types not supported"},
}

func MapType(pgType PGType) (TypeMapping, bool) {
	m, ok := typeMap[pgType]
	return m, ok
}

func MapTypeWithPrecision(pgType PGType, precision, scale int) string {
	m, ok := typeMap[pgType]
	if !ok || m.SupportLevel == Unsupported {
		return "TEXT"
	}

	base := m.MySQLType
	switch pgType {
	case PGNumeric, PGDecimal:
		if precision > 0 && scale > 0 {
			return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
		} else if precision > 0 {
			return fmt.Sprintf("DECIMAL(%d)", precision)
		}
		return "DECIMAL"
	case PGChar:
		if precision > 0 {
			return fmt.Sprintf("CHAR(%d)", precision)
		}
		return base
	case PGVarchar:
		if precision > 0 {
			return fmt.Sprintf("VARCHAR(%d)", precision)
		}
		return base
	case PGBit, PGVarbit:
		if precision > 0 {
			return fmt.Sprintf("BIT(%d)", precision)
		}
		return base
	}

	return base
}

func IsArray(pgType string) bool {
	return strings.HasPrefix(pgType, "_") || strings.HasSuffix(pgType, "[]")
}

func BaseArrayType(pgType string) string {
	t := strings.TrimPrefix(pgType, "_")
	t = strings.TrimSuffix(t, "[]")
	return t
}
