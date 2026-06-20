package cdc

import (
	"strings"
	"testing"
)

// TestMakeDDLIdempotent covers the at-least-once DDL replay guard (#t59 §4.2):
// CREATE/DROP become IF NOT EXISTS / IF EXISTS; ALTER and already-guarded
// statements are left alone.
func TestMakeDDLIdempotent(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"create adds IF NOT EXISTS", "CREATE TABLE t (id int)", "CREATE TABLE IF NOT EXISTS t (id int)"},
		{"create already guarded", "CREATE TABLE IF NOT EXISTS t (id int)", "CREATE TABLE IF NOT EXISTS t (id int)"},
		{"drop adds IF EXISTS", "DROP TABLE t", "DROP TABLE IF EXISTS t"},
		{"drop already guarded", "DROP TABLE IF EXISTS t", "DROP TABLE IF EXISTS t"},
		{"alter unchanged (non-idempotent; checkpoint protects)", "ALTER TABLE t ADD COLUMN c int", "ALTER TABLE t ADD COLUMN c int"},
		{"lowercase keyword handled", "create table t (id int)", "CREATE TABLE IF NOT EXISTS t (id int)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := makeDDLIdempotent(c.in); got != c.want {
				t.Errorf("makeDDLIdempotent(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestDDLTransform_TypeMappingAndIdempotency checks the full TABLE transform:
// PG→TiDB type mapping still applies AND the IF NOT EXISTS guard is added.
func TestDDLTransform_TypeMappingAndIdempotency(t *testing.T) {
	dt := NewDDLTransformer()
	got := dt.Transform("CREATE TABLE t (id SERIAL PRIMARY KEY, data JSONB, uid UUID)", "TABLE")
	for _, sub := range []string{"CREATE TABLE IF NOT EXISTS", "BIGINT AUTO_INCREMENT", "JSON", "CHAR(36)"} {
		if !strings.Contains(got, sub) {
			t.Errorf("Transform: output %q missing %q", got, sub)
		}
	}
	if strings.Contains(got, "SERIAL") {
		t.Errorf("Transform: SERIAL not mapped, got %q", got)
	}
}

// TestCheckpoint_LastDDLID round-trips the DDL id through the checkpoint
// manager (at-least-once resume, #t59).
func TestCheckpoint_LastDDLID(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/cp.json"
	cm := NewCheckpointManager(path)
	cm.SetSlotName("s")
	cm.SetLastDDLID(42)
	if got := cm.GetLastDDLID(); got != 42 {
		t.Fatalf("GetLastDDLID before save = %d, want 42", got)
	}
	if err := cm.Save(); err != nil {
		t.Fatal(err)
	}

	cm2 := NewCheckpointManager(path)
	if _, err := cm2.Load(); err != nil {
		t.Fatal(err)
	}
	if got := cm2.GetLastDDLID(); got != 42 {
		t.Errorf("GetLastDDLID after reload = %d, want 42 (must persist)", got)
	}
}

// TestShouldApplyDDL covers #t61: PG ddl_command_end fires once per sub-object,
// all carrying the parent statement. Only the entry whose object type matches
// the statement's primary kind is applied; piggybacked sub-objects (sequence,
// a table's PK index) are skipped. A standalone CREATE INDEX is still applied.
func TestShouldApplyDDL(t *testing.T) {
	createTable := "CREATE TABLE cdc_ddl_e2e (id SERIAL PRIMARY KEY, j JSONB)"
	cases := []struct {
		name string
		e    DDLEntry
		want bool
	}{
		{"table row of CREATE TABLE", DDLEntry{DDL: createTable, ObjectType: "table"}, true},
		{"sequence sub-object of CREATE TABLE", DDLEntry{DDL: createTable, ObjectType: "sequence"}, false},
		{"PK-index sub-object of CREATE TABLE", DDLEntry{DDL: createTable, ObjectType: "index"}, false},
		{"standalone CREATE INDEX", DDLEntry{DDL: "CREATE INDEX idx ON t (c)", ObjectType: "index"}, true},
		{"standalone CREATE UNIQUE INDEX", DDLEntry{DDL: "CREATE UNIQUE INDEX uq ON t (c)", ObjectType: "index"}, true},
		{"standalone DROP INDEX", DDLEntry{DDL: "DROP INDEX idx", ObjectType: "index"}, true},
		{"ALTER TABLE ADD COLUMN", DDLEntry{DDL: "ALTER TABLE t ADD COLUMN c INT", ObjectType: "table"}, true},
		{"DROP TABLE", DDLEntry{DDL: "DROP TABLE t", ObjectType: "table"}, true},
		{"standalone CREATE SEQUENCE", DDLEntry{DDL: "CREATE SEQUENCE s", ObjectType: "sequence"}, false},
		{"object-type case-insensitive", DDLEntry{DDL: createTable, ObjectType: "TABLE"}, true},
	}
	for _, c := range cases {
		if got := shouldApplyDDL(c.e); got != c.want {
			t.Errorf("%s: shouldApplyDDL = %v, want %v (ddl=%q type=%q)",
				c.name, got, c.want, c.e.DDL, c.e.ObjectType)
		}
	}
}
