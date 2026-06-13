package cdc

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestTransformer_Insert(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventInsert,
		Schema: "public",
		Table:  "users",
		Columns: []ColumnValue{
			{Name: "id", Value: "1", Type: "oid_23"},
			{Name: "name", Value: "Alice", Type: "oid_25"},
			{Name: "email", Value: "alice@example.com", Type: "oid_25"},
		},
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(insert): %v", err)
	}

	expected := "REPLACE INTO `users` (`id`, `name`, `email`) VALUES ('1', 'Alice', 'alice@example.com')"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTransformer_Update(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventUpdate,
		Schema: "public",
		Table:  "users",
		Columns: []ColumnValue{
			{Name: "id", Value: "1", Type: "oid_23", IsKey: true},
			{Name: "name", Value: "Bob", Type: "oid_25"},
		},
		OldColumns: []ColumnValue{
			{Name: "id", Value: "1", Type: "oid_23", IsKey: true},
			{Name: "name", Value: "Alice", Type: "oid_25"},
		},
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(update): %v", err)
	}

	// WHERE targets the PK (id) from the old image, not the full row.
	expected := "UPDATE `users` SET `id` = '1', `name` = 'Bob' WHERE `id` = '1'"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTransformer_Delete(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventDelete,
		Schema: "public",
		Table:  "users",
		Columns: []ColumnValue{
			{Name: "id", Value: "42", Type: "oid_23", IsKey: true},
		},
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(delete): %v", err)
	}

	expected := "DELETE FROM `users` WHERE `id` = '42'"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

// TestTransformer_UpdateNonKeyFallback covers #t48 Bug#5: under REPLICA IDENTITY
// DEFAULT a non-key UPDATE carries NO old tuple, so the WHERE must be built from
// the new image's PK (unchanged for a non-key update), not the new full-row
// values. The old code built WHERE name='v_upd' and matched 0 rows.
func TestTransformer_UpdateNonKeyFallback(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventUpdate,
		Schema: "public",
		Table:  "users",
		// No OldColumns: DEFAULT replica identity, non-key column change.
		Columns: []ColumnValue{
			{Name: "id", Value: "1", Type: "oid_23", IsKey: true},
			{Name: "name", Value: "v_upd", Type: "oid_25"},
		},
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(update non-key): %v", err)
	}

	expected := "UPDATE `users` SET `id` = '1', `name` = 'v_upd' WHERE `id` = '1'"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

// TestTransformer_DeleteKeyImage covers #t48 Bug#5 DELETE: under DEFAULT the
// 'K' key image carries only the PK (tagged IsKey), so the WHERE targets the PK.
func TestTransformer_DeleteKeyImage(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventDelete,
		Schema: "public",
		Table:  "users",
		Columns: []ColumnValue{
			{Name: "id", Value: "7", Type: "oid_23", IsKey: true},
		},
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(delete key image): %v", err)
	}

	expected := "DELETE FROM `users` WHERE `id` = '7'"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

// TestTransformer_NoPKErrors covers architect principle #2: a table with no
// usable replica identity (no IsKey column) must error on UPDATE/DELETE rather
// than emit a silent 0-row no-op.
func TestTransformer_NoPKErrors(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	upd := &CDCEvent{
		Kind:   EventUpdate,
		Schema: "public",
		Table:  "heap",
		Columns: []ColumnValue{
			{Name: "name", Value: "x", Type: "oid_25"}, // no IsKey
		},
	}
	if _, err := tr.TransformEvent(upd); err == nil {
		t.Error("expected error for UPDATE on table without PK")
	}

	del := &CDCEvent{
		Kind:   EventDelete,
		Schema: "public",
		Table:  "heap",
		Columns: []ColumnValue{
			{Name: "name", Value: "x", Type: "oid_25"}, // no IsKey
		},
	}
	if _, err := tr.TransformEvent(del); err == nil {
		t.Error("expected error for DELETE on table without PK")
	}
}

// TestDecodeTupleColumns covers #t48 Bug#5 source mapping: a 'K' key image
// carries ONLY the PK columns (must map to the relation's IsKey columns, in
// order — NOT positional), while a full image maps positionally. NULL ('n') ->
// nil value. This guards the "extra pit": positional mapping silently misnames
// key-image columns.
func TestDecodeTupleColumns(t *testing.T) {
	// Relation columns: [id(key), email(key), name] — PK spans cols 0 and 1.
	rel := &Relation{
		OID:    1,
		Schema: "public",
		Name:   "users",
		Columns: []RelationColumn{
			{Name: "id", TypeName: "oid_23", IsKey: true, Ordinal: 0},
			{Name: "email", TypeName: "oid_25", IsKey: true, Ordinal: 1},
			{Name: "name", TypeName: "oid_25", IsKey: false, Ordinal: 2},
		},
	}

	// Full image ('N'/'O'): all 3 columns positional; name is NULL.
	full := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("a@x")},
			{DataType: pglogrepl.TupleDataTypeNull},
		},
	}
	gotFull := decodeTupleColumns(rel, full, false)
	if len(gotFull) != 3 {
		t.Fatalf("full image: got %d cols, want 3", len(gotFull))
	}
	if gotFull[0].Name != "id" || !gotFull[0].IsKey || gotFull[0].Value != "1" {
		t.Errorf("full[0] = %+v, want id/IsKey/'1'", gotFull[0])
	}
	if gotFull[2].Value != nil {
		t.Errorf("full[2] value = %v, want nil for NULL column", gotFull[2].Value)
	}

	// Key image ('K'): only the 2 PK columns, mapped to id+email in order.
	key := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("a@x")},
		},
	}
	gotKey := decodeTupleColumns(rel, key, true)
	if len(gotKey) != 2 {
		t.Fatalf("key image: got %d cols, want 2 (PK only)", len(gotKey))
	}
	wantNames := []string{"id", "email"}
	for i, w := range wantNames {
		if gotKey[i].Name != w || !gotKey[i].IsKey || gotKey[i].Value == nil {
			t.Errorf("key[%d] = %+v, want name=%s IsKey=true non-nil", i, gotKey[i], w)
		}
	}
}

func TestTransformer_Truncate(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventTruncate,
		Schema: "public",
		Table:  "users",
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(truncate): %v", err)
	}

	expected := "TRUNCATE TABLE `users`"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTransformer_NullValue(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventInsert,
		Schema: "public",
		Table:  "t",
		Columns: []ColumnValue{
			{Name: "id", Value: "1", Type: "oid_23"},
			{Name: "description", Value: nil, Type: "oid_25"},
		},
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(null): %v", err)
	}

	expected := "REPLACE INTO `t` (`id`, `description`) VALUES ('1', NULL)"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTransformer_SpecialChars(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventInsert,
		Schema: "public",
		Table:  "users",
		Columns: []ColumnValue{
			{Name: "id", Value: "1", Type: "oid_23"},
			{Name: "bio", Value: "It's a \"test\"", Type: "oid_25"},
		},
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(special): %v", err)
	}

	// Single quotes should be escaped
	expected := "REPLACE INTO `users` (`id`, `bio`) VALUES ('1', 'It''s a \"test\"')"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTransformer_SchemaQuoting(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventInsert,
		Schema: "myschema",
		Table:  "users",
		Columns: []ColumnValue{
			{Name: "id", Value: "1", Type: "oid_23"},
		},
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(schema): %v", err)
	}

	expected := "REPLACE INTO `myschema`.`users` (`id`) VALUES ('1')"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestCheckpointManager_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_checkpoint.json")

	cm := NewCheckpointManager(path)
	cm.SetSlotName("test_slot")
	cm.Update(pglogrepl.LSN(12345))

	if err := cm.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify file was written
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if cp.LSN != pglogrepl.LSN(12345) {
		t.Errorf("LSN = %d, want 12345", cp.LSN)
	}
	if cp.SlotName != "test_slot" {
		t.Errorf("SlotName = %q, want test_slot", cp.SlotName)
	}

	// Load into a new manager
	cm2 := NewCheckpointManager(path)
	loaded, err := cm2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint, got nil")
	}
	if loaded.LSN != pglogrepl.LSN(12345) {
		t.Errorf("loaded LSN = %d, want 12345", loaded.LSN)
	}
}

func TestCheckpointManager_LoadNonExistent(t *testing.T) {
	cm := NewCheckpointManager("/nonexistent/path/checkpoint.json")
	cp, err := cm.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cp != nil {
		t.Errorf("expected nil checkpoint for non-existent file, got %v", cp)
	}
}

func TestCheckpointManager_DirtyFlag(t *testing.T) {
	cm := NewCheckpointManager(filepath.Join(t.TempDir(), "cp.json"))

	if cm.IsDirty() {
		t.Error("expected clean after creation")
	}

	cm.Update(pglogrepl.LSN(100))
	if !cm.IsDirty() {
		t.Error("expected dirty after Update")
	}

	cm.Save()
	if cm.IsDirty() {
		t.Error("expected clean after Save")
	}
}

func TestTransformer_UnknownEventKind(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())
	event := &CDCEvent{Kind: EventKind("unknown")}
	_, err := tr.TransformEvent(event)
	if err == nil {
		t.Error("expected error for unknown event kind")
	}
}

func TestTransformer_UpdateWithoutColumns(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())
	event := &CDCEvent{
		Kind:  EventUpdate,
		Table: "users",
	}
	_, err := tr.TransformEvent(event)
	if err == nil {
		t.Error("expected error for UPDATE without WHERE columns")
	}
}

func TestTransformer_DeleteWithoutColumns(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())
	event := &CDCEvent{
		Kind:  EventDelete,
		Table: "users",
	}
	_, err := tr.TransformEvent(event)
	if err == nil {
		t.Error("expected error for DELETE without WHERE columns")
	}
}

func TestSourceConfigDefaults(t *testing.T) {
	cfg := DefaultSourceConfig()
	if cfg.SlotName != "pg2tidb_cdc" {
		t.Errorf("SlotName = %q, want pg2tidb_cdc", cfg.SlotName)
	}
	if cfg.Publication != "pg2tidb_pub" {
		t.Errorf("Publication = %q, want pg2tidb_pub", cfg.Publication)
	}
	if cfg.OutputPlugin != "pgoutput" {
		t.Errorf("OutputPlugin = %q, want pgoutput", cfg.OutputPlugin)
	}
}
