package cdc

import (
	"testing"
	"time"
)

func TestApplier_ConflictStrategyReplace(t *testing.T) {
	a := &Applier{cfg: BatchConfig{ConflictStrategy: ConflictReplace}}

	// REPLACE INTO should be left as-is
	sql := a.applyConflictStrategy("REPLACE INTO `users` (`id`) VALUES ('1')", EventInsert)
	expected := "REPLACE INTO `users` (`id`) VALUES ('1')"
	if sql != expected {
		t.Errorf("ConflictReplace: got %q, want %q", sql, expected)
	}
}

func TestApplier_ConflictStrategyInsertIgnore(t *testing.T) {
	a := &Applier{cfg: BatchConfig{ConflictStrategy: ConflictInsertIgnore}}

	sql := a.applyConflictStrategy("REPLACE INTO `users` (`id`) VALUES ('1')", EventInsert)
	expected := "INSERT IGNORE INTO `users` (`id`) VALUES ('1')"
	if sql != expected {
		t.Errorf("ConflictInsertIgnore: got %q, want %q", sql, expected)
	}
}

func TestApplier_ConflictStrategySkip(t *testing.T) {
	a := &Applier{cfg: BatchConfig{ConflictStrategy: ConflictSkip}}

	// INSERT should become INSERT IGNORE
	sql := a.applyConflictStrategy("REPLACE INTO `users` (`id`) VALUES ('1')", EventInsert)
	expected := "INSERT IGNORE INTO `users` (`id`) VALUES ('1')"
	if sql != expected {
		t.Errorf("ConflictSkip insert: got %q, want %q", sql, expected)
	}

	// UPDATE should become UPDATE IGNORE
	sql = a.applyConflictStrategy("UPDATE `users` SET `name` = 'Bob' WHERE `id` = '1'", EventUpdate)
	expected = "UPDATE IGNORE `users` SET `name` = 'Bob' WHERE `id` = '1'"
	if sql != expected {
		t.Errorf("ConflictSkip update: got %q, want %q", sql, expected)
	}

	// DELETE should be unchanged
	sql = a.applyConflictStrategy("DELETE FROM `users` WHERE `id` = '1'", EventDelete)
	expected = "DELETE FROM `users` WHERE `id` = '1'"
	if sql != expected {
		t.Errorf("ConflictSkip delete: got %q, want %q", sql, expected)
	}
}

func TestApplier_ConflictStrategyUpsert(t *testing.T) {
	a := &Applier{cfg: BatchConfig{ConflictStrategy: ConflictUpsert}}

	// Upsert keeps REPLACE INTO (simplest approach)
	sql := a.applyConflictStrategy("REPLACE INTO `users` (`id`) VALUES ('1')", EventInsert)
	expected := "REPLACE INTO `users` (`id`) VALUES ('1')"
	if sql != expected {
		t.Errorf("ConflictUpsert: got %q, want %q", sql, expected)
	}
}

func TestIsFatalError(t *testing.T) {
	tests := []struct {
		errMsg string
		fatal  bool
	}{
		{"Error 1064: You have an error in your SQL syntax", true},
		{"Error 1146: Table 'test.users' doesn't exist", true},
		{"Error 1054: Unknown column 'foo' in 'field list'", true},
		{"syntax error near 'SELECT'", true},
		{"access denied for user 'root'", true},
		{"connection refused", false},
		{"i/o timeout", false},
		{"too many connections", false},
		{"deadlock found", false},
	}

	for _, tt := range tests {
		err := &testError{msg: tt.errMsg}
		got := isFatalError(err)
		if got != tt.fatal {
			t.Errorf("isFatalError(%q) = %v, want %v", tt.errMsg, got, tt.fatal)
		}
	}
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

func TestBatchConfigDefaults(t *testing.T) {
	cfg := DefaultBatchConfig()
	if cfg.BatchSize != 1000 {
		t.Errorf("BatchSize = %d, want 1000", cfg.BatchSize)
	}
	if cfg.Parallel != 1 {
		t.Errorf("Parallel = %d, want 1 (serial default, correctness-first — see #t48 Bug#8)", cfg.Parallel)
	}
	if cfg.FlushInterval != 5*time.Second {
		t.Errorf("FlushInterval = %v, want 5s", cfg.FlushInterval)
	}
	if cfg.ConflictStrategy != ConflictReplace {
		t.Errorf("ConflictStrategy = %q, want replace", cfg.ConflictStrategy)
	}
}

func TestWorkerFor_DeterministicRouting(t *testing.T) {
	workerChs := make([]chan *CDCEvent, 4)
	for i := range workerChs {
		workerChs[i] = make(chan *CDCEvent)
	}
	// Each table key must deterministically resolve to one configured channel —
	// the property that lets a single worker own a table's event order (#t48 Bug#8).
	for _, tk := range []string{"public.users", "public.orders", "public.single_pk", "", "x.y"} {
		ch := workerFor(tk, workerChs)
		if workerFor(tk, workerChs) != ch {
			t.Errorf("workerFor(%q) not deterministic across calls", tk)
		}
		found := false
		for _, c := range workerChs {
			if c == ch {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("workerFor(%q) returned an unknown channel", tk)
		}
	}
	// parallel=1 (the default): every table routes to the single worker.
	single := []chan *CDCEvent{make(chan *CDCEvent)}
	for _, tk := range []string{"public.users", "public.orders"} {
		if workerFor(tk, single) != single[0] {
			t.Errorf("parallel=1: table %q did not route to the single worker", tk)
		}
	}
}

func TestTableKey(t *testing.T) {
	tests := []struct {
		schema, table, expected string
	}{
		{"public", "users", "public.users"},
		{"", "users", "users"},
		{"myschema", "users", "myschema.users"},
	}

	for _, tt := range tests {
		got := tableKey(tt.schema, tt.table)
		if got != tt.expected {
			t.Errorf("tableKey(%q, %q) = %q, want %q", tt.schema, tt.table, got, tt.expected)
		}
	}
}

func TestApplierStats_Snapshot(t *testing.T) {
	s := &ApplierStats{}
	s.EventsReceived = 100
	s.EventsApplied = 95
	s.EventsFailed = 3
	s.EventsSkipped = 2

	snap := s.Snapshot()
	if snap.EventsReceived != 100 {
		t.Errorf("EventsReceived = %d", snap.EventsReceived)
	}
	if snap.EventsApplied != 95 {
		t.Errorf("EventsApplied = %d", snap.EventsApplied)
	}
}
