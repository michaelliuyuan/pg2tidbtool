package errors

import (
	"errors"
	"testing"
)

func TestNew(t *testing.T) {
	err := New(ErrConfigLoad, "failed to load config")
	if err.Code != ErrConfigLoad {
		t.Errorf("expected ErrConfigLoad, got %s", err.Code)
	}
	if err.Message != "failed to load config" {
		t.Errorf("unexpected message: %s", err.Message)
	}
}

func TestWrap(t *testing.T) {
	cause := errors.New("file not found")
	err := Wrap(ErrConfigLoad, "config load failed", cause)
	if err.Code != ErrConfigLoad {
		t.Errorf("expected ErrConfigLoad, got %s", err.Code)
	}
	if !errors.Is(err, cause) {
		t.Error("should wrap cause")
	}
}

func TestWithTable(t *testing.T) {
	err := WithTable(New(ErrDataExport, "export failed"), "users")
	if err.Table != "users" {
		t.Errorf("expected table users, got %s", err.Table)
	}
}

func TestWithStrategy(t *testing.T) {
	err := WithStrategy(New(ErrDataExport, "export failed"), StrategySkip)
	if err.Strategy != StrategySkip {
		t.Errorf("expected skip, got %s", err.Strategy)
	}
}

func TestShouldAbort(t *testing.T) {
	errAbort := WithStrategy(New(ErrDataExport, "fail"), StrategyAbort)
	if !ShouldAbort(errAbort, StrategySkip) {
		t.Error("abort strategy should abort")
	}

	errSkip := WithStrategy(New(ErrDataExport, "fail"), StrategySkip)
	if ShouldAbort(errSkip, StrategySkip) {
		t.Error("skip strategy should not abort")
	}

	plainErr := errors.New("plain error")
	if !ShouldAbort(plainErr, StrategyAbort) {
		t.Error("plain error with abort default should abort")
	}
}

func TestErrorFormatting(t *testing.T) {
	err := WithTable(Wrap(ErrDataExport, "export", errors.New("disk full")), "orders")
	msg := err.Error()
	if msg == "" {
		t.Error("error message should not be empty")
	}
}

func TestGetCode(t *testing.T) {
	err := New(ErrSchemaConvert, "convert failed")
	if code := GetCode(err); code != ErrSchemaConvert {
		t.Errorf("expected ErrSchemaConvert, got %s", code)
	}

	plainErr := errors.New("plain")
	if code := GetCode(plainErr); code != ErrInternal {
		t.Errorf("expected ErrInternal for plain error, got %s", code)
	}
}
