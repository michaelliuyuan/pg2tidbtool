package data

import (
	"testing"
	"time"
)

func TestConvertValue(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{nil, "\\N"},
		{true, "1"},
		{false, "0"},
		{int64(42), "42"},
		{float64(3.14), "3.14"},
		{"hello", "hello"},
		{[]byte("bytes"), "bytes"},
	}

	for _, tt := range tests {
		result := convertValue(tt.input)
		if result != tt.expected {
			t.Errorf("convertValue(%v) = %s, want %s", tt.input, result, tt.expected)
		}
	}
}

func TestConvertValueTime(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	result := convertValue(ts)
	if result != "2024-01-15 10:30:00" {
		t.Errorf("convertValue(time) = %s, want 2024-01-15 10:30:00", result)
	}
}

func TestQuotePG(t *testing.T) {
	if quotePG("table") != `"table"` {
		t.Error("should double-quote PG identifier")
	}
	if quotePG(`ta"ble`) != `"ta""ble"` {
		t.Error("should escape double quotes")
	}
}

func TestQuoteMySQL(t *testing.T) {
	if quoteMySQL("table") != "`table`" {
		t.Error("should backtick-quote MySQL identifier")
	}
	if quoteMySQL("ta`ble") != "`ta``ble`" {
		t.Error("should escape backticks")
	}
}

func TestContains(t *testing.T) {
	slice := []string{"a", "b", "c"}
	if !contains(slice, "a") {
		t.Error("should find a")
	}
	if contains(slice, "d") {
		t.Error("should not find d")
	}
	if contains(nil, "a") {
		t.Error("should not find in nil slice")
	}
}
