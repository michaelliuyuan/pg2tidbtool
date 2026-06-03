package validator

import (
	"testing"
)

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

func TestNormalizeDecimalString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"10.50", "10.5"},
		{"10.00", "10"},
		{"10.5", "10.5"},
		{"10", "10"},          // no decimal point, unchanged
		{"-3.1400", "-3.14"},
		{"0.00", "0"},
		{"hello", "hello"},   // not a decimal, unchanged
		{"1.0e5", "1.0e5"},   // scientific notation, not matched by decimalRe
		{"100", "100"},        // integer, unchanged
	}
	for _, tt := range tests {
		result := normalizeDecimalString(tt.input)
		if result != tt.expected {
			t.Errorf("normalizeDecimalString(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}
