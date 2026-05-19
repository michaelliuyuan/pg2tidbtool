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
