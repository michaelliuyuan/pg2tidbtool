package progress

import (
	"bytes"
	"testing"
	"time"
)

func TestNewBar(t *testing.T) {
	b := NewBar("users", 1000)
	if b.Total() != 1000 {
		t.Errorf("expected 1000, got %d", b.Total())
	}
}

func TestBarAdd(t *testing.T) {
	b := NewBar("users", 1000)
	b.Add(100)
	if b.Current() != 100 {
		t.Errorf("expected 100, got %d", b.Current())
	}
}

func TestBarSet(t *testing.T) {
	b := NewBar("users", 1000)
	b.Set(500)
	if b.Current() != 500 {
		t.Errorf("expected 500, got %d", b.Current())
	}
}

func TestBarPercent(t *testing.T) {
	b := NewBar("users", 1000)
	b.Set(500)
	if p := b.Percent(); p != 0.5 {
		t.Errorf("expected 0.5, got %f", p)
	}
}

func TestBarRender(t *testing.T) {
	b := NewBar("users", 1000)
	b.Set(500)
	var buf bytes.Buffer
	b.Render(&buf)
	if buf.Len() == 0 {
		t.Error("render should produce output")
	}
}

func TestBarRate(t *testing.T) {
	b := NewBar("users", 10000)
	b.startTime = time.Now().Add(-10 * time.Second)
	b.Set(1000)
	rate := b.Rate()
	if rate <= 0 {
		t.Error("rate should be positive")
	}
}

func TestDisplay(t *testing.T) {
	d := NewDisplay()
	b := d.AddBar("users", 1000)
	if b == nil {
		t.Error("should return bar")
	}
	b2 := d.GetBar("users")
	if b2 != b {
		t.Error("should return same bar")
	}
	d.RemoveBar("users")
	b3 := d.GetBar("users")
	if b3 != nil {
		t.Error("should be removed")
	}
}

func TestBarSetTotal(t *testing.T) {
	b := NewBar("users", 1000)
	b.SetTotal(2000)
	if b.Total() != 2000 {
		t.Errorf("expected 2000, got %d", b.Total())
	}
}
