package progress

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Bar struct {
	name      string
	total     int64
	current   atomic.Int64
	startTime time.Time
	mu        sync.Mutex
	width     int
}

func NewBar(name string, total int64) *Bar {
	return &Bar{
		name:      name,
		total:     total,
		startTime: time.Now(),
		width:     40,
	}
}

func (b *Bar) Add(n int64) {
	b.current.Add(n)
}

func (b *Bar) Set(n int64) {
	b.current.Store(n)
}

func (b *Bar) Current() int64 {
	return b.current.Load()
}

func (b *Bar) Total() int64 {
	return b.total
}

func (b *Bar) SetTotal(n int64) {
	b.total = n
}

func (b *Bar) Elapsed() time.Duration {
	return time.Since(b.startTime)
}

func (b *Bar) Rate() float64 {
	elapsed := b.Elapsed().Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(b.current.Load()) / elapsed
}

func (b *Bar) ETA() time.Duration {
	rate := b.Rate()
	if rate <= 0 {
		return 0
	}
	remaining := b.total - b.current.Load()
	if remaining <= 0 {
		return 0
	}
	return time.Duration(float64(remaining)/rate) * time.Second
}

func (b *Bar) Percent() float64 {
	if b.total <= 0 {
		return 0
	}
	pct := float64(b.current.Load()) / float64(b.total)
	if pct > 1.0 {
		pct = 1.0
	}
	return pct
}

func (b *Bar) Render(w io.Writer) {
	pct := b.Percent()
	filled := int(pct * float64(b.width))
	bar := strings.Repeat("█", filled) + strings.Repeat("░", b.width-filled)

	eta := b.ETA()
	var etaStr string
	if b.current.Load() >= b.total {
		etaStr = "done"
	} else if eta > 0 {
		etaStr = fmt.Sprintf("ETA: %s", eta.Truncate(time.Second))
	}

	fmt.Fprintf(w, "\r%-30s [%s] %6.1f%% %d/%d rows %.0f rows/s %s    ",
		b.name, bar, pct*100, b.current.Load(), b.total, b.Rate(), etaStr)
}

type Display struct {
	bars   map[string]*Bar
	mu     sync.Mutex
	stopCh chan struct{}
	out    io.Writer
}

func NewDisplay() *Display {
	return &Display{
		bars:   make(map[string]*Bar),
		stopCh: make(chan struct{}),
		out:    os.Stderr,
	}
}

func (d *Display) AddBar(name string, total int64) *Bar {
	d.mu.Lock()
	defer d.mu.Unlock()
	b := NewBar(name, total)
	d.bars[name] = b
	return b
}

func (d *Display) RemoveBar(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.bars, name)
}

func (d *Display) GetBar(name string) *Bar {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.bars[name]
}

func (d *Display) Start() {
	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				d.render()
			case <-d.stopCh:
				return
			}
		}
	}()
}

func (d *Display) Stop() {
	close(d.stopCh)
	d.render()
	fmt.Fprintln(d.out)
}

func (d *Display) render() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, b := range d.bars {
		b.Render(d.out)
	}
}
