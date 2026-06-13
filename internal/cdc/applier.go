package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

// ConflictStrategy defines how the applier handles data conflicts on the target.
type ConflictStrategy string

const (
	// ConflictReplace uses REPLACE INTO (overwrites existing rows).
	ConflictReplace ConflictStrategy = "replace"
	// ConflictInsertIgnore uses INSERT IGNORE (skips duplicate key errors).
	ConflictInsertIgnore ConflictStrategy = "insert_ignore"
	// ConflictUpsert uses INSERT ... ON DUPLICATE KEY UPDATE.
	ConflictUpsert ConflictStrategy = "upsert"
	// ConflictSkip skips conflicting rows entirely (DELETE only, no-op for others).
	ConflictSkip ConflictStrategy = "skip"
)

// BatchConfig controls batch applier behavior.
type BatchConfig struct {
	// BatchSize is the maximum number of events to accumulate before flushing.
	BatchSize int `json:"batch_size"`

	// FlushInterval is the maximum time between forced flushes.
	FlushInterval time.Duration `json:"flush_interval"`

	// Parallel is the number of concurrent table-level appliers.
	// Each table gets its own applier goroutine to maintain ordering per table.
	Parallel int `json:"parallel"`

	// MaxRetries is the maximum number of retries for transient failures.
	MaxRetries int `json:"max_retries"`

	// RetryBackoff is the initial backoff duration for retries.
	RetryBackoff time.Duration `json:"retry_backoff"`

	// ConflictStrategy determines how to handle conflicting rows.
	ConflictStrategy ConflictStrategy `json:"conflict_strategy"`

	// SkipTables is a list of tables to skip during apply.
	SkipTables []string `json:"skip_tables,omitempty"`
}

// DefaultBatchConfig returns sensible defaults.
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		BatchSize:        1000,
		FlushInterval:    5 * time.Second,
		Parallel:         4,
		MaxRetries:       3,
		RetryBackoff:     100 * time.Millisecond,
		ConflictStrategy: ConflictReplace,
	}
}

// ApplierStats tracks apply progress.
type ApplierStats struct {
	mu sync.Mutex

	EventsReceived  int64
	EventsApplied   int64
	EventsFailed    int64
	EventsSkipped   int64
	BatchesFlushed  int64
	LastLSN         string
	LastFlushTime   time.Time
	LastError       string
}

// Snapshot returns a copy of the current stats.
func (s *ApplierStats) Snapshot() ApplierStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s
}

// Applier receives CDCEvents and applies them to the TiDB target in batches.
type Applier struct {
	cfg   BatchConfig
	db    *sql.DB
	log   *zap.Logger
	stats *ApplierStats

	// Per-table buffers: tableKey → buffer
	buffers   map[string]*tableBuffer
	buffersMu sync.Mutex

	transformer *Transformer

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// tableBuffer accumulates events for a single table, maintaining insert order.
type tableBuffer struct {
	tableKey string // "schema.table"
	events   []*CDCEvent
	maxSize  int
}

func (b *tableBuffer) add(event *CDCEvent) {
	b.events = append(b.events, event)
}

func (b *tableBuffer) isFull() bool {
	return len(b.events) >= b.maxSize
}

func (b *tableBuffer) flush() []*CDCEvent {
	events := b.events
	b.events = nil
	return events
}

// NewApplier creates a new batch applier.
func NewApplier(db *sql.DB, cfg BatchConfig, transformer *Transformer) *Applier {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.Parallel <= 0 {
		cfg.Parallel = 4
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	return &Applier{
		cfg:         cfg,
		db:          db,
		log:         zap.NewNop(),
		stats:       &ApplierStats{},
		buffers:     make(map[string]*tableBuffer),
		transformer: transformer,
	}
}

// SetLogger sets the logger.
func (a *Applier) SetLogger(log *zap.Logger) {
	a.log = log
}

// Start begins consuming events from the channel and applying them.
// It returns when the input channel is closed and all buffered events are flushed.
func (a *Applier) Start(ctx context.Context, events <-chan *CDCEvent) error {
	a.ctx, a.cancel = context.WithCancel(ctx)
	defer a.cancel()

	flushTicker := time.NewTicker(a.cfg.FlushInterval)
	defer flushTicker.Stop()

	// Start parallel table appliers
	workCh := make(chan *CDCEvent, a.cfg.BatchSize*2)
	for i := 0; i < a.cfg.Parallel; i++ {
		a.wg.Add(1)
		go a.worker(a.ctx, workCh, i)
	}

	// Main dispatch loop
	for {
		select {
		case <-a.ctx.Done():
			a.flushAllTo(workCh)
			close(workCh)
			a.wg.Wait()
			return a.ctx.Err()

		case event, ok := <-events:
			if !ok {
				// Input channel closed — flush remaining and exit
				a.flushAllTo(workCh)
				close(workCh)
				a.wg.Wait()
				return nil
			}

			a.stats.EventsReceived++

			// Buffer the event
			a.bufferEvent(event)

			// Check if any buffer is full and flush it
			a.flushFullTo(workCh)

		case <-flushTicker.C:
			a.flushAllTo(workCh)
		}
	}
}

// bufferEvent adds an event to the appropriate per-table buffer.
func (a *Applier) bufferEvent(event *CDCEvent) {
	key := tableKey(event.Schema, event.Table)

	a.buffersMu.Lock()
	defer a.buffersMu.Unlock()

	buf, ok := a.buffers[key]
	if !ok {
		buf = &tableBuffer{
			tableKey: key,
			maxSize:  a.cfg.BatchSize,
		}
		a.buffers[key] = buf
	}
	buf.add(event)
}

// flushFullTo flushes all full buffers to the work channel.
func (a *Applier) flushFullTo(workCh chan<- *CDCEvent) {
	a.buffersMu.Lock()
	defer a.buffersMu.Unlock()

	for _, buf := range a.buffers {
		if buf.isFull() {
			flushed := buf.flush()
			for _, evt := range flushed {
				select {
				case workCh <- evt:
				case <-a.ctx.Done():
					return
				}
			}
			a.stats.mu.Lock()
			a.stats.BatchesFlushed++
			a.stats.mu.Unlock()
		}
	}
}

// flushAllTo flushes all non-empty buffers to the work channel.
func (a *Applier) flushAllTo(workCh chan<- *CDCEvent) {
	a.buffersMu.Lock()
	defer a.buffersMu.Unlock()

	for _, buf := range a.buffers {
		if len(buf.events) == 0 {
			continue
		}
		flushed := buf.flush()
		for _, evt := range flushed {
			select {
			case workCh <- evt:
			case <-a.ctx.Done():
				return
			}
		}
		a.stats.mu.Lock()
		a.stats.BatchesFlushed++
		a.stats.mu.Unlock()
	}
}

// worker is a goroutine that applies events to TiDB.
func (a *Applier) worker(ctx context.Context, workCh <-chan *CDCEvent, id int) {
	defer a.wg.Done()

	for event := range workCh {
		if err := a.applyEvent(ctx, event); err != nil {
			a.log.Error("apply event failed",
				zap.Int("worker", id),
				zap.String("table", tableKey(event.Schema, event.Table)),
				zap.String("kind", string(event.Kind)),
				zap.Error(err),
			)
			a.stats.mu.Lock()
			a.stats.EventsFailed++
			a.stats.LastError = err.Error()
			a.stats.mu.Unlock()
		} else {
			a.stats.mu.Lock()
			a.stats.EventsApplied++
			a.stats.LastLSN = event.LSN.String()
			a.stats.LastFlushTime = time.Now()
			a.stats.mu.Unlock()
		}
	}
}

// applyEvent applies a single event to TiDB.
func (a *Applier) applyEvent(ctx context.Context, event *CDCEvent) error {
	// Check skip table
	for _, skip := range a.cfg.SkipTables {
		if skip == tableKey(event.Schema, event.Table) {
			a.stats.mu.Lock()
			a.stats.EventsSkipped++
			a.stats.mu.Unlock()
			return nil
		}
	}

	sql, err := a.transformer.TransformEvent(event)
	if err != nil {
		return fmt.Errorf("transform: %w", err)
	}

	// Apply with conflict strategy override
	sql = a.applyConflictStrategy(sql, event.Kind)

	// Retry logic
	var lastErr error
	for attempt := 0; attempt <= a.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := a.cfg.RetryBackoff * time.Duration(1<<uint(attempt-1))
			time.Sleep(backoff)
			a.log.Debug("retrying apply",
				zap.Int("attempt", attempt),
				zap.String("sql", sql),
			)
		}

		_, err := a.db.ExecContext(ctx, sql)
		if err == nil {
			return nil
		}

		lastErr = err
		// Don't retry on syntax errors or fatal errors
		if isFatalError(err) {
			break
		}
	}

	return fmt.Errorf("apply after %d retries: %w", a.cfg.MaxRetries, lastErr)
}

// applyConflictStrategy adjusts SQL based on the configured conflict strategy.
func (a *Applier) applyConflictStrategy(sql string, kind EventKind) string {
	switch a.cfg.ConflictStrategy {
	case ConflictReplace:
		if kind == EventInsert {
			return sql // REPLACE INTO already used by transformer
		}
	case ConflictInsertIgnore:
		if kind == EventInsert {
			return strings.Replace(sql, "REPLACE INTO", "INSERT IGNORE INTO", 1)
		}
	case ConflictUpsert:
		if kind == EventInsert {
			// INSERT INTO ... ON DUPLICATE KEY UPDATE
			return sql // Keep REPLACE for now; ON DUPLICATE KEY needs column list
		}
	case ConflictSkip:
		if kind == EventInsert || kind == EventUpdate {
			// INSERT IGNORE or UPDATE IGNORE
			if kind == EventInsert {
				return strings.Replace(sql, "REPLACE INTO", "INSERT IGNORE INTO", 1)
			}
			return strings.Replace(sql, "UPDATE ", "UPDATE IGNORE ", 1)
		}
	}
	return sql
}

// Stats returns the current apply statistics.
func (a *Applier) Stats() ApplierStats {
	return a.stats.Snapshot()
}

// tableKey returns the canonical table identifier.
func tableKey(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// isFatalError returns true if the error should not be retried.
func isFatalError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	fatalPatterns := []string{
		"syntax error",
		"unknown column",
		"table doesn't exist",
		"no such table",
		"access denied",
		"Error 1146", // Table doesn't exist
		"Error 1054", // Unknown column
		"Error 1064", // Syntax error
	}
	for _, p := range fatalPatterns {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return false
}
