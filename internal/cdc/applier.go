package cdc

import (
	"context"
	"database/sql"
	"errors"
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

	// Parallel is the number of concurrent applier workers. Events are routed to
	// a fixed worker by table hash, so each table is applied serially by one
	// worker (WAL order preserved within a table) while different tables run in
	// parallel.
	//
	// Default is 1 (fully serial = correctness-first): a shared-channel design
	// reorders same-row events and silently loses updates (#t48 Bug#8). Opt into
	// >1 only when you accept the parallel-mode boundaries: cross-table FK apply
	// order and multi-table source-transaction atomicity are NOT guaranteed
	// (events are applied individually, not grouped by source transaction).
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
		Parallel:         1, // serial by default — correctness-first (see Parallel doc); opt into >1 explicitly.
		MaxRetries:       3,
		RetryBackoff:     100 * time.Millisecond,
		ConflictStrategy: ConflictReplace,
	}
}

// ApplierStats tracks apply progress.
type ApplierStats struct {
	mu sync.Mutex

	EventsReceived int64
	EventsApplied  int64
	EventsFailed   int64
	EventsSkipped  int64
	BatchesFlushed int64
	LastLSN        string
	LastFlushTime  time.Time
	LastError      string
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

	fatalMu  sync.Mutex
	fatalErr error // set when the applier halts on a structural failure; see Fatal()
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

	// Per-worker channels: each table is routed by hash to a FIXED worker, so all
	// events for a table are applied serially by one worker (preserving WAL order
	// within the table) while different tables apply in parallel. A shared channel
	// + N workers would reorder same-row events and silently lose updates
	// (#t48 Bug#8). Default Parallel=1 is fully serial.
	n := a.cfg.Parallel
	if n < 1 {
		n = 1
	}
	workerChs := make([]chan *CDCEvent, n)
	for i := range workerChs {
		workerChs[i] = make(chan *CDCEvent, a.cfg.BatchSize)
	}
	for i := 0; i < n; i++ {
		a.wg.Add(1)
		go a.worker(a.ctx, workerChs[i], i)
	}
	closeWorkers := func() {
		for _, ch := range workerChs {
			close(ch)
		}
	}

	// Main dispatch loop
	for {
		select {
		case <-a.ctx.Done():
			a.flushAllTo(workerChs)
			closeWorkers()
			a.wg.Wait()
			if fe := a.Fatal(); fe != nil {
				return fe // a structural halt surfaces as a real error, not ctx.Canceled
			}
			return a.ctx.Err()

		case event, ok := <-events:
			if !ok {
				// Input channel closed — flush remaining and exit
				a.flushAllTo(workerChs)
				closeWorkers()
				a.wg.Wait()
				return nil
			}

			a.stats.EventsReceived++

			// Buffer the event (per-table, preserves arrival order within a table)
			a.bufferEvent(event)

			// Flush any full buffer to its table's fixed worker
			a.flushFullTo(workerChs)

		case <-flushTicker.C:
			a.flushAllTo(workerChs)
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

// flushFullTo flushes all full buffers, routing each table's events to its
// fixed worker channel so per-table order is preserved.
func (a *Applier) flushFullTo(workerChs []chan *CDCEvent) {
	a.buffersMu.Lock()
	defer a.buffersMu.Unlock()

	for _, buf := range a.buffers {
		if buf.isFull() {
			ch := workerFor(buf.tableKey, workerChs)
			flushed := buf.flush()
			for _, evt := range flushed {
				select {
				case ch <- evt:
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

// flushAllTo flushes all non-empty buffers, routing each table's events to its
// fixed worker channel so per-table order is preserved.
func (a *Applier) flushAllTo(workerChs []chan *CDCEvent) {
	a.buffersMu.Lock()
	defer a.buffersMu.Unlock()

	for _, buf := range a.buffers {
		if len(buf.events) == 0 {
			continue
		}
		ch := workerFor(buf.tableKey, workerChs)
		flushed := buf.flush()
		for _, evt := range flushed {
			select {
			case ch <- evt:
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
			var se *StructuralError
			if errors.As(err, &se) {
				// Permanent failure (e.g. a table with no usable replica identity):
				// halt the pipeline loudly instead of silently accumulating
				// EventsFailed and diverging. See #t48 step 2 Part B.
				a.setFatal(err)
				if a.cancel != nil {
					a.cancel() // triggers the dispatch loop's ctx.Done shutdown
				}
				return
			}
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
	// Never replicate CDC's own internal tables (e.g. the DDLTracker's
	// pg2tidb_ddl_log). A `FOR ALL TABLES` publication streams their writes as
	// DML, but they don't exist on the target — applying them would 1146-halt
	// the pipeline on every DDL. #t61 production fix.
	if isCDCInternalTable(event.Table) {
		a.stats.mu.Lock()
		a.stats.EventsSkipped++
		a.stats.mu.Unlock()
		return nil
	}
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

	// Retry logic. Schema errors (e.g. a new table's DML arrives before its
	// CREATE DDL is replicated) get a longer retry window so DDL replication can
	// catch up before halting; other transient errors use the standard retry;
	// fatal errors abort immediately. #t59 §4.3.
	const (
		schemaRetries = 10
		schemaBackoff = 500 * time.Millisecond
	)
	var lastErr error
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			a.log.Debug("retrying apply", zap.Int("attempt", attempt), zap.String("sql", sql))
		}
		_, err := a.db.ExecContext(ctx, sql)
		if err == nil {
			return nil
		}
		lastErr = err
		switch {
		case isFatalError(err):
			// Non-schema fatal (syntax / unknown column / access): don't retry.
			return fmt.Errorf("apply fatal: %w", err)
		case isSchemaError(err):
			// Target table not created yet — wait for DDL replication, then halt
			// loudly if it never catches up (don't silently drop the DML).
			if attempt >= schemaRetries {
				return &StructuralError{Msg: fmt.Sprintf("schema mismatch after %d retries (target table likely not created by DDL yet): %v", schemaRetries, err)}
			}
			if err := sleepCtx(ctx, schemaBackoff); err != nil {
				return err
			}
		default:
			// Transient error: standard bounded retry.
			if attempt >= a.cfg.MaxRetries {
				return fmt.Errorf("apply after %d retries: %w", a.cfg.MaxRetries, lastErr)
			}
			if err := sleepCtx(ctx, a.cfg.RetryBackoff*time.Duration(1<<uint(attempt))); err != nil {
				return err
			}
		}
	}
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

// setFatal records a structural failure that halted the applier (e.g. a table
// with no usable replica identity) and is sticky. Used so a permanent failure
// halts loudly instead of accumulating EventsFailed and silently diverging.
// See #t48 step 2 Part B.
func (a *Applier) setFatal(err error) {
	a.fatalMu.Lock()
	if a.fatalErr == nil {
		a.fatalErr = err
	}
	a.fatalMu.Unlock()
	a.log.Error("cdc applier halted on structural failure (data-integrity halt)", zap.Error(err))
}

// Fatal returns the structural failure that halted the applier, or nil.
func (a *Applier) Fatal() error {
	a.fatalMu.Lock()
	defer a.fatalMu.Unlock()
	return a.fatalErr
}

// Stats returns the current apply statistics.
func (a *Applier) Stats() ApplierStats {
	return a.stats.Snapshot()
}

// workerFor returns the fixed worker channel for a table: hashing the table key
// means every event for a given table lands on the same worker, so that worker
// applies the table's events serially in arrival order (#t48 Bug#8). Different
// tables hash independently and may share a worker, but never reorder within a
// table. Modulo-before-cast keeps the index non-negative on all platforms.
func workerFor(tableKey string, workerChs []chan *CDCEvent) chan<- *CDCEvent {
	return workerChs[int(fnv1a32(tableKey)%uint32(len(workerChs)))]
}

// fnv1a32 is a stable, allocation-free string hash (FNV-1a 32) for routing.
func fnv1a32(s string) uint32 {
	const offset, prime uint32 = 2166136261, 16777619
	h := offset
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime
	}
	return h
}

// tableKey returns the canonical table identifier.
func tableKey(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// cdcInternalTables are tables CDC itself creates in the source for its own
// infrastructure. They must NEVER be replicated as DML: a `FOR ALL TABLES`
// publication streams their writes, but they don't exist on the target, so
// applying them would 1146-halt the pipeline (a self-capture loop). This is a
// hard-coded default blacklist independent of user config (#t61 production fix).
// pg2tidb_ddl_log is the DDLTracker's capture log (#t59).
var cdcInternalTables = map[string]bool{
	"pg2tidb_ddl_log": true,
}

// isCDCInternalTable reports whether a table is CDC's own infrastructure (matched
// by table name, case-insensitive, schema-agnostic — these names are reserved).
func isCDCInternalTable(table string) bool {
	return cdcInternalTables[strings.ToLower(table)]
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
		"access denied",
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

// isSchemaError reports a target-schema mismatch (e.g. a new table's DML
// arrived before its CREATE DDL was replicated). These are retried longer to
// let DDL replication catch up, then halt. #t59 §4.3. (Previously these were in
// fatalPatterns — i.e. not retried — which silently diverged on missing tables.)
func isSchemaError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, p := range []string{"table doesn't exist", "no such table", "Error 1146"} {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return false
}

// sleepCtx sleeps for d but returns early (with ctx.Err()) on context cancel.
func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
