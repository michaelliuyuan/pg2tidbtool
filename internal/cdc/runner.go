package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"go.uber.org/zap"
)

// Runner orchestrates the complete CDC pipeline: source → transform → apply.
type Runner struct {
	srcCfg    SourceConfig
	batchCfg  BatchConfig
	targetDSN string

	source      *Source
	applier     *Applier
	transformer *Transformer
	checkpoint  *CheckpointManager
	filter      *TableFilter
	ddlTracker  *DDLTracker
	enableDDL   bool    // cfg.EnableDDLTracking cached (DDL replication, #t59)
	ddlSrcDB    *sql.DB // source PG conn for ddlTracker (opened in Run, #t59)

	statusFile string    // CDC→Web status JSON path (#t48 B); empty = disabled
	startTime  time.Time // for uptime in the status report

	log *zap.Logger
}

// RunnerConfig combines all CDC sub-configs.
type RunnerConfig struct {
	Source      SourceConfig
	Batch       BatchConfig
	Transformer TransformerConfig
	Filter      *TableFilter

	// TiDB target DSN
	TargetDSN string

	// Checkpoint file path
	CheckpointFile string

	// StatusFile is the path the CDC process writes its status JSON to for the
	// web UI to read (CDC→Web cross-process channel, #t48 B). Empty = disabled.
	// Same-machine deployment must align this with the web server's read path.
	StatusFile string

	// Enable DDL tracking
	EnableDDLTracking bool
}

// NewRunner creates a new CDC runner.
func NewRunner(cfg RunnerConfig) (*Runner, error) {
	log := zap.NewNop()

	if cfg.CheckpointFile == "" {
		cfg.CheckpointFile = ".cdc_checkpoint.json"
	}
	if cfg.Filter == nil {
		cfg.Filter = NewTableFilter()
	}

	r := &Runner{
		srcCfg:     cfg.Source,
		batchCfg:   cfg.Batch,
		targetDSN:  cfg.TargetDSN,
		statusFile: cfg.StatusFile,
		log:        log,
	}

	// Create components
	r.transformer = NewTransformer(cfg.Transformer)
	r.transformer.SetLogger(log)

	r.checkpoint = NewCheckpointManager(cfg.CheckpointFile)
	r.checkpoint.SetLogger(log)
	r.checkpoint.SetSlotName(cfg.Source.SlotName)

	r.filter = cfg.Filter
	r.enableDDL = cfg.EnableDDLTracking

	// Source will be created in Run
	r.source = NewSource(cfg.Source)
	r.source.SetLogger(log)

	return r, nil
}

// SetLogger sets the logger for all components.
func (r *Runner) SetLogger(log *zap.Logger) {
	r.log = log
	r.source.SetLogger(log)
	r.transformer.SetLogger(log)
	r.checkpoint.SetLogger(log)
}

// Run executes the full CDC pipeline.
func (r *Runner) Run(ctx context.Context) error {
	r.log.Info("cdc runner: starting")
	r.startTime = time.Now()

	// Load checkpoint for resume
	cp, err := r.checkpoint.Load()
	if err != nil {
		return fmt.Errorf("cdc runner: load checkpoint: %w", err)
	}

	var startLSN = r.source.CurrentLSN()
	if cp != nil && cp.LSN > 0 {
		startLSN = cp.LSN
		r.log.Info("cdc runner: resuming from checkpoint",
			zap.String("lsn", startLSN.String()),
		)
	}

	// Setup replication source
	if err := r.source.Setup(ctx); err != nil {
		return fmt.Errorf("cdc runner: setup source: %w", err)
	}

	// Connect to TiDB target
	targetDB, err := sql.Open("mysql", r.targetDSN)
	if err != nil {
		return fmt.Errorf("cdc runner: connect to target: %w", err)
	}
	defer targetDB.Close()

	// Verify target connection
	if err := targetDB.PingContext(ctx); err != nil {
		return fmt.Errorf("cdc runner: ping target: %w", err)
	}
	r.log.Info("cdc runner: connected to TiDB target")

	// Create applier
	r.applier = NewApplier(targetDB, r.batchCfg, r.transformer)
	r.applier.SetLogger(r.log)

	// Start replication stream
	events, err := r.source.Start(ctx, startLSN)
	if err != nil {
		return fmt.Errorf("cdc runner: start source: %w", err)
	}

	// DDL replication (#t59): if enabled, install the source event trigger and
	// poll+apply DDL alongside the DML stream. A setup failure degrades to
	// DML-only; a DDL apply error halts loudly (reported via ddlErrCh).
	var ddlErrCh chan error
	if r.enableDDL {
		if ch, derr := r.setupDDLReplication(ctx, targetDB); derr != nil {
			r.log.Warn("cdc runner: ddl replication disabled (setup failed), continuing DML-only", zap.Error(derr))
		} else {
			ddlErrCh = ch
			defer r.teardownDDL() // remove event trigger + close source conn on exit
		}
	}

	// Start checkpoint ticker
	cpTicker := time.NewTicker(10 * time.Second)
	defer cpTicker.Stop()

	// Signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Run applier in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- r.applier.Start(ctx, events)
	}()

	// Main loop: wait for completion or signal
	for {
		select {
		case err := <-errCh:
			// Applier finished (events channel closed or error)
			if err != nil {
				r.log.Error("cdc runner: applier error", zap.Error(err))
			}
			// If the source halted on a fatal (e.g. parse failure), surface it so
			// the stop is reported as a failure, not a clean shutdown. The
			// checkpoint is saved at the last-good LSN (≤ the failure), so a
			// restart re-reads the failed record (at-least-once). #t48 step 2.
			srcErr := r.source.Err()
			// Final checkpoint save (last-good LSN, never past the failure)
			r.checkpoint.Update(r.source.CurrentLSN())
			if saveErr := r.checkpoint.Save(); saveErr != nil {
				r.log.Error("cdc runner: final checkpoint save failed", zap.Error(saveErr))
			}
			r.writeStatus() // final status: state=halted if source fatal (Part A)
			r.source.Stop()
			if srcErr != nil {
				return fmt.Errorf("cdc runner: source halted on fatal: %w", srcErr)
			}
			return err

		case sig := <-sigCh:
			r.log.Info("cdc runner: received signal, shutting down", zap.String("signal", sig.String()))
			r.source.Stop()
			// Wait for applier to finish
			select {
			case appErr := <-errCh:
				if appErr != nil {
					r.log.Error("cdc runner: applier shutdown error", zap.Error(appErr))
				}
			case <-time.After(30 * time.Second):
				r.log.Warn("cdc runner: applier shutdown timeout")
			}
			// Save checkpoint
			r.checkpoint.Update(r.source.CurrentLSN())
			if saveErr := r.checkpoint.Save(); saveErr != nil {
				r.log.Error("cdc runner: final checkpoint save failed", zap.Error(saveErr))
			}
			r.writeStatus() // final status on graceful shutdown
			return nil

		case ddlErr := <-ddlErrCh:
			// DDL replication hit a fatal apply error (e.g. a non-idempotent DDL
			// replay). Halt loudly; checkpoint saved at last-good LSN.
			r.log.Error("cdc runner: ddl replication halted", zap.Error(ddlErr))
			r.source.Stop()
			r.checkpoint.Update(r.source.CurrentLSN())
			if saveErr := r.checkpoint.Save(); saveErr != nil {
				r.log.Error("cdc runner: final checkpoint save failed", zap.Error(saveErr))
			}
			r.writeStatus()
			return fmt.Errorf("cdc runner: ddl replication halted: %w", ddlErr)

		case <-cpTicker.C:
			r.checkpoint.Update(r.source.CurrentLSN())
			if r.checkpoint.IsDirty() {
				if saveErr := r.checkpoint.Save(); saveErr != nil {
					r.log.Error("cdc runner: checkpoint save failed", zap.Error(saveErr))
				}
			}
			r.writeStatus() // ride the checkpoint ticker (~10s) — #t48 B cadence

		case <-ctx.Done():
			r.source.Stop()
			return ctx.Err()
		}
	}
}

// writeStatus writes the CDC→Web status JSON (best-effort; errors are logged,
// not fatal). Rides the checkpoint ticker + shutdown so it adds no goroutine or
// lifecycle of its own. state/fatal_error reflect Part A's setFatal so the web
// can show a halt even while the file is still fresh. #t48 B contract.
func (r *Runner) writeStatus() {
	if r.statusFile == "" {
		return
	}
	var stats CDCStatusStats
	if r.applier != nil {
		as := r.applier.Stats()
		stats = CDCStatusStats{
			Applied:   as.EventsApplied,
			Failed:    as.EventsFailed,
			Skipped:   as.EventsSkipped,
			Batches:   as.BatchesFlushed,
			LastError: as.LastError,
		}
	}
	stats.SourceEvents = r.source.EventsReceived()
	if !r.startTime.IsZero() {
		stats.UptimeSeconds = time.Since(r.startTime).Seconds()
	}

	state := CDCSelfRunning
	fatal := ""
	if srcErr := r.source.Err(); srcErr != nil {
		state = CDCSelfHalted
		fatal = srcErr.Error()
	}

	cp := r.checkpoint.GetCheckpoint()
	st := CDCStatusFile{
		Schema:      1,
		Timestamp:   time.Now(),
		PID:         os.Getpid(),
		Slot:        r.srcCfg.SlotName,
		Publication: r.srcCfg.Publication,
		LSN:         r.source.CurrentLSN().String(),
		State:       state,
		FatalError:  fatal,
		Stats:       stats,
		Checkpoint: CDCStatusCheckpoint{
			LSN:       cp.LSN.String(),
			UpdatedAt: cp.Timestamp,
		},
	}
	if err := WriteStatusFile(r.statusFile, st); err != nil {
		r.log.Warn("cdc runner: write status file failed", zap.Error(err))
	}
}

// Stats returns a summary of the current CDC state.
func (r *Runner) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"source_events":  r.source.EventsReceived(),
		"source_lsn":     r.source.CurrentLSN().String(),
		"source_running": r.source.IsRunning(),
		"checkpoint_lsn": r.checkpoint.GetLSN().String(),
	}

	if r.applier != nil {
		appStats := r.applier.Stats()
		stats["applier_events_received"] = appStats.EventsReceived
		stats["applier_events_applied"] = appStats.EventsApplied
		stats["applier_events_failed"] = appStats.EventsFailed
		stats["applier_events_skipped"] = appStats.EventsSkipped
		stats["applier_batches"] = appStats.BatchesFlushed
		stats["applier_last_lsn"] = appStats.LastLSN
		stats["applier_last_error"] = appStats.LastError
	}

	return stats
}

// setupDDLReplication opens a source PG connection, installs the DDL event
// trigger, and starts the DDL poll goroutine. Returns the DDL error channel
// (receives a fatal DDL apply error that should halt the pipeline). #t59.
func (r *Runner) setupDDLReplication(ctx context.Context, targetDB *sql.DB) (chan error, error) {
	sslmode := r.srcCfg.SSLMode
	if sslmode == "" {
		sslmode = "disable"
	}
	dsn := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=%s",
		r.srcCfg.User, r.srcCfg.Password, r.srcCfg.Host, r.srcCfg.Port, r.srcCfg.Database, sslmode)
	srcDB, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open source for ddl tracker: %w", err)
	}
	r.ddlSrcDB = srcDB
	r.ddlTracker = NewDDLTracker(srcDB, r.filter)
	r.ddlTracker.SetLogger(r.log)
	if err := r.ddlTracker.SetupEventTrigger(ctx); err != nil {
		r.ddlSrcDB.Close()
		r.ddlSrcDB = nil
		r.ddlTracker = nil
		return nil, fmt.Errorf("setup ddl event trigger: %w", err)
	}
	r.log.Info("cdc runner: ddl replication enabled (event trigger installed)")
	errCh := make(chan error, 1)
	go r.runDDLPoller(ctx, targetDB, errCh)
	return errCh, nil
}

// teardownDDL removes the event trigger and closes the source PG connection
// (best-effort, at runner exit). Uses Background() since the run ctx may be
// canceled by the time we tear down.
func (r *Runner) teardownDDL() {
	if r.ddlTracker != nil {
		if err := r.ddlTracker.TeardownEventTrigger(context.Background()); err != nil {
			r.log.Warn("cdc runner: ddl teardown", zap.Error(err))
		}
	}
	if r.ddlSrcDB != nil {
		r.ddlSrcDB.Close()
	}
}

// runDDLPoller periodically fetches source DDL captured by the event trigger,
// transforms it to TiDB DDL, applies it to the target, and checkpoints the
// last applied ddl_log.id (at-least-once). On a fatal apply error (e.g. a
// non-idempotent ALTER replayed in a crash window) it reports via errCh and
// stops, halting the pipeline. #t59 §4.1/§4.2.
func (r *Runner) runDDLPoller(ctx context.Context, targetDB *sql.DB, errCh chan<- error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	lastID := r.checkpoint.GetLastDDLID()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		entries, err := r.ddlTracker.FetchNewDDL(ctx, lastID)
		if err != nil {
			r.log.Warn("cdc runner: ddl fetch failed", zap.Error(err))
			continue
		}
		for _, e := range entries {
			ddl := strings.TrimSpace(e.TiDBDDL)
			// Commented transforms (VIEW/FUNCTION/TRIGGER/incompatible index) are
			// manual-review placeholders — skip + log, don't apply. #t59 §4.4.
			if ddl == "" || strings.HasPrefix(ddl, "--") {
				r.log.Info("cdc runner: ddl skipped (incompatible/commented)",
					zap.String("type", e.ObjectType), zap.String("ddl", e.DDL))
				lastID = e.ID
				r.checkpoint.SetLastDDLID(e.ID)
				continue
			}
			if !shouldApplyDDL(e) {
				// Piggybacked sub-object (e.g. the sequence/PK-index rows of a
				// CREATE TABLE ... SERIAL) or out-of-scope statement — skip so we
				// don't re-apply the parent statement and diverge. #t61.
				r.log.Info("cdc runner: ddl skipped (sub-object/out-of-scope)",
					zap.String("type", e.ObjectType), zap.String("ddl", e.DDL))
				lastID = e.ID
				r.checkpoint.SetLastDDLID(e.ID)
				continue
			}
			if _, err := targetDB.ExecContext(ctx, ddl); err != nil {
				r.log.Error("cdc runner: ddl apply failed; halting",
					zap.Int64("id", e.ID), zap.String("ddl", ddl), zap.Error(err))
				errCh <- fmt.Errorf("ddl apply (id=%d, %q): %w", e.ID, ddl, err)
				return
			}
			lastID = e.ID
			r.checkpoint.SetLastDDLID(e.ID)
			r.log.Info("cdc runner: ddl applied", zap.Int64("id", e.ID), zap.String("ddl", ddl))
		}
	}
}
