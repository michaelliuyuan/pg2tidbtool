package cdc

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"go.uber.org/zap"
)

// CheckpointManager persists and recovers LSN checkpoints for CDC resume.
type CheckpointManager struct {
	filePath string
	log      *zap.Logger

	mu         sync.Mutex
	checkpoint Checkpoint
	dirty      bool
}

// NewCheckpointManager creates a new checkpoint manager.
func NewCheckpointManager(filePath string) *CheckpointManager {
	return &CheckpointManager{
		filePath: filePath,
		log:      zap.NewNop(),
		checkpoint: Checkpoint{
			SlotName: "pg2tidb_cdc",
		},
	}
}

// SetLogger sets the logger.
func (c *CheckpointManager) SetLogger(log *zap.Logger) {
	c.log = log
}

// Load reads the checkpoint from disk. Returns nil, nil if no checkpoint exists.
func (c *CheckpointManager) Load() (*Checkpoint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := os.ReadFile(c.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			c.log.Info("cdc checkpoint: no existing checkpoint file, starting fresh")
			return nil, nil
		}
		return nil, fmt.Errorf("cdc checkpoint: read file: %w", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("cdc checkpoint: parse: %w", err)
	}

	c.checkpoint = cp
	c.dirty = false

	c.log.Info("cdc checkpoint loaded",
		zap.String("lsn", cp.LSN.String()),
		zap.Time("timestamp", cp.Timestamp),
	)
	return &cp, nil
}

// Save writes the current checkpoint to disk.
func (c *CheckpointManager) Save() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cp := c.checkpoint
	cp.Timestamp = time.Now()

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("cdc checkpoint: marshal: %w", err)
	}

	if err := os.WriteFile(c.filePath, data, 0644); err != nil {
		return fmt.Errorf("cdc checkpoint: write: %w", err)
	}

	c.dirty = false
	c.log.Debug("cdc checkpoint saved",
		zap.String("lsn", cp.LSN.String()),
	)
	return nil
}

// Update records a new LSN position. Call this after successfully applying a batch.
// The checkpoint is marked dirty; call Save() to persist.
func (c *CheckpointManager) Update(lsn pglogrepl.LSN) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.checkpoint.LSN = lsn
	c.checkpoint.Timestamp = time.Now()
	c.dirty = true
}

// GetLSN returns the current checkpoint LSN.
func (c *CheckpointManager) GetLSN() pglogrepl.LSN {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.checkpoint.LSN
}

// GetCheckpoint returns a copy of the current checkpoint.
func (c *CheckpointManager) GetCheckpoint() Checkpoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.checkpoint
}

// IsDirty returns true if there are unpersisted LSN updates.
func (c *CheckpointManager) IsDirty() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dirty
}

// Reset clears the checkpoint (for fresh start).
func (c *CheckpointManager) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkpoint = Checkpoint{
		SlotName: c.checkpoint.SlotName,
	}
	c.dirty = true
}

// SetSlotName sets the replication slot name in the checkpoint.
func (c *CheckpointManager) SetSlotName(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkpoint.SlotName = name
}
