package webapi

import (
	"sync/atomic"

	"go.uber.org/zap/zapcore"
)

type TaskLogCore struct {
	collector *LogCollector
	taskID    string
	enabled   atomic.Bool
	next      zapcore.Core
}

func NewTaskLogCore(collector *LogCollector, taskID string, next zapcore.Core) *TaskLogCore {
	c := &TaskLogCore{
		collector: collector,
		taskID:    taskID,
		next:      next,
	}
	c.enabled.Store(true)
	return c
}

func (c *TaskLogCore) Enable()  { c.enabled.Store(true) }
func (c *TaskLogCore) Disable() { c.enabled.Store(false) }

func (c *TaskLogCore) Enabled(level zapcore.Level) bool {
	return c.enabled.Load() && (c.next != nil && c.next.Enabled(level))
}

func (c *TaskLogCore) With(fields []zapcore.Field) zapcore.Core {
	var nextCore zapcore.Core
	if c.next != nil {
		nextCore = c.next.With(fields)
	}
	return &TaskLogCore{
		collector: c.collector,
		taskID:    c.taskID,
		next:      nextCore,
	}
}

func (c *TaskLogCore) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		ce = ce.AddCore(entry, c)
	}
	if c.next != nil {
		ce = c.next.Check(entry, ce)
	}
	return ce
}

func (c *TaskLogCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	c.collector.Append(c.taskID, ZapLevelString(entry.Level), entry.Message, entry.Caller.String())
	return nil
}

func (c *TaskLogCore) Sync() error {
	if c.next != nil {
		return c.next.Sync()
	}
	return nil
}
